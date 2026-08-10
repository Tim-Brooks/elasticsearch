/*
 * @notice
 *
 * Copyright 2021-2024 The simdjson-java contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Based on a modification of https://github.com/simdjson/simdjson-java,
 * licensed under the Apache License 2.0.
 */

package org.elasticsearch.sourcebatch.simdjson;

import org.elasticsearch.sourcebatch.simdjson.ExponentParser.ExponentParsingResult;

import static org.elasticsearch.sourcebatch.simdjson.CharacterUtils.isStructuralOrWhitespace;
import static org.elasticsearch.sourcebatch.simdjson.ExponentParser.isExponentIndicator;

/**
 * Parses JSON numbers onto the tape. Schema-based typed accessors (parseByte, parseInt, etc.)
 * have been removed; only the tape-writing {@link #parseNumber} path is retained.
 */
class NumberParser {

    private static final int LONG_MAX_DIGIT_COUNT = 19;

    private final DigitsParsingResult digitsParsingResult = new DigitsParsingResult();
    private final ExponentParser exponentParser = new ExponentParser();
    private final DoubleParser doubleParser = new DoubleParser();

    void parseNumber(byte[] buffer, int offset, Tape tape) {
        boolean negative = buffer[offset] == '-';

        int currentIdx = negative ? offset + 1 : offset;

        int digitsStartIdx = currentIdx;
        DigitsParsingResult digitsParsingResult = parseDigits(buffer, currentIdx, 0);
        long digits = digitsParsingResult.digits();
        currentIdx = digitsParsingResult.currentIdx();
        int digitCount = currentIdx - digitsStartIdx;
        if (digitCount == 0) {
            throw new JsonParsingException("Invalid number. Minus has to be followed by a digit.");
        }
        if ('0' == buffer[digitsStartIdx] && digitCount > 1) {
            throw new JsonParsingException("Invalid number. Leading zeroes are not allowed.");
        }

        long exponent = 0;
        boolean floatingPointNumber = false;
        if ('.' == buffer[currentIdx]) {
            floatingPointNumber = true;
            currentIdx++;
            int firstIdxAfterPeriod = currentIdx;
            digitsParsingResult = parseDigits(buffer, currentIdx, digits);
            digits = digitsParsingResult.digits();
            currentIdx = digitsParsingResult.currentIdx();
            exponent = firstIdxAfterPeriod - currentIdx;
            if (exponent == 0) {
                throw new JsonParsingException("Invalid number. Decimal point has to be followed by a digit.");
            }
            digitCount = currentIdx - digitsStartIdx;
        }
        if (isExponentIndicator(buffer[currentIdx])) {
            floatingPointNumber = true;
            currentIdx++;
            ExponentParsingResult exponentParsingResult = exponentParser.parse(buffer, currentIdx, exponent);
            exponent = exponentParsingResult.exponent();
            currentIdx = exponentParsingResult.currentIdx();
        }
        if (!isStructuralOrWhitespace(buffer[currentIdx])) {
            throw new JsonParsingException("Number has to be followed by a structural character or whitespace.");
        }
        if (floatingPointNumber) {
            double value = doubleParser.parse(buffer, offset, negative, digitsStartIdx, digitCount, digits, exponent);
            tape.appendDouble(value);
        } else {
            if (isOutOfLongRange(negative, digits, digitCount)) {
                throw new JsonParsingException("Number value is out of long range ([" + Long.MIN_VALUE + ", " + Long.MAX_VALUE + "]).");
            }
            tape.appendInt64(negative ? (~digits + 1) : digits);
        }
    }

    private static boolean isOutOfLongRange(boolean negative, long digits, int digitCount) {
        if (digitCount < LONG_MAX_DIGIT_COUNT) {
            return false;
        }
        if (digitCount > LONG_MAX_DIGIT_COUNT) {
            return true;
        }
        if (negative && digits == Long.MIN_VALUE) {
            // The maximum value we can store in a long is 9223372036854775807. When we try to store 9223372036854775808,
            // a long wraps around, resulting in -9223372036854775808 (Long.MIN_VALUE). If the number we are parsing is
            // negative, and we've attempted to store 9223372036854775808 in "digits", we can be sure that we are
            // dealing with Long.MIN_VALUE, which obviously does not fall outside the acceptable range.
            return false;
        }
        return digits < 0;
    }

    private DigitsParsingResult parseDigits(byte[] buffer, int currentIdx, long digits) {
        byte digit = convertCharacterToDigit(buffer[currentIdx]);
        while (digit >= 0 && digit <= 9) {
            digits = 10 * digits + digit;
            currentIdx++;
            digit = convertCharacterToDigit(buffer[currentIdx]);
        }
        return digitsParsingResult.of(digits, currentIdx);
    }

    private static byte convertCharacterToDigit(byte b) {
        return (byte) (b - '0');
    }

    private static class DigitsParsingResult {

        private long digits;
        private int currentIdx;

        DigitsParsingResult of(long digits, int currentIdx) {
            this.digits = digits;
            this.currentIdx = currentIdx;
            return this;
        }

        long digits() {
            return digits;
        }

        int currentIdx() {
            return currentIdx;
        }
    }
}
