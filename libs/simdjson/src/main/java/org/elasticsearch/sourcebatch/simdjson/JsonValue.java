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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.sourcebatch.simdjson.Tape.DOUBLE;
import static org.elasticsearch.sourcebatch.simdjson.Tape.FALSE_VALUE;
import static org.elasticsearch.sourcebatch.simdjson.Tape.INT64;
import static org.elasticsearch.sourcebatch.simdjson.Tape.NULL_VALUE;
import static org.elasticsearch.sourcebatch.simdjson.Tape.START_ARRAY;
import static org.elasticsearch.sourcebatch.simdjson.Tape.START_OBJECT;
import static org.elasticsearch.sourcebatch.simdjson.Tape.STRING;
import static org.elasticsearch.sourcebatch.simdjson.Tape.TRUE_VALUE;

/** A parsed JSON value backed by the internal tape. Instances are valid only for the lifetime of the parser call. */
public class JsonValue {

    private final Tape tape;
    private final byte[] buffer;
    private final int tapeIdx;
    private final byte[] stringBuffer;

    JsonValue(Tape tape, int tapeIdx, byte[] stringBuffer, byte[] buffer) {
        this.tape = tape;
        this.tapeIdx = tapeIdx;
        this.stringBuffer = stringBuffer;
        this.buffer = buffer;
    }

    /** Returns true if this value is a JSON array. */
    public boolean isArray() {
        return tape.getType(tapeIdx) == START_ARRAY;
    }

    /** Returns true if this value is a JSON object. */
    public boolean isObject() {
        return tape.getType(tapeIdx) == START_OBJECT;
    }

    /** Returns true if this value is a JSON integer (parsed as {@code long}). */
    public boolean isLong() {
        return tape.getType(tapeIdx) == INT64;
    }

    /** Returns true if this value is a JSON floating-point number. */
    public boolean isDouble() {
        return tape.getType(tapeIdx) == DOUBLE;
    }

    /** Returns true if this value is a JSON boolean. */
    public boolean isBoolean() {
        char type = tape.getType(tapeIdx);
        return type == TRUE_VALUE || type == FALSE_VALUE;
    }

    /** Returns true if this value is JSON {@code null}. */
    public boolean isNull() {
        return tape.getType(tapeIdx) == NULL_VALUE;
    }

    /** Returns true if this value is a JSON string. */
    public boolean isString() {
        return tape.getType(tapeIdx) == STRING;
    }

    /** Returns an iterator over array elements. */
    public Iterator<JsonValue> arrayIterator() {
        return new ArrayIterator(tapeIdx);
    }

    /** Returns an iterator over object key-value pairs. */
    public Iterator<Map.Entry<String, JsonValue>> objectIterator() {
        return new ObjectIterator(tapeIdx);
    }

    /** Returns this value as a {@code long}. */
    public long asLong() {
        return tape.getInt64Value(tapeIdx);
    }

    /** Returns this value as a {@code double}. */
    public double asDouble() {
        return tape.getDouble(tapeIdx);
    }

    /** Returns this value as a {@code boolean}. */
    public boolean asBoolean() {
        return tape.getType(tapeIdx) == TRUE_VALUE;
    }

    /** Returns this value as a {@link String}. */
    public String asString() {
        return getString(tapeIdx);
    }

    private String getString(int idx) {
        int stringBufferIdx = (int) tape.getValue(idx);
        int len = IntegerUtils.toInt(stringBuffer, stringBufferIdx);
        return new String(stringBuffer, stringBufferIdx + Integer.BYTES, len, UTF_8);
    }

    /**
     * Looks up an object field by name.
     *
     * @return the value, or {@code null} if the key is not present
     */
    public JsonValue get(String name) {
        byte[] bytes = name.getBytes(UTF_8);
        int idx = tapeIdx + 1;
        int endIdx = tape.getMatchingBraceIndex(tapeIdx) - 1;
        while (idx < endIdx) {
            int stringBufferIdx = (int) tape.getValue(idx);
            int len = IntegerUtils.toInt(stringBuffer, stringBufferIdx);
            int valIdx = tape.computeNextIndex(idx);
            idx = tape.computeNextIndex(valIdx);
            int stringBufferFromIdx = stringBufferIdx + Integer.BYTES;
            int stringBufferToIdx = stringBufferFromIdx + len;
            if (Arrays.compare(bytes, 0, bytes.length, stringBuffer, stringBufferFromIdx, stringBufferToIdx) == 0) {
                return new JsonValue(tape, valIdx, stringBuffer, buffer);
            }
        }
        return null;
    }

    /** Returns the number of elements in an array or object. */
    public int getSize() {
        return tape.getScopeCount(tapeIdx);
    }

    @Override
    public String toString() {
        switch (tape.getType(tapeIdx)) {
            case INT64 -> {
                return String.valueOf(asLong());
            }
            case DOUBLE -> {
                return String.valueOf(asDouble());
            }
            case TRUE_VALUE, FALSE_VALUE -> {
                return String.valueOf(asBoolean());
            }
            case STRING -> {
                return asString();
            }
            case NULL_VALUE -> {
                return "null";
            }
            case START_OBJECT -> {
                return "<object>";
            }
            case START_ARRAY -> {
                return "<array>";
            }
            default -> {
                return "unknown";
            }
        }
    }

    private class ArrayIterator implements Iterator<JsonValue> {

        private final int endIdx;

        private int idx;

        ArrayIterator(int startIdx) {
            idx = startIdx + 1;
            endIdx = tape.getMatchingBraceIndex(startIdx) - 1;
        }

        @Override
        public boolean hasNext() {
            return idx < endIdx;
        }

        @Override
        public JsonValue next() {
            if (hasNext()) {
                JsonValue value = new JsonValue(tape, idx, stringBuffer, buffer);
                idx = tape.computeNextIndex(idx);
                return value;
            }
            throw new NoSuchElementException("No more elements");
        }
    }

    private class ObjectIterator implements Iterator<Map.Entry<String, JsonValue>> {

        private final int endIdx;

        private int idx;

        ObjectIterator(int startIdx) {
            idx = startIdx + 1;
            endIdx = tape.getMatchingBraceIndex(startIdx) - 1;
        }

        @Override
        public boolean hasNext() {
            return idx < endIdx;
        }

        @Override
        public Map.Entry<String, JsonValue> next() {
            String key = getString(idx);
            idx = tape.computeNextIndex(idx);
            JsonValue value = new JsonValue(tape, idx, stringBuffer, buffer);
            idx = tape.computeNextIndex(idx);
            return new ObjectField(key, value);
        }
    }

    private static class ObjectField implements Map.Entry<String, JsonValue> {

        private final String key;
        private final JsonValue value;

        ObjectField(String key, JsonValue value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public JsonValue getValue() {
            return value;
        }

        @Override
        public JsonValue setValue(JsonValue value) {
            throw new UnsupportedOperationException("Object fields are immutable");
        }
    }
}
