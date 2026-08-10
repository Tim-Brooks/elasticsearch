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

class JsonIterator {

    private static final int OBJECT_BEGIN = 0;
    private static final int ARRAY_BEGIN = 1;
    private static final int DOCUMENT_END = 2;
    private static final int OBJECT_FIELD = 3;
    private static final int OBJECT_CONTINUE = 4;
    private static final int SCOPE_END = 5;
    private static final int ARRAY_CONTINUE = 6;
    private static final int ARRAY_VALUE = 7;

    private final TapeBuilder tapeBuilder;
    private final BitIndexes indexer;
    private final boolean[] isArray;

    JsonIterator(BitIndexes indexer, byte[] stringBuffer, int capacity, int maxDepth, int padding) {
        this.indexer = indexer;
        this.isArray = new boolean[maxDepth];
        this.tapeBuilder = new TapeBuilder(capacity, maxDepth, padding, stringBuffer);
    }

    /**
     * Walks the structural index and builds the tape, then returns the root {@link JsonValue}.
     * Prefer {@link #walkToTape} when the caller intends to walk the tape as a cursor rather
     * than use the random-access {@link JsonValue} API.
     */
    JsonValue walkDocument(byte[] buffer, int len) {
        walkToTape(buffer, len);
        return tapeBuilder.createJsonValue(buffer);
    }

    /**
     * Walks the structural index and builds the tape without allocating a {@link JsonValue}.
     * Use this when the tape will be consumed via a cursor (e.g. {@link SimdJsonXContentParser}).
     */
    void walkToTape(byte[] buffer, int len) {
        if (indexer.isEnd()) {
            throw new JsonParsingException("No structural element found.");
        }

        tapeBuilder.visitDocumentStart();

        int depth = 0;
        int state;

        int idx = indexer.getAndAdvance();
        switch (buffer[idx]) {
            case '{' -> {
                if (buffer[indexer.getLast()] != '}') {
                    throw new JsonParsingException("Unclosed object. Missing '}' for starting '{'.");
                }
                if (buffer[indexer.peek()] == '}') {
                    indexer.advance();
                    tapeBuilder.visitEmptyObject();
                    state = DOCUMENT_END;
                } else {
                    state = OBJECT_BEGIN;
                }
            }
            case '[' -> {
                if (buffer[indexer.getLast()] != ']') {
                    throw new JsonParsingException("Unclosed array. Missing ']' for starting '['.");
                }
                if (buffer[indexer.peek()] == ']') {
                    indexer.advance();
                    tapeBuilder.visitEmptyArray();
                    state = DOCUMENT_END;
                } else {
                    state = ARRAY_BEGIN;
                }
            }
            default -> {
                tapeBuilder.visitRootPrimitive(buffer, idx, len);
                state = DOCUMENT_END;
            }
        }

        while (state != DOCUMENT_END) {
            if (state == OBJECT_BEGIN) {
                depth++;
                isArray[depth] = false;
                tapeBuilder.visitObjectStart(depth);

                int keyIdx = indexer.getAndAdvance();
                if (buffer[keyIdx] != '"') {
                    throw new JsonParsingException("Object does not start with a key");
                }
                tapeBuilder.incrementCount(depth);
                tapeBuilder.visitKey(buffer, keyIdx);
                state = OBJECT_FIELD;
            }

            if (state == OBJECT_FIELD) {
                if (buffer[indexer.getAndAdvance()] != ':') {
                    throw new JsonParsingException("Missing colon after key in object");
                }
                idx = indexer.getAndAdvance();
                switch (buffer[idx]) {
                    case '{' -> {
                        if (buffer[indexer.peek()] == '}') {
                            indexer.advance();
                            tapeBuilder.visitEmptyObject();
                            state = OBJECT_CONTINUE;
                        } else {
                            state = OBJECT_BEGIN;
                        }
                    }
                    case '[' -> {
                        if (buffer[indexer.peek()] == ']') {
                            indexer.advance();
                            tapeBuilder.visitEmptyArray();
                            state = OBJECT_CONTINUE;
                        } else {
                            state = ARRAY_BEGIN;
                        }
                    }
                    default -> {
                        tapeBuilder.visitPrimitive(buffer, idx);
                        state = OBJECT_CONTINUE;
                    }
                }
            }

            if (state == OBJECT_CONTINUE) {
                switch (buffer[indexer.getAndAdvance()]) {
                    case ',' -> {
                        tapeBuilder.incrementCount(depth);
                        int keyIdx = indexer.getAndAdvance();
                        if (buffer[keyIdx] != '"') {
                            throw new JsonParsingException("Key string missing at beginning of field in object");
                        }
                        tapeBuilder.visitKey(buffer, keyIdx);
                        state = OBJECT_FIELD;
                    }
                    case '}' -> {
                        tapeBuilder.visitObjectEnd(depth);
                        state = SCOPE_END;
                    }
                    default -> throw new JsonParsingException("No comma between object fields");
                }
            }

            if (state == SCOPE_END) {
                depth--;
                if (depth == 0) {
                    state = DOCUMENT_END;
                } else if (isArray[depth]) {
                    state = ARRAY_CONTINUE;
                } else {
                    state = OBJECT_CONTINUE;
                }
            }

            if (state == ARRAY_BEGIN) {
                depth++;
                isArray[depth] = true;
                tapeBuilder.visitArrayStart(depth);
                tapeBuilder.incrementCount(depth);
                state = ARRAY_VALUE;
            }

            if (state == ARRAY_VALUE) {
                idx = indexer.getAndAdvance();
                switch (buffer[idx]) {
                    case '{' -> {
                        if (buffer[indexer.peek()] == '}') {
                            indexer.advance();
                            tapeBuilder.visitEmptyObject();
                            state = ARRAY_CONTINUE;
                        } else {
                            state = OBJECT_BEGIN;
                        }
                    }
                    case '[' -> {
                        if (buffer[indexer.peek()] == ']') {
                            indexer.advance();
                            tapeBuilder.visitEmptyArray();
                            state = ARRAY_CONTINUE;
                        } else {
                            state = ARRAY_BEGIN;
                        }
                    }
                    default -> {
                        tapeBuilder.visitPrimitive(buffer, idx);
                        state = ARRAY_CONTINUE;
                    }
                }
            }

            if (state == ARRAY_CONTINUE) {
                switch (buffer[indexer.getAndAdvance()]) {
                    case ',' -> {
                        tapeBuilder.incrementCount(depth);
                        state = ARRAY_VALUE;
                    }
                    case ']' -> {
                        tapeBuilder.visitArrayEnd(depth);
                        state = SCOPE_END;
                    }
                    default -> throw new JsonParsingException("Missing comma between array values");
                }
            }
        }
        tapeBuilder.visitDocumentEnd();

        if (!indexer.isEnd()) {
            throw new JsonParsingException(
                "More than one JSON value at the root of the document, or extra characters at the end of the JSON!"
            );
        }
    }

    void reset() {
        tapeBuilder.reset();
        Arrays.fill(isArray, false);
    }

    Tape tape() {
        return tapeBuilder.tape();
    }

    byte[] stringBuffer() {
        return tapeBuilder.stringBuffer();
    }
}
