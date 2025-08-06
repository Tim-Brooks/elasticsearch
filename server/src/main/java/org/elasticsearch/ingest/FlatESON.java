/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FlatESON {

    public static class Builder {
        private final BytesStreamOutput bytes;
        private final List<ESONSource.KeyEntry> keyArray;

        public Builder() {
            this(0);
        }

        public Builder(int expectedSize) {
            this(BytesRefRecycler.NON_RECYCLING_INSTANCE, expectedSize);
        }

        public Builder(Recycler<BytesRef> refRecycler, int expectedSize) {
            this.bytes = new BytesStreamOutput(expectedSize);
            this.keyArray = new ArrayList<>();
        }

        public ESONSource.ESONObject parse(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected START_OBJECT but got " + token);
            }

            parseObject(parser, bytes, keyArray, null);

            return new ESONSource.ESONObject(0, keyArray, new ESONSource.Values(bytes.bytes()));
        }

        private static void parseObject(
            XContentParser parser,
            BytesStreamOutput bytes,
            List<ESONSource.KeyEntry> keyArray,
            String objectFieldName
        ) throws IOException {
            ESONSource.ObjectEntry objEntry = new ESONSource.ObjectEntry(objectFieldName);
            keyArray.add(objEntry);

            int count = 0;
            String fieldName;
            while ((fieldName = parser.nextFieldName()) != null) {
                parseValue(parser, fieldName, bytes, keyArray);
                count++;
            }

            objEntry.fieldCount = count;
        }

        private static void parseArray(
            XContentParser parser,
            BytesStreamOutput bytes,
            List<ESONSource.KeyEntry> keyArray,
            String arrayFieldName
        ) throws IOException {
            ESONSource.ArrayEntry arrEntry = new ESONSource.ArrayEntry(arrayFieldName);
            keyArray.add(arrEntry);

            int count = 0;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                switch (token) {
                    case START_OBJECT -> parseObject(parser, bytes, keyArray, null);
                    case START_ARRAY -> parseArray(parser, bytes, keyArray, null);
                    default -> {
                        ESONSource.Type type = parseSimpleValue(parser, bytes, token);
                        keyArray.add(new ESONSource.FieldEntry(null, type));
                    }
                }
                count++;
            }

            arrEntry.elementCount = count;
        }

        private static void parseValue(XContentParser parser, String fieldName, BytesStreamOutput bytes, List<ESONSource.KeyEntry> keyArray)
            throws IOException {
            XContentParser.Token token = parser.nextToken();

            switch (token) {
                case START_OBJECT -> parseObject(parser, bytes, keyArray, fieldName);
                case START_ARRAY -> parseArray(parser, bytes, keyArray, fieldName);
                default -> {
                    ESONSource.Type type = parseSimpleValue(parser, bytes, token);
                    keyArray.add(new ESONSource.FieldEntry(fieldName, type));
                }
            }
        }

        private static ESONSource.Type parseSimpleValue(XContentParser parser, BytesStreamOutput bytes, XContentParser.Token token)
            throws IOException {
            long position = bytes.position();

            return switch (token) {
                case VALUE_STRING -> {
                    XContentString.UTF8Bytes stringBytes = parser.optimizedText().bytes();
                    bytes.write(stringBytes.bytes(), stringBytes.offset(), stringBytes.length());
                    yield new ESONSource.VariableValue((int) position, stringBytes.length(), ESONSource.ValueType.STRING);
                }
                case VALUE_NUMBER -> {
                    XContentParser.NumberType numberType = parser.numberType();
                    yield switch (numberType) {
                        case INT -> {
                            bytes.writeInt(parser.intValue());
                            yield new ESONSource.FixedValue((int) position, ESONSource.ValueType.INT);
                        }
                        case LONG -> {
                            bytes.writeLong(parser.longValue());
                            yield new ESONSource.FixedValue((int) position, ESONSource.ValueType.LONG);
                        }
                        case FLOAT -> {
                            bytes.writeFloat(parser.floatValue());
                            yield new ESONSource.FixedValue((int) position, ESONSource.ValueType.FLOAT);
                        }
                        case DOUBLE -> {
                            bytes.writeDouble(parser.doubleValue());
                            yield new ESONSource.FixedValue((int) position, ESONSource.ValueType.DOUBLE);
                        }
                        case BIG_INTEGER, BIG_DECIMAL -> {
                            ESONSource.ValueType valueType = numberType == XContentParser.NumberType.BIG_INTEGER
                                ? ESONSource.ValueType.BIG_INTEGER
                                : ESONSource.ValueType.BIG_DECIMAL;
                            byte[] numberBytes = parser.text().getBytes(StandardCharsets.UTF_8);
                            bytes.write(numberBytes);
                            yield new ESONSource.VariableValue((int) position, numberBytes.length, valueType);
                        }
                    };
                }
                case VALUE_BOOLEAN -> {
                    bytes.writeBoolean(parser.booleanValue());
                    yield new ESONSource.FixedValue((int) position, ESONSource.ValueType.BOOLEAN);
                }
                case VALUE_NULL -> ESONSource.NullValue.INSTANCE;
                case VALUE_EMBEDDED_OBJECT -> {
                    byte[] binaryValue = parser.binaryValue();
                    bytes.write(binaryValue);
                    yield new ESONSource.VariableValue((int) position, binaryValue.length, ESONSource.ValueType.BINARY);
                }
                default -> throw new IllegalArgumentException("Unexpected token: " + token);
            };
        }
    }

    public enum ValueType {
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        BOOLEAN,
        BIG_INTEGER,
        BIG_DECIMAL,
        STRING,
        BINARY
    }

    public interface KeyEntry {

        String key();

    }

    public static class ObjectEntry implements KeyEntry {

        private final String key;
        public int fieldCount = 0;
        private Map<String, ESONSource.Type> mutationMap = null;

        public ObjectEntry(String key) {
            this.key = key;
        }

        @Override
        public String key() {
            return key;
        }

        public boolean hasMutations() {
            return mutationMap != null;
        }

        @Override
        public String toString() {
            return "ObjectEntry{" + "key='" + key + '\'' + ", fieldCount=" + fieldCount + ", hasMutations=" + hasMutations() + '}';
        }
    }

    public static class ArrayEntry implements KeyEntry {

        private final String key;
        public int elementCount = 0;
        private List<ESONSource.Type> mutationArray = null;

        public ArrayEntry(String key) {
            this.key = key;
        }

        @Override
        public String key() {
            return key;
        }

        public boolean hasMutations() {
            return mutationArray != null;
        }

        @Override
        public String toString() {
            return "ArrayEntry{" + "key='" + key + '\'' + ", elementCount=" + elementCount + ", hasMutations=" + hasMutations() + '}';
        }
    }

    public record FieldEntry(String key, ESONSource.Type type) implements KeyEntry {}
}
