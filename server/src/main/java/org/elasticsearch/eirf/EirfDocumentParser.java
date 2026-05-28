/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Stateless XContent-to-EIRF document parser.
 *
 * <p>Parses a single document from XContent bytes into an {@link BufferedRow}, growing the supplied
 * {@link EirfSchema} as new fields are encountered. A {@link LeafSink} may be attached to receive
 * per-leaf callbacks during the parse (e.g. for routing extraction in a single pass).
 *
 * <p>All methods are static — this class holds no instance state and a single instance of it is
 * never needed. It should be used as a utility class.
 */
public final class EirfDocumentParser {

    private EirfDocumentParser() {}

    /**
     * Parses {@code source} into {@code row}, growing {@code schema} as new fields appear, and
     * firing {@code sink} for every primitive leaf value.
     *
     * <p>{@code row} must have been {@link BufferedRow#reset(int) reset} by the caller with the
     * current {@code schema.leafCount()} before calling this method.
     *
     * @param source      the XContent document bytes
     * @param xContentType content type of the source bytes
     * @param schema      cumulative schema for the encoding session; grown in place by this call
     * @param row         per-document scratch row, populated in place by this call
     * @param sink        leaf callback sink; may be {@link LeafSink#NO_OP}
     * @param pathCache   memoized column-index-to-dotted-path mapping shared with the schema
     */
    public static void parseXContent(
        BytesReference source,
        XContentType xContentType,
        EirfSchema schema,
        BufferedRow row,
        LeafSink sink,
        ColumnPathCache pathCache
    ) throws IOException {
        try (XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, source, xContentType)) {
            // The schema prevents duplicate columns — no need for JSON's internal duplicate prevention.
            parser.allowDuplicateKeys(true);
            parser.nextToken(); // START_OBJECT
            flattenObject(parser, 0, schema, row, parser.nextToken(), pathCache, sink);
        }
    }

    private static void flattenObject(
        XContentParser parser,
        int parentNonLeafIdx,
        EirfSchema schema,
        BufferedRow row,
        XContentParser.Token firstToken,
        ColumnPathCache pathCache,
        LeafSink sink
    ) throws IOException {
        XContentParser.Token token = firstToken;
        while (token != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalStateException("Expected FIELD_NAME but got " + token);
            }
            String fieldName = parser.currentName();
            token = parser.nextToken();

            if (token == XContentParser.Token.START_OBJECT) {
                // Peek inside the object: an empty object is encoded as a KEY_VALUE leaf;
                // non-empty objects recurse into flattenObject.
                XContentParser.Token inner = parser.nextToken();
                if (inner == XContentParser.Token.END_OBJECT) {
                    int emptyColIdx = schema.appendLeaf(fieldName, parentNonLeafIdx);
                    row.ensureCapacity(emptyColIdx + 1);
                    if (row.columnsSet.getAndSet(emptyColIdx)) {
                        throw new IllegalArgumentException("Duplicate field [" + fieldName + "]");
                    }
                    row.typeBytes[emptyColIdx] = EirfType.KEY_VALUE;
                    row.varData[emptyColIdx] = BytesArray.EMPTY;
                    row.varColumnCount++;
                } else {
                    int nonLeafIdx = schema.appendNonLeaf(fieldName, parentNonLeafIdx);
                    flattenObject(parser, nonLeafIdx, schema, row, inner, pathCache, sink);
                }
                token = parser.nextToken();
                continue;
            }

            int colIdx = schema.appendLeaf(fieldName, parentNonLeafIdx);
            row.ensureCapacity(colIdx + 1);
            if (row.columnsSet.getAndSet(colIdx)) {
                throw new IllegalArgumentException("Duplicate field [" + fieldName + "]");
            }

            boolean firePathSink = sink != LeafSink.NO_OP;
            boolean rawTextMode = firePathSink && sink.passRawText();
            switch (token) {
                case START_ARRAY -> {
                    PackedArray arr = parseArray(parser, row);
                    row.typeBytes[colIdx] = arr.arrayType;
                    row.varData[colIdx] = new BytesArray(arr.packed);
                    row.totalVarSize += arr.packed.length;
                    row.varColumnCount++;
                    if (firePathSink) {
                        sink.onArrayLeaf(colIdx, pathCache.get(colIdx, schema));
                    }
                }
                case VALUE_STRING -> {
                    row.typeBytes[colIdx] = EirfType.STRING;
                    XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                    row.varData[colIdx] = str;
                    row.totalVarSize += str.length();
                    row.varColumnCount++;
                    if (firePathSink) {
                        // Strings flow through onTextPrimitive in both modes.
                        sink.onTextPrimitive(colIdx, pathCache.get(colIdx, schema), EirfType.STRING, str);
                    }
                }
                case VALUE_NUMBER -> {
                    XContentParser.NumberType numType = parser.numberType();
                    switch (numType) {
                        case INT, LONG -> {
                            long val = parser.longValue();
                            byte type;
                            if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                                type = EirfType.INT;
                                row.typeBytes[colIdx] = type;
                                ByteUtils.writeIntLE((int) val, row.fixedData, colIdx * 8);
                                row.scalarFixedSize += 4;
                            } else {
                                type = EirfType.LONG;
                                row.typeBytes[colIdx] = type;
                                ByteUtils.writeLongLE(val, row.fixedData, colIdx * 8);
                                row.scalarFixedSize += 8;
                            }
                            if (rawTextMode) {
                                sink.onTextPrimitive(colIdx, pathCache.get(colIdx, schema), type, parser.optimizedText().bytes());
                            } else if (firePathSink) {
                                sink.onLongPrimitive(colIdx, pathCache.get(colIdx, schema), type, val);
                            }
                        }
                        case FLOAT, DOUBLE -> {
                            double val = parser.doubleValue();
                            float fval = (float) val;
                            byte type;
                            if ((double) fval == val) {
                                type = EirfType.FLOAT;
                                row.typeBytes[colIdx] = type;
                                ByteUtils.writeIntLE(Float.floatToRawIntBits(fval), row.fixedData, colIdx * 8);
                                row.scalarFixedSize += 4;
                            } else {
                                type = EirfType.DOUBLE;
                                row.typeBytes[colIdx] = type;
                                ByteUtils.writeLongLE(Double.doubleToRawLongBits(val), row.fixedData, colIdx * 8);
                                row.scalarFixedSize += 8;
                            }
                            if (rawTextMode) {
                                sink.onTextPrimitive(colIdx, pathCache.get(colIdx, schema), type, parser.optimizedText().bytes());
                            } else if (firePathSink) {
                                sink.onDoublePrimitive(colIdx, pathCache.get(colIdx, schema), type, val);
                            }
                        }
                        default -> {
                            // BIG_INTEGER / BIG_DECIMAL fall back to a string column. Both modes use
                            // onTextPrimitive (typed sinks treat this as "unrecognized" and may fall back).
                            row.typeBytes[colIdx] = EirfType.STRING;
                            XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                            row.varData[colIdx] = str;
                            row.totalVarSize += str.length();
                            row.varColumnCount++;
                            if (firePathSink) {
                                sink.onTextPrimitive(colIdx, pathCache.get(colIdx, schema), EirfType.STRING, str);
                            }
                        }
                    }
                }
                case VALUE_BOOLEAN -> {
                    boolean v = parser.booleanValue();
                    byte type = v ? EirfType.TRUE : EirfType.FALSE;
                    row.typeBytes[colIdx] = type;
                    if (rawTextMode) {
                        // Non-JSON formats render booleans differently (YAML "yes"/"True", CBOR/SMILE
                        // binary tags exposed as canonical text). Routing-hash parity depends on the
                        // parser's canonical text bytes.
                        sink.onTextPrimitive(colIdx, pathCache.get(colIdx, schema), type, parser.optimizedText().bytes());
                    } else if (firePathSink) {
                        sink.onBooleanPrimitive(colIdx, pathCache.get(colIdx, schema), v);
                    }
                }
                case VALUE_NULL -> row.typeBytes[colIdx] = EirfType.NULL;
                default -> throw new IllegalStateException("Unexpected token: " + token);
            }
            token = parser.nextToken();
        }
    }

    private record PackedArray(byte arrayType, byte[] packed) {}

    /**
     * Parses an array from the parser (positioned after START_ARRAY).
     *
     * @param row if non-null, array element buffers are borrowed from the row to avoid allocation;
     *            null is passed for recursive calls where the buffers are already in use.
     */
    private static PackedArray parseArray(XContentParser parser, BufferedRow row) throws IOException {
        byte[] elemTypes;
        long[] elemNumeric;
        Object[] elemVar;
        boolean borrowed = row != null && row.arrayElemTypes != null;
        if (borrowed) {
            elemTypes = row.arrayElemTypes;
            elemNumeric = row.arrayElemNumeric;
            elemVar = row.arrayElemVar;
            row.arrayElemTypes = null;
            row.arrayElemNumeric = null;
            row.arrayElemVar = null;
        } else {
            elemTypes = new byte[16];
            elemNumeric = new long[16];
            elemVar = new Object[16];
        }

        int count = 0;
        boolean forceUnion = false;
        try {
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (count >= elemTypes.length) {
                    int newCap = elemTypes.length * 2;
                    elemTypes = Arrays.copyOf(elemTypes, newCap);
                    elemNumeric = Arrays.copyOf(elemNumeric, newCap);
                    elemVar = Arrays.copyOf(elemVar, newCap);
                }
                switch (token) {
                    case START_OBJECT -> {
                        elemTypes[count] = EirfType.KEY_VALUE;
                        elemVar[count] = serializeKeyValue(parser);
                        forceUnion = true;
                    }
                    case START_ARRAY -> {
                        PackedArray nested = parseArray(parser, row);
                        elemTypes[count] = nested.arrayType;
                        elemVar[count] = nested.packed;
                        forceUnion = true;
                    }
                    case VALUE_STRING -> {
                        elemTypes[count] = EirfType.STRING;
                        elemVar[count] = parser.optimizedText().bytes();
                    }
                    case VALUE_NUMBER -> {
                        XContentParser.NumberType numType = parser.numberType();
                        switch (numType) {
                            case INT, LONG -> {
                                long val = parser.longValue();
                                if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                                    elemTypes[count] = EirfType.INT;
                                    elemNumeric[count] = val;
                                } else {
                                    elemTypes[count] = EirfType.LONG;
                                    elemNumeric[count] = val;
                                }
                            }
                            case FLOAT, DOUBLE -> {
                                double val = parser.doubleValue();
                                float fval = (float) val;
                                if ((double) fval == val) {
                                    elemTypes[count] = EirfType.FLOAT;
                                    elemNumeric[count] = Float.floatToRawIntBits(fval);
                                } else {
                                    elemTypes[count] = EirfType.DOUBLE;
                                    elemNumeric[count] = Double.doubleToRawLongBits(val);
                                }
                            }
                            default -> {
                                elemTypes[count] = EirfType.STRING;
                                elemVar[count] = parser.optimizedText().bytes();
                            }
                        }
                    }
                    case VALUE_BOOLEAN -> elemTypes[count] = parser.booleanValue() ? EirfType.TRUE : EirfType.FALSE;
                    case VALUE_NULL -> elemTypes[count] = EirfType.NULL;
                    default -> throw new IllegalStateException("Unexpected token in array: " + token);
                }
                count++;
            }

            boolean useFixed = false;
            byte sharedType = 0;
            if (forceUnion == false && count > 0) {
                sharedType = elemTypes[0];
                useFixed = true;
                for (int i = 1; i < count; i++) {
                    if (elemTypes[i] != sharedType) {
                        useFixed = false;
                        break;
                    }
                }
                // FIXED_ARRAY is byte-length-terminated with no element count, so a zero-data-size shared
                // type (NULL/TRUE/FALSE) would be indistinguishable from an empty array. Force UNION in
                // that case so each element contributes its type byte and the reader can iterate.
                if (useFixed && EirfType.elemDataSize(sharedType) == 0) {
                    useFixed = false;
                }
            }

            byte[] packed;
            byte arrayType;
            if (useFixed) {
                packed = packFixedArray(sharedType, elemNumeric, elemVar, count);
                arrayType = EirfType.FIXED_ARRAY;
            } else {
                packed = packUnionArray(elemTypes, elemNumeric, elemVar, count);
                arrayType = EirfType.UNION_ARRAY;
            }
            return new PackedArray(arrayType, packed);
        } finally {
            if (row != null) {
                Arrays.fill(elemVar, 0, count, null);
                row.arrayElemTypes = elemTypes;
                row.arrayElemNumeric = elemNumeric;
                row.arrayElemVar = elemVar;
            }
        }
    }

    /**
     * Packs a union array: per element: type(1) + data. No count byte — byte length terminates.
     */
    static byte[] packUnionArray(byte[] elemTypes, long[] elemNumeric, Object[] elemVar, int count) {
        int size = 0;
        for (int i = 0; i < count; i++) {
            size += 1; // type byte
            size += elemDataSize(elemTypes[i], elemVar[i]);
        }

        byte[] packed = new byte[size];
        int pos = 0;
        for (int i = 0; i < count; i++) {
            packed[pos++] = elemTypes[i];
            pos = writeElemData(packed, pos, elemTypes[i], elemNumeric[i], elemVar[i]);
        }
        return packed;
    }

    /**
     * Packs a fixed array: element_type(1) + per element: data only. No count byte — byte length terminates.
     */
    static byte[] packFixedArray(byte sharedType, long[] elemNumeric, Object[] elemVar, int count) {
        int size = 1; // shared type byte
        for (int i = 0; i < count; i++) {
            size += elemDataSize(sharedType, elemVar[i]);
        }

        byte[] packed = new byte[size];
        packed[0] = sharedType;
        int pos = 1;
        for (int i = 0; i < count; i++) {
            pos = writeElemData(packed, pos, sharedType, elemNumeric[i], elemVar[i]);
        }
        return packed;
    }

    private static int elemDataSize(byte type, Object varData) {
        return switch (type) {
            case EirfType.INT, EirfType.FLOAT -> 4;
            case EirfType.LONG, EirfType.DOUBLE -> 8;
            case EirfType.STRING -> {
                XContentString.UTF8Bytes str = (XContentString.UTF8Bytes) varData;
                yield 4 + (str != null ? str.length() : 0);
            }
            case EirfType.KEY_VALUE, EirfType.UNION_ARRAY, EirfType.FIXED_ARRAY -> {
                byte[] bytes = (byte[]) varData;
                yield 4 + bytes.length;
            }
            default -> 0; // NULL, TRUE, FALSE
        };
    }

    private static int writeElemData(byte[] packed, int pos, byte type, long numeric, Object var) {
        switch (type) {
            case EirfType.INT, EirfType.FLOAT -> {
                ByteUtils.writeIntLE((int) numeric, packed, pos);
                pos += 4;
            }
            case EirfType.LONG, EirfType.DOUBLE -> {
                ByteUtils.writeLongLE(numeric, packed, pos);
                pos += 8;
            }
            case EirfType.STRING -> {
                XContentString.UTF8Bytes str = (XContentString.UTF8Bytes) var;
                int len = str.length();
                ByteUtils.writeIntLE(len, packed, pos);
                pos += 4;
                System.arraycopy(str.bytes(), str.offset(), packed, pos, len);
                pos += len;
            }
            case EirfType.KEY_VALUE, EirfType.UNION_ARRAY, EirfType.FIXED_ARRAY -> {
                byte[] bytes = (byte[]) var;
                ByteUtils.writeIntLE(bytes.length, packed, pos);
                pos += 4;
                System.arraycopy(bytes, 0, packed, pos, bytes.length);
                pos += bytes.length;
            }
        }
        return pos;
    }

    /**
     * Serializes an object from the parser into KEY_VALUE binary format.
     * Parser must be positioned after START_OBJECT.
     */
    static byte[] serializeKeyValue(XContentParser parser) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput(64);

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalStateException("Expected FIELD_NAME but got " + token);
            }
            byte[] keyBytes = parser.currentName().getBytes(StandardCharsets.UTF_8);
            token = parser.nextToken(); // value token

            // key_length(i32) + key_bytes
            out.writeIntLE(keyBytes.length);
            out.writeBytes(keyBytes, 0, keyBytes.length);

            // type(1) + value_data
            writeElementValue(out, parser, token);
        }

        return BytesReference.toBytes(out.bytes());
    }

    private static void writeElementValue(BytesStreamOutput out, XContentParser parser, XContentParser.Token token) throws IOException {
        switch (token) {
            case VALUE_STRING -> {
                XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                out.writeByte(EirfType.STRING);
                out.writeIntLE(str.length());
                out.writeBytes(str.bytes(), str.offset(), str.length());
            }
            case VALUE_NUMBER -> {
                XContentParser.NumberType numType = parser.numberType();
                switch (numType) {
                    case INT, LONG -> {
                        long val = parser.longValue();
                        if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                            out.writeByte(EirfType.INT);
                            out.writeIntLE((int) val);
                        } else {
                            out.writeByte(EirfType.LONG);
                            out.writeLongLE(val);
                        }
                    }
                    case FLOAT, DOUBLE -> {
                        double val = parser.doubleValue();
                        float fval = (float) val;
                        if ((double) fval == val) {
                            out.writeByte(EirfType.FLOAT);
                            out.writeIntLE(Float.floatToRawIntBits(fval));
                        } else {
                            out.writeByte(EirfType.DOUBLE);
                            out.writeLongLE(Double.doubleToRawLongBits(val));
                        }
                    }
                    default -> {
                        XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                        out.writeByte(EirfType.STRING);
                        out.writeIntLE(str.length());
                        out.writeBytes(str.bytes(), str.offset(), str.length());
                    }
                }
            }
            case VALUE_BOOLEAN -> out.writeByte(parser.booleanValue() ? EirfType.TRUE : EirfType.FALSE);
            case VALUE_NULL -> out.writeByte(EirfType.NULL);
            case START_OBJECT -> {
                byte[] nested = serializeKeyValue(parser);
                out.writeByte(EirfType.KEY_VALUE);
                out.writeIntLE(nested.length);
                out.writeBytes(nested, 0, nested.length);
            }
            case START_ARRAY -> {
                PackedArray arr = parseArray(parser, null);
                out.writeByte(arr.arrayType);
                out.writeIntLE(arr.packed.length);
                out.writeBytes(arr.packed, 0, arr.packed.length);
            }
            default -> throw new IllegalStateException("Unexpected token: " + token);
        }
    }
}
