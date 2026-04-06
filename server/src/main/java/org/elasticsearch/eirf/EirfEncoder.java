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
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Encodes documents into EIRF (Elastic Internal Row Format) batches.
 *
 * <p>Supports both incremental and batch usage:
 * <pre>
 * // Incremental
 * try (EirfEncoder encoder = new EirfEncoder()) {
 *     encoder.addDocument(source1, XContentType.JSON);
 *     encoder.addDocument(source2, XContentType.JSON);
 *     EirfBatch batch = encoder.build();
 * }
 *
 * // Batch convenience
 * EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON);
 * </pre>
 */
public class EirfEncoder implements Releasable {

    private static final int HEADER_SIZE = 32;
    private static final int INITIAL_CAPACITY = 16;

    private final EirfSchema schema;
    private final ScratchBuffers scratch;
    private final RecyclerBytesStreamOutput rowOutput;

    private int[] rowOffsets;
    private int[] rowLengths;
    private int docCount;

    public EirfEncoder() {
        this(new RecyclerBytesStreamOutput(BytesRefRecycler.NON_RECYCLING_INSTANCE));
    }

    public EirfEncoder(final RecyclerBytesStreamOutput rowOutput) {
        this.schema = new EirfSchema();
        this.scratch = new ScratchBuffers(INITIAL_CAPACITY);
        this.rowOutput = rowOutput;
        this.rowOffsets = new int[INITIAL_CAPACITY];
        this.rowLengths = new int[INITIAL_CAPACITY];
        this.docCount = 0;
    }

    /**
     * Adds a single document to the batch.
     */
    public void addDocument(BytesReference source, XContentType xContentType) throws IOException {
        int columnCountBefore = schema.leafCount();
        Arrays.fill(scratch.typeBytes, 0, columnCountBefore, (byte) 0);
        Arrays.fill(scratch.varData, 0, columnCountBefore, null);

        try (XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, source, xContentType)) {
            // The schema will prevent duplicate columns. No need to double with JSON's internal duplicate prevention.
            parser.allowDuplicateKeys(true);
            parser.nextToken(); // START_OBJECT
            flattenObject(parser, 0, schema, scratch, xContentType);
        }

        if (docCount >= rowOffsets.length) {
            int newCap = rowOffsets.length << 1;
            rowOffsets = Arrays.copyOf(rowOffsets, newCap);
            rowLengths = Arrays.copyOf(rowLengths, newCap);
        }

        int columnCount = schema.leafCount();
        int rowStart = (int) rowOutput.position();
        rowOffsets[docCount] = rowStart;
        writeRow(rowOutput, columnCount, scratch);
        rowLengths[docCount] = (int) rowOutput.position() - rowStart;
        docCount++;
    }

    /**
     * Builds the final {@link EirfBatch} from all accumulated documents.
     */
    public EirfBatch build() {
        ReleasableBytesReference rowBytes = rowOutput.moveToBytesReference();
        BytesReference headerBytes = buildHeader(schema, docCount, rowOffsets, rowLengths, rowBytes.length());
        BytesReference combined = CompositeBytesReference.of(headerBytes, rowBytes);
        return new EirfBatch(combined, rowBytes);
    }

    public int docCount() {
        return docCount;
    }

    @Override
    public void close() {
        rowOutput.close();
    }

    public static EirfBatch encode(List<BytesReference> sources, XContentType xContentType) throws IOException {
        try (EirfEncoder encoder = new EirfEncoder()) {
            for (BytesReference source : sources) {
                encoder.addDocument(source, xContentType);
            }
            return encoder.build();
        }
    }

    static final class ScratchBuffers {
        byte[] typeBytes;
        byte[] fixedData; // 8 bytes per slot (even for 4-byte types, for simplicity)
        Object[] varData; // XContentString.UTF8Bytes or BytesReference

        ScratchBuffers(int capacity) {
            this.typeBytes = new byte[capacity];
            this.fixedData = new byte[capacity * 8];
            this.varData = new Object[capacity];
        }

        void ensureCapacity(int needed) {
            if (needed <= typeBytes.length) return;
            int cap = typeBytes.length;
            while (cap <= needed) {
                cap <<= 1;
            }
            typeBytes = Arrays.copyOf(typeBytes, cap);
            fixedData = Arrays.copyOf(fixedData, cap * 8);
            varData = Arrays.copyOf(varData, cap);
        }
    }

    private static void flattenObject(
        XContentParser parser,
        int parentNonLeafIdx,
        EirfSchema schema,
        ScratchBuffers scratch,
        XContentType xContentType
    ) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalStateException("Expected FIELD_NAME but got " + token);
            }
            String fieldName = parser.currentName();
            token = parser.nextToken();

            if (token == XContentParser.Token.START_OBJECT) {
                int nonLeafIdx = schema.appendNonLeaf(fieldName, parentNonLeafIdx);
                flattenObject(parser, nonLeafIdx, schema, scratch, xContentType);
                continue;
            }

            int colIdx = schema.appendLeaf(fieldName, parentNonLeafIdx);
            scratch.ensureCapacity(colIdx + 1);

            switch (token) {
                case START_ARRAY -> encodeArray(parser, xContentType, colIdx, scratch);
                case VALUE_STRING -> {
                    scratch.typeBytes[colIdx] = EirfType.STRING;
                    scratch.varData[colIdx] = parser.optimizedText().bytes();
                }
                case VALUE_NUMBER -> {
                    XContentParser.NumberType numType = parser.numberType();
                    switch (numType) {
                        case INT, LONG -> {
                            long val = parser.longValue();
                            if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                                scratch.typeBytes[colIdx] = EirfType.INT;
                                writeIntToFixed(scratch.fixedData, colIdx, (int) val);
                            } else {
                                scratch.typeBytes[colIdx] = EirfType.LONG;
                                writeLongToFixed(scratch.fixedData, colIdx, val);
                            }
                        }
                        case FLOAT, DOUBLE -> {
                            double val = parser.doubleValue();
                            float fval = (float) val;
                            if ((double) fval == val) {
                                scratch.typeBytes[colIdx] = EirfType.FLOAT;
                                writeIntToFixed(scratch.fixedData, colIdx, Float.floatToRawIntBits(fval));
                            } else {
                                scratch.typeBytes[colIdx] = EirfType.DOUBLE;
                                writeLongToFixed(scratch.fixedData, colIdx, Double.doubleToRawLongBits(val));
                            }
                        }
                        default -> {
                            scratch.typeBytes[colIdx] = EirfType.STRING;
                            scratch.varData[colIdx] = parser.optimizedText().bytes();
                        }
                    }
                }
                case VALUE_BOOLEAN -> scratch.typeBytes[colIdx] = parser.booleanValue() ? EirfType.TRUE : EirfType.FALSE;
                case VALUE_NULL -> scratch.typeBytes[colIdx] = EirfType.NULL;
                default -> throw new IllegalStateException("Unexpected token: " + token);
            }
        }
    }

    /** Maximum number of elements to buffer when detecting FIXED_ARRAY type. */
    private static final int ARRAY_BUFFER_SIZE = 128;

    private static void encodeArray(XContentParser parser, XContentType xContentType, int colIdx, ScratchBuffers scratch)
        throws IOException {
        byte[] elemTypes = new byte[ARRAY_BUFFER_SIZE];
        Object[] elemData = new Object[ARRAY_BUFFER_SIZE];
        int count = 0;
        boolean forceUnion = false;

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (count >= elemTypes.length) {
                int newCap = elemTypes.length * 2;
                elemTypes = Arrays.copyOf(elemTypes, newCap);
                elemData = Arrays.copyOf(elemData, newCap);
            }
            if (count >= ARRAY_BUFFER_SIZE) {
                forceUnion = true; // exceeded buffer → union
            }
            switch (token) {
                case START_OBJECT -> {
                    elemTypes[count] = EirfType.KEY_VALUE;
                    elemData[count] = serializeKeyValue(parser, xContentType);
                    forceUnion = true;
                }
                case START_ARRAY -> {
                    byte[] nestedPayload = serializeArrayPayload(parser, xContentType);
                    elemTypes[count] = nestedPayload[0]; // first byte is the array type marker
                    elemData[count] = Arrays.copyOfRange(nestedPayload, 1, nestedPayload.length);
                    forceUnion = true;
                }
                case VALUE_STRING -> {
                    elemTypes[count] = EirfType.STRING;
                    elemData[count] = parser.optimizedText().bytes();
                }
                case VALUE_NUMBER -> {
                    XContentParser.NumberType numType = parser.numberType();
                    switch (numType) {
                        case INT, LONG -> {
                            long val = parser.longValue();
                            if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                                elemTypes[count] = EirfType.INT;
                                elemData[count] = val;
                            } else {
                                elemTypes[count] = EirfType.LONG;
                                elemData[count] = val;
                            }
                        }
                        case FLOAT, DOUBLE -> {
                            double val = parser.doubleValue();
                            float fval = (float) val;
                            if ((double) fval == val) {
                                elemTypes[count] = EirfType.FLOAT;
                                elemData[count] = (long) Float.floatToRawIntBits(fval);
                            } else {
                                elemTypes[count] = EirfType.DOUBLE;
                                elemData[count] = Double.doubleToRawLongBits(val);
                            }
                        }
                        default -> {
                            elemTypes[count] = EirfType.STRING;
                            elemData[count] = parser.optimizedText().bytes();
                        }
                    }
                }
                case VALUE_BOOLEAN -> elemTypes[count] = parser.booleanValue() ? EirfType.TRUE : EirfType.FALSE;
                case VALUE_NULL -> elemTypes[count] = EirfType.NULL;
                default -> throw new IllegalStateException("Unexpected token in array: " + token);
            }
            count++;
        }

        // Decide FIXED vs UNION: fixed only if all same leaf type and within buffer
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
        }

        byte[] packed;
        byte arrayType;
        if (useFixed) {
            packed = packFixedArray(sharedType, elemData, count);
            arrayType = EirfType.FIXED_ARRAY;
        } else {
            packed = packUnionArray(elemTypes, elemData, count);
            arrayType = EirfType.UNION_ARRAY;
        }
        scratch.typeBytes[colIdx] = arrayType;
        scratch.varData[colIdx] = new BytesArray(packed);
    }

    /**
     * Packs a union array: per element: type(1) + data. No count byte — byte length terminates.
     */
    static byte[] packUnionArray(byte[] elemTypes, Object[] elemData, int count) {
        int size = 0;
        for (int i = 0; i < count; i++) {
            size += 1; // type byte
            size += elemDataSize(elemTypes[i], elemData[i]);
        }

        byte[] packed = new byte[size];
        int pos = 0;
        for (int i = 0; i < count; i++) {
            packed[pos++] = elemTypes[i];
            pos = writeElemData(packed, pos, elemTypes[i], elemData[i]);
        }
        return packed;
    }

    /**
     * Packs a fixed array: element_type(1) + per element: data only. No count byte — byte length terminates.
     */
    static byte[] packFixedArray(byte sharedType, Object[] elemData, int count) {
        int size = 1; // shared type byte
        for (int i = 0; i < count; i++) {
            size += elemDataSize(sharedType, elemData[i]);
        }

        byte[] packed = new byte[size];
        packed[0] = sharedType;
        int pos = 1;
        for (int i = 0; i < count; i++) {
            pos = writeElemData(packed, pos, sharedType, elemData[i]);
        }
        return packed;
    }

    private static int elemDataSize(byte type, Object data) {
        return switch (type) {
            case EirfType.INT, EirfType.FLOAT -> 4;
            case EirfType.LONG, EirfType.DOUBLE -> 8;
            case EirfType.STRING -> {
                XContentString.UTF8Bytes str = (XContentString.UTF8Bytes) data;
                yield 4 + (str != null ? str.length() : 0);
            }
            case EirfType.KEY_VALUE, EirfType.UNION_ARRAY, EirfType.FIXED_ARRAY -> {
                byte[] bytes = (byte[]) data;
                yield 4 + bytes.length; // 4-byte length prefix + payload
            }
            default -> 0; // NULL, TRUE, FALSE
        };
    }

    private static int writeElemData(byte[] packed, int pos, byte type, Object data) {
        switch (type) {
            case EirfType.INT, EirfType.FLOAT -> {
                ByteUtils.writeIntLE((int) (long) data, packed, pos);
                pos += 4;
            }
            case EirfType.LONG, EirfType.DOUBLE -> {
                ByteUtils.writeLongLE((long) data, packed, pos);
                pos += 8;
            }
            case EirfType.STRING -> {
                XContentString.UTF8Bytes str = (XContentString.UTF8Bytes) data;
                int len = str.length();
                ByteUtils.writeIntLE(len, packed, pos);
                pos += 4;
                System.arraycopy(str.bytes(), str.offset(), packed, pos, len);
                pos += len;
            }
            case EirfType.KEY_VALUE, EirfType.UNION_ARRAY, EirfType.FIXED_ARRAY -> {
                byte[] bytes = (byte[]) data;
                ByteUtils.writeIntLE(bytes.length, packed, pos);
                pos += 4;
                System.arraycopy(bytes, 0, packed, pos, bytes.length);
                pos += bytes.length;
            }
        }
        return pos;
    }

    /** Growable byte buffer for serialization. */
    static final class GrowableBuffer {
        byte[] buf;
        int pos;

        GrowableBuffer(int initialCapacity) {
            this.buf = new byte[initialCapacity];
            this.pos = 0;
        }

        void ensure(int needed) {
            if (needed <= buf.length) return;
            int newCap = buf.length;
            while (newCap < needed) {
                newCap <<= 1;
            }
            buf = Arrays.copyOf(buf, newCap);
        }

        byte[] toArray() {
            return Arrays.copyOf(buf, pos);
        }
    }

    /**
     * Serializes an object from the parser into KEY_VALUE binary format.
     * Parser must be positioned after START_OBJECT.
     */
    static byte[] serializeKeyValue(XContentParser parser, XContentType xContentType) throws IOException {
        GrowableBuffer gb = new GrowableBuffer(64);

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalStateException("Expected FIELD_NAME but got " + token);
            }
            byte[] keyBytes = parser.currentName().getBytes(StandardCharsets.UTF_8);
            token = parser.nextToken(); // value token

            gb.ensure(gb.pos + 4 + keyBytes.length + 1 + 12);

            // key_length(i32) + key_bytes
            ByteUtils.writeIntLE(keyBytes.length, gb.buf, gb.pos);
            gb.pos += 4;
            System.arraycopy(keyBytes, 0, gb.buf, gb.pos, keyBytes.length);
            gb.pos += keyBytes.length;

            // type(1) + value_data
            writeElementValue(gb, parser, token, xContentType);
        }

        return gb.toArray();
    }

    /**
     * Serializes an array from the parser. Returns type_marker(1) + payload.
     * The first byte indicates UNION_ARRAY or FIXED_ARRAY.
     * Parser must be positioned after START_ARRAY.
     */
    private static byte[] serializeArrayPayload(XContentParser parser, XContentType xContentType) throws IOException {
        byte[] elemTypes = new byte[ARRAY_BUFFER_SIZE];
        Object[] elemData = new Object[ARRAY_BUFFER_SIZE];
        int count = 0;
        boolean forceUnion = false;

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (count >= elemTypes.length) {
                int newCap = elemTypes.length * 2;
                elemTypes = Arrays.copyOf(elemTypes, newCap);
                elemData = Arrays.copyOf(elemData, newCap);
            }
            if (count >= ARRAY_BUFFER_SIZE) {
                forceUnion = true;
            }
            switch (token) {
                case START_OBJECT -> {
                    elemTypes[count] = EirfType.KEY_VALUE;
                    elemData[count] = serializeKeyValue(parser, xContentType);
                    forceUnion = true;
                }
                case START_ARRAY -> {
                    byte[] nested = serializeArrayPayload(parser, xContentType);
                    elemTypes[count] = nested[0];
                    elemData[count] = Arrays.copyOfRange(nested, 1, nested.length);
                    forceUnion = true;
                }
                case VALUE_STRING -> {
                    elemTypes[count] = EirfType.STRING;
                    elemData[count] = parser.optimizedText().bytes();
                }
                case VALUE_NUMBER -> {
                    XContentParser.NumberType numType = parser.numberType();
                    switch (numType) {
                        case INT, LONG -> {
                            long val = parser.longValue();
                            if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                                elemTypes[count] = EirfType.INT;
                                elemData[count] = val;
                            } else {
                                elemTypes[count] = EirfType.LONG;
                                elemData[count] = val;
                            }
                        }
                        case FLOAT, DOUBLE -> {
                            double val = parser.doubleValue();
                            float fval = (float) val;
                            if ((double) fval == val) {
                                elemTypes[count] = EirfType.FLOAT;
                                elemData[count] = (long) Float.floatToRawIntBits(fval);
                            } else {
                                elemTypes[count] = EirfType.DOUBLE;
                                elemData[count] = Double.doubleToRawLongBits(val);
                            }
                        }
                        default -> {
                            elemTypes[count] = EirfType.STRING;
                            elemData[count] = parser.optimizedText().bytes();
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
        }

        byte[] packed;
        byte arrayType;
        if (useFixed) {
            packed = packFixedArray(sharedType, elemData, count);
            arrayType = EirfType.FIXED_ARRAY;
        } else {
            packed = packUnionArray(elemTypes, elemData, count);
            arrayType = EirfType.UNION_ARRAY;
        }

        // Prepend the type marker byte
        byte[] result = new byte[1 + packed.length];
        result[0] = arrayType;
        System.arraycopy(packed, 0, result, 1, packed.length);
        return result;
    }

    /**
     * Writes a single element value (type byte + data) into the growable buffer.
     */
    private static void writeElementValue(GrowableBuffer gb, XContentParser parser, XContentParser.Token token, XContentType xContentType)
        throws IOException {
        switch (token) {
            case VALUE_STRING -> {
                XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                gb.ensure(gb.pos + 1 + 4 + str.length());
                gb.buf[gb.pos++] = EirfType.STRING;
                ByteUtils.writeIntLE(str.length(), gb.buf, gb.pos);
                gb.pos += 4;
                System.arraycopy(str.bytes(), str.offset(), gb.buf, gb.pos, str.length());
                gb.pos += str.length();
            }
            case VALUE_NUMBER -> {
                XContentParser.NumberType numType = parser.numberType();
                gb.ensure(gb.pos + 9); // worst case: type + long
                switch (numType) {
                    case INT, LONG -> {
                        long val = parser.longValue();
                        if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                            gb.buf[gb.pos++] = EirfType.INT;
                            ByteUtils.writeIntLE((int) val, gb.buf, gb.pos);
                            gb.pos += 4;
                        } else {
                            gb.buf[gb.pos++] = EirfType.LONG;
                            ByteUtils.writeLongLE(val, gb.buf, gb.pos);
                            gb.pos += 8;
                        }
                    }
                    case FLOAT, DOUBLE -> {
                        double val = parser.doubleValue();
                        float fval = (float) val;
                        if ((double) fval == val) {
                            gb.buf[gb.pos++] = EirfType.FLOAT;
                            ByteUtils.writeIntLE(Float.floatToRawIntBits(fval), gb.buf, gb.pos);
                            gb.pos += 4;
                        } else {
                            gb.buf[gb.pos++] = EirfType.DOUBLE;
                            ByteUtils.writeLongLE(Double.doubleToRawLongBits(val), gb.buf, gb.pos);
                            gb.pos += 8;
                        }
                    }
                    default -> {
                        XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                        gb.ensure(gb.pos + 1 + 4 + str.length());
                        gb.buf[gb.pos++] = EirfType.STRING;
                        ByteUtils.writeIntLE(str.length(), gb.buf, gb.pos);
                        gb.pos += 4;
                        System.arraycopy(str.bytes(), str.offset(), gb.buf, gb.pos, str.length());
                        gb.pos += str.length();
                    }
                }
            }
            case VALUE_BOOLEAN -> {
                gb.ensure(gb.pos + 1);
                gb.buf[gb.pos++] = parser.booleanValue() ? EirfType.TRUE : EirfType.FALSE;
            }
            case VALUE_NULL -> {
                gb.ensure(gb.pos + 1);
                gb.buf[gb.pos++] = EirfType.NULL;
            }
            case START_OBJECT -> {
                byte[] nested = serializeKeyValue(parser, xContentType);
                gb.ensure(gb.pos + 1 + 4 + nested.length);
                gb.buf[gb.pos++] = EirfType.KEY_VALUE;
                ByteUtils.writeIntLE(nested.length, gb.buf, gb.pos);
                gb.pos += 4;
                System.arraycopy(nested, 0, gb.buf, gb.pos, nested.length);
                gb.pos += nested.length;
            }
            case START_ARRAY -> {
                byte[] nested = serializeArrayPayload(parser, xContentType);
                byte nestedType = nested[0];
                int payloadLen = nested.length - 1;
                gb.ensure(gb.pos + 1 + 4 + payloadLen);
                gb.buf[gb.pos++] = nestedType;
                ByteUtils.writeIntLE(payloadLen, gb.buf, gb.pos);
                gb.pos += 4;
                System.arraycopy(nested, 1, gb.buf, gb.pos, payloadLen);
                gb.pos += payloadLen;
            }
            default -> throw new IllegalStateException("Unexpected token: " + token);
        }
    }

    /**
     * Writes a row to output.
     *
     * <p>Row layout: row_flags(u8) | column_count(u16) | var_offset(u16 or i32) | type_bytes | fixed_section | var_section
     */
    static void writeRow(RecyclerBytesStreamOutput output, int columnCount, ScratchBuffers scratch) throws IOException {
        byte[] typeBytes = scratch.typeBytes;
        byte[] fixedData = scratch.fixedData;
        Object[] varData = scratch.varData;

        // First pass: compute total var section size
        int totalVarSize = 0;
        for (int col = 0; col < columnCount; col++) {
            byte typeByte = typeBytes[col];
            if (EirfType.isVariable(typeByte)) {
                totalVarSize += getVarDataLength(typeByte, varData[col]);
            }
        }
        boolean smallRow = totalVarSize <= EirfType.SMALL_ROW_MAX_VAR_SIZE;

        // Compute fixed section size to determine var_offset
        int fixedSectionSize = 0;
        for (int col = 0; col < columnCount; col++) {
            fixedSectionSize += EirfType.fixedSize(typeBytes[col], smallRow);
        }

        // row_flags(1) + column_count(2) + var_offset(2 or 4) + type_bytes(columnCount) + fixed_section
        int varOffsetFieldSize = smallRow ? 2 : 4;
        int varOffset = 1 + 2 + varOffsetFieldSize + columnCount + fixedSectionSize;

        // Write row_flags (u8): bit 0 = small_row
        output.writeByte(smallRow ? (byte) 0x01 : (byte) 0x00);

        // Write column_count as u16 LE
        writeShortLE(output, columnCount);

        // Write var_offset
        if (smallRow) {
            writeShortLE(output, varOffset);
        } else {
            output.writeIntLE(varOffset);
        }

        // Write type bytes (unchanged — type codes are the same regardless of row size)
        for (int col = 0; col < columnCount; col++) {
            output.writeByte(typeBytes[col]);
        }

        // Write fixed section
        int varDataOffset = 0;
        for (int col = 0; col < columnCount; col++) {
            byte typeByte = typeBytes[col];
            int fs = EirfType.fixedSize(typeByte, smallRow);
            if (fs == 0) continue;

            if (typeByte == EirfType.INT || typeByte == EirfType.FLOAT) {
                output.writeBytes(fixedData, col * 8, 4);
            } else if (typeByte == EirfType.LONG || typeByte == EirfType.DOUBLE) {
                output.writeBytes(fixedData, col * 8, 8);
            } else if (EirfType.isVariable(typeByte)) {
                int len = getVarDataLength(typeByte, varData[col]);
                if (smallRow) {
                    // 4-byte entry: u16 offset | u16 length (both LE)
                    writeShortLE(output, varDataOffset);
                    writeShortLE(output, len);
                } else {
                    // 8-byte entry: i32 offset | i32 length (both LE)
                    output.writeIntLE(varDataOffset);
                    output.writeIntLE(len);
                }
                varDataOffset += len;
            }
        }

        // Write var section
        for (int col = 0; col < columnCount; col++) {
            byte typeByte = typeBytes[col];
            if (EirfType.isVariable(typeByte)) {
                writeVarData(output, typeByte, varData[col]);
            }
        }
    }

    private static int getVarDataLength(byte typeByte, Object data) {
        if (typeByte == EirfType.STRING) {
            return ((XContentString.UTF8Bytes) data).length();
        } else if (typeByte == EirfType.BINARY) {
            return ((BytesReference) data).length();
        } else if (typeByte == EirfType.UNION_ARRAY || typeByte == EirfType.FIXED_ARRAY || typeByte == EirfType.KEY_VALUE) {
            return ((BytesArray) data).length();
        }
        return 0;
    }

    private static void writeVarData(RecyclerBytesStreamOutput output, byte typeByte, Object data) throws IOException {
        if (typeByte == EirfType.STRING) {
            XContentString.UTF8Bytes str = (XContentString.UTF8Bytes) data;
            output.writeBytes(str.bytes(), str.offset(), str.length());
        } else if (typeByte == EirfType.BINARY) {
            BytesReference ref = (BytesReference) data;
            ref.writeTo(output);
        } else if (typeByte == EirfType.UNION_ARRAY || typeByte == EirfType.FIXED_ARRAY || typeByte == EirfType.KEY_VALUE) {
            BytesArray arr = (BytesArray) data;
            output.writeBytes(arr.array(), arr.arrayOffset(), arr.length());
        }
    }

    static BytesReference buildHeader(EirfSchema schema, int docCount, int[] rowOffsets, int[] rowLengths, int rowDataSize) {
        int nonLeafCount = schema.nonLeafCount();
        int leafCount = schema.leafCount();

        // Compute schema section size (all u16)
        int schemaSize = 2; // non_leaf_count u16
        byte[][] nonLeafNameBytes = new byte[nonLeafCount][];
        for (int i = 0; i < nonLeafCount; i++) {
            nonLeafNameBytes[i] = schema.getNonLeafName(i).getBytes(StandardCharsets.UTF_8);
            schemaSize += 2 + 2 + nonLeafNameBytes[i].length; // parent_index u16 + name_length u16 + name_bytes
        }
        schemaSize += 2; // leaf_count u16
        byte[][] leafNameBytes = new byte[leafCount][];
        for (int i = 0; i < leafCount; i++) {
            leafNameBytes[i] = schema.getLeafName(i).getBytes(StandardCharsets.UTF_8);
            schemaSize += 2 + 2 + leafNameBytes[i].length;
        }

        int docIndexSize = docCount * 8;
        int headerTotal = HEADER_SIZE + schemaSize + docIndexSize;

        byte[] header = new byte[headerTotal];

        int schemaOffset = HEADER_SIZE;
        int docIndexOffset = schemaOffset + schemaSize;
        int dataOffset = headerTotal;
        int totalSize = headerTotal + rowDataSize;

        // Header fields (i32 LE)
        header[0] = 'e';
        header[1] = 'i';
        header[2] = 'r';
        header[3] = 'f';
        ByteUtils.writeIntLE(EirfBatch.VERSION, header, 4);
        ByteUtils.writeIntLE(0, header, 8); // flags
        ByteUtils.writeIntLE(docCount, header, 12);
        ByteUtils.writeIntLE(schemaOffset, header, 16);
        ByteUtils.writeIntLE(docIndexOffset, header, 20);
        ByteUtils.writeIntLE(dataOffset, header, 24);
        ByteUtils.writeIntLE(totalSize, header, 28);

        // Schema section: non-leaf fields (u16 LE)
        int pos = schemaOffset;
        writeShortLE(header, pos, nonLeafCount);
        pos += 2;
        for (int i = 0; i < nonLeafCount; i++) {
            writeShortLE(header, pos, schema.getNonLeafParent(i));
            pos += 2;
            writeShortLE(header, pos, nonLeafNameBytes[i].length);
            pos += 2;
            System.arraycopy(nonLeafNameBytes[i], 0, header, pos, nonLeafNameBytes[i].length);
            pos += nonLeafNameBytes[i].length;
        }

        // Schema section: leaf fields (u16 LE)
        writeShortLE(header, pos, leafCount);
        pos += 2;
        for (int i = 0; i < leafCount; i++) {
            writeShortLE(header, pos, schema.getLeafParent(i));
            pos += 2;
            writeShortLE(header, pos, leafNameBytes[i].length);
            pos += 2;
            System.arraycopy(leafNameBytes[i], 0, header, pos, leafNameBytes[i].length);
            pos += leafNameBytes[i].length;
        }

        // Doc index section (i32 LE)
        for (int i = 0; i < docCount; i++) {
            ByteUtils.writeIntLE(rowOffsets[i], header, docIndexOffset + i * 8);
            ByteUtils.writeIntLE(rowLengths[i], header, docIndexOffset + i * 8 + 4);
        }

        return new BytesArray(header);
    }

    static void writeLongToFixed(byte[] fixedData, int colIdx, long value) {
        ByteUtils.writeLongLE(value, fixedData, colIdx * 8);
    }

    static void writeIntToFixed(byte[] fixedData, int colIdx, int value) {
        ByteUtils.writeIntLE(value, fixedData, colIdx * 8);
    }

    private static void writeShortLE(byte[] buf, int offset, int value) {
        buf[offset] = (byte) value;
        buf[offset + 1] = (byte) (value >>> 8);
    }

    private static void writeShortLE(RecyclerBytesStreamOutput output, int value) throws IOException {
        output.writeByte((byte) value);
        output.writeByte((byte) (value >>> 8));
    }
}
