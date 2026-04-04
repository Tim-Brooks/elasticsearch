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
import org.elasticsearch.xcontent.XContentFactory;
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
        this.schema = new EirfSchema();
        this.scratch = new ScratchBuffers(INITIAL_CAPACITY);
        this.rowOutput = new RecyclerBytesStreamOutput(BytesRefRecycler.NON_RECYCLING_INSTANCE);
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

        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, source, xContentType)
        ) {
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

    // ---- Convenience static methods ----

    public static EirfBatch encode(List<BytesReference> sources, XContentType xContentType) throws IOException {
        try (EirfEncoder encoder = new EirfEncoder()) {
            for (BytesReference source : sources) {
                encoder.addDocument(source, xContentType);
            }
            return encoder.build();
        }
    }

    // ---- Internal: shared with EirfRowBuilder ----

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
                    // Stored as large-variant type in scratch; writeRow decides small vs large
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

    private static void encodeArray(XContentParser parser, XContentType xContentType, int colIdx, ScratchBuffers scratch)
        throws IOException {
        int maxSmall = EirfType.MAX_SMALL_ARRAY_SIZE;
        byte[] elemTypes = new byte[maxSmall];
        long[] elemFixed = new long[maxSmall];
        XContentString.UTF8Bytes[] elemStrings = new XContentString.UTF8Bytes[maxSmall];
        int count = 0;
        boolean fallback = false;

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (count >= maxSmall || token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                fallback = true;
                break;
            }
            switch (token) {
                case VALUE_STRING -> {
                    elemTypes[count] = EirfType.STRING;
                    elemStrings[count] = parser.optimizedText().bytes();
                }
                case VALUE_NUMBER -> {
                    XContentParser.NumberType numType = parser.numberType();
                    switch (numType) {
                        case INT, LONG -> {
                            long val = parser.longValue();
                            if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                                elemTypes[count] = EirfType.INT;
                                elemFixed[count] = val;
                            } else {
                                elemTypes[count] = EirfType.LONG;
                                elemFixed[count] = val;
                            }
                        }
                        case FLOAT, DOUBLE -> {
                            double val = parser.doubleValue();
                            float fval = (float) val;
                            if ((double) fval == val) {
                                elemTypes[count] = EirfType.FLOAT;
                                elemFixed[count] = Float.floatToRawIntBits(fval);
                            } else {
                                elemTypes[count] = EirfType.DOUBLE;
                                elemFixed[count] = Double.doubleToRawLongBits(val);
                            }
                        }
                        default -> {
                            elemTypes[count] = EirfType.STRING;
                            elemStrings[count] = parser.optimizedText().bytes();
                        }
                    }
                }
                case VALUE_BOOLEAN -> elemTypes[count] = parser.booleanValue() ? EirfType.TRUE : EirfType.FALSE;
                case VALUE_NULL -> elemTypes[count] = EirfType.NULL;
                default -> throw new IllegalStateException("Unexpected token in array: " + token);
            }
            count++;
        }

        if (fallback) {
            BytesReference rawBytes = buildXContentFallback(parser, xContentType, elemTypes, elemFixed, elemStrings, count, token);
            // Stored as large variant; writeRow remaps to small if var section fits
            scratch.typeBytes[colIdx] = EirfType.XCONTENT;
            scratch.varData[colIdx] = rawBytes;
        } else {
            // Check if all elements share the same type -> FIXED_ARRAY, otherwise UNION_ARRAY
            boolean allSameType = count > 0;
            byte sharedType = count > 0 ? elemTypes[0] : 0;
            for (int i = 1; i < count && allSameType; i++) {
                if (elemTypes[i] != sharedType) {
                    allSameType = false;
                }
            }

            byte[] packed;
            byte arrayType;
            if (allSameType && count > 0) {
                packed = packFixedArray(elemTypes[0], elemFixed, elemStrings, count);
                arrayType = EirfType.FIXED_ARRAY;
            } else {
                packed = packUnionArray(elemTypes, elemFixed, elemStrings, count);
                arrayType = EirfType.UNION_ARRAY;
            }
            scratch.typeBytes[colIdx] = arrayType;
            scratch.varData[colIdx] = new BytesArray(packed);
        }
    }

    /**
     * Packs a union array: count(1) | per element: type(1) + data.
     */
    static byte[] packUnionArray(byte[] elemTypes, long[] elemFixed, XContentString.UTF8Bytes[] elemStrings, int count) {
        int size = 1; // count byte
        for (int i = 0; i < count; i++) {
            size += 1; // type byte
            size += elemDataSize(elemTypes[i], elemFixed[i], elemStrings != null ? elemStrings[i] : null);
        }

        byte[] packed = new byte[size];
        packed[0] = (byte) count;
        int pos = 1;
        for (int i = 0; i < count; i++) {
            packed[pos++] = elemTypes[i];
            pos = writeElemData(packed, pos, elemTypes[i], elemFixed[i], elemStrings != null ? elemStrings[i] : null);
        }
        return packed;
    }

    /**
     * Packs a fixed array: count(1) | element_type(1) | per element: data only.
     */
    static byte[] packFixedArray(byte sharedType, long[] elemFixed, XContentString.UTF8Bytes[] elemStrings, int count) {
        int size = 2; // count byte + shared type byte
        for (int i = 0; i < count; i++) {
            size += elemDataSize(sharedType, elemFixed[i], elemStrings != null ? elemStrings[i] : null);
        }

        byte[] packed = new byte[size];
        packed[0] = (byte) count;
        packed[1] = sharedType;
        int pos = 2;
        for (int i = 0; i < count; i++) {
            pos = writeElemData(packed, pos, sharedType, elemFixed[i], elemStrings != null ? elemStrings[i] : null);
        }
        return packed;
    }

    private static int elemDataSize(byte type, long fixedVal, XContentString.UTF8Bytes str) {
        return switch (type) {
            case EirfType.INT, EirfType.FLOAT -> 4;
            case EirfType.LONG, EirfType.DOUBLE -> 8;
            case EirfType.STRING -> 4 + (str != null ? str.length() : 0);
            default -> 0; // NULL, TRUE, FALSE
        };
    }

    private static int writeElemData(byte[] packed, int pos, byte type, long fixedVal, XContentString.UTF8Bytes str) {
        switch (type) {
            case EirfType.INT, EirfType.FLOAT -> {
                ByteUtils.writeIntBE((int) fixedVal, packed, pos);
                pos += 4;
            }
            case EirfType.LONG, EirfType.DOUBLE -> {
                ByteUtils.writeLongBE(fixedVal, packed, pos);
                pos += 8;
            }
            case EirfType.STRING -> {
                int len = str.length();
                ByteUtils.writeIntBE(len, packed, pos);
                pos += 4;
                System.arraycopy(str.bytes(), str.offset(), packed, pos, len);
                pos += len;
            }
        }
        return pos;
    }

    private static BytesReference buildXContentFallback(
        XContentParser parser,
        XContentType xContentType,
        byte[] elemTypes,
        long[] elemFixed,
        XContentString.UTF8Bytes[] elemStrings,
        int bufferedCount,
        XContentParser.Token currentToken
    ) throws IOException {
        try (var builder = XContentFactory.contentBuilder(xContentType)) {
            builder.startArray();
            for (int i = 0; i < bufferedCount; i++) {
                switch (elemTypes[i]) {
                    case EirfType.STRING -> {
                        XContentString.UTF8Bytes str = elemStrings[i];
                        builder.utf8Value(str.bytes(), str.offset(), str.length());
                    }
                    case EirfType.INT -> builder.value((int) elemFixed[i]);
                    case EirfType.LONG -> builder.value(elemFixed[i]);
                    case EirfType.FLOAT -> builder.value(Float.intBitsToFloat((int) elemFixed[i]));
                    case EirfType.DOUBLE -> builder.value(Double.longBitsToDouble(elemFixed[i]));
                    case EirfType.TRUE -> builder.value(true);
                    case EirfType.FALSE -> builder.value(false);
                    case EirfType.NULL -> builder.nullValue();
                }
            }
            if (currentToken != null && currentToken != XContentParser.Token.END_ARRAY) {
                if (currentToken == XContentParser.Token.START_OBJECT || currentToken == XContentParser.Token.START_ARRAY) {
                    builder.copyCurrentStructure(parser);
                } else {
                    writeLeafToken(builder, parser, currentToken);
                }
                while ((currentToken = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    if (currentToken == XContentParser.Token.START_OBJECT || currentToken == XContentParser.Token.START_ARRAY) {
                        builder.copyCurrentStructure(parser);
                    } else {
                        writeLeafToken(builder, parser, currentToken);
                    }
                }
            }
            builder.endArray();
            return BytesReference.bytes(builder);
        }
    }

    private static void writeLeafToken(
        org.elasticsearch.xcontent.XContentBuilder builder,
        XContentParser parser,
        XContentParser.Token token
    ) throws IOException {
        switch (token) {
            case VALUE_STRING -> builder.value(parser.text());
            case VALUE_NUMBER -> {
                XContentParser.NumberType numType = parser.numberType();
                switch (numType) {
                    case INT -> builder.value(parser.intValue());
                    case LONG -> builder.value(parser.longValue());
                    case FLOAT -> builder.value(parser.floatValue());
                    case DOUBLE -> builder.value(parser.doubleValue());
                    default -> builder.value(parser.text());
                }
            }
            case VALUE_BOOLEAN -> builder.value(parser.booleanValue());
            case VALUE_NULL -> builder.nullValue();
            default -> throw new IllegalStateException("Unexpected token: " + token);
        }
    }

    /**
     * Writes a row to output. Scratch type bytes use large-variant codes for variable types.
     * This method decides small vs large based on total var section size.
     */
    static void writeRow(RecyclerBytesStreamOutput output, int columnCount, ScratchBuffers scratch) throws IOException {
        byte[] typeBytes = scratch.typeBytes;
        byte[] fixedData = scratch.fixedData;
        Object[] varData = scratch.varData;

        // First pass: compute total var section size
        int totalVarSize = 0;
        for (int col = 0; col < columnCount; col++) {
            byte typeByte = typeBytes[col];
            if (EirfType.isLargeVariable(typeByte)) {
                totalVarSize += getVarDataLength(typeByte, varData[col]);
            }
        }
        boolean useSmall = totalVarSize < EirfType.SMALL_VAR_THRESHOLD;

        // Write column_count as u16
        output.writeShort((short) columnCount);

        // Write type bytes, remapping large variable types to small if needed
        for (int col = 0; col < columnCount; col++) {
            byte typeByte = typeBytes[col];
            if (useSmall && EirfType.isLargeVariable(typeByte)) {
                output.writeByte(EirfType.largeToSmall(typeByte));
            } else {
                output.writeByte(typeByte);
            }
        }

        // Write fixed section
        int varOffset = 0;
        for (int col = 0; col < columnCount; col++) {
            byte typeByte = typeBytes[col];
            int fs = EirfType.fixedSize(typeByte);
            if (fs == 0) continue;

            if (typeByte == EirfType.INT || typeByte == EirfType.FLOAT) {
                output.writeBytes(fixedData, col * 8, 4);
            } else if (typeByte == EirfType.LONG || typeByte == EirfType.DOUBLE) {
                output.writeBytes(fixedData, col * 8, 8);
            } else if (EirfType.isLargeVariable(typeByte)) {
                int len = getVarDataLength(typeByte, varData[col]);
                if (useSmall) {
                    // 4-byte entry: u16 offset | u16 length
                    output.writeShort((short) varOffset);
                    output.writeShort((short) len);
                } else {
                    // 8-byte entry: u32 offset | u32 length
                    output.writeLong(((long) varOffset << 32) | (len & 0xFFFFFFFFL));
                }
                varOffset += len;
            }
        }

        // Write var section
        for (int col = 0; col < columnCount; col++) {
            byte typeByte = typeBytes[col];
            if (EirfType.isLargeVariable(typeByte)) {
                writeVarData(output, typeByte, varData[col]);
            }
        }
    }

    private static int getVarDataLength(byte typeByte, Object data) {
        if (typeByte == EirfType.STRING) {
            return ((XContentString.UTF8Bytes) data).length();
        } else if (typeByte == EirfType.BINARY || typeByte == EirfType.XCONTENT) {
            return ((BytesReference) data).length();
        } else if (typeByte == EirfType.UNION_ARRAY || typeByte == EirfType.FIXED_ARRAY) {
            return ((BytesArray) data).length();
        }
        return 0;
    }

    private static void writeVarData(RecyclerBytesStreamOutput output, byte typeByte, Object data) throws IOException {
        if (typeByte == EirfType.STRING) {
            XContentString.UTF8Bytes str = (XContentString.UTF8Bytes) data;
            output.writeBytes(str.bytes(), str.offset(), str.length());
        } else if (typeByte == EirfType.BINARY || typeByte == EirfType.XCONTENT) {
            BytesReference ref = (BytesReference) data;
            ref.writeTo(output);
        } else if (typeByte == EirfType.UNION_ARRAY || typeByte == EirfType.FIXED_ARRAY) {
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

        // Header fields (still u32)
        ByteUtils.writeIntBE(EirfBatch.MAGIC, header, 0);
        ByteUtils.writeIntBE(EirfBatch.VERSION, header, 4);
        ByteUtils.writeIntBE(0, header, 8); // flags
        ByteUtils.writeIntBE(docCount, header, 12);
        ByteUtils.writeIntBE(schemaOffset, header, 16);
        ByteUtils.writeIntBE(docIndexOffset, header, 20);
        ByteUtils.writeIntBE(dataOffset, header, 24);
        ByteUtils.writeIntBE(totalSize, header, 28);

        // Schema section: non-leaf fields (u16)
        int pos = schemaOffset;
        writeShortBE(header, pos, nonLeafCount);
        pos += 2;
        for (int i = 0; i < nonLeafCount; i++) {
            writeShortBE(header, pos, schema.getNonLeafParent(i));
            pos += 2;
            writeShortBE(header, pos, nonLeafNameBytes[i].length);
            pos += 2;
            System.arraycopy(nonLeafNameBytes[i], 0, header, pos, nonLeafNameBytes[i].length);
            pos += nonLeafNameBytes[i].length;
        }

        // Schema section: leaf fields (u16)
        writeShortBE(header, pos, leafCount);
        pos += 2;
        for (int i = 0; i < leafCount; i++) {
            writeShortBE(header, pos, schema.getLeafParent(i));
            pos += 2;
            writeShortBE(header, pos, leafNameBytes[i].length);
            pos += 2;
            System.arraycopy(leafNameBytes[i], 0, header, pos, leafNameBytes[i].length);
            pos += leafNameBytes[i].length;
        }

        // Doc index section (still u32)
        for (int i = 0; i < docCount; i++) {
            ByteUtils.writeIntBE(rowOffsets[i], header, docIndexOffset + i * 8);
            ByteUtils.writeIntBE(rowLengths[i], header, docIndexOffset + i * 8 + 4);
        }

        return new BytesArray(header);
    }

    static void writeLongToFixed(byte[] fixedData, int colIdx, long value) {
        ByteUtils.writeLongBE(value, fixedData, colIdx * 8);
    }

    static void writeIntToFixed(byte[] fixedData, int colIdx, int value) {
        ByteUtils.writeIntBE(value, fixedData, colIdx * 8);
    }

    private static void writeShortBE(byte[] buf, int offset, int value) {
        buf[offset] = (byte) (value >>> 8);
        buf[offset + 1] = (byte) value;
    }
}
