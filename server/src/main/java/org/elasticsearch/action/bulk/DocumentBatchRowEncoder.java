/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * Encodes a list of {@link IndexRequest}s into a row-oriented {@link RowDocumentBatch}.
 * Single-pass streaming: each row is written immediately after parsing, with reusable scratch buffers.
 * The header (schema + doc index) is built last and combined with row data via {@link CompositeBytesReference}.
 */
public class DocumentBatchRowEncoder {

    private static final int HEADER_SIZE = 32;
    private static final int INITIAL_CAPACITY = 16;

    private DocumentBatchRowEncoder() {}

    public static RowDocumentBatch encode(List<IndexRequest> requests) throws IOException {
        return encode(requests, () -> new RecyclerBytesStreamOutput(BytesRefRecycler.NON_RECYCLING_INSTANCE));
    }

    public static RowDocumentBatch encode(List<IndexRequest> requests, Supplier<RecyclerBytesStreamOutput> supplier) throws IOException {
        int docCount = requests.size();
        DocBatchSchema schema = new DocBatchSchema();
        ScratchBuffers scratch = new ScratchBuffers(INITIAL_CAPACITY);

        int[] rowOffsets = new int[docCount];
        int[] rowLengths = new int[docCount];

        try (RecyclerBytesStreamOutput rowOutput = supplier.get()) {
            for (int docIdx = 0; docIdx < docCount; docIdx++) {
                IndexRequest request = requests.get(docIdx);
                BytesReference source = request.source();
                XContentType xContentType = request.getContentType();
                if (xContentType == null) {
                    xContentType = XContentType.JSON;
                }

                // Zero scratch buffers for columns known so far
                int columnCountBefore = schema.columnCount();
                Arrays.fill(scratch.typeBytes, 0, columnCountBefore, (byte) 0);
                Arrays.fill(scratch.varData, 0, columnCountBefore, null);

                // Parse document, populating scratch buffers
                try (
                    XContentParser parser = XContentHelper.createParserNotCompressed(
                        XContentParserConfiguration.EMPTY,
                        source,
                        xContentType
                    )
                ) {
                    parser.allowDuplicateKeys(true);
                    parser.nextToken(); // START_OBJECT
                    flattenObject(parser, "", 0, schema, scratch, xContentType);
                }

                // Write row to output
                int columnCount = schema.columnCount();
                int rowStart = (int) rowOutput.position();
                rowOffsets[docIdx] = rowStart;
                writeRow(rowOutput, columnCount, scratch);
                rowLengths[docIdx] = (int) rowOutput.position() - rowStart;
            }

            ReleasableBytesReference rowBytes = rowOutput.moveToBytesReference();
            BytesReference headerBytes = buildHeader(schema, docCount, rowOffsets, rowLengths, rowBytes.length());
            BytesReference combined = CompositeBytesReference.of(headerBytes, rowBytes);
            return new RowDocumentBatch(combined, rowBytes);
        }
    }

    /**
     * Mutable holder for scratch buffers that survives growth across recursive calls.
     */
    static final class ScratchBuffers {
        byte[] typeBytes;
        byte[] fixedData;
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
        String prefix,
        int objectDepth,
        DocBatchSchema schema,
        ScratchBuffers scratch,
        XContentType xContentType
    ) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalStateException("Expected FIELD_NAME but got " + token);
            }
            String fieldName = parser.currentName();
            String fieldPath = prefix.isEmpty() ? fieldName : prefix + "." + fieldName;

            token = parser.nextToken(); // value token

            if (token == XContentParser.Token.START_OBJECT) {
                flattenObject(parser, fieldPath, objectDepth + 1, schema, scratch, xContentType);
                continue;
            }

            int colIdx = schema.appendColumn(fieldPath);
            scratch.ensureCapacity(colIdx + 1);

            switch (token) {
                case START_ARRAY -> {
                    BytesReference rawBytes = captureRawXContent(parser, xContentType);
                    scratch.typeBytes[colIdx] = objectDepth > 0 ? (byte) (RowType.ARRAY | RowType.OBJECT_FLAG) : RowType.ARRAY;
                    scratch.varData[colIdx] = rawBytes;
                }
                case VALUE_STRING -> {
                    scratch.typeBytes[colIdx] = objectDepth > 0 ? (byte) (RowType.STRING | RowType.OBJECT_FLAG) : RowType.STRING;
                    scratch.varData[colIdx] = parser.optimizedText().bytes();
                }
                case VALUE_NUMBER -> {
                    XContentParser.NumberType numType = parser.numberType();
                    switch (numType) {
                        case INT, LONG -> {
                            scratch.typeBytes[colIdx] = objectDepth > 0 ? (byte) (RowType.LONG | RowType.OBJECT_FLAG) : RowType.LONG;
                            writeLongToFixed(scratch.fixedData, colIdx, parser.longValue());
                        }
                        case FLOAT, DOUBLE -> {
                            scratch.typeBytes[colIdx] = objectDepth > 0 ? (byte) (RowType.DOUBLE | RowType.OBJECT_FLAG) : RowType.DOUBLE;
                            writeLongToFixed(scratch.fixedData, colIdx, Double.doubleToRawLongBits(parser.doubleValue()));
                        }
                        default -> {
                            scratch.typeBytes[colIdx] = objectDepth > 0 ? (byte) (RowType.STRING | RowType.OBJECT_FLAG) : RowType.STRING;
                            scratch.varData[colIdx] = parser.optimizedText().bytes();
                        }
                    }
                }
                case VALUE_BOOLEAN -> {
                    boolean val = parser.booleanValue();
                    byte base = val ? RowType.TRUE : RowType.FALSE;
                    scratch.typeBytes[colIdx] = objectDepth > 0 ? (byte) (base | RowType.OBJECT_FLAG) : base;
                }
                case VALUE_NULL -> {
                    scratch.typeBytes[colIdx] = objectDepth > 0 ? (byte) (RowType.NULL | RowType.OBJECT_FLAG) : RowType.NULL;
                }
                default -> throw new IllegalStateException("Unexpected token: " + token);
            }
        }
    }

    private static BytesReference captureRawXContent(XContentParser parser, XContentType xContentType) throws IOException {
        try (var builder = XContentFactory.jsonBuilder()) {
            builder.copyCurrentStructure(parser);
            return BytesReference.bytes(builder);
        }
    }

    private static void writeRow(RecyclerBytesStreamOutput output, int columnCount, ScratchBuffers scratch) throws IOException {
        byte[] typeBytes = scratch.typeBytes;
        byte[] fixedData = scratch.fixedData;
        Object[] varData = scratch.varData;

        // Write: column_count(4) + type_bytes + fixed_section + var_section
        output.writeInt(columnCount);
        output.writeBytes(typeBytes, 0, columnCount);

        // Fixed section: compute var offsets inline
        int varOffset = 0;
        for (int col = 0; col < columnCount; col++) {
            byte typeByte = typeBytes[col];
            int fs = RowType.fixedSize(typeByte);
            if (fs == 0) continue;

            byte baseType = RowType.baseType(typeByte);
            if (baseType == RowType.LONG || baseType == RowType.DOUBLE) {
                output.writeBytes(fixedData, col * 8, 8);
            } else if (baseType == RowType.STRING) {
                XContentString.UTF8Bytes str = (XContentString.UTF8Bytes) varData[col];
                output.writeInt(varOffset);
                output.writeInt(str.length());
                varOffset += str.length();
            } else if (baseType == RowType.BINARY || baseType == RowType.ARRAY) {
                BytesReference ref = (BytesReference) varData[col];
                output.writeInt(varOffset);
                output.writeInt(ref.length());
                varOffset += ref.length();
            }
        }

        // Var section: write actual bytes
        for (int col = 0; col < columnCount; col++) {
            byte baseType = RowType.baseType(typeBytes[col]);
            if (baseType == RowType.STRING) {
                XContentString.UTF8Bytes str = (XContentString.UTF8Bytes) varData[col];
                output.writeBytes(str.bytes(), str.offset(), str.length());
            } else if (baseType == RowType.BINARY || baseType == RowType.ARRAY) {
                BytesReference ref = (BytesReference) varData[col];
                ref.writeTo(output);
            }
        }
    }

    private static BytesReference buildHeader(DocBatchSchema schema, int docCount, int[] rowOffsets, int[] rowLengths, int rowDataSize) {
        int columnCount = schema.columnCount();

        // Compute schema section size
        int schemaSize = 4; // column_count int
        byte[][] nameBytes = new byte[columnCount][];
        for (int i = 0; i < columnCount; i++) {
            nameBytes[i] = schema.getColumnName(i).getBytes(StandardCharsets.UTF_8);
            schemaSize += 4 + nameBytes[i].length;
        }

        int docIndexSize = docCount * 8;
        int headerTotal = HEADER_SIZE + schemaSize + docIndexSize;

        byte[] header = new byte[headerTotal];
        ByteBuffer buf = ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN);

        int schemaOffset = HEADER_SIZE;
        int docIndexOffset = schemaOffset + schemaSize;
        int dataOffset = headerTotal;
        int totalSize = headerTotal + rowDataSize;

        // Header fields
        buf.putInt(0, RowDocumentBatch.MAGIC);
        buf.putInt(4, RowDocumentBatch.VERSION);
        buf.putInt(8, 0); // flags
        buf.putInt(12, docCount);
        buf.putInt(16, schemaOffset);
        buf.putInt(20, docIndexOffset);
        buf.putInt(24, dataOffset);
        buf.putInt(28, totalSize);

        // Schema section
        int pos = schemaOffset;
        buf.putInt(pos, columnCount);
        pos += 4;
        for (int i = 0; i < columnCount; i++) {
            buf.putInt(pos, nameBytes[i].length);
            pos += 4;
            System.arraycopy(nameBytes[i], 0, header, pos, nameBytes[i].length);
            pos += nameBytes[i].length;
        }

        // Doc index section
        for (int i = 0; i < docCount; i++) {
            buf.putInt(docIndexOffset + i * 8, rowOffsets[i]);
            buf.putInt(docIndexOffset + i * 8 + 4, rowLengths[i]);
        }

        return new BytesArray(header);
    }

    private static void writeLongToFixed(byte[] fixedData, int colIdx, long value) {
        int offset = colIdx * 8;
        fixedData[offset] = (byte) (value >>> 56);
        fixedData[offset + 1] = (byte) (value >>> 48);
        fixedData[offset + 2] = (byte) (value >>> 40);
        fixedData[offset + 3] = (byte) (value >>> 32);
        fixedData[offset + 4] = (byte) (value >>> 24);
        fixedData[offset + 5] = (byte) (value >>> 16);
        fixedData[offset + 6] = (byte) (value >>> 8);
        fixedData[offset + 7] = (byte) value;
    }
}
