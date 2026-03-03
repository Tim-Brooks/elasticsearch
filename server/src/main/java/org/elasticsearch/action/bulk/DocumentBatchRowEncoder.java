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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Encodes a list of {@link IndexRequest}s into a row-oriented {@link RowDocumentBatch}.
 * Each document becomes a self-contained row with per-field type bytes.
 *
 * <p>Two-phase approach:
 * <ol>
 *   <li>Phase 1: Parse all docs, build schema incrementally, collect field entries per doc</li>
 *   <li>Phase 2: Serialize batch with final schema so all rows use final column count</li>
 * </ol>
 */
public class DocumentBatchRowEncoder {

    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle SHORT_HANDLE = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    private DocumentBatchRowEncoder() {}

    /**
     * Encode a list of IndexRequests into a row-oriented RowDocumentBatch.
     */
    public static RowDocumentBatch encode(List<IndexRequest> requests) throws IOException {
        int docCount = requests.size();
        DocBatchSchema schema = new DocBatchSchema();
        List<List<FieldEntry>> allDocFields = new ArrayList<>(docCount);

        // Phase 1: Parse all documents, build schema, collect field entries
        for (int docIdx = 0; docIdx < docCount; docIdx++) {
            IndexRequest request = requests.get(docIdx);
            BytesReference source = request.source();
            XContentType xContentType = request.getContentType();
            if (xContentType == null) {
                xContentType = XContentType.JSON;
            }

            List<FieldEntry> fields = new ArrayList<>();
            try (
                XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, source, xContentType)
            ) {
                parser.nextToken(); // START_OBJECT
                flattenObject(parser, "", 0, schema, fields, xContentType);
            }
            allDocFields.add(fields);
        }

        // Phase 2: Serialize with final schema
        return serialize(schema, allDocFields, docCount);
    }

    private static void flattenObject(
        XContentParser parser,
        String prefix,
        int objectDepth,
        DocBatchSchema schema,
        List<FieldEntry> fields,
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

            switch (token) {
                case START_OBJECT -> {
                    flattenObject(parser, fieldPath, objectDepth + 1, schema, fields, xContentType);
                }
                case START_ARRAY -> {
                    int colIdx = schema.appendColumn(fieldPath);
                    BytesReference rawBytes = captureRawXContent(parser, xContentType);
                    byte typeByte = objectDepth > 0 ? (byte) (RowType.ARRAY | RowType.OBJECT_FLAG) : RowType.ARRAY;
                    fields.add(new FieldEntry(colIdx, typeByte, null, 0, 0, rawBytes));
                }
                case VALUE_STRING -> {
                    int colIdx = schema.appendColumn(fieldPath);
                    byte typeByte = objectDepth > 0 ? (byte) (RowType.STRING | RowType.OBJECT_FLAG) : RowType.STRING;
                    XContentString.UTF8Bytes strBytes = parser.optimizedText().bytes();
                    fields.add(new FieldEntry(colIdx, typeByte, strBytes, 0, 0, null));
                }
                case VALUE_NUMBER -> {
                    int colIdx = schema.appendColumn(fieldPath);
                    XContentParser.NumberType numType = parser.numberType();
                    switch (numType) {
                        case INT, LONG -> {
                            byte typeByte = objectDepth > 0 ? (byte) (RowType.LONG | RowType.OBJECT_FLAG) : RowType.LONG;
                            fields.add(new FieldEntry(colIdx, typeByte, null, parser.longValue(), 0, null));
                        }
                        case FLOAT, DOUBLE -> {
                            byte typeByte = objectDepth > 0 ? (byte) (RowType.DOUBLE | RowType.OBJECT_FLAG) : RowType.DOUBLE;
                            fields.add(new FieldEntry(colIdx, typeByte, null, 0, parser.doubleValue(), null));
                        }
                        default -> {
                            // BIG_INTEGER, BIG_DECIMAL -> store as string
                            byte typeByte = objectDepth > 0 ? (byte) (RowType.STRING | RowType.OBJECT_FLAG) : RowType.STRING;
                            XContentString.UTF8Bytes strBytes = parser.optimizedText().bytes();
                            fields.add(new FieldEntry(colIdx, typeByte, strBytes, 0, 0, null));
                        }
                    }
                }
                case VALUE_BOOLEAN -> {
                    int colIdx = schema.appendColumn(fieldPath);
                    boolean val = parser.booleanValue();
                    byte base = val ? RowType.TRUE : RowType.FALSE;
                    byte typeByte = objectDepth > 0 ? (byte) (base | RowType.OBJECT_FLAG) : base;
                    fields.add(new FieldEntry(colIdx, typeByte, null, 0, 0, null));
                }
                case VALUE_NULL -> {
                    int colIdx = schema.appendColumn(fieldPath);
                    byte typeByte = objectDepth > 0 ? (byte) (RowType.NULL | RowType.OBJECT_FLAG) : RowType.NULL;
                    fields.add(new FieldEntry(colIdx, typeByte, null, 0, 0, null));
                }
                default -> throw new IllegalStateException("Unexpected token: " + token);
            }
        }
    }

    private static BytesReference captureRawXContent(XContentParser parser, XContentType xContentType) throws IOException {
        // TODO: Currently broken
        try (var builder = XContentFactory.jsonBuilder()) {
            builder.copyCurrentStructure(parser);
            return BytesReference.bytes(builder);
        }
    }

    private static RowDocumentBatch serialize(DocBatchSchema schema, List<List<FieldEntry>> allDocFields, int docCount) {
        int columnCount = schema.columnCount();

        // --- Encode schema section ---
        ByteArrayOutputStream schemaOut = new ByteArrayOutputStream();
        writeShort(schemaOut, (short) columnCount);
        for (int i = 0; i < columnCount; i++) {
            byte[] nameBytes = schema.getColumnName(i).getBytes(StandardCharsets.UTF_8);
            writeShort(schemaOut, (short) nameBytes.length);
            schemaOut.write(nameBytes, 0, nameBytes.length);
        }
        byte[] schemaBytes = schemaOut.toByteArray();

        // --- Encode each row ---
        byte[][] rowDataArray = new byte[docCount][];
        for (int docIdx = 0; docIdx < docCount; docIdx++) {
            rowDataArray[docIdx] = encodeRow(columnCount, allDocFields.get(docIdx));
        }

        // --- Compute offsets ---
        int headerSize = 28;
        int schemaOffset = headerSize;
        int docIndexOffset = schemaOffset + schemaBytes.length;
        int docIndexSize = docCount * 8; // each entry: offset(4) + length(4)
        int dataOffset = docIndexOffset + docIndexSize;

        int totalRowData = 0;
        for (byte[] row : rowDataArray) {
            totalRowData += row.length;
        }
        int totalSize = dataOffset + totalRowData;

        // --- Assemble final byte array ---
        byte[] result = new byte[totalSize];

        // Header
        INT_HANDLE.set(result, 0, RowDocumentBatch.MAGIC);
        SHORT_HANDLE.set(result, 4, RowDocumentBatch.VERSION);
        SHORT_HANDLE.set(result, 6, (short) 0); // flags
        INT_HANDLE.set(result, 8, docCount);
        INT_HANDLE.set(result, 12, schemaOffset);
        INT_HANDLE.set(result, 16, docIndexOffset);
        INT_HANDLE.set(result, 20, dataOffset);
        INT_HANDLE.set(result, 24, totalSize);

        // Schema
        System.arraycopy(schemaBytes, 0, result, schemaOffset, schemaBytes.length);

        // Doc index + row data
        int currentRowOffset = 0;
        for (int docIdx = 0; docIdx < docCount; docIdx++) {
            int entryOffset = docIndexOffset + docIdx * 8;
            INT_HANDLE.set(result, entryOffset, currentRowOffset);
            INT_HANDLE.set(result, entryOffset + 4, rowDataArray[docIdx].length);
            System.arraycopy(rowDataArray[docIdx], 0, result, dataOffset + currentRowOffset, rowDataArray[docIdx].length);
            currentRowOffset += rowDataArray[docIdx].length;
        }

        return new RowDocumentBatch(result);
    }

    private static byte[] encodeRow(int columnCount, List<FieldEntry> fields) {
        // Build type bytes array (NULL for absent columns)
        byte[] typeBytes = new byte[columnCount];
        // typeBytes are already 0x00 (NULL) by default

        // Index field entries by column for quick lookup
        FieldEntry[] fieldByCol = new FieldEntry[columnCount];
        for (FieldEntry fe : fields) {
            fieldByCol[fe.columnIndex] = fe;
            typeBytes[fe.columnIndex] = fe.typeByte;
        }

        // Compute fixed section size
        int fixedSize = 0;
        for (int col = 0; col < columnCount; col++) {
            fixedSize += RowType.fixedSize(typeBytes[col]);
        }

        // First pass: collect variable data and compute var offsets
        ByteArrayOutputStream varOut = new ByteArrayOutputStream();
        int[] varOffsets = new int[columnCount];
        int[] varLengths = new int[columnCount];

        for (int col = 0; col < columnCount; col++) {
            FieldEntry fe = fieldByCol[col];
            if (fe == null) continue;
            byte baseType = RowType.baseType(fe.typeByte);
            if (baseType == RowType.STRING) {
                varOffsets[col] = varOut.size();
                varLengths[col] = fe.stringBytes.length();
                varOut.write(fe.stringBytes.bytes(), fe.stringBytes.offset(), fe.stringBytes.length());
            } else if (baseType == RowType.BINARY || baseType == RowType.ARRAY) {
                byte[] raw = BytesReference.toBytes(fe.binaryValue);
                varOffsets[col] = varOut.size();
                varLengths[col] = raw.length;
                varOut.write(raw, 0, raw.length);
            }
        }
        byte[] varData = varOut.toByteArray();

        // Assemble row: column_count(2) + type_bytes + fixed_section + var_section
        int rowSize = 2 + columnCount + fixedSize + varData.length;
        byte[] row = new byte[rowSize];
        int pos = 0;

        // row_column_count
        SHORT_HANDLE.set(row, pos, (short) columnCount);
        pos += 2;

        // type bytes
        System.arraycopy(typeBytes, 0, row, pos, columnCount);
        pos += columnCount;

        // fixed section
        for (int col = 0; col < columnCount; col++) {
            byte typeByte = typeBytes[col];
            int fs = RowType.fixedSize(typeByte);
            if (fs == 0) continue;

            byte baseType = RowType.baseType(typeByte);
            FieldEntry fe = fieldByCol[col];
            if (baseType == RowType.LONG) {
                LONG_HANDLE.set(row, pos, fe.longValue);
            } else if (baseType == RowType.DOUBLE) {
                LONG_HANDLE.set(row, pos, Double.doubleToRawLongBits(fe.doubleValue));
            } else if (baseType == RowType.STRING || baseType == RowType.BINARY || baseType == RowType.ARRAY) {
                INT_HANDLE.set(row, pos, varOffsets[col]);
                INT_HANDLE.set(row, pos + 4, varLengths[col]);
            }
            pos += fs;
        }

        // var section
        System.arraycopy(varData, 0, row, pos, varData.length);

        return row;
    }

    private static void writeShort(ByteArrayOutputStream out, short value) {
        out.write((value >>> 8) & 0xFF);
        out.write(value & 0xFF);
    }

    /**
     * Internal representation of a single field value collected during Phase 1.
     */
    private record FieldEntry(
        int columnIndex,
        byte typeByte,
        XContentString.UTF8Bytes stringBytes,
        long longValue,
        double doubleValue,
        BytesReference binaryValue
    ) {}
}
