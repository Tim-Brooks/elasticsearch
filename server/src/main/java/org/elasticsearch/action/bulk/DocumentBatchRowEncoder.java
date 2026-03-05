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
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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

    private static final int HEADER_SIZE = 32;

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
                parser.allowDuplicateKeys(true);
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

    private static RowDocumentBatch serialize(DocBatchSchema schema, List<List<FieldEntry>> allDocFields, int docCount) throws IOException {
        int columnCount = schema.columnCount();

        try (RecyclerBytesStreamOutput output = new RecyclerBytesStreamOutput(BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
            // --- Write header with placeholder values (32 bytes) ---
            output.writeInt(RowDocumentBatch.MAGIC);  // 0: magic
            output.writeInt(RowDocumentBatch.VERSION); // 4: version (now int)
            output.writeInt(0);                        // 8: flags (now int)
            output.writeInt(docCount);                 // 12: doc_count
            output.writeInt(0);                        // 16: schema_offset (placeholder)
            output.writeInt(0);                        // 20: doc_index_offset (placeholder)
            output.writeInt(0);                        // 24: data_offset (placeholder)
            output.writeInt(0);                        // 28: total_size (placeholder)

            // --- Write schema section ---
            int schemaOffset = (int) output.position();
            output.writeInt(columnCount);
            for (int i = 0; i < columnCount; i++) {
                byte[] nameBytes = schema.getColumnName(i).getBytes(StandardCharsets.UTF_8);
                output.writeInt(nameBytes.length);
                output.writeBytes(nameBytes, 0, nameBytes.length);
            }

            // --- Reserve doc index section (docCount * 8 bytes) ---
            int docIndexOffset = (int) output.position();
            int docIndexSize = docCount * 8;
            output.skip(docIndexSize);

            // --- Write each row inline ---
            int dataOffset = (int) output.position();
            int[] rowOffsets = new int[docCount];
            int[] rowLengths = new int[docCount];

            for (int docIdx = 0; docIdx < docCount; docIdx++) {
                int rowStart = (int) output.position();
                rowOffsets[docIdx] = rowStart - dataOffset;

                List<FieldEntry> fields = allDocFields.get(docIdx);
                writeRow(output, columnCount, fields);

                rowLengths[docIdx] = (int) output.position() - rowStart;
            }

            int totalSize = (int) output.position();

            // --- Seek back and fill in header offsets ---
            output.seek(16);
            output.writeInt(schemaOffset);
            output.writeInt(docIndexOffset);
            output.writeInt(dataOffset);
            output.writeInt(totalSize);

            // --- Fill in doc index entries ---
            output.seek(docIndexOffset);
            for (int docIdx = 0; docIdx < docCount; docIdx++) {
                output.writeInt(rowOffsets[docIdx]);
                output.writeInt(rowLengths[docIdx]);
            }

            // Seek back to end so bytes() returns correct size
            output.seek(totalSize);

            BytesReference bytes = output.bytes();
            return new RowDocumentBatch(bytes);
        }
    }

    private static void writeRow(RecyclerBytesStreamOutput output, int columnCount, List<FieldEntry> fields) throws IOException {
        // Build type bytes array (NULL for absent columns)
        byte[] typeBytes = new byte[columnCount];

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

        // Collect variable data
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

        // Write row: column_count(4) + type_bytes + fixed_section + var_section
        output.writeInt(columnCount);
        output.writeBytes(typeBytes, 0, columnCount);

        // fixed section
        for (int col = 0; col < columnCount; col++) {
            byte typeByte = typeBytes[col];
            int fs = RowType.fixedSize(typeByte);
            if (fs == 0) continue;

            byte baseType = RowType.baseType(typeByte);
            FieldEntry fe = fieldByCol[col];
            if (baseType == RowType.LONG) {
                output.writeLong(fe.longValue);
            } else if (baseType == RowType.DOUBLE) {
                output.writeLong(Double.doubleToRawLongBits(fe.doubleValue));
            } else if (baseType == RowType.STRING || baseType == RowType.BINARY || baseType == RowType.ARRAY) {
                output.writeInt(varOffsets[col]);
                output.writeInt(varLengths[col]);
            }
        }

        // var section
        output.writeBytes(varData, 0, varData.length);
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
