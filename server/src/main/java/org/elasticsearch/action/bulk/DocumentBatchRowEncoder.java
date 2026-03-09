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
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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

    public static RowDocumentBatch encode(
        List<IndexRequest> requests,
        Supplier<RecyclerBytesStreamOutput> supplier,
        @Nullable RoutingMetadataExtractor routingExtractor
    ) throws IOException {
        return doEncode(requests, supplier, routingExtractor);
    }

    public static RowDocumentBatch encode(List<IndexRequest> requests, Supplier<RecyclerBytesStreamOutput> supplier) throws IOException {
        return doEncode(requests, supplier, null);
    }

    private static RowDocumentBatch doEncode(
        List<IndexRequest> requests,
        Supplier<RecyclerBytesStreamOutput> supplier,
        @Nullable RoutingMetadataExtractor routingExtractor
    ) throws IOException {
        int docCount = requests.size();
        DocBatchSchema schema = new DocBatchSchema();
        ScratchBuffers scratch = new ScratchBuffers(INITIAL_CAPACITY);

        int[] rowOffsets = new int[docCount];
        int[] rowLengths = new int[docCount];

        // Lazily generated timestamp bytes, shared across all docs in the batch that need it
        XContentString.UTF8Bytes generatedTimestampBytes = null;
        String generatedTimestampString = null;

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

                // If this request needs a logs timestamp, ensure the @timestamp column exists before parsing
                boolean needsLogsTimestamp = request.isNeedsLogsTimestamp();
                int tsColIdx = -1;
                if (needsLogsTimestamp) {
                    tsColIdx = schema.appendColumn(DataStream.TIMESTAMP_FIELD_NAME);
                    scratch.ensureCapacity(tsColIdx + 1);
                }

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
                    scratch.pathBuilder.setLength(0);
                    flattenObject(parser, 0, 0, schema, scratch, xContentType);
                }

                // Resolve any newly discovered columns for routing extraction
                if (routingExtractor != null) {
                    int columnCountAfter = schema.columnCount();
                    for (int col = 0; col < columnCountAfter; col++) {
                        if (routingExtractor.isColumnResolved(col) == false) {
                            routingExtractor.resolveColumn(col, schema.getColumnName(col));
                        }
                    }
                }

                // Handle @timestamp for logs: generate if missing, then set rawTimestamp on the request
                if (needsLogsTimestamp) {
                    byte tsType = scratch.typeBytes[tsColIdx];
                    if (tsType == RowType.NULL) {
                        // No @timestamp in the document, generate one
                        if (generatedTimestampBytes == null) {
                            generatedTimestampString = DateTimeFormatter.ISO_INSTANT.format(ZonedDateTime.now(ZoneOffset.UTC));
                            generatedTimestampBytes = new XContentString.UTF8Bytes(
                                generatedTimestampString.getBytes(StandardCharsets.UTF_8)
                            );
                        }
                        scratch.typeBytes[tsColIdx] = RowType.STRING;
                        scratch.varData[tsColIdx] = generatedTimestampBytes;
                        request.setRawTimestamp(generatedTimestampString);
                    } else {
                        // @timestamp was parsed from the doc, extract its value for rawTimestamp
                        byte baseType = RowType.baseType(tsType);
                        if (baseType == RowType.STRING) {
                            XContentString.UTF8Bytes bytes = (XContentString.UTF8Bytes) scratch.varData[tsColIdx];
                            request.setRawTimestamp(new String(bytes.bytes(), bytes.offset(), bytes.length(), StandardCharsets.UTF_8));
                        } else if (baseType == RowType.LONG) {
                            request.setRawTimestamp(readLongFromFixed(scratch.fixedData, tsColIdx));
                        }
                    }
                }

                // Extract routing metadata from scratch buffers before writing the row
                if (routingExtractor != null) {
                    routingExtractor.extractFromScratch(scratch, request);
                    routingExtractor.resetDocument();
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
            RowDocumentBatch batch = new RowDocumentBatch(combined, rowBytes);

            // Set batch metadata on each request
            for (int i = 0; i < docCount; i++) {
                requests.get(i).setBatchRowIndex(i);
                requests.get(i).setBatchRef(batch);
            }

            return batch;
        }
    }

    /**
     * Mutable holder for scratch buffers that survives growth across recursive calls.
     */
    static final class ScratchBuffers {
        byte[] typeBytes;
        byte[] fixedData;
        Object[] varData; // XContentString.UTF8Bytes or BytesReference
        final StringBuilder pathBuilder = new StringBuilder(64);

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
        int prefixLen,
        int objectDepth,
        DocBatchSchema schema,
        ScratchBuffers scratch,
        XContentType xContentType
    ) throws IOException {
        StringBuilder pathBuilder = scratch.pathBuilder;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalStateException("Expected FIELD_NAME but got " + token);
            }
            String fieldName = parser.currentName();

            token = parser.nextToken(); // value token

            if (token == XContentParser.Token.START_OBJECT) {
                if (prefixLen == 0) {
                    pathBuilder.setLength(0);
                    pathBuilder.append(fieldName);
                } else {
                    pathBuilder.setLength(prefixLen);
                    pathBuilder.append('.').append(fieldName);
                }
                flattenObject(parser, pathBuilder.length(), objectDepth + 1, schema, scratch, xContentType);
                continue;
            }

            String columnPath;
            if (prefixLen == 0) {
                columnPath = fieldName;
            } else {
                pathBuilder.setLength(prefixLen);
                pathBuilder.append('.').append(fieldName);
                columnPath = pathBuilder.toString();
            }
            int colIdx = schema.appendColumn(columnPath);
            scratch.ensureCapacity(colIdx + 1);

            switch (token) {
                case START_ARRAY -> {
                    encodeArray(parser, xContentType, colIdx, objectDepth, scratch);
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

    /**
     * Encodes an array value. Attempts to produce a compact typed array (≤8 leaf elements).
     * Falls back to raw x-content if the array has more than 8 elements or contains non-leaf values.
     */
    private static void encodeArray(XContentParser parser, XContentType xContentType, int colIdx, int objectDepth, ScratchBuffers scratch)
        throws IOException {
        // Buffer up to MAX_SMALL_ARRAY_SIZE leaf elements
        int maxSmall = RowType.MAX_SMALL_ARRAY_SIZE;
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
                    elemTypes[count] = RowType.STRING;
                    elemStrings[count] = parser.optimizedText().bytes();
                }
                case VALUE_NUMBER -> {
                    XContentParser.NumberType numType = parser.numberType();
                    switch (numType) {
                        case INT, LONG -> {
                            elemTypes[count] = RowType.LONG;
                            elemFixed[count] = parser.longValue();
                        }
                        case FLOAT, DOUBLE -> {
                            elemTypes[count] = RowType.DOUBLE;
                            elemFixed[count] = Double.doubleToRawLongBits(parser.doubleValue());
                        }
                        default -> {
                            elemTypes[count] = RowType.STRING;
                            elemStrings[count] = parser.optimizedText().bytes();
                        }
                    }
                }
                case VALUE_BOOLEAN -> elemTypes[count] = parser.booleanValue() ? RowType.TRUE : RowType.FALSE;
                case VALUE_NULL -> elemTypes[count] = RowType.NULL;
                default -> throw new IllegalStateException("Unexpected token in array: " + token);
            }
            count++;
        }

        if (fallback) {
            // Write buffered elements + remaining tokens as raw x-content
            BytesReference rawBytes = buildXContentArrayFallback(parser, xContentType, elemTypes, elemFixed, elemStrings, count, token);
            byte base = RowType.XCONTENT_ARRAY;
            scratch.typeBytes[colIdx] = objectDepth > 0 ? (byte) (base | RowType.OBJECT_FLAG) : base;
            scratch.varData[colIdx] = rawBytes;
        } else {
            // Serialize as compact typed array
            byte[] packed = packSmallArray(elemTypes, elemFixed, elemStrings, count);
            byte base = RowType.ARRAY;
            scratch.typeBytes[colIdx] = objectDepth > 0 ? (byte) (base | RowType.OBJECT_FLAG) : base;
            scratch.varData[colIdx] = new BytesArray(packed);
        }
    }

    /**
     * Packs a small array (≤8 leaf elements) into a compact byte format:
     * count(1) | for each element: type(1) + data
     * where data is: nothing for NULL/TRUE/FALSE, 8 bytes for LONG/DOUBLE, 4 bytes length + UTF-8 for STRING.
     */
    private static byte[] packSmallArray(byte[] elemTypes, long[] elemFixed, XContentString.UTF8Bytes[] elemStrings, int count) {
        // Compute total size
        int size = 1; // count byte
        for (int i = 0; i < count; i++) {
            size += 1; // type byte
            switch (elemTypes[i]) {
                case RowType.LONG, RowType.DOUBLE -> size += 8;
                case RowType.STRING -> size += 4 + elemStrings[i].length();
                // NULL, TRUE, FALSE: no data
            }
        }

        byte[] packed = new byte[size];
        packed[0] = (byte) count;
        int pos = 1;
        for (int i = 0; i < count; i++) {
            packed[pos++] = elemTypes[i];
            switch (elemTypes[i]) {
                case RowType.LONG, RowType.DOUBLE -> {
                    ByteUtils.writeLongBE(elemFixed[i], packed, pos);
                    pos += 8;
                }
                case RowType.STRING -> {
                    XContentString.UTF8Bytes str = elemStrings[i];
                    int len = str.length();
                    ByteUtils.writeIntBE(len, packed, pos);
                    pos += 4;
                    System.arraycopy(str.bytes(), str.offset(), packed, pos, len);
                    pos += len;
                }
                // NULL, TRUE, FALSE: no additional data
            }
        }
        return packed;
    }

    /**
     * Builds a raw x-content array from already-consumed leaf elements plus the remaining parser tokens.
     * Called when the small array buffer overflows or a non-leaf element is encountered.
     */
    private static BytesReference buildXContentArrayFallback(
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
            // Write buffered elements
            for (int i = 0; i < bufferedCount; i++) {
                switch (elemTypes[i]) {
                    case RowType.STRING -> {
                        XContentString.UTF8Bytes str = elemStrings[i];
                        builder.utf8Value(str.bytes(), str.offset(), str.length());
                    }
                    case RowType.LONG -> builder.value(elemFixed[i]);
                    case RowType.DOUBLE -> builder.value(Double.longBitsToDouble(elemFixed[i]));
                    case RowType.TRUE -> builder.value(true);
                    case RowType.FALSE -> builder.value(false);
                    case RowType.NULL -> builder.nullValue();
                }
            }
            // Write the current token and remaining tokens
            if (currentToken != null && currentToken != XContentParser.Token.END_ARRAY) {
                if (currentToken == XContentParser.Token.START_OBJECT || currentToken == XContentParser.Token.START_ARRAY) {
                    builder.copyCurrentStructure(parser);
                } else {
                    writeLeafToken(builder, parser, currentToken);
                }
                // Continue with remaining tokens
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
                output.writeLong(((long) varOffset << 32) | (str.length() & 0xFFFFFFFFL));
                varOffset += str.length();
            } else if (baseType == RowType.BINARY || baseType == RowType.XCONTENT_ARRAY) {
                BytesReference ref = (BytesReference) varData[col];
                output.writeLong(((long) varOffset << 32) | (ref.length() & 0xFFFFFFFFL));
                varOffset += ref.length();
            } else if (baseType == RowType.ARRAY) {
                BytesArray arr = (BytesArray) varData[col];
                output.writeLong(((long) varOffset << 32) | (arr.length() & 0xFFFFFFFFL));
                varOffset += arr.length();
            }
        }

        // Var section: write actual bytes
        for (int col = 0; col < columnCount; col++) {
            byte baseType = RowType.baseType(typeBytes[col]);
            if (baseType == RowType.STRING) {
                XContentString.UTF8Bytes str = (XContentString.UTF8Bytes) varData[col];
                output.writeBytes(str.bytes(), str.offset(), str.length());
            } else if (baseType == RowType.BINARY || baseType == RowType.XCONTENT_ARRAY) {
                BytesReference ref = (BytesReference) varData[col];
                ref.writeTo(output);
            } else if (baseType == RowType.ARRAY) {
                BytesArray arr = (BytesArray) varData[col];
                output.writeBytes(arr.array(), arr.arrayOffset(), arr.length());
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

        int schemaOffset = HEADER_SIZE;
        int docIndexOffset = schemaOffset + schemaSize;
        int dataOffset = headerTotal;
        int totalSize = headerTotal + rowDataSize;

        // Header fields
        ByteUtils.writeIntBE(RowDocumentBatch.MAGIC, header, 0);
        ByteUtils.writeIntBE(RowDocumentBatch.VERSION, header, 4);
        ByteUtils.writeIntBE(0, header, 8); // flags
        ByteUtils.writeIntBE(docCount, header, 12);
        ByteUtils.writeIntBE(schemaOffset, header, 16);
        ByteUtils.writeIntBE(docIndexOffset, header, 20);
        ByteUtils.writeIntBE(dataOffset, header, 24);
        ByteUtils.writeIntBE(totalSize, header, 28);

        // Schema section
        int pos = schemaOffset;
        ByteUtils.writeIntBE(columnCount, header, pos);
        pos += 4;
        for (int i = 0; i < columnCount; i++) {
            ByteUtils.writeIntBE(nameBytes[i].length, header, pos);
            pos += 4;
            System.arraycopy(nameBytes[i], 0, header, pos, nameBytes[i].length);
            pos += nameBytes[i].length;
        }

        // Doc index section
        for (int i = 0; i < docCount; i++) {
            ByteUtils.writeIntBE(rowOffsets[i], header, docIndexOffset + i * 8);
            ByteUtils.writeIntBE(rowLengths[i], header, docIndexOffset + i * 8 + 4);
        }

        return new BytesArray(header);
    }

    private static void writeLongToFixed(byte[] fixedData, int colIdx, long value) {
        ByteUtils.writeLongBE(value, fixedData, colIdx * 8);
    }

    private static long readLongFromFixed(byte[] fixedData, int colIdx) {
        return ByteUtils.readLongBE(fixedData, colIdx * 8);
    }
}
