/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eicf;

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.eirf.EirfEncoder;
import org.elasticsearch.eirf.EirfSchema;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Encodes JSON documents into an {@link EicfBatch} (Elastic Internal Column Format).
 *
 * <p>Unlike {@link EirfEncoder} which stores one blob per document, this encoder stores one
 * typed vector per leaf column. Numbers are upcast aggressively: JSON ints and longs both
 * become {@code long}, JSON floats and doubles both become {@code double}. Mixed-numeric
 * columns become {@link EicfColumnKind#NUMERIC_UNION}; other type conflicts or explicit nulls
 * produce {@link EicfColumnKind#UNION}.
 *
 * <p>Usage:
 * <pre>
 * try (EicfEncoder enc = new EicfEncoder()) {
 *     enc.addDocument(source1, XContentType.JSON);
 *     enc.addDocument(source2, XContentType.JSON);
 *     EicfBatch batch = enc.build();
 * }
 * </pre>
 *
 * <p>Or via the static convenience method:
 * <pre>
 * EicfBatch batch = EicfEncoder.encode(List.of(source1, source2), XContentType.JSON);
 * </pre>
 *
 * <p><b>Limitations (prototype):</b> Top-level empty objects ({@code {}}) are not yet
 * supported and will throw {@link UnsupportedOperationException}. Arrays and objects nested
 * inside arrays are encoded using the existing EIRF array packing codec
 * ({@link EirfEncoder#parseArray}).
 */
public final class EicfEncoder implements Releasable {

    private static final int INITIAL_CAPACITY = 16;

    private final EirfSchema schema;
    /** One builder per leaf column, created lazily and backfilled with absent entries. */
    private final List<EicfColumnBuilder> columnBuilders;
    /** Tracks which columns have been set in the current document (for duplicate detection). */
    private FixedBitSet columnsSet;
    /** Number of documents added so far. */
    private int docCount;
    private boolean closed;

    public EicfEncoder() {
        this.schema = new EirfSchema();
        this.columnBuilders = new ArrayList<>(INITIAL_CAPACITY);
        this.columnsSet = new FixedBitSet(INITIAL_CAPACITY);
    }

    /**
     * Adds a single document to the encoder.
     *
     * @throws UnsupportedOperationException if the document contains a top-level empty object
     */
    public void addDocument(BytesReference source, XContentType xContentType) throws IOException {
        try (XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, source, xContentType)) {
            parser.allowDuplicateKeys(true);
            parser.nextToken(); // START_OBJECT
            columnsSet.clear();
            flattenObject(parser, 0, parser.nextToken());
        }

        // Back-fill absent for every column not set in this document.
        // ensureBuilders before the loop so any new columns seen in this doc have builders.
        // ensureCapacity before the loop so get(c) is valid up to leafCount-1.
        int leafCount = schema.leafCount();
        ensureBuilders(leafCount, docCount);
        columnsSet = FixedBitSet.ensureCapacity(columnsSet, leafCount);
        for (int c = 0; c < leafCount; c++) {
            if (columnsSet.get(c) == false) {
                columnBuilders.get(c).addAbsent();
            }
        }
        docCount++;
    }

    /**
     * Builds and returns the {@link EicfBatch}. Calling this method does not consume the encoder;
     * additional documents may be added and a new batch built. The returned batch is independent
     * of this encoder.
     */
    public EicfBatch build() {
        int colCount = schema.leafCount();
        byte[] kindBytes = new byte[colCount];
        byte[][] blobs = new byte[colCount][];

        for (int c = 0; c < colCount; c++) {
            byte[] finishResult = columnBuilders.get(c).finish(docCount);
            kindBytes[c] = finishResult[0];
            blobs[c] = Arrays.copyOfRange(finishResult, 1, finishResult.length);
        }

        BytesReference batchBytes = buildBatchBytes(schema, docCount, kindBytes, blobs);
        return new EicfBatch(batchBytes, () -> {});
    }

    @Override
    public void close() {
        closed = true;
    }

    /**
     * Convenience method: encodes all {@code sources} in a single batch.
     */
    public static EicfBatch encode(List<BytesReference> sources, XContentType xContentType) throws IOException {
        try (EicfEncoder encoder = new EicfEncoder()) {
            for (BytesReference source : sources) {
                encoder.addDocument(source, xContentType);
            }
            return encoder.build();
        }
    }

    private void flattenObject(XContentParser parser, int parentNonLeafIdx, XContentParser.Token firstToken) throws IOException {
        XContentParser.Token token = firstToken;
        while (token != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalStateException("Expected FIELD_NAME but got " + token);
            }
            String fieldName = parser.currentName();
            token = parser.nextToken();

            if (token == XContentParser.Token.START_OBJECT) {
                XContentParser.Token inner = parser.nextToken();
                if (inner == XContentParser.Token.END_OBJECT) {
                    // Top-level empty objects are not yet supported as standalone KEY_VALUE columns
                    throw new UnsupportedOperationException(
                        "Empty objects as standalone columns are not yet supported in EICF (field: [" + fieldName + "])"
                    );
                } else {
                    int nonLeafIdx = schema.appendNonLeaf(fieldName, parentNonLeafIdx);
                    flattenObject(parser, nonLeafIdx, inner);
                }
                token = parser.nextToken();
                continue;
            }

            int colIdx = schema.appendLeaf(fieldName, parentNonLeafIdx);
            ensureBuilders(colIdx + 1, docCount);
            columnsSet = FixedBitSet.ensureCapacity(columnsSet, colIdx + 1);
            if (columnsSet.getAndSet(colIdx)) {
                throw new IllegalArgumentException("Duplicate field [" + fieldName + "]");
            }

            EicfColumnBuilder builder = columnBuilders.get(colIdx);
            switch (token) {
                case START_ARRAY -> {
                    EirfEncoder.PackedArray arr = EirfEncoder.parseArray(parser, null);
                    builder.addArray(arr.arrayType(), arr.packed());
                }
                case VALUE_STRING -> {
                    XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                    byte[] utf8 = Arrays.copyOfRange(str.bytes(), str.offset(), str.offset() + str.length());
                    builder.addString(utf8);
                }
                case VALUE_NUMBER -> {
                    XContentParser.NumberType numType = parser.numberType();
                    switch (numType) {
                        case INT, LONG -> builder.addLong(parser.longValue());
                        case FLOAT, DOUBLE -> builder.addDouble(parser.doubleValue());
                        default -> {
                            // BIG_INTEGER / BIG_DECIMAL: fall back to string
                            XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                            byte[] utf8 = Arrays.copyOfRange(str.bytes(), str.offset(), str.offset() + str.length());
                            builder.addString(utf8);
                        }
                    }
                }
                case VALUE_BOOLEAN -> builder.addBoolean(parser.booleanValue());
                case VALUE_NULL -> builder.addNull();
                default -> throw new IllegalStateException("Unexpected token: " + token);
            }
            token = parser.nextToken();
        }
    }

    /**
     * Ensures {@code columnBuilders} has entries for indices {@code [0, size)}.
     * Any newly created builder is pre-populated with {@code docsBefore} absent entries to
     * account for documents processed before this column first appeared in the schema.
     */
    private void ensureBuilders(int size, int docsBefore) {
        while (columnBuilders.size() < size) {
            EicfColumnBuilder builder = new EicfColumnBuilder();
            for (int i = 0; i < docsBefore; i++) {
                builder.addAbsent();
            }
            columnBuilders.add(builder);
        }
    }

    // -------------------------------------------------------------------------
    // Batch serialisation
    // -------------------------------------------------------------------------

    /**
     * Assembles the full EICF batch bytes from the schema, per-column kind codes and blobs.
     *
     * <p>Layout:
     * <pre>
     * header(32) | schema | column_index(colCount * 9) | column_blobs
     * </pre>
     */
    static BytesReference buildBatchBytes(EirfSchema schema, int docCount, byte[] kindBytes, byte[][] blobs) {
        int colCount = schema.leafCount();
        int nonLeafCount = schema.nonLeafCount();

        // --- schema section ---
        byte[][] nonLeafNameBytes = new byte[nonLeafCount][];
        int schemaSize = 2; // non_leaf_count u16
        for (int i = 0; i < nonLeafCount; i++) {
            nonLeafNameBytes[i] = schema.getNonLeafName(i).getBytes(StandardCharsets.UTF_8);
            schemaSize += 2 + 2 + nonLeafNameBytes[i].length;
        }
        schemaSize += 2; // leaf_count u16
        byte[][] leafNameBytes = new byte[colCount][];
        for (int i = 0; i < colCount; i++) {
            leafNameBytes[i] = schema.getLeafName(i).getBytes(StandardCharsets.UTF_8);
            schemaSize += 2 + 2 + leafNameBytes[i].length;
        }

        // --- column index section: 9 bytes per column (kind u8 + offset i32 + length i32) ---
        int columnIndexSize = colCount * 9;

        // --- column data offsets ---
        int[] dataOffsets = new int[colCount];
        int[] dataLengths = new int[colCount];
        int cumDataOffset = 0;
        for (int c = 0; c < colCount; c++) {
            dataOffsets[c] = cumDataOffset;
            dataLengths[c] = blobs[c].length;
            cumDataOffset += blobs[c].length;
        }
        int totalDataSize = cumDataOffset;

        // --- assemble ---
        int headerSize = 32;
        int schemaOffset = headerSize;
        int columnIndexOffset = schemaOffset + schemaSize;
        int dataOffset = columnIndexOffset + columnIndexSize;
        int totalSize = dataOffset + totalDataSize;

        byte[] header = new byte[dataOffset]; // header + schema + column index

        // Header (i32 LE)
        ByteUtils.writeIntLE(EicfBatch.MAGIC_LE, header, 0);
        ByteUtils.writeIntLE(EicfBatch.VERSION, header, 4);
        ByteUtils.writeIntLE(0, header, 8); // flags
        ByteUtils.writeIntLE(docCount, header, 12);
        ByteUtils.writeIntLE(schemaOffset, header, 16);
        ByteUtils.writeIntLE(columnIndexOffset, header, 20);
        ByteUtils.writeIntLE(dataOffset, header, 24);
        ByteUtils.writeIntLE(totalSize, header, 28);

        // Schema section (u16 LE)
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
        writeShortLE(header, pos, colCount);
        pos += 2;
        for (int i = 0; i < colCount; i++) {
            writeShortLE(header, pos, schema.getLeafParent(i));
            pos += 2;
            writeShortLE(header, pos, leafNameBytes[i].length);
            pos += 2;
            System.arraycopy(leafNameBytes[i], 0, header, pos, leafNameBytes[i].length);
            pos += leafNameBytes[i].length;
        }

        // Column index section (kind u8 + data_offset i32 + data_length i32 per column)
        pos = columnIndexOffset;
        for (int c = 0; c < colCount; c++) {
            header[pos] = kindBytes[c];
            pos += 1;
            ByteUtils.writeIntLE(dataOffsets[c], header, pos);
            pos += 4;
            ByteUtils.writeIntLE(dataLengths[c], header, pos);
            pos += 4;
        }

        // Concatenate header/schema/index with all column blobs
        BytesReference[] parts = new BytesReference[1 + colCount];
        parts[0] = new BytesArray(header);
        for (int c = 0; c < colCount; c++) {
            parts[c + 1] = new BytesArray(blobs[c]);
        }
        return org.elasticsearch.common.bytes.CompositeBytesReference.of(parts);
    }

    private static void writeShortLE(byte[] buf, int offset, int value) {
        buf[offset] = (byte) value;
        buf[offset + 1] = (byte) (value >>> 8);
    }
}
