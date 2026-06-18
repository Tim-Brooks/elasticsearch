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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.eirf.EirfEncoder;
import org.elasticsearch.eirf.EirfSchema;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Encodes JSON documents into an {@link EicfBatch} (Elastic Internal Column Format).
 *
 * <p>Unlike {@link EirfEncoder} which stores one blob per document, this encoder accumulates one
 * column per leaf field. Numbers are upcast aggressively: JSON ints and longs both become
 * {@code long}, JSON floats and doubles both become {@code double}. A type conflict (including a
 * long+double mix) or an explicit null promotes the column to {@link EicfColumnKind#UNION}; see
 * {@link EicfColumnBuilder}.
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
     * Finalises the accumulated columns and returns the in-memory {@link EicfBatch}. This consumes
     * the per-column data streams, so it must be called at most once. The returned batch owns the
     * streams and releases them on {@link EicfBatch#close()}.
     */
    public EicfBatch build() {
        int colCount = schema.leafCount();
        EicfColumnData[] columns = new EicfColumnData[colCount];
        List<Releasable> releasables = new ArrayList<>(colCount);
        for (int c = 0; c < colCount; c++) {
            EicfColumnData col = columnBuilders.get(c).finish(docCount);
            columns[c] = col;
            if (col.data() instanceof Releasable releasable) {
                releasables.add(releasable);
            }
        }
        return new EicfBatch(schema, docCount, columns, Releasables.wrap(releasables));
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
                // The UTF-8 slice points into the parser's reusable buffer; the builder writes it
                // directly into the column data stream, so it does not outlive this call.
                case VALUE_STRING -> builder.addString(parser.optimizedText().bytes());
                case VALUE_NUMBER -> {
                    XContentParser.NumberType numType = parser.numberType();
                    switch (numType) {
                        case INT, LONG -> builder.addLong(parser.longValue());
                        case FLOAT, DOUBLE -> builder.addDouble(parser.doubleValue());
                        // BIG_INTEGER / BIG_DECIMAL: fall back to the raw string representation
                        default -> builder.addString(parser.optimizedText().bytes());
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
}
