/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Programmatic API for building a {@link RowDocumentBatch} without parsing XContent.
 * Uses the same binary format as {@link DocumentBatchRowEncoder} but allows callers
 * to set column values directly via setter methods.
 */
public class DocumentBatchRowBuilder implements Releasable {

    private static final int INITIAL_CAPACITY = 16;

    private final DocBatchSchema schema;
    private final DocumentBatchRowEncoder.ScratchBuffers scratch;
    private final RecyclerBytesStreamOutput rowOutput;

    private int[] rowOffsets;
    private int[] rowLengths;
    private int docCount;
    private boolean inDocument;

    public DocumentBatchRowBuilder() {
        this(null);
    }

    /**
     * Creates a builder with an optional fixed schema. When a fixed schema is provided,
     * its columns are pre-registered so that lookups for known fields short-circuit
     * without a HashMap insert.
     *
     * @param fixedSchema a pre-built fixed schema (from {@link DocBatchSchema#fixed}), or null
     */
    public DocumentBatchRowBuilder(DocBatchSchema fixedSchema) {
        this.schema = new DocBatchSchema(fixedSchema);
        int initialCapacity = fixedSchema != null ? Math.max(INITIAL_CAPACITY, fixedSchema.columnCount()) : INITIAL_CAPACITY;
        this.scratch = new DocumentBatchRowEncoder.ScratchBuffers(initialCapacity);
        this.rowOutput = new RecyclerBytesStreamOutput(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        this.rowOffsets = new int[INITIAL_CAPACITY];
        this.rowLengths = new int[INITIAL_CAPACITY];
        this.docCount = 0;
        this.inDocument = false;
    }

    /**
     * Begins a new document. Must be paired with {@link #endDocument()}.
     */
    public void startDocument() {
        if (inDocument) {
            throw new IllegalStateException("Already in a document");
        }
        inDocument = true;
        // Zero scratch buffers for columns known so far
        int columnCount = schema.columnCount();
        Arrays.fill(scratch.typeBytes, 0, columnCount, (byte) 0);
        Arrays.fill(scratch.varData, 0, columnCount, null);
    }

    /**
     * Finishes the current document and writes the row data.
     */
    public void endDocument() {
        if (inDocument == false) {
            throw new IllegalStateException("Not in a document");
        }
        inDocument = false;

        // Ensure capacity for doc tracking arrays
        if (docCount >= rowOffsets.length) {
            int newCap = rowOffsets.length << 1;
            rowOffsets = Arrays.copyOf(rowOffsets, newCap);
            rowLengths = Arrays.copyOf(rowLengths, newCap);
        }

        int columnCount = schema.columnCount();
        int rowStart = (int) rowOutput.position();
        rowOffsets[docCount] = rowStart;
        try {
            DocumentBatchRowEncoder.writeRow(rowOutput, columnCount, scratch);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write row", e);
        }
        rowLengths[docCount] = (int) rowOutput.position() - rowStart;
        docCount++;
    }

    // ---- Name-based setters (resolve column via schema lookup) ----

    public void setString(String path, String value) {
        int colIdx = resolveColumn(path);
        setStringAt(colIdx, value);
    }

    public void setString(String path, byte[] utf8, int offset, int length) {
        int colIdx = resolveColumn(path);
        setStringAt(colIdx, utf8, offset, length);
    }

    public void setLong(String path, long value) {
        int colIdx = resolveColumn(path);
        setLongAt(colIdx, value);
    }

    public void setDouble(String path, double value) {
        int colIdx = resolveColumn(path);
        setDoubleAt(colIdx, value);
    }

    public void setBoolean(String path, boolean value) {
        int colIdx = resolveColumn(path);
        scratch.typeBytes[colIdx] = value ? RowType.TRUE : RowType.FALSE;
    }

    public void setNull(String path) {
        int colIdx = resolveColumn(path);
        scratch.typeBytes[colIdx] = RowType.NULL;
    }

    public void setBinary(String path, BytesReference bytes) {
        int colIdx = resolveColumn(path);
        scratch.typeBytes[colIdx] = RowType.BINARY;
        scratch.varData[colIdx] = bytes;
    }

    public void setPackedArray(String path, byte[] packed) {
        int colIdx = resolveColumn(path);
        scratch.typeBytes[colIdx] = RowType.ARRAY;
        scratch.varData[colIdx] = new org.elasticsearch.common.bytes.BytesArray(packed);
    }

    public void setXContentArray(String path, BytesReference bytes) {
        int colIdx = resolveColumn(path);
        scratch.typeBytes[colIdx] = RowType.XCONTENT_ARRAY;
        scratch.varData[colIdx] = bytes;
    }

    // ---- Index-based setters (bypass schema lookup — use for fixed columns with known indices) ----

    public void setStringAt(int colIdx, String value) {
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        scratch.typeBytes[colIdx] = RowType.STRING;
        scratch.varData[colIdx] = new XContentString.UTF8Bytes(utf8, 0, utf8.length);
    }

    public void setStringAt(int colIdx, byte[] utf8, int offset, int length) {
        byte[] copy = new byte[length];
        System.arraycopy(utf8, offset, copy, 0, length);
        scratch.typeBytes[colIdx] = RowType.STRING;
        scratch.varData[colIdx] = new XContentString.UTF8Bytes(copy, 0, length);
    }

    public void setLongAt(int colIdx, long value) {
        scratch.typeBytes[colIdx] = RowType.LONG;
        DocumentBatchRowEncoder.writeLongToFixed(scratch.fixedData, colIdx, value);
    }

    public void setDoubleAt(int colIdx, double value) {
        scratch.typeBytes[colIdx] = RowType.DOUBLE;
        DocumentBatchRowEncoder.writeLongToFixed(scratch.fixedData, colIdx, Double.doubleToRawLongBits(value));
    }

    /**
     * Builds the final {@link RowDocumentBatch} from all accumulated documents.
     */
    public RowDocumentBatch build() {
        if (inDocument) {
            throw new IllegalStateException("Cannot build while in a document");
        }

        ReleasableBytesReference rowBytes = rowOutput.moveToBytesReference();
        BytesReference headerBytes = DocumentBatchRowEncoder.buildHeader(schema, docCount, rowOffsets, rowLengths, rowBytes.length());
        BytesReference combined = CompositeBytesReference.of(headerBytes, rowBytes);
        return new RowDocumentBatch(combined, rowBytes);
    }

    /**
     * Returns the current document count.
     */
    public int docCount() {
        return docCount;
    }

    private int resolveColumn(String path) {
        if (inDocument == false) {
            throw new IllegalStateException("Not in a document");
        }
        int colIdx = schema.appendColumn(path);
        scratch.ensureCapacity(colIdx + 1);
        return colIdx;
    }

    @Override
    public void close() {
        rowOutput.close();
    }
}
