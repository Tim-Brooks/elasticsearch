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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Programmatic API for building an {@link EirfBatch} without parsing XContent.
 * Uses the same binary format as {@link EirfDocumentParser} but allows callers
 * to set column values directly via setter methods.
 *
 * <p>Field paths use dot notation for nested objects (e.g., "user.name").
 * The builder automatically creates non-leaf fields in the schema as needed.
 */
public final class EirfRowBuilder implements Releasable {

    private static final int INITIAL_CAPACITY = 16;

    private final EirfSchema schema;
    private final BufferedRow row;
    private final RecyclerBytesStreamOutput rowOutput;

    private int[] rowOffsets;
    private int[] rowLengths;
    private int docCount;
    private boolean inDocument;

    public EirfRowBuilder() {
        this.schema = new EirfSchema();
        this.row = new BufferedRow();
        this.rowOutput = new RecyclerBytesStreamOutput(BytesRefRecycler.NON_RECYCLING_INSTANCE);
        this.rowOffsets = new int[INITIAL_CAPACITY];
        this.rowLengths = new int[INITIAL_CAPACITY];
        this.docCount = 0;
        this.inDocument = false;
    }

    public void startDocument() {
        if (inDocument) {
            throw new IllegalStateException("Already in a document");
        }
        inDocument = true;
        row.reset(schema.leafCount());
    }

    public void endDocument() {
        if (inDocument == false) {
            throw new IllegalStateException("Not in a document");
        }
        inDocument = false;

        if (docCount >= rowOffsets.length) {
            int newCap = rowOffsets.length << 1;
            rowOffsets = Arrays.copyOf(rowOffsets, newCap);
            rowLengths = Arrays.copyOf(rowLengths, newCap);
        }

        int columnCount = schema.leafCount();
        int rowStart = (int) rowOutput.position();
        rowOffsets[docCount] = rowStart;
        try {
            EirfPartitionWriter.writeRow(rowOutput, columnCount, row);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write row", e);
        }
        rowLengths[docCount] = (int) rowOutput.position() - rowStart;
        docCount++;
    }

    public void setString(String path, String value) {
        int colIdx = resolveColumn(path);
        setStringAt(colIdx, value);
    }

    public void setString(String path, byte[] utf8, int offset, int length) {
        int colIdx = resolveColumn(path);
        setStringAt(colIdx, utf8, offset, length);
    }

    public void setInt(String path, int value) {
        int colIdx = resolveColumn(path);
        setIntAt(colIdx, value);
    }

    public void setLong(String path, long value) {
        int colIdx = resolveColumn(path);
        setLongAt(colIdx, value);
    }

    public void setFloat(String path, float value) {
        int colIdx = resolveColumn(path);
        setFloatAt(colIdx, value);
    }

    public void setDouble(String path, double value) {
        int colIdx = resolveColumn(path);
        setDoubleAt(colIdx, value);
    }

    public void setBoolean(String path, boolean value) {
        int colIdx = resolveColumn(path);
        checkNotSet(colIdx);
        row.typeBytes[colIdx] = value ? EirfType.TRUE : EirfType.FALSE;
    }

    public void setNull(String path) {
        int colIdx = resolveColumn(path);
        checkNotSet(colIdx);
        row.typeBytes[colIdx] = EirfType.NULL;
    }

    public void setBinary(String path, BytesReference bytes) {
        int colIdx = resolveColumn(path);
        checkNotSet(colIdx);
        row.typeBytes[colIdx] = EirfType.BINARY;
        row.varData[colIdx] = bytes;
        row.totalVarSize += bytes.length();
        row.varColumnCount++;
    }

    public void setUnionArray(String path, byte[] packed) {
        int colIdx = resolveColumn(path);
        checkNotSet(colIdx);
        row.typeBytes[colIdx] = EirfType.UNION_ARRAY;
        row.varData[colIdx] = new BytesArray(packed);
        row.totalVarSize += packed.length;
        row.varColumnCount++;
    }

    public void setFixedArray(String path, byte[] packed) {
        int colIdx = resolveColumn(path);
        checkNotSet(colIdx);
        row.typeBytes[colIdx] = EirfType.FIXED_ARRAY;
        row.varData[colIdx] = new BytesArray(packed);
        row.totalVarSize += packed.length;
        row.varColumnCount++;
    }

    public void setKeyValue(String path, byte[] bytes) {
        int colIdx = resolveColumn(path);
        checkNotSet(colIdx);
        row.typeBytes[colIdx] = EirfType.KEY_VALUE;
        row.varData[colIdx] = new BytesArray(bytes);
        row.totalVarSize += bytes.length;
        row.varColumnCount++;
    }

    public void setStringAt(int colIdx, String value) {
        checkNotSet(colIdx);
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        row.typeBytes[colIdx] = EirfType.STRING;
        row.varData[colIdx] = new XContentString.UTF8Bytes(utf8, 0, utf8.length);
        row.totalVarSize += utf8.length;
        row.varColumnCount++;
    }

    public void setStringAt(int colIdx, byte[] utf8, int offset, int length) {
        checkNotSet(colIdx);
        byte[] copy = new byte[length];
        System.arraycopy(utf8, offset, copy, 0, length);
        row.typeBytes[colIdx] = EirfType.STRING;
        row.varData[colIdx] = new XContentString.UTF8Bytes(copy, 0, length);
        row.totalVarSize += length;
        row.varColumnCount++;
    }

    public void setIntAt(int colIdx, int value) {
        checkNotSet(colIdx);
        row.typeBytes[colIdx] = EirfType.INT;
        ByteUtils.writeIntLE(value, row.fixedData, colIdx * 8);
        row.scalarFixedSize += 4;
    }

    public void setLongAt(int colIdx, long value) {
        checkNotSet(colIdx);
        row.typeBytes[colIdx] = EirfType.LONG;
        ByteUtils.writeLongLE(value, row.fixedData, colIdx * 8);
        row.scalarFixedSize += 8;
    }

    public void setFloatAt(int colIdx, float value) {
        checkNotSet(colIdx);
        row.typeBytes[colIdx] = EirfType.FLOAT;
        ByteUtils.writeIntLE(Float.floatToRawIntBits(value), row.fixedData, colIdx * 8);
        row.scalarFixedSize += 4;
    }

    public void setDoubleAt(int colIdx, double value) {
        checkNotSet(colIdx);
        row.typeBytes[colIdx] = EirfType.DOUBLE;
        ByteUtils.writeLongLE(Double.doubleToRawLongBits(value), row.fixedData, colIdx * 8);
        row.scalarFixedSize += 8;
    }

    /**
     * Builds the final {@link EirfBatch} from all accumulated documents.
     */
    public EirfBatch build() {
        if (inDocument) {
            throw new IllegalStateException("Cannot build while in a document");
        }

        ReleasableBytesReference rowBytes = rowOutput.moveToBytesReference();
        BytesReference headerBytes = EirfPartitionWriter.buildHeader(schema, docCount, rowOffsets, rowLengths, rowBytes.length());
        BytesReference combined = CompositeBytesReference.of(headerBytes, rowBytes);
        return new EirfBatch(combined, rowBytes);
    }

    public int docCount() {
        return docCount;
    }

    private void checkNotSet(int colIdx) {
        if (row.columnsSet.getAndSet(colIdx)) {
            throw new IllegalStateException("Column [" + colIdx + "] already set");
        }
    }

    /**
     * Resolves a dot-separated path into a leaf column index, creating
     * intermediate non-leaf fields as needed.
     */
    private int resolveColumn(String path) {
        if (inDocument == false) {
            throw new IllegalStateException("Not in a document");
        }
        int parentIdx = 0; // root
        int lastDot = path.lastIndexOf('.');
        if (lastDot >= 0) {
            int start = 0;
            while (start <= lastDot) {
                int nextDot = path.indexOf('.', start);
                if (nextDot < 0 || nextDot > lastDot) {
                    nextDot = lastDot;
                }
                String segment = path.substring(start, nextDot);
                parentIdx = schema.appendNonLeaf(segment, parentIdx);
                start = nextDot + 1;
            }
        }
        String leafName = lastDot >= 0 ? path.substring(lastDot + 1) : path;
        int colIdx = schema.appendLeaf(leafName, parentIdx);
        row.ensureCapacity(colIdx + 1);
        return colIdx;
    }

    @Override
    public void close() {
        rowOutput.close();
    }
}
