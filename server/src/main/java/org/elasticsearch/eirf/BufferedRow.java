/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.xcontent.XContentString;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Reusable in-memory representation of a single document's column data, produced by
 * {@link EirfDocumentParser} and consumed by {@link EirfPartitionWriter}.
 *
 * <p>Holds parallel arrays indexed by leaf column index: a type byte per column, 8 bytes of
 * fixed-width numeric storage per column, and an object reference slot per column for
 * variable-length data. Fields are package-private so the parser and partition writer can
 * access them directly without accessor overhead on the hot path.
 *
 * <p>An instance may be reused across documents and across different {@link EirfSchema} instances.
 * The internal arrays grow to accommodate the largest schema seen and are never shrunk.
 * {@link #reset(int)} must be called before each parse to clear the live column range.
 *
 * <pre>
 * EirfRow row = new EirfRow();
 * row.reset(schema.leafCount());
 * EirfDocumentParser.parseXContent(source, type, schema, row, sink, pathCache);
 * Object ts = row.readTimestamp(timestampColumnIndex);
 * writer.commit(row, concreteIndex, shardNum);
 * </pre>
 */
public final class BufferedRow {

    private static final int INITIAL_CAPACITY = 16;

    /** Per-column type byte; 0 means absent (not set during parsing). */
    byte[] typeBytes;
    /** 8 bytes per column slot, holding fixed-width numerics (ints and floats use the low 4 bytes). */
    byte[] fixedData;
    /** Per-column variable-length data: {@link XContentString.UTF8Bytes} for strings,
     *  {@link org.elasticsearch.common.bytes.BytesReference} for binary, or
     *  {@link org.elasticsearch.common.bytes.BytesArray} for arrays/key-values. */
    Object[] varData;

    /** Total byte size of all variable-length column values accumulated during parsing. */
    int totalVarSize;
    /** Number of columns with variable-length data accumulated during parsing. */
    int varColumnCount;
    /** Total byte size of all scalar fixed-width values accumulated during parsing. */
    int scalarFixedSize;

    /** Tracks which columns have been set in the current document, to detect duplicates. */
    FixedBitSet columnsSet;

    /** Reusable buffers for array element parsing; null until first array is encountered.
     *  Set to null while borrowed by the parser to handle re-entrant nested array parsing. */
    byte[] arrayElemTypes;
    long[] arrayElemNumeric;
    Object[] arrayElemVar;

    public BufferedRow() {
        this.typeBytes = new byte[INITIAL_CAPACITY];
        this.fixedData = new byte[INITIAL_CAPACITY * 8];
        this.varData = new Object[INITIAL_CAPACITY];
        this.columnsSet = new FixedBitSet(Math.max(INITIAL_CAPACITY, 64));
    }

    /**
     * Prepares this row for a new document parse. Clears type bytes and var refs for the live
     * column range [0, columnCount), resets counters, and clears the columns-set bitmap.
     *
     * @param columnCount the current schema leaf count (only this prefix need be cleared)
     */
    public void reset(int columnCount) {
        Arrays.fill(typeBytes, 0, columnCount, (byte) 0);
        Arrays.fill(varData, 0, columnCount, null);
        totalVarSize = 0;
        varColumnCount = 0;
        scalarFixedSize = 0;
        columnsSet.clear();
    }

    /** Ensures the parallel arrays can hold at least {@code needed} column indices. */
    void ensureCapacity(int needed) {
        if (needed <= typeBytes.length) return;
        int cap = typeBytes.length;
        while (cap <= needed) {
            cap <<= 1;
        }
        typeBytes = Arrays.copyOf(typeBytes, cap);
        fixedData = Arrays.copyOf(fixedData, cap * 8);
        varData = Arrays.copyOf(varData, cap);
        columnsSet = FixedBitSet.ensureCapacity(columnsSet, cap);
    }

    /**
     * Reads the {@code @timestamp} value from the given column index, returning it in one of
     * the shapes that {@code DataStream.selectTimeSeriesWriteIndexFromValue} accepts:
     * <ul>
     *   <li>{@link Long} — epoch millis, when the column holds an {@code INT} or {@code LONG}
     *       value.</li>
     *   <li>{@link String} — raw text, when the column holds a {@code STRING} value.</li>
     *   <li>{@code null} — when {@code columnIndex} is negative (field not yet seen in schema),
     *       the column was not set in the current row, or its type cannot be used as a
     *       timestamp (e.g. boolean or array).</li>
     * </ul>
     */
    public Object readTimestamp(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= typeBytes.length) {
            return null;
        }
        byte type = typeBytes[columnIndex];
        return switch (type) {
            case EirfType.STRING -> {
                XContentString.UTF8Bytes bytes = (XContentString.UTF8Bytes) varData[columnIndex];
                yield bytes == null ? null : new String(bytes.bytes(), bytes.offset(), bytes.length(), StandardCharsets.UTF_8);
            }
            case EirfType.INT -> (long) ByteUtils.readIntLE(fixedData, columnIndex * 8);
            case EirfType.LONG -> ByteUtils.readLongLE(fixedData, columnIndex * 8);
            default -> null;
        };
    }

    /**
     * Replays this row's column values to {@code sink}, firing the same {@link LeafSink}
     * callbacks that the parser would have fired during a live parse.
     *
     * <p>Only valid for typed-mode sinks ({@link LeafSink#passRawText()} returning {@code false}):
     * the original UTF-8 byte text of numeric and boolean leaves is not retained in row storage,
     * so a raw-text replay would not produce byte-identical hashes.
     *
     * @param columnCount number of active columns (from the schema's {@code leafCount()})
     * @param schema      schema to resolve column paths from
     * @param pathCache   memoized path cache for the schema
     * @param sink        the sink to replay into (must have {@code passRawText() == false})
     */
    public void replayTo(int columnCount, EirfSchema schema, ColumnPathCache pathCache, LeafSink sink) {
        if (sink == LeafSink.NO_OP) {
            return;
        }
        byte[] tb = typeBytes;
        byte[] fd = fixedData;
        Object[] vd = varData;
        for (int col = 0; col < columnCount; col++) {
            byte type = tb[col];
            switch (type) {
                case EirfType.STRING -> {
                    XContentString.UTF8Bytes str = (XContentString.UTF8Bytes) vd[col];
                    if (str != null) {
                        sink.onTextPrimitive(col, pathCache.get(col, schema), EirfType.STRING, str);
                    }
                }
                case EirfType.INT -> sink.onLongPrimitive(col, pathCache.get(col, schema), EirfType.INT, ByteUtils.readIntLE(fd, col * 8));
                case EirfType.LONG -> sink.onLongPrimitive(
                    col,
                    pathCache.get(col, schema),
                    EirfType.LONG,
                    ByteUtils.readLongLE(fd, col * 8)
                );
                case EirfType.FLOAT -> sink.onDoublePrimitive(
                    col,
                    pathCache.get(col, schema),
                    EirfType.FLOAT,
                    Float.intBitsToFloat(ByteUtils.readIntLE(fd, col * 8))
                );
                case EirfType.DOUBLE -> sink.onDoublePrimitive(
                    col,
                    pathCache.get(col, schema),
                    EirfType.DOUBLE,
                    Double.longBitsToDouble(ByteUtils.readLongLE(fd, col * 8))
                );
                case EirfType.TRUE -> sink.onBooleanPrimitive(col, pathCache.get(col, schema), true);
                case EirfType.FALSE -> sink.onBooleanPrimitive(col, pathCache.get(col, schema), false);
                case EirfType.FIXED_ARRAY, EirfType.UNION_ARRAY -> sink.onArrayLeaf(col, pathCache.get(col, schema));
                default -> {
                    // 0 (unset / cleared), NULL, KEY_VALUE — no sink callback
                }
            }
        }
    }
}
