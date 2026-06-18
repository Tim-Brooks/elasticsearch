/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eicf;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.eirf.EirfSchema;
import org.elasticsearch.eirf.EirfType;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceColumn;
import org.elasticsearch.sourcebatch.SourceRow;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Immutable reader for an EICF (Elastic Internal Column Format) batch.
 *
 * <p>Binary layout (32-byte header, all multi-byte integers little-endian):
 * <pre>
 * magic('eicf') version(i32) flags(i32) doc_count(i32)
 * schema_offset(i32) column_index_offset(i32) data_offset(i32) total_size(i32)
 * [Schema]        same binary format as EIRF (non_leaf_count + entries, leaf_count + entries)
 * [Column Index]  per leaf: kind(u8) + data_offset(i32) + data_length(i32)  [= 9 bytes each]
 * [Column Data]   concatenated per-column blobs; layout per {@link EicfColumnKind}
 * </pre>
 */
public final class EicfBatch implements SourceBatch {

    /** Magic as a little-endian int: bytes 'e','i','c','f' read as LE i32. */
    public static final int MAGIC_LE = ('e' & 0xFF) | (('i' & 0xFF) << 8) | (('c' & 0xFF) << 16) | (('f' & 0xFF) << 24);
    public static final int VERSION = 1;

    private final BytesReference data;
    private final Releasable releasable;
    private final int docCount;
    private final EirfSchema schema;
    /** Offset of the column data section (past header + schema + column index). */
    private final int dataOffset;
    /** Column kind per leaf column. */
    private final byte[] columnKinds;
    /** Byte offset within the column data section for each leaf column's blob. */
    private final int[] columnDataOffsets;
    /** Byte length of each leaf column's blob. */
    private final int[] columnDataLengths;

    public EicfBatch(BytesReference data, Releasable releasable) {
        this.data = data;
        this.releasable = releasable;

        int magic = data.getIntLE(0);
        if (magic != MAGIC_LE) {
            throw new IllegalArgumentException(
                "Invalid magic: expected 'eicf', got '"
                    + (char) (magic & 0xFF)
                    + (char) ((magic >> 8) & 0xFF)
                    + (char) ((magic >> 16) & 0xFF)
                    + (char) ((magic >> 24) & 0xFF)
                    + "'"
            );
        }
        int version = data.getIntLE(4);
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported EICF version: " + version);
        }
        this.docCount = data.getIntLE(12);
        int schemaOffset = data.getIntLE(16);
        int columnIndexOffset = data.getIntLE(20);
        this.dataOffset = data.getIntLE(24);

        this.schema = parseSchema(data, schemaOffset);

        int colCount = schema.leafCount();
        this.columnKinds = new byte[colCount];
        this.columnDataOffsets = new int[colCount];
        this.columnDataLengths = new int[colCount];
        for (int c = 0; c < colCount; c++) {
            int entryBase = columnIndexOffset + c * 9;
            columnKinds[c] = data.get(entryBase);
            columnDataOffsets[c] = data.getIntLE(entryBase + 1);
            columnDataLengths[c] = data.getIntLE(entryBase + 5);
        }
    }

    /** Internal constructor used by {@link #slice} to avoid re-parsing. */
    private EicfBatch(
        BytesReference data,
        Releasable releasable,
        EirfSchema schema,
        int docCount,
        int dataOffset,
        byte[] columnKinds,
        int[] columnDataOffsets,
        int[] columnDataLengths
    ) {
        this.data = data;
        this.releasable = releasable;
        this.schema = schema;
        this.docCount = docCount;
        this.dataOffset = dataOffset;
        this.columnKinds = columnKinds;
        this.columnDataOffsets = columnDataOffsets;
        this.columnDataLengths = columnDataLengths;
    }

    // -------------------------------------------------------------------------
    // SourceBatch API
    // -------------------------------------------------------------------------

    @Override
    public int docCount() {
        return docCount;
    }

    @Override
    public EirfSchema schema() {
        return schema;
    }

    @Override
    public BytesReference data() {
        return data;
    }

    @Override
    public int columnCount() {
        return schema.leafCount();
    }

    @Override
    public SourceRow row(int docIndex) {
        if (docIndex < 0 || docIndex >= docCount) {
            throw new IndexOutOfBoundsException("docIndex " + docIndex + " out of range [0, " + docCount + ")");
        }
        return new EicfRow(this, docIndex);
    }

    @Override
    public SourceColumn column(int columnIndex) {
        int colCount = columnCount();
        if (columnIndex < 0 || columnIndex >= colCount) {
            throw new IndexOutOfBoundsException("columnIndex " + columnIndex + " out of range [0, " + colCount + ")");
        }
        BytesReference colData = data.slice(dataOffset + columnDataOffsets[columnIndex], columnDataLengths[columnIndex]);
        return new EicfColumn(columnIndex, columnKinds[columnIndex], colData, docCount);
    }

    /**
     * Returns a view of this batch covering rows {@code [from, to)}.
     *
     * <p>The slice is built by extracting the relevant sub-range from each column blob and
     * assembling a new EICF batch. The returned batch holds no ownership of the parent's
     * buffers; its {@link #close()} is a no-op.
     */
    @Override
    public SourceBatch slice(int from, int to) {
        if (from < 0 || to > docCount || from > to) {
            throw new IndexOutOfBoundsException("slice [" + from + ", " + to + ") out of [0, " + docCount + ")");
        }
        int newDocCount = to - from;
        if (from == 0 && newDocCount == docCount) {
            // Full-range view: share bytes, no-op close
            return new EicfBatch(data, () -> {}, schema, docCount, dataOffset, columnKinds, columnDataOffsets, columnDataLengths);
        }
        if (newDocCount == 0) {
            // Build an empty batch
            BytesReference emptyBytes = EicfEncoder.buildBatchBytes(
                schema,
                0,
                new byte[columnKinds.length],
                emptyBlobs(columnKinds.length)
            );
            return new EicfBatch(emptyBytes, () -> {});
        }
        int colCount = columnKinds.length;
        byte[] newKinds = Arrays.copyOf(columnKinds, colCount);
        byte[][] newBlobs = new byte[colCount][];
        for (int c = 0; c < colCount; c++) {
            BytesReference colData = data.slice(dataOffset + columnDataOffsets[c], columnDataLengths[c]);
            newBlobs[c] = sliceColumnBlob(columnKinds[c], colData, docCount, from, newDocCount);
        }
        BytesReference slicedBytes = EicfEncoder.buildBatchBytes(schema, newDocCount, newKinds, newBlobs);
        return new EicfBatch(slicedBytes, () -> {});
    }

    @Override
    public void close() {
        releasable.close();
    }

    @Override
    public long ramBytesUsed() {
        return data.length() + 64L;
    }

    // -------------------------------------------------------------------------
    // Schema parsing (identical to EIRF schema format)
    // -------------------------------------------------------------------------

    private static EirfSchema parseSchema(BytesReference data, int offset) {
        int nonLeafCount = readU16LE(data, offset);
        offset += 2;
        List<String> nonLeafNames = new ArrayList<>(nonLeafCount);
        int[] nonLeafParents = new int[nonLeafCount];
        for (int i = 0; i < nonLeafCount; i++) {
            nonLeafParents[i] = readU16LE(data, offset);
            offset += 2;
            int nameLen = readU16LE(data, offset);
            offset += 2;
            if (nameLen > 0) {
                var ref = data.slice(offset, nameLen).toBytesRef();
                nonLeafNames.add(new String(ref.bytes, ref.offset, ref.length, StandardCharsets.UTF_8));
            } else {
                nonLeafNames.add("");
            }
            offset += nameLen;
        }
        int leafCount = readU16LE(data, offset);
        offset += 2;
        List<String> leafNames = new ArrayList<>(leafCount);
        int[] leafParents = new int[leafCount];
        for (int i = 0; i < leafCount; i++) {
            leafParents[i] = readU16LE(data, offset);
            offset += 2;
            int nameLen = readU16LE(data, offset);
            offset += 2;
            var ref = data.slice(offset, nameLen).toBytesRef();
            leafNames.add(new String(ref.bytes, ref.offset, ref.length, StandardCharsets.UTF_8));
            offset += nameLen;
        }
        return new EirfSchema(nonLeafNames, nonLeafParents, leafNames, leafParents);
    }

    private static int readU16LE(BytesReference data, int offset) {
        return (data.get(offset) & 0xFF) | ((data.get(offset + 1) & 0xFF) << 8);
    }

    // -------------------------------------------------------------------------
    // Slice helpers
    // -------------------------------------------------------------------------

    private static byte[][] emptyBlobs(int colCount) {
        byte[][] blobs = new byte[colCount][];
        Arrays.fill(blobs, new byte[0]);
        return blobs;
    }

    /**
     * Extracts rows {@code [from, from+newDocCount)} from {@code colData} and returns a new
     * column blob for the sub-range. The layout is identical to the original but sized for
     * {@code newDocCount} documents.
     */
    static byte[] sliceColumnBlob(byte kind, BytesReference colData, int totalDocCount, int from, int newDocCount) {
        return switch (kind) {
            case EicfColumnKind.LONG, EicfColumnKind.DOUBLE -> sliceLongOrDoubleBlob(colData, totalDocCount, from, newDocCount);
            case EicfColumnKind.BOOL -> sliceBoolBlob(colData, totalDocCount, from, newDocCount);
            case EicfColumnKind.STRING, EicfColumnKind.BINARY -> sliceStringOrBinaryBlob(colData, totalDocCount, from, newDocCount);
            case EicfColumnKind.ARRAY -> sliceArrayBlob(colData, totalDocCount, from, newDocCount);
            case EicfColumnKind.NUMERIC_UNION -> sliceNumericUnionBlob(colData, totalDocCount, from, newDocCount);
            case EicfColumnKind.UNION -> sliceUnionBlob(colData, totalDocCount, from, newDocCount);
            default -> throw new IllegalStateException("Unknown column kind: " + EicfColumnKind.name(kind));
        };
    }

    /** LONG / DOUBLE: {@code absent_bitset | values[totalDocCount * 8]} → slice. */
    private static byte[] sliceLongOrDoubleBlob(BytesReference src, int total, int from, int count) {
        int srcBsBytes = EicfColumnBuilder.bitsetBytes(total);
        int dstBsBytes = EicfColumnBuilder.bitsetBytes(count);
        byte[] out = new byte[dstBsBytes + count * 8];
        for (int i = 0; i < count; i++) {
            if (isBitSetAt(src, 0, from + i)) {
                EicfColumnBuilder.setBit(out, 0, i);
            } else {
                long val = src.getLongLE(srcBsBytes + (from + i) * 8);
                ByteUtils.writeLongLE(val, out, dstBsBytes + i * 8);
            }
        }
        return out;
    }

    /** BOOL: {@code absent_bitset | value_bitset} → slice. */
    private static byte[] sliceBoolBlob(BytesReference src, int total, int from, int count) {
        int srcBsBytes = EicfColumnBuilder.bitsetBytes(total);
        int dstBsBytes = EicfColumnBuilder.bitsetBytes(count);
        byte[] out = new byte[2 * dstBsBytes];
        for (int i = 0; i < count; i++) {
            if (isBitSetAt(src, 0, from + i)) {
                EicfColumnBuilder.setBit(out, 0, i);
            }
            if (isBitSetAt(src, srcBsBytes, from + i)) {
                EicfColumnBuilder.setBit(out, dstBsBytes, i);
            }
        }
        return out;
    }

    /** STRING / BINARY: {@code absent_bitset | offsets[(total+1)*4] | bytes} → slice. */
    private static byte[] sliceStringOrBinaryBlob(BytesReference src, int total, int from, int count) {
        int srcBsBytes = EicfColumnBuilder.bitsetBytes(total);
        int dstBsBytes = EicfColumnBuilder.bitsetBytes(count);
        int srcOffsetsStart = srcBsBytes;
        // Data range for [from, from+count)
        int srcDataStart = srcBsBytes + (total + 1) * 4;
        int byteFrom = src.getIntLE(srcOffsetsStart + from * 4);
        int byteTo = src.getIntLE(srcOffsetsStart + (from + count) * 4);
        int dataLen = byteTo - byteFrom;

        int dstOffsetsSize = (count + 1) * 4;
        byte[] out = new byte[dstBsBytes + dstOffsetsSize + dataLen];
        int cumOffset = 0;
        ByteUtils.writeIntLE(0, out, dstBsBytes);
        int writePos = dstBsBytes + dstOffsetsSize;
        for (int i = 0; i < count; i++) {
            if (isBitSetAt(src, 0, from + i)) {
                EicfColumnBuilder.setBit(out, 0, i);
                // no bytes added; offset stays the same
            } else {
                int off0 = src.getIntLE(srcOffsetsStart + (from + i) * 4);
                int off1 = src.getIntLE(srcOffsetsStart + (from + i + 1) * 4);
                int len = off1 - off0;
                if (len > 0) {
                    var ref = src.slice(srcDataStart + off0, len).toBytesRef();
                    System.arraycopy(ref.bytes, ref.offset, out, writePos, len);
                    writePos += len;
                    cumOffset += len;
                }
            }
            ByteUtils.writeIntLE(cumOffset, out, dstBsBytes + (i + 1) * 4);
        }
        return out;
    }

    /** ARRAY: {@code absent_bitset | typeVec[total] | offsets[(total+1)*4] | packed} → slice. */
    private static byte[] sliceArrayBlob(BytesReference src, int total, int from, int count) {
        int srcBsBytes = EicfColumnBuilder.bitsetBytes(total);
        int dstBsBytes = EicfColumnBuilder.bitsetBytes(count);
        int srcTypeVecOffset = srcBsBytes;
        int srcOffsetsStart = srcBsBytes + total;
        int srcPackedStart = srcOffsetsStart + (total + 1) * 4;

        int byteFrom = src.getIntLE(srcOffsetsStart + from * 4);
        int byteTo = src.getIntLE(srcOffsetsStart + (from + count) * 4);
        int dataLen = byteTo - byteFrom;

        int dstTypeVecSize = count;
        int dstOffsetsSize = (count + 1) * 4;
        byte[] out = new byte[dstBsBytes + dstTypeVecSize + dstOffsetsSize + dataLen];
        int dstTypeVecOffset = dstBsBytes;
        int dstOffsetsStart = dstTypeVecOffset + dstTypeVecSize;
        int cumOffset = 0;
        int writePos = dstOffsetsStart + dstOffsetsSize;
        ByteUtils.writeIntLE(0, out, dstOffsetsStart);
        for (int i = 0; i < count; i++) {
            if (isBitSetAt(src, 0, from + i)) {
                EicfColumnBuilder.setBit(out, 0, i);
                // typeVec[i] stays 0
            } else {
                out[dstTypeVecOffset + i] = src.get(srcTypeVecOffset + from + i);
                int off0 = src.getIntLE(srcOffsetsStart + (from + i) * 4);
                int off1 = src.getIntLE(srcOffsetsStart + (from + i + 1) * 4);
                int len = off1 - off0;
                if (len > 0) {
                    var ref = src.slice(srcPackedStart + off0, len).toBytesRef();
                    System.arraycopy(ref.bytes, ref.offset, out, writePos, len);
                    writePos += len;
                    cumOffset += len;
                }
            }
            ByteUtils.writeIntLE(cumOffset, out, dstOffsetsStart + (i + 1) * 4);
        }
        return out;
    }

    /** NUMERIC_UNION: {@code absent_bitset | isDecimal_bitset | values[total*8]} → slice. */
    private static byte[] sliceNumericUnionBlob(BytesReference src, int total, int from, int count) {
        int srcBsBytes = EicfColumnBuilder.bitsetBytes(total);
        int dstBsBytes = EicfColumnBuilder.bitsetBytes(count);
        byte[] out = new byte[2 * dstBsBytes + count * 8];
        for (int i = 0; i < count; i++) {
            if (isBitSetAt(src, 0, from + i)) {
                EicfColumnBuilder.setBit(out, 0, i);
            } else {
                if (isBitSetAt(src, srcBsBytes, from + i)) {
                    EicfColumnBuilder.setBit(out, dstBsBytes, i);
                }
                long val = src.getLongLE(2 * srcBsBytes + (from + i) * 8);
                ByteUtils.writeLongLE(val, out, 2 * dstBsBytes + i * 8);
            }
        }
        return out;
    }

    /** UNION: {@code absent_bitset | typeVec[total] | offsets[(total+1)*4] | dense} → slice. */
    private static byte[] sliceUnionBlob(BytesReference src, int total, int from, int count) {
        int srcBsBytes = EicfColumnBuilder.bitsetBytes(total);
        int dstBsBytes = EicfColumnBuilder.bitsetBytes(count);
        int srcTypeVecOffset = srcBsBytes;
        int srcOffsetsStart = srcBsBytes + total;
        int srcDenseStart = srcOffsetsStart + (total + 1) * 4;

        int byteFrom = src.getIntLE(srcOffsetsStart + from * 4);
        int byteTo = src.getIntLE(srcOffsetsStart + (from + count) * 4);
        int dataLen = byteTo - byteFrom;

        int dstTypeVecSize = count;
        int dstOffsetsSize = (count + 1) * 4;
        byte[] out = new byte[dstBsBytes + dstTypeVecSize + dstOffsetsSize + dataLen];
        int dstTypeVecOffset = dstBsBytes;
        int dstOffsetsStart = dstTypeVecOffset + dstTypeVecSize;
        int cumOffset = 0;
        int writePos = dstOffsetsStart + dstOffsetsSize;
        ByteUtils.writeIntLE(0, out, dstOffsetsStart);
        for (int i = 0; i < count; i++) {
            byte t = src.get(srcTypeVecOffset + from + i);
            out[dstTypeVecOffset + i] = t;
            if (t == EirfType.ABSENT) {
                EicfColumnBuilder.setBit(out, 0, i);
            }
            int off0 = src.getIntLE(srcOffsetsStart + (from + i) * 4);
            int off1 = src.getIntLE(srcOffsetsStart + (from + i + 1) * 4);
            int len = off1 - off0;
            if (len > 0) {
                var ref = src.slice(srcDenseStart + off0, len).toBytesRef();
                System.arraycopy(ref.bytes, ref.offset, out, writePos, len);
                writePos += len;
                cumOffset += len;
            }
            ByteUtils.writeIntLE(cumOffset, out, dstOffsetsStart + (i + 1) * 4);
        }
        return out;
    }

    /**
     * Returns true if bit {@code d} is set in the bitset stored at {@code bitsetOffset} in
     * {@code src}. Layout: LE longs, bit {@code d} at word {@code d/64}, position {@code d%64}.
     */
    static boolean isBitSetAt(BytesReference src, int bitsetOffset, int d) {
        long word = src.getLongLE(bitsetOffset + (d / 64) * 8);
        return ((word >>> (d & 63)) & 1L) != 0;
    }
}
