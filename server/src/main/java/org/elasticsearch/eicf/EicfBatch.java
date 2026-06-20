/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eicf;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.eirf.EirfSchema;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceColumn;
import org.elasticsearch.sourcebatch.SourceRow;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * An EICF (Elastic Internal Column Format) batch, backed by an array of {@link EicfColumnData}
 * (each a set of up to four field references). A batch has two construction paths that share the
 * same in-memory column representation:
 * <ul>
 *   <li><b>In-memory</b> — built directly by {@link EicfEncoder}; reads go straight to the column
 *       fields and no serialization happens until {@link #data()} is called.</li>
 *   <li><b>Serialized</b> — reconstructed from wire/translog bytes via
 *       {@link #EicfBatch(BytesReference, Releasable)}; the header and per-column index are parsed
 *       and each column's fields are sliced out of the blob.</li>
 * </ul>
 *
 * <p>Serialized binary layout (32-byte header, all multi-byte integers little-endian):
 * <pre>
 * magic('eicf') version(i32) flags(i32) doc_count(i32)
 * schema_offset(i32) column_index_offset(i32) data_offset(i32) total_size(i32)
 * [Schema]        same binary format as EIRF (non_leaf_count + entries, leaf_count + entries)
 * [Column Index]  per leaf: kind(u8) present_flags(u8) base_offset(i32)
 *                 absent_len(i32) typevec_len(i32) offsets_len(i32) data_len(i32)   [= 22 bytes]
 * [Column Data]   per leaf, the present fields concatenated in order:
 *                 [absent_bitset] [type_vector] [offsets] [data]
 * </pre>
 * {@code present_flags} bit 0 = absent bitset present, bit 1 = type vector present, bit 2 = offset
 * vector present; the data field is always present (its length may be 0). {@code base_offset} is
 * the field group's start relative to {@code data_offset}.
 */
public final class EicfBatch implements SourceBatch {

    /** Magic as a little-endian int: bytes 'e','i','c','f' read as LE i32. */
    public static final int MAGIC_LE = ('e' & 0xFF) | (('i' & 0xFF) << 8) | (('c' & 0xFF) << 16) | (('f' & 0xFF) << 24);
    public static final int VERSION = 1;

    private static final int HEADER_SIZE = 32;
    private static final int COLUMN_INDEX_ENTRY_SIZE = 22;

    private static final int FLAG_ABSENT = 0x1;
    private static final int FLAG_TYPE_VECTOR = 0x2;
    private static final int FLAG_OFFSETS = 0x4;

    private final EirfSchema schema;
    private final int docCount;
    private final EicfColumnData[] columns;
    /** Lazily-built, cached typed column views — one slot per leaf column (see {@link #column(int)}). */
    private final EicfColumn[] columnCache;
    private final Releasable releasable;
    /** The serialized blob: the original bytes (serialized path) or a lazily-built cache (in-memory path). */
    private BytesReference serialized;
    /** The Lucene column batch assembled by the columnar bulk-mapping path; null until attached. */
    private org.elasticsearch.sourcebatch.ColumnBatchProvider columnBatchProvider;

    /** In-memory construction path used by {@link EicfEncoder#build()}. */
    EicfBatch(EirfSchema schema, int docCount, EicfColumnData[] columns, Releasable releasable) {
        this.schema = schema;
        this.docCount = docCount;
        this.columns = columns;
        this.columnCache = new EicfColumn[columns.length];
        this.releasable = releasable;
        this.serialized = null;
    }

    /** Serialized construction path: parse a batch from its wire/translog bytes. */
    public EicfBatch(BytesReference data, Releasable releasable) {
        this.releasable = releasable;
        this.serialized = data;

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
        int dataOffset = data.getIntLE(24);

        this.schema = parseSchema(data, schemaOffset);

        int colCount = schema.leafCount();
        this.columns = new EicfColumnData[colCount];
        this.columnCache = new EicfColumn[colCount];
        for (int c = 0; c < colCount; c++) {
            int entryBase = columnIndexOffset + c * COLUMN_INDEX_ENTRY_SIZE;
            byte kind = data.get(entryBase);
            int flags = data.get(entryBase + 1) & 0xFF;
            int base = dataOffset + data.getIntLE(entryBase + 2);
            int absentLen = data.getIntLE(entryBase + 6);
            int typeVecLen = data.getIntLE(entryBase + 10);
            int offsetsLen = data.getIntLE(entryBase + 14);
            int dataLen = data.getIntLE(entryBase + 18);

            int pos = base;
            BytesReference absent = null;
            if ((flags & FLAG_ABSENT) != 0) {
                absent = data.slice(pos, absentLen);
                pos += absentLen;
            }
            BytesReference typeVector = null;
            if ((flags & FLAG_TYPE_VECTOR) != 0) {
                typeVector = data.slice(pos, typeVecLen);
                pos += typeVecLen;
            }
            BytesReference offsets = null;
            if ((flags & FLAG_OFFSETS) != 0) {
                offsets = data.slice(pos, offsetsLen);
                pos += offsetsLen;
            }
            BytesReference colData = data.slice(pos, dataLen);
            columns[c] = new EicfColumnData(kind, docCount, absent, typeVector, offsets, colData);
        }
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
        if (serialized == null) {
            serialized = serialize(schema, docCount, columns);
        }
        return serialized;
    }

    @Override
    public int columnCount() {
        return schema.leafCount();
    }

    @Override
    public org.elasticsearch.sourcebatch.ColumnBatchProvider columnBatchProvider() {
        return columnBatchProvider;
    }

    @Override
    public void setColumnBatchProvider(org.elasticsearch.sourcebatch.ColumnBatchProvider provider) {
        this.columnBatchProvider = provider;
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
        if (columnIndex < 0 || columnIndex >= columns.length) {
            throw new IndexOutOfBoundsException("columnIndex " + columnIndex + " out of range [0, " + columns.length + ")");
        }
        EicfColumn cached = columnCache[columnIndex];
        if (cached != null) {
            return cached;
        }
        EicfColumn built = EicfColumn.from(columnIndex, columns[columnIndex]);
        columnCache[columnIndex] = built;
        return built;
    }

    /**
     * Returns a view of this batch covering rows {@code [from, to)}. A full-range slice shares this
     * batch's columns (no-op close); any other slice copies the relevant field sub-ranges into new
     * columns, so it is independent of this batch's lifetime.
     */
    @Override
    public SourceBatch slice(int from, int to) {
        if (from < 0 || to > docCount || from > to) {
            throw new IndexOutOfBoundsException("slice [" + from + ", " + to + ") out of [0, " + docCount + ")");
        }
        if (from == 0 && to == docCount) {
            return new EicfBatch(schema, docCount, columns, () -> {});
        }
        int newDocCount = to - from;
        EicfColumnData[] newColumns = new EicfColumnData[columns.length];
        for (int c = 0; c < columns.length; c++) {
            newColumns[c] = sliceColumn(columns[c], from, newDocCount);
        }
        return new EicfBatch(schema, newDocCount, newColumns, () -> {});
    }

    @Override
    public void close() {
        releasable.close();
    }

    @Override
    public long ramBytesUsed() {
        if (serialized != null) {
            return serialized.length() + 64L;
        }
        long total = 64L;
        for (EicfColumnData col : columns) {
            total += refLen(col.absentBitset()) + refLen(col.typeVector()) + refLen(col.offsets()) + refLen(col.data());
        }
        return total;
    }

    // -------------------------------------------------------------------------
    // Slicing (field-wise, copying)
    // -------------------------------------------------------------------------

    private static EicfColumnData sliceColumn(EicfColumnData col, int from, int newCount) {
        BytesReference absent = newCount > 0 && col.absentBitset() != null ? copyBitset(col.absentBitset(), from, newCount) : null;
        BytesReference typeVector = col.typeVector() != null ? copyRange(col.typeVector(), from, newCount) : null;

        BytesReference offsets;
        BytesReference data;
        if (col.offsets() != null) {
            int byteFrom = col.offsets().getIntLE(from * 4);
            int byteTo = col.offsets().getIntLE((from + newCount) * 4);
            data = copyRange(col.data(), byteFrom, byteTo - byteFrom);
            byte[] newOffsets = new byte[(newCount + 1) * 4];
            for (int i = 0; i <= newCount; i++) {
                ByteUtils.writeIntLE(col.offsets().getIntLE((from + i) * 4) - byteFrom, newOffsets, i * 4);
            }
            offsets = new BytesArray(newOffsets);
        } else if (col.kind() == EicfColumnKind.BOOL) {
            offsets = null;
            data = copyBitset(col.data(), from, newCount);
        } else {
            // LONG / DOUBLE fixed 8-byte slots
            offsets = null;
            data = copyRange(col.data(), from * 8, newCount * 8);
        }
        return new EicfColumnData(col.kind(), newCount, absent, typeVector, offsets, data);
    }

    /** Copies {@code length} bytes from {@code src} starting at {@code from} into a fresh array. */
    private static BytesReference copyRange(BytesReference src, int from, int length) {
        BytesRef ref = src.slice(from, length).toBytesRef();
        return new BytesArray(Arrays.copyOfRange(ref.bytes, ref.offset, ref.offset + length));
    }

    /** Copies bits {@code [from, from+count)} of the source bitset into a fresh bitset of {@code count} bits. */
    private static BytesReference copyBitset(BytesReference src, int from, int count) {
        byte[] out = new byte[EicfColumnBuilder.bitsetBytes(count)];
        for (int i = 0; i < count; i++) {
            if (EicfColumnBuilder.isBitSet(src, 0, from + i)) {
                EicfColumnBuilder.setBit(out, 0, i);
            }
        }
        return new BytesArray(out);
    }

    // -------------------------------------------------------------------------
    // Serialization (in-memory column fields -> combined blob)
    // -------------------------------------------------------------------------

    private static BytesReference serialize(EirfSchema schema, int docCount, EicfColumnData[] columns) {
        int colCount = schema.leafCount();
        int nonLeafCount = schema.nonLeafCount();

        // --- schema section sizing ---
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

        int columnIndexSize = colCount * COLUMN_INDEX_ENTRY_SIZE;
        int schemaOffset = HEADER_SIZE;
        int columnIndexOffset = schemaOffset + schemaSize;
        int dataOffset = columnIndexOffset + columnIndexSize;

        // --- per-column field layout within the data section ---
        int[] flags = new int[colCount];
        int[] baseOffsets = new int[colCount];
        int cumDataOffset = 0;
        for (int c = 0; c < colCount; c++) {
            EicfColumnData col = columns[c];
            baseOffsets[c] = cumDataOffset;
            int f = 0;
            if (col.absentBitset() != null) {
                f |= FLAG_ABSENT;
                cumDataOffset += col.absentBitset().length();
            }
            if (col.typeVector() != null) {
                f |= FLAG_TYPE_VECTOR;
                cumDataOffset += col.typeVector().length();
            }
            if (col.offsets() != null) {
                f |= FLAG_OFFSETS;
                cumDataOffset += col.offsets().length();
            }
            cumDataOffset += col.data().length();
            flags[c] = f;
        }
        int totalSize = dataOffset + cumDataOffset;

        byte[] header = new byte[dataOffset]; // header + schema + column index

        // Header (i32 LE)
        ByteUtils.writeIntLE(MAGIC_LE, header, 0);
        ByteUtils.writeIntLE(VERSION, header, 4);
        ByteUtils.writeIntLE(0, header, 8); // flags
        ByteUtils.writeIntLE(docCount, header, 12);
        ByteUtils.writeIntLE(schemaOffset, header, 16);
        ByteUtils.writeIntLE(columnIndexOffset, header, 20);
        ByteUtils.writeIntLE(dataOffset, header, 24);
        ByteUtils.writeIntLE(totalSize, header, 28);

        // Schema section (u16 LE) — identical encoding to EIRF
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

        // Column index section
        pos = columnIndexOffset;
        for (int c = 0; c < colCount; c++) {
            EicfColumnData col = columns[c];
            header[pos] = col.kind();
            header[pos + 1] = (byte) flags[c];
            ByteUtils.writeIntLE(baseOffsets[c], header, pos + 2);
            ByteUtils.writeIntLE(col.absentBitset() != null ? col.absentBitset().length() : 0, header, pos + 6);
            ByteUtils.writeIntLE(col.typeVector() != null ? col.typeVector().length() : 0, header, pos + 10);
            ByteUtils.writeIntLE(col.offsets() != null ? col.offsets().length() : 0, header, pos + 14);
            ByteUtils.writeIntLE(col.data().length(), header, pos + 18);
            pos += COLUMN_INDEX_ENTRY_SIZE;
        }

        // Concatenate header/schema/index with each column's present fields (the field bytes are not copied)
        List<BytesReference> parts = new ArrayList<>(1 + colCount * 4);
        parts.add(new BytesArray(header));
        for (int c = 0; c < colCount; c++) {
            EicfColumnData col = columns[c];
            if (col.absentBitset() != null) {
                parts.add(col.absentBitset());
            }
            if (col.typeVector() != null) {
                parts.add(col.typeVector());
            }
            if (col.offsets() != null) {
                parts.add(col.offsets());
            }
            parts.add(col.data());
        }
        return CompositeBytesReference.of(parts.toArray(new BytesReference[0]));
    }

    private static void writeShortLE(byte[] buf, int offset, int value) {
        buf[offset] = (byte) value;
        buf[offset + 1] = (byte) (value >>> 8);
    }

    private static long refLen(BytesReference ref) {
        return ref == null ? 0L : ref.length();
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
}
