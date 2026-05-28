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
import org.elasticsearch.index.Index;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages the per-shard partitioned output buffers for a single encoding session.
 *
 * <p>Receives committed {@link BufferedRow} instances and serializes them into per-({@link Index},
 * shard) {@link RecyclerBytesStreamOutput} instances. Call {@link #commit(BufferedRow, Index, int)}
 * once per document after parsing into the row, then {@link #buildPartition(Index, int)} to
 * produce the final {@link EirfBatch} for each shard.
 *
 * <p>The writer holds a reference to the {@link EirfSchema} it was constructed with so that
 * {@link #buildPartition(Index, int)} can produce the correct schema section in the batch header.
 * The schema is owned externally and expected to grow concurrently with commits.
 *
 * <p>Must be {@link #close() closed} when the encoding session ends to release the underlying
 * byte stream resources.
 */
public final class EirfPartitionWriter implements Releasable {

    private static final int HEADER_SIZE = 32;
    private static final int INITIAL_CAPACITY = 16;
    private static final int INITIAL_PARTITION_CAPACITY = 4;

    private final EirfSchema schema;
    /** Per-concrete-index partition arrays, sparse-by-shard-number. */
    private final Map<Index, Partition[]> partitionsByIndex = new HashMap<>();

    public EirfPartitionWriter(EirfSchema schema) {
        this.schema = schema;
    }

    /**
     * Serializes the current state of {@code row} into the partition identified by
     * ({@code concreteIndex}, {@code shardNum}), returning the row's index within that partition.
     */
    public int commit(BufferedRow row, Index concreteIndex, int shardNum) throws IOException {
        Partition partition = getOrCreatePartition(concreteIndex, shardNum);
        int columnCount = schema.leafCount();
        int rowStart = (int) partition.rowOutput.position();
        partition.ensureRowCapacity();
        partition.rowOffsets[partition.docCount] = rowStart;
        writeRow(partition.rowOutput, columnCount, row);
        partition.rowLengths[partition.docCount] = (int) partition.rowOutput.position() - rowStart;
        int rowIndex = partition.docCount;
        partition.docCount++;
        return rowIndex;
    }

    /**
     * Builds an {@link EirfBatch} for the partition identified by ({@code concreteIndex},
     * {@code shardNum}). Producing a batch consumes that partition's row data; subsequent calls
     * for the same key will produce an empty batch.
     */
    public EirfBatch buildPartition(Index concreteIndex, int shardNum) {
        Partition partition = getOrCreatePartition(concreteIndex, shardNum);
        ReleasableBytesReference rowBytes = partition.rowOutput.moveToBytesReference();
        BytesReference headerBytes = buildHeader(schema, partition.docCount, partition.rowOffsets, partition.rowLengths, rowBytes.length());
        BytesReference combined = CompositeBytesReference.of(headerBytes, rowBytes);
        return new EirfBatch(combined, rowBytes);
    }

    /**
     * Returns the number of rows committed to the partition identified by
     * ({@code concreteIndex}, {@code shardNum}). Returns 0 for partitions never written to.
     */
    public int docCount(Index concreteIndex, int shardNum) {
        Partition[] arr = partitionsByIndex.get(concreteIndex);
        Partition partition = (arr != null && shardNum < arr.length) ? arr[shardNum] : null;
        return partition == null ? 0 : partition.docCount;
    }

    /**
     * Returns true if at least one row has been committed to the partition identified by
     * ({@code concreteIndex}, {@code shardNum}).
     */
    public boolean hasPartition(Index concreteIndex, int shardNum) {
        Partition[] arr = partitionsByIndex.get(concreteIndex);
        Partition partition = (arr != null && shardNum < arr.length) ? arr[shardNum] : null;
        return partition != null && partition.docCount > 0;
    }

    @Override
    public void close() {
        for (Partition[] arr : partitionsByIndex.values()) {
            for (Partition partition : arr) {
                if (partition != null) {
                    partition.rowOutput.close();
                }
            }
        }
        partitionsByIndex.clear();
    }

    private Partition getOrCreatePartition(Index concreteIndex, int shardNum) {
        Partition[] partitions = partitionsByIndex.get(concreteIndex);
        if (partitions == null) {
            int initialCap = Math.max(INITIAL_PARTITION_CAPACITY, Integer.highestOneBit(shardNum) << 1);
            partitions = new Partition[Math.max(INITIAL_PARTITION_CAPACITY, initialCap)];
            partitionsByIndex.put(concreteIndex, partitions);
        }
        if (shardNum >= partitions.length) {
            int newCap = partitions.length;
            while (shardNum >= newCap) {
                newCap <<= 1;
            }
            partitions = Arrays.copyOf(partitions, newCap);
            partitionsByIndex.put(concreteIndex, partitions);
        }
        Partition partition = partitions[shardNum];
        if (partition == null) {
            partition = new Partition(new RecyclerBytesStreamOutput(BytesRefRecycler.NON_RECYCLING_INSTANCE));
            partitions[shardNum] = partition;
        }
        return partition;
    }

    private static final class Partition {
        final RecyclerBytesStreamOutput rowOutput;
        int[] rowOffsets;
        int[] rowLengths;
        int docCount;

        Partition(RecyclerBytesStreamOutput rowOutput) {
            this.rowOutput = rowOutput;
            this.rowOffsets = new int[INITIAL_CAPACITY];
            this.rowLengths = new int[INITIAL_CAPACITY];
            this.docCount = 0;
        }

        void ensureRowCapacity() {
            if (docCount >= rowOffsets.length) {
                int newCap = rowOffsets.length << 1;
                rowOffsets = Arrays.copyOf(rowOffsets, newCap);
                rowLengths = Arrays.copyOf(rowLengths, newCap);
            }
        }
    }

    /**
     * Serializes {@code row} into {@code output}.
     *
     * <p>Row layout: row_flags(u8) | column_count(u16) | var_offset(u16 or i32) | type_bytes | fixed_section | var_section
     */
    static void writeRow(RecyclerBytesStreamOutput output, int columnCount, BufferedRow row) throws IOException {
        byte[] typeBytes = row.typeBytes;
        byte[] fixedData = row.fixedData;
        Object[] varData = row.varData;

        boolean smallRow = row.totalVarSize <= EirfType.SMALL_ROW_MAX_VAR_SIZE;
        int fixedSectionSize = row.scalarFixedSize + row.varColumnCount * (smallRow ? 4 : 8);

        // row_flags(1) + column_count(2) + var_offset(2 or 4) + type_bytes(columnCount) + fixed_section
        int varOffsetFieldSize = smallRow ? 2 : 4;
        int varOffset = 1 + 2 + varOffsetFieldSize + columnCount + fixedSectionSize;

        // Write row_flags (u8): bit 0 = small_row
        output.writeByte(smallRow ? (byte) 0x01 : (byte) 0x00);

        // Write column_count as u16 LE
        writeShortLE(output, columnCount);

        // Write var_offset
        if (smallRow) {
            writeShortLE(output, varOffset);
        } else {
            output.writeIntLE(varOffset);
        }

        // Write type bytes (type codes are the same regardless of row size)
        for (int col = 0; col < columnCount; col++) {
            output.writeByte(typeBytes[col]);
        }

        // Write fixed section
        int varDataOffset = 0;
        for (int col = 0; col < columnCount; col++) {
            byte typeByte = typeBytes[col];
            if (typeByte < EirfType.INT) continue;

            if (typeByte == EirfType.INT || typeByte == EirfType.FLOAT) {
                output.writeBytes(fixedData, col * 8, 4);
            } else if (typeByte == EirfType.LONG || typeByte == EirfType.DOUBLE) {
                output.writeBytes(fixedData, col * 8, 8);
            } else if (EirfType.isVariable(typeByte)) {
                int len = getVarDataLength(typeByte, varData[col]);
                if (smallRow) {
                    // 4-byte entry: u16 offset | u16 length (both LE)
                    writeShortLE(output, varDataOffset);
                    writeShortLE(output, len);
                } else {
                    // 8-byte entry: i32 offset | i32 length (both LE)
                    output.writeIntLE(varDataOffset);
                    output.writeIntLE(len);
                }
                varDataOffset += len;
            }
        }

        // Write var section
        for (int col = 0; col < columnCount; col++) {
            byte typeByte = typeBytes[col];
            if (EirfType.isVariable(typeByte)) {
                writeVarData(output, typeByte, varData[col]);
            }
        }
    }

    static int getVarDataLength(byte typeByte, Object data) {
        if (typeByte == EirfType.STRING) {
            return ((XContentString.UTF8Bytes) data).length();
        } else if (typeByte == EirfType.BINARY) {
            return ((BytesReference) data).length();
        } else if (typeByte == EirfType.UNION_ARRAY || typeByte == EirfType.FIXED_ARRAY || typeByte == EirfType.KEY_VALUE) {
            return ((BytesArray) data).length();
        }
        return 0;
    }

    private static void writeVarData(RecyclerBytesStreamOutput output, byte typeByte, Object data) throws IOException {
        if (typeByte == EirfType.STRING) {
            XContentString.UTF8Bytes str = (XContentString.UTF8Bytes) data;
            output.writeBytes(str.bytes(), str.offset(), str.length());
        } else if (typeByte == EirfType.BINARY) {
            BytesReference ref = (BytesReference) data;
            ref.writeTo(output);
        } else if (typeByte == EirfType.UNION_ARRAY || typeByte == EirfType.FIXED_ARRAY || typeByte == EirfType.KEY_VALUE) {
            BytesArray arr = (BytesArray) data;
            output.writeBytes(arr.array(), arr.arrayOffset(), arr.length());
        }
    }

    static BytesReference buildHeader(EirfSchema schema, int docCount, int[] rowOffsets, int[] rowLengths, int rowDataSize) {
        int nonLeafCount = schema.nonLeafCount();
        int leafCount = schema.leafCount();

        // Compute schema section size (all u16)
        int schemaSize = 2; // non_leaf_count u16
        byte[][] nonLeafNameBytes = new byte[nonLeafCount][];
        for (int i = 0; i < nonLeafCount; i++) {
            nonLeafNameBytes[i] = schema.getNonLeafName(i).getBytes(StandardCharsets.UTF_8);
            schemaSize += 2 + 2 + nonLeafNameBytes[i].length; // parent_index u16 + name_length u16 + name_bytes
        }
        schemaSize += 2; // leaf_count u16
        byte[][] leafNameBytes = new byte[leafCount][];
        for (int i = 0; i < leafCount; i++) {
            leafNameBytes[i] = schema.getLeafName(i).getBytes(StandardCharsets.UTF_8);
            schemaSize += 2 + 2 + leafNameBytes[i].length;
        }

        int docIndexSize = docCount * 8;
        int headerTotal = HEADER_SIZE + schemaSize + docIndexSize;

        byte[] header = new byte[headerTotal];

        int schemaOffset = HEADER_SIZE;
        int docIndexOffset = schemaOffset + schemaSize;
        int dataOffset = headerTotal;
        int totalSize = headerTotal + rowDataSize;

        // Header fields (i32 LE)
        ByteUtils.writeIntLE(EirfBatch.MAGIC_LE, header, 0);
        ByteUtils.writeIntLE(EirfBatch.VERSION, header, 4);
        ByteUtils.writeIntLE(0, header, 8); // flags
        ByteUtils.writeIntLE(docCount, header, 12);
        ByteUtils.writeIntLE(schemaOffset, header, 16);
        ByteUtils.writeIntLE(docIndexOffset, header, 20);
        ByteUtils.writeIntLE(dataOffset, header, 24);
        ByteUtils.writeIntLE(totalSize, header, 28);

        // Schema section: non-leaf fields (u16 LE)
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

        // Schema section: leaf fields (u16 LE)
        writeShortLE(header, pos, leafCount);
        pos += 2;
        for (int i = 0; i < leafCount; i++) {
            writeShortLE(header, pos, schema.getLeafParent(i));
            pos += 2;
            writeShortLE(header, pos, leafNameBytes[i].length);
            pos += 2;
            System.arraycopy(leafNameBytes[i], 0, header, pos, leafNameBytes[i].length);
            pos += leafNameBytes[i].length;
        }

        // Doc index section (i32 LE)
        for (int i = 0; i < docCount; i++) {
            ByteUtils.writeIntLE(rowOffsets[i], header, docIndexOffset + i * 8);
            ByteUtils.writeIntLE(rowLengths[i], header, docIndexOffset + i * 8 + 4);
        }

        return new BytesArray(header);
    }

    private static void writeShortLE(byte[] buf, int offset, int value) {
        buf[offset] = (byte) value;
        buf[offset + 1] = (byte) (value >>> 8);
    }

    private static void writeShortLE(RecyclerBytesStreamOutput output, int value) throws IOException {
        output.writeByte((byte) value);
        output.writeByte((byte) (value >>> 8));
    }
}
