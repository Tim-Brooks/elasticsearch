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
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.eirf.EirfEncoder;
import org.elasticsearch.eirf.EirfSchema;
import org.elasticsearch.eirf.EirfType;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Encodes JSON documents into {@link EicfBatch}es (Elastic Internal Column Format).
 *
 * <p>Unlike {@link EirfEncoder} which stores one blob per document, this encoder accumulates one
 * column per leaf field. Numbers are upcast aggressively: JSON ints and longs both become
 * {@code long}, JSON floats and doubles both become {@code double}. A type conflict (including a
 * long+double mix) or an explicit null promotes the column to {@link EicfColumnKind#UNION}; see
 * {@link EicfColumnBuilder}.
 *
 * <p>Two usage modes are supported, both backed by a shared {@link EirfSchema}:
 *
 * <p><b>Single partition</b> (tests / simple callers):
 * <pre>
 * EicfBatch batch = EicfEncoder.encode(List.of(source1, source2), XContentType.JSON);
 * </pre>
 *
 * <p><b>Multi-partition</b> (single parse pass per document, columns fanned out to one of several
 * destination partitions — typically one per shard — after routing has been decided from the
 * {@link EirfEncoder.LeafSink} fired during the parse):
 * <pre>
 * try (EicfEncoder enc = new EicfEncoder()) {
 *     enc.parseToScratch(source, XContentType.JSON, leafSink);
 *     int rowIndex = enc.commitScratchTo(shardId);
 *     // ...repeat for additional documents...
 *     EicfBatch shardBatch = enc.buildPartition(shardId);
 * }
 * </pre>
 *
 * <p><b>Limitations (prototype):</b> Empty objects ({@code {}}) and {@code KEY_VALUE} leaves are not
 * supported and throw {@link UnsupportedOperationException}. Arrays and objects nested inside arrays
 * are encoded using the existing EIRF array packing codec ({@link EirfEncoder#parseArray}).
 */
public final class EicfEncoder implements Releasable {

    private static final int INITIAL_CAPACITY = 16;
    private static final int INITIAL_PARTITION_CAPACITY = 4;

    private final EirfSchema schema;

    /** Recycler backing the per-column data streams created by every {@link EicfColumnBuilder}. */
    private final Recycler<BytesRef> recycler;

    /** Per-partition column builders (one per leaf) plus that partition's committed doc count. */
    private Partition[] partitions;

    // Per-document scratch, populated by parseToScratch and drained by commitScratchTo. Indexed by leaf
    // column. scratchType holds the EIRF type byte (ABSENT == unset); scratchNumeric holds the long
    // value (INT/LONG) or the raw double bits (FLOAT/DOUBLE); scratchVar holds the string UTF-8 slice
    // (valid only until the next parse) or a packed array byte[].
    private byte[] scratchType;
    private long[] scratchNumeric;
    private Object[] scratchVar;
    /** Tracks which columns have been set in the staged document (duplicate detection). */
    private FixedBitSet columnsSet;
    /** True after {@link #parseToScratch} returns and before {@link #commitScratchTo} is called. */
    private boolean rowStaged;

    /** Cached dotted path per leaf column index, for the {@link EirfEncoder.LeafSink} callbacks. */
    private String[] cachedPath;

    public EicfEncoder() {
        this(BytesRefRecycler.NON_RECYCLING_8K_INSTANCE);
    }

    public EicfEncoder(Recycler<BytesRef> recycler) {
        this.recycler = recycler;
        this.schema = new EirfSchema();
        this.partitions = new Partition[INITIAL_PARTITION_CAPACITY];
        this.scratchType = new byte[INITIAL_CAPACITY];
        this.scratchNumeric = new long[INITIAL_CAPACITY];
        this.scratchVar = new Object[INITIAL_CAPACITY];
        this.columnsSet = new FixedBitSet(Math.max(INITIAL_CAPACITY, 64));
        this.cachedPath = new String[INITIAL_CAPACITY];
    }

    /**
     * Adds a single document to the default partition (0). Equivalent to
     * {@code parseToScratch(source, xContentType, LeafSink.NO_OP); commitScratchTo(0);}.
     */
    public void addDocument(BytesReference source, XContentType xContentType) throws IOException {
        parseToScratch(source, xContentType, EirfEncoder.LeafSink.NO_OP);
        commitScratchTo(0);
    }

    /**
     * Parses {@code source} into the per-document scratch and fires {@code sink} for every primitive
     * leaf value (string / number / boolean — null and array values are not forwarded as primitives;
     * arrays are signalled via {@link EirfEncoder.LeafSink#onArrayLeaf}). The parsed row is held in
     * scratch until the next {@link #commitScratchTo(int)} call.
     *
     * @throws UnsupportedOperationException if the document contains an empty object
     */
    public void parseToScratch(BytesReference source, XContentType xContentType, EirfEncoder.LeafSink sink) throws IOException {
        int columnCountBefore = schema.leafCount();
        Arrays.fill(scratchType, 0, Math.min(columnCountBefore, scratchType.length), (byte) 0);
        Arrays.fill(scratchVar, 0, Math.min(columnCountBefore, scratchVar.length), null);
        columnsSet.clear();
        try (XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, source, xContentType)) {
            parser.allowDuplicateKeys(true);
            parser.nextToken(); // START_OBJECT
            flattenObject(parser, 0, parser.nextToken(), sink);
        }
        rowStaged = true;
    }

    /**
     * Flushes the row staged in scratch into the partition identified by {@code partitionKey},
     * appending one value (or absent) to every leaf column builder. Returns the row's index within
     * that partition.
     *
     * @throws IllegalStateException if no row is currently staged
     */
    public int commitScratchTo(int partitionKey) {
        if (rowStaged == false) {
            throw new IllegalStateException("commitScratchTo called without a staged row");
        }
        final Partition partition = getOrCreatePartition(partitionKey);
        final int leafCount = schema.leafCount();
        ensurePartitionBuilders(partition, leafCount);
        for (int c = 0; c < leafCount; c++) {
            appendScratchValue(partition.builders.get(c), c);
        }
        final int rowIndex = partition.docCount;
        partition.docCount++;
        rowStaged = false;
        return rowIndex;
    }

    /**
     * Builds an {@link EicfBatch} for the partition identified by {@code partitionKey}, consuming that
     * partition's per-column data streams. Must be called at most once per partition.
     */
    public EicfBatch buildPartition(int partitionKey) {
        final Partition partition = getOrCreatePartition(partitionKey);
        final int leafCount = schema.leafCount();
        // Columns may have been appended to the shared schema after this partition's last commit (by a
        // document routed elsewhere); make sure every leaf has a builder back-filled to docCount.
        ensurePartitionBuilders(partition, leafCount);
        final EicfColumnData[] columns = new EicfColumnData[leafCount];
        final List<Releasable> releasables = new ArrayList<>(leafCount);
        for (int c = 0; c < leafCount; c++) {
            final EicfColumnData col = partition.builders.get(c).finish(partition.docCount);
            columns[c] = col;
            if (col.data() instanceof Releasable releasable) {
                releasables.add(releasable);
            }
        }
        return new EicfBatch(schema, partition.docCount, columns, Releasables.wrap(releasables));
    }

    /** Returns the {@link EicfBatch} for the single default partition (0); see {@link #encode}. */
    public EicfBatch build() {
        return buildPartition(0);
    }

    /**
     * Returns the dotted path for the given leaf column, cached so callers can use the column index as
     * a stable per-column key.
     */
    public String columnPath(int columnIndex) {
        if (columnIndex >= cachedPath.length) {
            cachedPath = Arrays.copyOf(cachedPath, Integer.highestOneBit(columnIndex) << 1);
        }
        String path = cachedPath[columnIndex];
        if (path == null) {
            path = schema.getFullPath(columnIndex);
            cachedPath[columnIndex] = path;
        }
        return path;
    }

    @Override
    public void close() {
        // Release any column builder whose bytes were never moved out via buildPartition (e.g. partitions
        // for shards marked non-batchable, or columns left behind when encoding was disabled mid-bulk),
        // returning their pages to the recycler. Builders whose bytes were already moved out are a no-op
        // here (their stream's pages were transferred to the produced EicfBatch).
        for (Partition partition : partitions) {
            if (partition != null) {
                for (EicfColumnBuilder builder : partition.builders) {
                    builder.discard();
                }
            }
        }
        Arrays.fill(partitions, null);
    }

    /** Convenience method: encodes all {@code sources} into a single batch. */
    public static EicfBatch encode(List<BytesReference> sources, XContentType xContentType) throws IOException {
        try (EicfEncoder encoder = new EicfEncoder()) {
            for (BytesReference source : sources) {
                encoder.addDocument(source, xContentType);
            }
            return encoder.buildPartition(0);
        }
    }

    private void appendScratchValue(EicfColumnBuilder builder, int columnIndex) {
        final byte type = scratchType[columnIndex];
        switch (type) {
            case EirfType.ABSENT -> builder.addAbsent();
            case EirfType.NULL -> builder.addNull();
            case EirfType.TRUE -> builder.addBoolean(true);
            case EirfType.FALSE -> builder.addBoolean(false);
            case EirfType.INT, EirfType.LONG -> builder.addLong(scratchNumeric[columnIndex]);
            case EirfType.FLOAT, EirfType.DOUBLE -> builder.addDouble(Double.longBitsToDouble(scratchNumeric[columnIndex]));
            case EirfType.STRING -> builder.addString((XContentString.UTF8Bytes) scratchVar[columnIndex]);
            case EirfType.FIXED_ARRAY, EirfType.UNION_ARRAY -> builder.addArray(type, (byte[]) scratchVar[columnIndex]);
            default -> throw new IllegalStateException("unexpected scratch EIRF type [" + EirfType.name(type) + "]");
        }
    }

    private void flattenObject(XContentParser parser, int parentNonLeafIdx, XContentParser.Token firstToken, EirfEncoder.LeafSink sink)
        throws IOException {
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
                    // Top-level empty objects are not yet supported as standalone KEY_VALUE columns.
                    throw new UnsupportedOperationException(
                        "Empty objects as standalone columns are not yet supported in EICF (field: [" + fieldName + "])"
                    );
                } else {
                    int nonLeafIdx = schema.appendNonLeaf(fieldName, parentNonLeafIdx);
                    flattenObject(parser, nonLeafIdx, inner, sink);
                }
                token = parser.nextToken();
                continue;
            }

            int colIdx = schema.appendLeaf(fieldName, parentNonLeafIdx);
            ensureScratchCapacity(colIdx + 1);
            if (columnsSet.getAndSet(colIdx)) {
                throw new IllegalArgumentException("Duplicate field [" + fieldName + "]");
            }

            final boolean firePathSink = sink != EirfEncoder.LeafSink.NO_OP;
            final boolean rawTextMode = firePathSink && sink.passRawText();
            switch (token) {
                case START_ARRAY -> {
                    EirfEncoder.PackedArray arr = EirfEncoder.parseArray(parser, null);
                    scratchType[colIdx] = arr.arrayType();
                    scratchVar[colIdx] = arr.packed();
                    if (firePathSink) {
                        sink.onArrayLeaf(colIdx, columnPath(colIdx));
                    }
                }
                // The UTF-8 slice points into the parser's reusable buffer; it stays valid until the
                // next parseToScratch, which always follows a commitScratchTo that drains it.
                case VALUE_STRING -> {
                    XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                    scratchType[colIdx] = EirfType.STRING;
                    scratchVar[colIdx] = str;
                    if (firePathSink) {
                        sink.onTextPrimitive(colIdx, columnPath(colIdx), EirfType.STRING, str);
                    }
                }
                case VALUE_NUMBER -> {
                    XContentParser.NumberType numType = parser.numberType();
                    switch (numType) {
                        case INT, LONG -> {
                            long val = parser.longValue();
                            byte type = (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) ? EirfType.INT : EirfType.LONG;
                            scratchType[colIdx] = type;
                            scratchNumeric[colIdx] = val;
                            if (rawTextMode) {
                                sink.onTextPrimitive(colIdx, columnPath(colIdx), type, parser.optimizedText().bytes());
                            } else if (firePathSink) {
                                sink.onLongPrimitive(colIdx, columnPath(colIdx), type, val);
                            }
                        }
                        case FLOAT, DOUBLE -> {
                            double val = parser.doubleValue();
                            float fval = (float) val;
                            byte type = ((double) fval == val) ? EirfType.FLOAT : EirfType.DOUBLE;
                            scratchType[colIdx] = type;
                            scratchNumeric[colIdx] = Double.doubleToRawLongBits(val);
                            if (rawTextMode) {
                                sink.onTextPrimitive(colIdx, columnPath(colIdx), type, parser.optimizedText().bytes());
                            } else if (firePathSink) {
                                sink.onDoublePrimitive(colIdx, columnPath(colIdx), type, val);
                            }
                        }
                        // BIG_INTEGER / BIG_DECIMAL fall back to a string column, matching EirfEncoder.
                        default -> {
                            XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                            scratchType[colIdx] = EirfType.STRING;
                            scratchVar[colIdx] = str;
                            if (firePathSink) {
                                sink.onTextPrimitive(colIdx, columnPath(colIdx), EirfType.STRING, str);
                            }
                        }
                    }
                }
                case VALUE_BOOLEAN -> {
                    boolean v = parser.booleanValue();
                    byte type = v ? EirfType.TRUE : EirfType.FALSE;
                    scratchType[colIdx] = type;
                    if (rawTextMode) {
                        sink.onTextPrimitive(colIdx, columnPath(colIdx), type, parser.optimizedText().bytes());
                    } else if (firePathSink) {
                        sink.onBooleanPrimitive(colIdx, columnPath(colIdx), v);
                    }
                }
                case VALUE_NULL -> scratchType[colIdx] = EirfType.NULL;
                default -> throw new IllegalStateException("Unexpected token: " + token);
            }
            token = parser.nextToken();
        }
    }

    private void ensureScratchCapacity(int size) {
        if (size <= scratchType.length) {
            return;
        }
        int cap = scratchType.length;
        while (cap < size) {
            cap <<= 1;
        }
        scratchType = Arrays.copyOf(scratchType, cap);
        scratchNumeric = Arrays.copyOf(scratchNumeric, cap);
        scratchVar = Arrays.copyOf(scratchVar, cap);
        columnsSet = FixedBitSet.ensureCapacity(columnsSet, cap);
    }

    private Partition getOrCreatePartition(int partitionKey) {
        if (partitionKey >= partitions.length) {
            int newCap = partitions.length;
            while (partitionKey >= newCap) {
                newCap <<= 1;
            }
            partitions = Arrays.copyOf(partitions, newCap);
        }
        Partition partition = partitions[partitionKey];
        if (partition == null) {
            partition = new Partition();
            partitions[partitionKey] = partition;
        }
        return partition;
    }

    /**
     * Ensures {@code partition} has a builder for every leaf in {@code [0, size)}. A newly created
     * builder is back-filled with {@code partition.docCount} absent entries to account for documents
     * committed to this partition before the column first appeared in the schema.
     */
    private void ensurePartitionBuilders(Partition partition, int size) {
        while (partition.builders.size() < size) {
            EicfColumnBuilder builder = new EicfColumnBuilder(recycler);
            for (int i = 0; i < partition.docCount; i++) {
                builder.addAbsent();
            }
            partition.builders.add(builder);
        }
    }

    /** Per-partition column state: one builder per leaf column, plus that partition's committed doc count. */
    private static final class Partition {
        final List<EicfColumnBuilder> builders = new ArrayList<>(INITIAL_CAPACITY);
        int docCount;
    }
}
