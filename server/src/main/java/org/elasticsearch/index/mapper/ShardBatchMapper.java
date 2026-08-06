/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.ShardBatchIndexer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineBatch;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceSchema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Batch-time mapper resolution and columnar batch mapping for the bulk batch-indexing fast path.
 *
 * <p>Workflow:
 * <ol>
 *     <li>{@link #resolveMappers(SourceSchema, MappingLookup, IndexSettings)} runs once per batch. It walks the
 *     schema leaves and binds each column to a {@link FieldMapper} (or records {@code null} for
 *     columns that are silently ignored under a {@code dynamic=false} parent or owned by a column
 *     group). Any configuration outside the v1 support matrix — runtime fields, index-time scripts,
 *     dynamic mapping, unsupported mapper types, etc. — causes the method to return {@code null}, at
 *     which point {@link ShardBatchIndexer} falls back to the sequential path.</li>
 *     <li>{@link #mapColumnBatch(BulkItemRequest[], SourceBatch, IndexShard, int, int, BatchMapperResolution, Engine.Operation.Origin)}
 *     runs per chunk. It invokes each mapper once for the whole chunk — attaching one Lucene column per batch-wide value
 *     (id, source, engine-assigned seq-no/version, ...) via {@link BatchMappingContext}, and assembles {@link Engine.Index} operations
 *     plus the resulting {@link EngineBatch}. Column-group mappers (e.g. {@code flattened}) are invoked once per group after the
 *     per-leaf pass.</li>
 * </ol>
 */
public final class ShardBatchMapper {

    private static final Logger logger = LogManager.getLogger(ShardBatchMapper.class);
    private static final ColumnGroupResolution[] NO_COLUMN_GROUPS = new ColumnGroupResolution[0];

    private ShardBatchMapper() {}

    /**
     * The {@link FieldMapper#resolvesColumnGroup() group mapper} that owns a schema leaf, that
     * mapper's own path, and the leaf's path relative to it.
     */
    public record ColumnGroupMatch(FieldMapper mapper, String ownerPath, String relativeKey) {}

    /**
     * A resolved column group: one {@link FieldMapper#resolvesColumnGroup() group mapper} plus the
     * schema leaves it owns, in schema order. {@code leafIndexes[i]} is the leaf's index into the
     * batch's column array and {@code relativeKeys[i]} is that leaf's path with
     * {@code mapper.fullPath()} and the separating dot stripped.
     *
     * <p>Leaf indexes are stable across chunk slices ({@link EscfBatch#slice} preserves column
     * ordering), so this structure is computed once per batch and reused across all chunks.
     *
     * <p>See the TODO in {@link #resolveMappers} on {@code depth_limit} and relative-key uniqueness.
     */
    public record ColumnGroupResolution(FieldMapper mapper, int[] leafIndexes, String[] relativeKeys) {}

    /**
     * Result of {@link #resolveMappers(SourceSchema, MappingLookup, IndexSettings)}.
     *
     * <p>{@code columnMappers} holds one entry per schema leaf; a {@code null} entry means the
     * column is not mapped by a leaf mapper — either because it is silently ignored under a
     * {@code dynamic=false} parent, or because it is owned by an entry in {@code columnGroups}.
     *
     * <p>{@code columnGroups} is ordered by the first appearance of each group's leaves in the
     * schema, so output column order is deterministic.
     */
    public record BatchMapperResolution(FieldMapper[] columnMappers, ColumnGroupResolution[] columnGroups) {}

    /**
     * Resolve each schema leaf to a {@link FieldMapper}. Returns {@code null} if any scenario
     * falls outside the v1 batch-indexing support matrix and the caller should fall back to the
     * sequential path.
     */
    public static BatchMapperResolution resolveMappers(SourceSchema schema, MappingLookup lookup, IndexSettings indexSettings) {
        // Runtime fields or index-time scripts anywhere in the mapping would require the normal
        // parsing flow; the batch path does not support them.
        if (lookup.getMapping().getRoot().runtimeFields().isEmpty() == false) {
            logger.debug("batch indexing disabled: mapping defines runtime fields");
            return null;
        }
        if (lookup.indexTimeScriptMappers().isEmpty() == false) {
            logger.debug("batch indexing disabled: mapping defines index-time scripts");
            return null;
        }
        if (lookup.getMapping().getMetadataMapperByName(IdFieldMapper.NAME) instanceof SliceIdFieldMapper) {
            logger.debug("batch indexing disabled: slice-enabled index");
            return null;
        }

        for (MetadataFieldMapper mapper : lookup.getMapping().getSortedMetadataMappers()) {
            if (mapper.supportsColumnarMetadataParse(indexSettings) == false) {
                logger.debug(
                    "columnar batch mapping disabled: metadata mapper of type [{}] does not support columnar parsing",
                    mapper.typeName()
                );
                return null;
            }
        }

        final int leafCount = schema.leafCount();
        final FieldMapper[] columnMappers = new FieldMapper[leafCount];
        // Lazily allocated: the overwhelming majority of batches have no group mappers. LinkedHashMap
        // so groups are dispatched in the order their first leaf appears in the schema, making output
        // column order deterministic.
        Map<String, ColumnGroupBuilder> groupsByOwner = null;

        for (int leaf = 0; leaf < leafCount; leaf++) {
            final String fullPath = schema.getFullPath(leaf);
            final Mapper resolved = lookup.getMapper(fullPath);

            if (resolved == null) {
                // TODO: neither depth_limit nor relativeKeys uniqueness is enforced here. SourceSchema
                // keeps the real tree, but getFullPath() flattens it to a dotted string, so a literal
                // key "a.b" and a nested {"a":{"b":..}} under the same owner collapse to the same
                // relative key and land in the group as separate columns. Nothing bounds how deep below
                // the owner a leaf may sit either. The fix is to propagate the schema shape (leaf /
                // non-leaf indices) rather than the dotted path string. Also unhandled: a sibling
                // mapper declared directly at a group key's path (e.g. a flattened field "flat" plus a
                // separate mapper at "flat.k") would be resolved as an ordinary leaf here, whereas the
                // row path folds it into the flattened field. Mirrors the TODO on
                // FlattenedFieldMapper#mapColumnGroupBatch.

                // MUST check group ownership before the runtime-field-shadow check below: a flattened
                // subkey has no mapper of its own, but MappingLookup#getFieldType *does* resolve it
                // (FlattenedFieldType is a DynamicFieldType and FieldTypeLookup#get falls through to
                // getDynamicField, producing a KeyedFlattenedFieldType). Ordered the other way, every
                // flattened batch would be classified as a runtime-field shadow and fall back.
                final ColumnGroupMatch match = findColumnGroup(fullPath, lookup);
                if (match != null) {
                    if (match.mapper().supportsColumnarParse(indexSettings) == false) {
                        logger.debug(
                            "columnar batch mapping disabled: group mapper at [{}] of type [{}] does not support columnar parsing",
                            match.ownerPath(),
                            match.mapper().typeName()
                        );
                        return null;
                    }
                    if (groupsByOwner == null) {
                        groupsByOwner = new LinkedHashMap<>();
                    }
                    groupsByOwner.computeIfAbsent(match.ownerPath(), p -> new ColumnGroupBuilder(match.mapper()))
                        .add(leaf, match.relativeKey());
                    // Owned by the group; the per-leaf loop in mapColumnBatch must skip this entry.
                    columnMappers[leaf] = null;
                    continue;
                }

                // A field type without a mapper indicates a runtime field shadow.
                if (lookup.getFieldType(fullPath) != null) {
                    logger.debug("batch indexing disabled: runtime-field shadow at [{}]", fullPath);
                    return null;
                }
                final ObjectMapper.Dynamic parentDynamic = findNearestParentDynamic(fullPath, lookup);
                if (parentDynamic == ObjectMapper.Dynamic.FALSE) {
                    // TODO: Look into ignored source
                    // leaf silently ignored
                    columnMappers[leaf] = null;
                    continue;
                }
                logger.debug("batch indexing disabled: unmapped leaf [{}] under dynamic={} parent", fullPath, parentDynamic);
                return null;
            }

            if ((resolved instanceof FieldMapper) == false) {
                logger.debug("batch indexing disabled: non-field mapper at [{}]", fullPath);
                return null;
            }
            final FieldMapper fieldMapper = (FieldMapper) resolved;
            if (fieldMapper.supportsColumnarParse(indexSettings) == false) {
                logger.debug(
                    "columnar batch mapping disabled: mapper at [{}] of type [{}] does not support columnar parsing",
                    fullPath,
                    fieldMapper.typeName()
                );
                return null;
            }
            columnMappers[leaf] = fieldMapper;
        }

        final ColumnGroupResolution[] columnGroups;
        if (groupsByOwner == null) {
            columnGroups = NO_COLUMN_GROUPS;
        } else {
            columnGroups = new ColumnGroupResolution[groupsByOwner.size()];
            int g = 0;
            for (ColumnGroupBuilder builder : groupsByOwner.values()) {
                columnGroups[g++] = builder.build();
            }
        }
        return new BatchMapperResolution(columnMappers, columnGroups);
    }

    /**
     * Walks up the dotted ancestors of {@code leafPath}. If the nearest ancestor that has a mapper
     * is a {@link FieldMapper} that {@link FieldMapper#resolvesColumnGroup() resolves a column group},
     * returns that match; otherwise returns {@code null}. A non-group {@link FieldMapper} ancestor
     * cannot own descendant leaves, so the walk stops there.
     */
    @Nullable
    public static ColumnGroupMatch findColumnGroup(String leafPath, MappingLookup lookup) {
        int dot = leafPath.lastIndexOf('.');
        while (dot > 0) {
            final String ancestorPath = leafPath.substring(0, dot);
            final Mapper ancestor = lookup.getMapper(ancestorPath);
            if (ancestor instanceof FieldMapper fieldMapper) {
                return fieldMapper.resolvesColumnGroup()
                    ? new ColumnGroupMatch(fieldMapper, ancestorPath, leafPath.substring(dot + 1))
                    : null;
            }
            dot = leafPath.lastIndexOf('.', dot - 1);
        }
        return null;
    }

    /**
     * Walks up the parent-object chain for {@code leafPath}, returning the effective
     * {@link ObjectMapper.Dynamic} setting of the nearest ancestor that declares one, or the
     * root mapping's setting (defaulting to {@link ObjectMapper.Dynamic#TRUE}) if none do.
     */
    private static ObjectMapper.Dynamic findNearestParentDynamic(String leafPath, MappingLookup lookup) {
        String current = leafPath;
        while (true) {
            final int dot = current.lastIndexOf('.');
            if (dot <= 0) {
                break;
            }
            current = current.substring(0, dot);
            final ObjectMapper parent = lookup.objectMappers().get(current);
            if (parent != null && parent.dynamic() != null) {
                return parent.dynamic();
            }
        }
        // In COLUMNAR mode, objects are flattened (subobjects:DISABLED) and do not appear in
        // objectMappers(). Their dynamic settings are instead stored in prefixProperties on
        // RootObjectMapper. resolveDynamic() consults those when prefixProperties is non-empty,
        // and returns the fallback unchanged when it is empty (non-COLUMNAR path).
        final ObjectMapper.Dynamic rootDynamic = lookup.getMapping().getRoot().dynamic();
        final ObjectMapper.Dynamic rootFallback = rootDynamic == null ? ObjectMapper.Dynamic.TRUE : rootDynamic;
        return lookup.getMapping().getRoot().resolveDynamic(leafPath, rootFallback);
    }

    /**
     * Executes the columnar batch-mapping fast path for one chunk. Returns {@code null} (the
     * fallback signal — same contract as {@link #resolveMappers}) if mapping hits an unexpected
     * exception.
     */
    public static EngineBatch mapColumnBatch(
        BulkItemRequest[] items,
        SourceBatch batch,
        IndexShard shard,
        int chunkStart,
        int chunkEnd,
        BatchMapperResolution resolution,
        Engine.Operation.Origin origin
    ) {
        final MappingLookup mappingLookup = shard.mapperService().mappingLookup();
        final MetadataFieldMapper[] metadataMappers = mappingLookup.getMapping().getSortedMetadataMappers();

        final IndexOperationBatch indexBatch = IndexOperationBatch.initFromBulk(
            items,
            chunkStart,
            chunkEnd,
            batch.slice(chunkStart, chunkEnd),
            origin,
            shard.getOperationPrimaryTerm(),
            shard.getRelativeTimeInNanos()
        );
        final BatchMappingContext context = new BatchMappingContext(indexBatch, mappingLookup, shard.indexSettings());

        try {
            for (MetadataFieldMapper metadataMapper : metadataMappers) {
                metadataMapper.preColumnarParse(context);
            }
            // Invoke field mappers
            final SourceBatch sourceBatch = indexBatch.sourceBatch();
            if (sourceBatch instanceof EscfBatch escfChunk) {
                final FieldMapper[] columnMappers = resolution.columnMappers();
                for (int c = 0; c < columnMappers.length; c++) {
                    final FieldMapper mapper = columnMappers[c];
                    if (mapper != null) {
                        mapper.mapColumnBatch(context, escfChunk.column(c));
                    }
                }
                // Group mappers consume all of their leaves at once, so they run after the per-leaf
                // pass. Column order in MappedColumns is irrelevant (it is an unordered list consumed
                // by the row cursor). An UnsupportedOperationException from mapColumnGroupBatch (e.g.
                // ignore_above exceeded) is caught by the outer catch and triggers the row-path fallback.
                for (ColumnGroupResolution group : resolution.columnGroups()) {
                    final int[] leafIndexes = group.leafIndexes();
                    final EscfColumn[] groupColumns = new EscfColumn[leafIndexes.length];
                    for (int i = 0; i < leafIndexes.length; i++) {
                        groupColumns[i] = escfChunk.column(leafIndexes[i]);
                    }
                    group.mapper().mapColumnGroupBatch(context, groupColumns, group.relativeKeys());
                }
            } else {
                throw new IllegalStateException("unexpected batch mapping - only use escf currently");
            }
            for (MetadataFieldMapper metadataMapper : metadataMappers) {
                metadataMapper.postColumnarParse(context);
            }
        } catch (Exception e) {
            logger.warn("columnar batch mapping failed on [{}], falling back", origin, e);
            return null;
        }

        return new EngineBatch(indexBatch, context.columns());
    }

    /** Accumulates a group mapper's leaf indexes and relative keys during resolution. */
    private static final class ColumnGroupBuilder {
        private final FieldMapper mapper;
        private final List<Integer> leafIndexes = new ArrayList<>();
        private final List<String> relativeKeys = new ArrayList<>();

        ColumnGroupBuilder(FieldMapper mapper) {
            this.mapper = mapper;
        }

        void add(int leafIndex, String relativeKey) {
            leafIndexes.add(leafIndex);
            relativeKeys.add(relativeKey);
        }

        ColumnGroupResolution build() {
            final int[] indexes = new int[leafIndexes.size()];
            for (int i = 0; i < indexes.length; i++) {
                indexes[i] = leafIndexes.get(i);
            }
            return new ColumnGroupResolution(mapper, indexes, relativeKeys.toArray(String[]::new));
        }
    }
}
