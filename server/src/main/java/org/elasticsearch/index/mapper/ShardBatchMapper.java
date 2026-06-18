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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.eirf.EirfRowXContentParser;
import org.elasticsearch.eirf.EirfSchema;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceRow;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;

/**
 * Batch-time mapper resolution and columnar field mapping for the bulk batch-indexing fast path.
 *
 * <p>Workflow:
 * <ol>
 *     <li>{@link #resolveMappers(EirfSchema, MappingLookup)} runs once per batch, binding each schema
 *     leaf to a {@link FieldMapper} (or {@code null} for columns silently ignored under a
 *     {@code dynamic=false} parent). Any configuration outside the support matrix returns {@code null}
 *     and the caller falls back to the sequential path.</li>
 *     <li>{@link #mapColumnBatch} runs per chunk. It builds one
 *     {@link BatchDocumentParserContext} per document, drives every metadata mapper's
 *     {@link MetadataFieldMapper#mapMetadataColumns} and every resolved field mapper's
 *     {@link FieldMapper#mapColumnBatch} to assemble a Lucene {@code ColumnBatch} (attached to the
 *     chunk's {@link SourceBatch} as a {@link org.elasticsearch.sourcebatch.ColumnBatchProvider}), and
 *     returns the per-document {@link Engine.Index} operations.</li>
 * </ol>
 *
 * <p>The columnar path requires the column-major (EICF) format; field mappers cast the
 * {@link org.elasticsearch.sourcebatch.SourceColumn} to an EICF column and throw (triggering
 * fallback) otherwise.
 */
public final class ShardBatchMapper {

    private static final Logger logger = LogManager.getLogger(ShardBatchMapper.class);

    private ShardBatchMapper() {}

    /**
     * Result of {@link #resolveMappers(EirfSchema, MappingLookup)}. Holds one entry per schema
     * leaf; a {@code null} entry means the column is silently ignored because its nearest
     * existing parent {@link ObjectMapper} has {@code dynamic=false}.
     */
    public record BatchMapperResolution(FieldMapper[] columnMappers) {}

    /**
     * Resolve each schema leaf to a {@link FieldMapper}. Returns {@code null} if any scenario
     * falls outside the batch-indexing support matrix and the caller should fall back to the
     * sequential path.
     */
    public static BatchMapperResolution resolveMappers(EirfSchema schema, MappingLookup lookup) {
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

        final int leafCount = schema.leafCount();
        final FieldMapper[] columnMappers = new FieldMapper[leafCount];

        for (int leaf = 0; leaf < leafCount; leaf++) {
            final String fullPath = schema.getFullPath(leaf);
            final Mapper resolved = lookup.getMapper(fullPath);

            if (resolved == null) {
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

            if (fieldMapper.supportsBatchIndexing() == false) {
                logger.debug(
                    "batch indexing disabled: mapper at [{}] of type [{}] does not support batch indexing",
                    fullPath,
                    fieldMapper.typeName()
                );
                return null;
            }

            columnMappers[leaf] = fieldMapper;
        }

        return new BatchMapperResolution(columnMappers);
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
        final ObjectMapper.Dynamic rootDynamic = lookup.getMapping().getRoot().dynamic();
        return rootDynamic == null ? ObjectMapper.Dynamic.TRUE : rootDynamic;
    }

    /**
     * Assembles a Lucene {@code ColumnBatch} for the chunk represented by {@code chunkBatch} (whose
     * local rows {@code [0, docCount)} correspond to {@code items[chunkStart ..]}), attaches it to
     * {@code chunkBatch} as a {@link org.elasticsearch.sourcebatch.ColumnBatchProvider}, and returns
     * the per-document {@link Engine.Index} operations. Returns {@code null} on any unexpected
     * condition so the caller falls back to the sequential path.
     */
    public static List<Engine.Index> mapColumnBatch(
        BulkItemRequest[] items,
        SourceBatch chunkBatch,
        IndexShard primary,
        int chunkStart,
        BatchMapperResolution resolution
    ) {
        final int docCount = chunkBatch.docCount();
        final EirfSchema schema = chunkBatch.schema();
        final MappingLookup mappingLookup = primary.mapperService().mappingLookup();
        final MappingParserContext parserContext = primary.mapperService().parserContext();
        final MetadataFieldMapper[] metadataMappers = mappingLookup.getMapping().getSortedMetadataMappers();
        final FieldMapper[] columnMappers = resolution.columnMappers();
        // The schema tree lets each SourceToParse materialize its source lazily for stored-source columns.
        final EirfRowXContentParser.SchemaNode schemaTree = EirfRowXContentParser.buildSchemaTree(schema);

        final BatchDocumentParserContext[] contexts = new BatchDocumentParserContext[docCount];
        final IndexRequest[] requests = new IndexRequest[docCount];
        final XContentType[] contentTypes = new XContentType[docCount];

        for (int d = 0; d < docCount; d++) {
            final IndexRequest indexRequest = (IndexRequest) items[chunkStart + d].request();
            requests[d] = indexRequest;
            final XContentType xContentType = indexRequest.getContentType() != null ? indexRequest.getContentType() : XContentType.JSON;
            contentTypes[d] = xContentType;
            final SourceRow row = chunkBatch.row(d);
            final SourceToParse sourceToParse = new SourceToParse(
                indexRequest.id(),
                schemaTree,
                row,
                xContentType,
                indexRequest.routing(),
                indexRequest.getDynamicTemplates(),
                indexRequest.getDynamicTemplateParams(),
                indexRequest.getIncludeSourceOnError(),
                XContentMeteringParserDecorator.NOOP,
                indexRequest.tsid()
            );
            contexts[d] = new BatchDocumentParserContext(mappingLookup, parserContext, sourceToParse);
            // The columnar path does not run VersionFieldMapper.preParse; populate the placeholder version
            // field so the carrier ParsedDocument is well-formed (real value is set on the column by the engine).
            contexts[d].version(VersionFieldMapper.versionField());
        }

        final ColumnBatchBuilder builder = new ColumnBatchBuilder(docCount, contexts);
        try {
            for (MetadataFieldMapper metadataMapper : metadataMappers) {
                if (metadataMapper != null) {
                    metadataMapper.mapMetadataColumns(contexts, builder);
                }
            }
            for (int leaf = 0; leaf < columnMappers.length; leaf++) {
                final FieldMapper mapper = columnMappers[leaf];
                if (mapper == null) {
                    continue;
                }
                mapper.mapColumnBatch(chunkBatch.column(leaf), contexts, builder);
            }
        } catch (Exception e) {
            logger.warn("batch indexing on primary failed to assemble column batch, falling back", e);
            return null;
        }

        chunkBatch.setColumnBatchProvider(builder);

        final List<Engine.Index> operations = new ArrayList<>(docCount);
        for (int d = 0; d < docCount; d++) {
            final BatchDocumentParserContext ctx = contexts[d];
            final IndexRequest indexRequest = requests[d];
            final String id = ctx.id();
            final ParsedDocument parsedDoc = new ParsedDocument(
                ctx.version(),
                ctx.seqID(),
                id,
                indexRequest.routing(),
                List.of(ctx.doc()),
                ctx.sourceToParse().source(),
                null,
                XContentMeteringParserDecorator.UNKNOWN_SIZE
            );
            operations.add(
                new Engine.Index(
                    Uid.encodeId(id),
                    parsedDoc,
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    primary.getOperationPrimaryTerm(),
                    indexRequest.version(),
                    indexRequest.versionType(),
                    Engine.Operation.Origin.PRIMARY,
                    primary.getRelativeTimeInNanos(),
                    indexRequest.getAutoGeneratedTimestamp(),
                    indexRequest.isRetry(),
                    indexRequest.ifSeqNo(),
                    indexRequest.ifPrimaryTerm()
                )
            );
        }
        return operations;
    }
}
