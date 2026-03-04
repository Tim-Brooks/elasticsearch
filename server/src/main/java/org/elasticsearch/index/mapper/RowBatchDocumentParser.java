/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.bulk.DocBatchRowReader;
import org.elasticsearch.action.bulk.DocBatchSchema;
import org.elasticsearch.action.bulk.RowDocumentBatch;
import org.elasticsearch.action.bulk.RowType;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Parses a {@link RowDocumentBatch} (row-oriented binary format) into a list of {@link ParsedDocument}s.
 * <p>
 * Unlike {@link BatchDocumentParser} (which iterates columns across all docs), this iterates
 * doc-by-doc, field-by-field, aligning with Elasticsearch's document-centric pipeline.
 * <p>
 * The algorithm per document:
 * <ol>
 *   <li>Create a {@link SourceToParse} from {@link IndexRequest} metadata</li>
 *   <li>Create a {@link BatchDocumentParser.BatchDocumentParserContext}</li>
 *   <li>Run metadata preParse</li>
 *   <li>Get {@link DocBatchRowReader} for the row</li>
 *   <li>For each non-null field: resolve mapper, create parser, call fieldMapper.parse()</li>
 *   <li>Run metadata postParse</li>
 *   <li>Build {@link ParsedDocument}</li>
 * </ol>
 */
public final class RowBatchDocumentParser {

    private final XContentParserConfiguration parserConfiguration;
    private final MappingParserContext mappingParserContext;

    public RowBatchDocumentParser(XContentParserConfiguration parserConfiguration, MappingParserContext mappingParserContext) {
        this.parserConfiguration = parserConfiguration;
        this.mappingParserContext = mappingParserContext;
    }

    /**
     * Parse all documents in a row batch into parsed documents.
     *
     * @param rowBatch      the row-oriented document batch
     * @param indexRequests  the index requests (for metadata: id, routing, xContentType, tsid)
     * @param mappingLookup the current mapping lookup
     * @return a result containing parsed documents and per-document exceptions
     */
    public BatchDocumentParser.BatchResult parseRowBatch(
        RowDocumentBatch rowBatch,
        List<IndexRequest> indexRequests,
        MappingLookup mappingLookup
    ) {
        final int docCount = rowBatch.docCount();
        final DocBatchSchema schema = rowBatch.schema();
        final MetadataFieldMapper[] metadataFieldMappers = mappingLookup.getMapping().getSortedMetadataMappers();

        // Pre-compute shared collections (same as BatchDocumentParser)
        final Set<String> sharedCopyToFields = mappingLookup.fieldTypesLookup().getCopyToDestinationFields();
        final Map<String, List<Mapper.Builder>> sharedDynamicMappers = new HashMap<>();
        final Map<String, ObjectMapper.Builder> sharedDynamicObjectMappers = new HashMap<>();
        final Map<String, List<RuntimeField>> sharedDynamicRuntimeFields = new HashMap<>();

        final SourceToParse[] sources = new SourceToParse[docCount];
        final BatchDocumentParser.BatchDocumentParserContext[] contexts = new BatchDocumentParser.BatchDocumentParserContext[docCount];
        final ParsedDocument[] results = new ParsedDocument[docCount];
        final Exception[] exceptions = new Exception[docCount];

        // Pre-resolve mappers once per column and validate strict dynamic mappings upfront
        final Mapper[] columnMappers = new Mapper[schema.columnCount()];
        final String[][] columnParentSegments = new String[schema.columnCount()][];
        for (int col = 0; col < schema.columnCount(); col++) {
            String fieldName = schema.getColumnName(col);
            columnMappers[col] = BatchDocumentParser.resolveMapper(fieldName, mappingLookup);
            columnParentSegments[col] = computeParentSegments(fieldName);
            if (columnMappers[col] == null && mappingLookup.getFieldType(fieldName) == null) {
                // Field has no mapper and is not a runtime field — check strict dynamic
                ObjectMapper.Dynamic dynamic = BatchDocumentParser.getEffectiveDynamic(fieldName, mappingLookup);
                if (dynamic == ObjectMapper.Dynamic.STRICT) {
                    int lastDot = fieldName.lastIndexOf('.');
                    String parentPath = lastDot > 0 ? fieldName.substring(0, lastDot) : "";
                    String leafName = lastDot > 0 ? fieldName.substring(lastDot + 1) : fieldName;
                    throw new StrictDynamicMappingException(XContentLocation.UNKNOWN, parentPath, leafName);
                }
            }
        }

        for (int i = 0; i < docCount; i++) {
            try {
                // Step 1: Create SourceToParse from IndexRequest metadata
                IndexRequest indexRequest = indexRequests.get(i);
                XContentType xContentType = indexRequest.getContentType();
                if (xContentType == null) {
                    xContentType = XContentType.JSON;
                }
                sources[i] = new SourceToParse(
                    indexRequest.id(),
                    BytesArray.EMPTY,
                    xContentType,
                    indexRequest.routing(),
                    Map.of(),
                    indexRequest.tsid()
                );

                // Step 2: Create context
                contexts[i] = new BatchDocumentParser.BatchDocumentParserContext(
                    mappingLookup,
                    mappingParserContext,
                    sources[i],
                    sharedCopyToFields,
                    sharedDynamicMappers,
                    sharedDynamicObjectMappers,
                    sharedDynamicRuntimeFields
                );

                // Step 3: metadata preParse
                for (MetadataFieldMapper metadataMapper : metadataFieldMappers) {
                    metadataMapper.preParse(contexts[i]);
                }

                // Step 4: Get row reader and parse fields
                DocBatchRowReader rowReader = rowBatch.getRowReader(i);
                final int docIdx = i;
                final XContentType docXContentType = xContentType;

                rowReader.forEachField((col, typeByte) -> {
                    if (exceptions[docIdx] != null) return;
                    try {
                        Mapper mapper = columnMappers[col];
                        if (mapper == null) {
                            return; // unmapped field, skip
                        }

                        String[] parentSegments = columnParentSegments[col];
                        byte baseType = RowType.baseType(typeByte);

                        if (mapper instanceof FieldMapper fieldMapper) {
                            parseFieldForDocument(fieldMapper, rowReader, col, baseType, contexts[docIdx], parentSegments, docXContentType);
                        } else if (mapper instanceof ObjectMapper) {
                            if (baseType == RowType.BINARY || baseType == RowType.ARRAY) {
                                parseBinaryObjectForDocument(rowReader, col, contexts[docIdx], mapper, parentSegments, docXContentType);
                            }
                        }
                    } catch (Exception e) {
                        exceptions[docIdx] = e;
                    }
                });

                if (exceptions[i] != null) continue;

                // Step 5: metadata postParse
                for (MetadataFieldMapper metadataMapper : metadataFieldMappers) {
                    metadataMapper.postParse(contexts[i]);
                }

                // Step 6: Build ParsedDocument
                BatchDocumentParser.BatchDocumentParserContext ctx = contexts[i];
                CompressedXContent dynamicUpdate = DocumentParser.createDynamicUpdate(ctx);

                results[i] = new ParsedDocument(
                    ctx.version(),
                    ctx.seqID(),
                    ctx.id(),
                    ctx.routing(),
                    ctx.reorderParentAndGetDocs(),
                    new BytesArray("{\"marker\":true}"),
                    sources[i].getXContentType(),
                    dynamicUpdate,
                    XContentMeteringParserDecorator.UNKNOWN_SIZE
                ) {
                    @Override
                    public String documentDescription() {
                        IdFieldMapper idMapper = (IdFieldMapper) mappingLookup.getMapping().getMetadataMapperByName(IdFieldMapper.NAME);
                        return idMapper.documentDescription(this);
                    }
                };
            } catch (Exception e) {
                exceptions[i] = e;
            }
        }

        return new BatchDocumentParser.BatchResult(results, exceptions);
    }

    private static void parseFieldForDocument(
        FieldMapper fieldMapper,
        DocBatchRowReader rowReader,
        int col,
        byte baseType,
        BatchDocumentParser.BatchDocumentParserContext context,
        String[] parentSegments,
        XContentType xContentType
    ) throws IOException {
        setPathForField(parentSegments, context.path());
        try {
            XContentParser fieldParser;
            if (baseType == RowType.BINARY || baseType == RowType.ARRAY) {
                fieldParser = RowValueXContentParser.forBinary(rowReader, col, xContentType);
            } else {
                fieldParser = RowValueXContentParser.forLeafValue(rowReader, col);
            }

            try {
                fieldParser.nextToken();
                context.setParser(fieldParser);
                fieldMapper.parse(context);
            } finally {
                fieldParser.close();
            }
        } finally {
            resetPath(parentSegments.length, context.path());
        }
    }

    private static void parseBinaryObjectForDocument(
        DocBatchRowReader rowReader,
        int col,
        BatchDocumentParser.BatchDocumentParserContext context,
        Mapper mapper,
        String[] parentSegments,
        XContentType xContentType
    ) throws IOException {
        setPathForField(parentSegments, context.path());
        try {
            XContentParser binaryParser = RowValueXContentParser.forBinary(rowReader, col, xContentType);
            try {
                binaryParser.nextToken();
                DocumentParserContext subContext = context.switchParser(binaryParser);
                if (mapper instanceof ObjectMapper objectMapper) {
                    DocumentParser.parseObjectOrNested(subContext.createChildContext(objectMapper));
                }
            } finally {
                binaryParser.close();
            }
        } finally {
            resetPath(parentSegments.length, context.path());
        }
    }

    private static final String[] EMPTY_SEGMENTS = new String[0];

    private static String[] computeParentSegments(String fieldPath) {
        int lastDot = fieldPath.lastIndexOf('.');
        if (lastDot < 0) return EMPTY_SEGMENTS;
        return fieldPath.substring(0, lastDot).split("\\.");
    }

    private static void setPathForField(String[] parentSegments, ContentPath path) {
        for (String segment : parentSegments) {
            path.add(segment);
        }
    }

    private static void resetPath(int segmentCount, ContentPath path) {
        for (int i = 0; i < segmentCount; i++) {
            path.remove();
        }
    }
}
