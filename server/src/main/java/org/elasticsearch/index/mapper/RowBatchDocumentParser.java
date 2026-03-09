/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.DocBatchRowIterator;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Parses a {@link RowDocumentBatch} (row-oriented binary format) into a list of {@link ParsedDocument}s.
 * <p>
 * The algorithm per document:
 * <ol>
 *   <li>Create a {@link SourceToParse} from {@link IndexRequest} metadata</li>
 *   <li>Create a {@link BatchDocumentParserContext}</li>
 *   <li>Run metadata preParse</li>
 *   <li>Get {@link DocBatchRowReader} for the row</li>
 *   <li>For each non-null field: resolve mapper, create parser, call fieldMapper.parse()</li>
 *   <li>Run metadata postParse</li>
 *   <li>Build {@link ParsedDocument}</li>
 * </ol>
 */
public final class RowBatchDocumentParser {

    private static final Logger logger = LogManager.getLogger(RowBatchDocumentParser.class);

    private final XContentParserConfiguration parserConfiguration;
    private final MappingParserContext mappingParserContext;

    public RowBatchDocumentParser(XContentParserConfiguration parserConfiguration, MappingParserContext mappingParserContext) {
        this.parserConfiguration = parserConfiguration;
        this.mappingParserContext = mappingParserContext;
    }

    /**
     * Resolves a mapper for the given field path. Handles dot-notation paths by looking up
     * the full path directly in the mapping lookup (which stores leaf mappers by full path).
     */
    public static Mapper resolveMapper(String fieldPath, MappingLookup mappingLookup) {
        // First try direct lookup - MappingLookup stores field mappers by full dotted path
        Mapper mapper = mappingLookup.getMapper(fieldPath);
        if (mapper != null) {
            return mapper;
        }
        // Also check object mappers for object-typed columns
        ObjectMapper objectMapper = mappingLookup.objectMappers().get(fieldPath);
        if (objectMapper != null) {
            return objectMapper;
        }
        return null;
    }

    /**
     * Determines whether an unmapped field would require a dynamic mapping update.
     * Returns {@code true} if the field has no mapper and the effective dynamic setting
     * for its location in the mapping hierarchy is {@link ObjectMapper.Dynamic#TRUE} or
     * {@link ObjectMapper.Dynamic#RUNTIME}, meaning the serial path must handle it.
     * Returns {@code false} if the field is already mapped (including as a runtime field),
     * or if it's unmapped but would be ignored ({@code dynamic=false}) or rejected at
     * parse time ({@code dynamic=strict}).
     */
    public static boolean requiresDynamicMapping(String fieldPath, MappingLookup mappingLookup) {
        if (resolveMapper(fieldPath, mappingLookup) != null) {
            return false;
        }
        // Runtime fields are not in the field mappers but are in the field type lookup.
        // An existing runtime field does not require a dynamic mapping update.
        if (mappingLookup.getFieldType(fieldPath) != null) {
            return false;
        }
        ObjectMapper.Dynamic dynamic = getEffectiveDynamic(fieldPath, mappingLookup);
        return dynamic == ObjectMapper.Dynamic.TRUE || dynamic == ObjectMapper.Dynamic.RUNTIME;
    }

    /**
     * Walks the object mapper hierarchy to find the effective dynamic setting for a field path.
     * Checks each parent object from the immediate parent up to the root, returning the first
     * explicitly configured dynamic setting found. Falls back to the root dynamic setting.
     */
    static ObjectMapper.Dynamic getEffectiveDynamic(String fieldPath, MappingLookup mappingLookup) {
        // Walk up from the immediate parent to the root, looking for an explicit dynamic setting
        String parentPath = fieldPath;
        while (true) {
            int lastDot = parentPath.lastIndexOf('.');
            if (lastDot < 0) {
                break;
            }
            parentPath = parentPath.substring(0, lastDot);
            ObjectMapper parentMapper = mappingLookup.objectMappers().get(parentPath);
            if (parentMapper != null && parentMapper.dynamic() != null) {
                return parentMapper.dynamic();
            }
        }
        return ObjectMapper.Dynamic.getRootDynamic(mappingLookup);
    }

    /**
     * Parse all documents in a row batch into parsed documents.
     *
     * @param rowBatch      the row-oriented document batch
     * @param indexRequests  the index requests (for metadata: id, routing, xContentType, tsid)
     * @param mappingLookup the current mapping lookup
     * @return a result containing parsed documents and per-document exceptions
     */
    public BatchResult parseRowBatch(RowDocumentBatch rowBatch, List<IndexRequest> indexRequests, MappingLookup mappingLookup) {
        final int docCount = indexRequests.size();
        final DocBatchSchema schema = rowBatch.schema();
        final MetadataFieldMapper[] metadataFieldMappers = mappingLookup.getMapping().getSortedMetadataMappers();

        // Pre-compute shared collections (same as BatchDocumentParser)
        final Set<String> sharedCopyToFields = mappingLookup.fieldTypesLookup().getCopyToDestinationFields();
        final Map<String, List<Mapper.Builder>> sharedDynamicMappers = new HashMap<>();
        final Map<String, ObjectMapper.Builder> sharedDynamicObjectMappers = new HashMap<>();
        final Map<String, List<RuntimeField>> sharedDynamicRuntimeFields = new HashMap<>();

        final SourceToParse[] sources = new SourceToParse[docCount];
        final BatchDocumentParserContext[] contexts = new BatchDocumentParserContext[docCount];
        final ParsedDocument[] results = new ParsedDocument[docCount];
        final Exception[] exceptions = new Exception[docCount];

        // Pre-resolve mappers once per column and validate strict dynamic mappings upfront
        final Mapper[] columnMappers = new Mapper[schema.columnCount()];
        final String[][] columnParentSegments = new String[schema.columnCount()][];
        for (int col = 0; col < schema.columnCount(); col++) {
            String fieldName = schema.getColumnName(col);
            columnMappers[col] = resolveMapper(fieldName, mappingLookup);
            columnParentSegments[col] = computeParentSegments(fieldName);
            if (columnMappers[col] == null && mappingLookup.getFieldType(fieldName) == null) {
                // Field has no mapper and is not a runtime field — check strict dynamic
                ObjectMapper.Dynamic dynamic = getEffectiveDynamic(fieldName, mappingLookup);
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
                contexts[i] = new BatchDocumentParserContext(
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

                // Step 4: Get row iterator and parse fields
                int rowIndex = indexRequest.batchRowIndex() >= 0 ? indexRequest.batchRowIndex() : i;
                DocBatchRowReader rowReader = rowBatch.getRowReader(rowIndex);
                DocBatchRowIterator rowIterator = rowReader.iterator();

                while (rowIterator.next()) {
                    if (exceptions[i] != null) break;
                    if (rowIterator.isNull()) continue;

                    int col = rowIterator.column();
                    Mapper mapper = columnMappers[col];
                    if (mapper == null) {
                        continue; // unmapped field, skip
                    }

                    try {
                        String[] parentSegments = columnParentSegments[col];
                        byte baseType = rowIterator.baseType();

                        if (mapper instanceof FieldMapper fieldMapper) {
                            parseFieldForDocument(fieldMapper, rowIterator, baseType, contexts[i], parentSegments, xContentType);
                        } else if (mapper instanceof ObjectMapper) {
                            if (baseType == RowType.BINARY || baseType == RowType.XCONTENT_ARRAY) {
                                parseBinaryObjectForDocument(rowIterator, contexts[i], mapper, parentSegments, xContentType);
                            }
                        }
                    } catch (Exception e) {
                        exceptions[i] = e;
                    }
                }

                if (exceptions[i] != null) continue;

                // Step 5: metadata postParse
                for (MetadataFieldMapper metadataMapper : metadataFieldMappers) {
                    metadataMapper.postParse(contexts[i]);
                }

                // Step 6: Build ParsedDocument
                BatchDocumentParserContext ctx = contexts[i];
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

        return new BatchResult(results, exceptions);
    }

    private static void parseFieldForDocument(
        FieldMapper fieldMapper,
        DocBatchRowIterator iterator,
        byte baseType,
        BatchDocumentParserContext context,
        String[] parentSegments,
        XContentType xContentType
    ) throws IOException {
        setPathForField(parentSegments, context.path());
        try {
            if (baseType == RowType.ARRAY) {
                // Small typed array
                parseSmallArrayField(fieldMapper, iterator, context);
            } else if (baseType == RowType.XCONTENT_ARRAY) {
                // Raw x-content array (fallback)
                parseXContentArrayField(fieldMapper, iterator, context, xContentType);
            } else if (baseType == RowType.BINARY) {
                // Binary leaf - needs its own xcontent parser
                XContentParser fieldParser = RowValueXContentParser.forBinary(iterator, xContentType);
                try {
                    fieldParser.nextToken();
                    context.setParser(fieldParser);
                    fieldMapper.parse(context);
                } finally {
                    fieldParser.close();
                }
            } else {
                // Scalar leaf - use the iterator directly as the parser (no allocation)
                iterator.resetParser();
                iterator.nextToken();
                context.setParser(iterator);
                fieldMapper.parse(context);
            }
        } finally {
            resetPath(parentSegments.length, context.path());
        }
    }

    private static void parseSmallArrayField(FieldMapper fieldMapper, DocBatchRowIterator iterator, BatchDocumentParserContext context)
        throws IOException {
        try (XContentParser arrayParser = RowValueXContentParser.forSmallArray(iterator)) {
            parseArrayElements(fieldMapper, arrayParser, context);
        }
    }

    private static void parseXContentArrayField(
        FieldMapper fieldMapper,
        DocBatchRowIterator iterator,
        BatchDocumentParserContext context,
        XContentType xContentType
    ) throws IOException {
        XContentParser arrayParser = RowValueXContentParser.forBinary(iterator, xContentType);
        try {
            parseArrayElements(fieldMapper, arrayParser, context);
        } finally {
            arrayParser.close();
        }
    }

    private static void parseArrayElements(FieldMapper fieldMapper, XContentParser arrayParser, BatchDocumentParserContext context)
        throws IOException {
        if (fieldMapper.parsesArrayValue()) {
            arrayParser.nextToken(); // START_ARRAY
            context.setParser(arrayParser);
            fieldMapper.parse(context);
        } else {
            XContentParser.Token startToken = arrayParser.nextToken();
            assert startToken == XContentParser.Token.START_ARRAY;
            while (arrayParser.nextToken() != XContentParser.Token.END_ARRAY) {
                context.setParser(arrayParser);
                fieldMapper.parse(context);
            }
        }
    }

    private static void parseBinaryObjectForDocument(
        DocBatchRowIterator iterator,
        BatchDocumentParserContext context,
        Mapper mapper,
        String[] parentSegments,
        XContentType xContentType
    ) throws IOException {
        setPathForField(parentSegments, context.path());
        try {
            XContentParser binaryParser = RowValueXContentParser.forBinary(iterator, xContentType);
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

    /**
     * Result of parsing a batch of documents.
     *
     * @param documents  array of parsed documents; null entries indicate failures
     * @param exceptions array of exceptions; null entries indicate success
     */
    public record BatchResult(ParsedDocument[] documents, Exception[] exceptions) {

        /**
         * Returns the number of documents in the batch.
         */
        public int size() {
            return documents.length;
        }

        /**
         * Returns the parsed document at the given index, or null if parsing failed.
         */
        public ParsedDocument getDocument(int index) {
            return documents[index];
        }

        /**
         * Returns the exception for the given index, or null if parsing succeeded.
         */
        public Exception getException(int index) {
            return exceptions[index];
        }

        /**
         * Returns true if parsing succeeded for the given document index.
         */
        public boolean isSuccess(int index) {
            return exceptions[index] == null;
        }

        /**
         * Returns all successfully parsed documents.
         */
        public List<ParsedDocument> successfulDocuments() {
            List<ParsedDocument> result = new ArrayList<>();
            for (ParsedDocument document : documents) {
                if (document != null) {
                    result.add(document);
                }
            }
            return result;
        }
    }

    /**
     * A DocumentParserContext implementation for batch parsing.
     * Unlike the standard RootDocumentParserContext (which is a private inner class of DocumentParser),
     * this provides a mutable parser reference so that different column parsers can be swapped in
     * for each field being parsed.
     */
    static final class BatchDocumentParserContext extends DocumentParserContext {
        private final ContentPath path = new ContentPath();
        private XContentParser parser;
        private final LuceneDocument document;
        private final List<LuceneDocument> documents = new ArrayList<>();
        private final long maxAllowedNumNestedDocs;
        private long numNestedDocs;
        private boolean docsReversed = false;

        BatchDocumentParserContext(
            MappingLookup mappingLookup,
            MappingParserContext mappingParserContext,
            SourceToParse source,
            Set<String> copyToFields,
            Map<String, List<Mapper.Builder>> dynamicMappers,
            Map<String, ObjectMapper.Builder> dynamicObjectMappers,
            Map<String, List<RuntimeField>> dynamicRuntimeFields
        ) {
            super(
                mappingLookup,
                mappingParserContext,
                source,
                mappingLookup.getMapping().getRoot(),
                ObjectMapper.Dynamic.getRootDynamic(mappingLookup),
                copyToFields,
                dynamicMappers,
                dynamicObjectMappers,
                dynamicRuntimeFields
            );
            // Create a no-op parser as default. The real parser is set per-field via setParser().
            // forNullValue() provides a minimal parser that satisfies the non-null requirement
            // for metadata preParse/postParse calls.
            this.parser = RowValueXContentParser.forNullValue();
            this.document = new LuceneDocument();
            this.documents.add(document);
            this.maxAllowedNumNestedDocs = mappingParserContext.getIndexSettings().getMappingNestedDocsLimit();
            this.numNestedDocs = 0L;
        }

        @Override
        public Mapper getMapper(String name) {
            Mapper mapper = getMetadataMapper(name);
            if (mapper != null) {
                return mapper;
            }
            return super.getMapper(name);
        }

        @Override
        public ContentPath path() {
            return this.path;
        }

        @Override
        public XContentParser parser() {
            return this.parser;
        }

        void setParser(XContentParser parser) {
            this.parser = parser;
        }

        @Override
        public LuceneDocument rootDoc() {
            return documents.get(0);
        }

        @Override
        public LuceneDocument doc() {
            return this.document;
        }

        @Override
        protected void addDoc(LuceneDocument doc) {
            numNestedDocs++;
            if (numNestedDocs > maxAllowedNumNestedDocs) {
                throw new DocumentParsingException(
                    parser.getTokenLocation(),
                    "The number of nested documents has exceeded the allowed limit of ["
                        + maxAllowedNumNestedDocs
                        + "]."
                        + " This limit can be set by changing the ["
                        + MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING.getKey()
                        + "] index level setting."
                );
            }
            this.documents.add(doc);
        }

        @Override
        public Iterable<LuceneDocument> nonRootDocuments() {
            if (docsReversed) {
                throw new IllegalStateException("documents are already reversed");
            }
            return documents.subList(1, documents.size());
        }

        @Override
        public BytesRef getTsid() {
            return sourceToParse().tsid();
        }

        /**
         * Returns a copy of the provided List where parent documents appear after their children.
         */
        List<LuceneDocument> reorderParentAndGetDocs() {
            if (documents.size() > 1 && docsReversed == false) {
                docsReversed = true;
                List<LuceneDocument> newDocs = new ArrayList<>(documents.size());
                LinkedList<LuceneDocument> parents = new LinkedList<>();
                for (LuceneDocument doc : documents) {
                    while (parents.peek() != doc.getParent()) {
                        newDocs.add(parents.poll());
                    }
                    parents.add(0, doc);
                }
                newDocs.addAll(parents);
                documents.clear();
                documents.addAll(newDocs);
            }
            return documents;
        }
    }
}
