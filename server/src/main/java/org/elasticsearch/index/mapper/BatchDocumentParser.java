/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Parses row-oriented documents (as {@code List<Object>} rows) into {@link ParsedDocument}s
 * using pre-resolved {@link BatchMappingsResolver.Resolved} mappings.
 * <p>
 * Each row is a list of leaf values aligned with the leaf columns in the resolved schema.
 * Per-document metadata (id, routing, dynamic templates) comes from {@link IndexRequest}.
 */
public class BatchDocumentParser {

    private final MappingParserContext mappingParserContext;

    public BatchDocumentParser(MappingParserContext mappingParserContext) {
        this.mappingParserContext = mappingParserContext;
    }

    /**
     * Iterates over rows of values, providing per-row access to the IndexRequest and values.
     */
    public interface RowIterator {
        int size();

        IndexRequest indexRequest(int row);

        List<Object> values(int row);
    }

    /**
     * Parses all rows into ParsedDocuments.
     *
     * @param resolved      pre-resolved mappings from {@link BatchMappingsResolver}
     * @param rows          the row data
     * @param mappingLookup the current mapping lookup
     * @return list of parsed documents
     */
    public List<ParsedDocument> parse(BatchMappingsResolver.Resolved resolved, RowIterator rows, MappingLookup mappingLookup)
        throws IOException {
        int rowCount = rows.size();
        int leafCount = resolved.leafFullPaths().length;

        // Pre-compute parent segments and leaf names from resolved leaf paths
        String[][] leafParentSegments = new String[leafCount][];
        String[] leafNames = new String[leafCount];
        for (int i = 0; i < leafCount; i++) {
            leafParentSegments[i] = computeParentSegments(resolved.leafFullPaths()[i]);
            leafNames[i] = computeLeafName(resolved.leafFullPaths()[i]);
        }

        // Pre-compute compound group parent segments
        Map<String, String[]> compoundGroupParentSegments = new HashMap<>();
        for (var entry : resolved.compoundGroups().entrySet()) {
            compoundGroupParentSegments.put(entry.getKey(), computeParentSegments(entry.getValue().parentPath()));
        }

        // Shared collections for batch context
        Set<String> sharedCopyToFields = mappingLookup.fieldTypesLookup().getCopyToDestinationFields();
        Map<String, List<Mapper.Builder>> sharedDynamicMappers = new HashMap<>();
        Map<String, ObjectMapper.Builder> sharedDynamicObjectMappers = new HashMap<>();
        Map<String, List<RuntimeField>> sharedDynamicRuntimeFields = new HashMap<>();
        MetadataFieldMapper[] metadataFieldMappers = resolved.metadataFieldMappers();

        // Compound group buffers (reused across rows)
        Map<String, List<Object>> compoundBuffers = new HashMap<>();
        for (var entry : resolved.compoundGroups().entrySet()) {
            compoundBuffers.put(entry.getKey(), new ArrayList<>());
        }

        List<ParsedDocument> results = new ArrayList<>(rowCount);

        for (int row = 0; row < rowCount; row++) {
            IndexRequest indexRequest = rows.indexRequest(row);
            List<Object> values = rows.values(row);

            XContentType xContentType = indexRequest.getContentType();
            if (xContentType == null) {
                xContentType = XContentType.JSON;
            }

            SourceToParse source = new SourceToParse(
                indexRequest.id(),
                BytesArray.EMPTY,
                xContentType,
                indexRequest.routing(),
                indexRequest.getDynamicTemplates(),
                indexRequest.getDynamicTemplateParams(),
                false,
                XContentMeteringParserDecorator.NOOP,
                indexRequest.tsid()
            );

            BatchDocumentParserContext context = new BatchDocumentParserContext(
                mappingLookup,
                mappingParserContext,
                source,
                sharedCopyToFields,
                sharedDynamicMappers,
                sharedDynamicObjectMappers,
                sharedDynamicRuntimeFields,
                leafCount + 2
            );

            // Metadata preParse
            for (MetadataFieldMapper metadataMapper : metadataFieldMappers) {
                metadataMapper.preParse(context);
            }

            // Clear compound group buffers
            for (List<Object> buffer : compoundBuffers.values()) {
                buffer.clear();
            }

            // Iterate leaf columns
            for (int i = 0; i < leafCount && i < values.size(); i++) {
                Object value = values.get(i);
                if (value == null) continue;

                // Skip disabled object ancestors — but track for synthetic source
                if (resolved.leafParentDisabled()[i]) {
                    addIgnoredFieldForSyntheticSource(context, resolved.leafFullPaths()[i], value);
                    continue;
                }

                // Skip runtime fields
                if (resolved.isRuntimeField()[i]) continue;

                // Buffer compound sub-field values
                if (resolved.isCompoundSubField()[i]) {
                    bufferCompoundSubField(i, value, resolved.compoundGroups(), compoundBuffers);
                    continue;
                }

                Mapper mapper = resolved.leafMappers()[i];

                // Handle unmapped fields based on effective dynamic setting
                if (mapper == null) {
                    ObjectMapper.Dynamic effectiveDynamic = resolved.leafEffectiveDynamic()[i];
                    if (effectiveDynamic != null) {
                        handleUnmappedField(context, effectiveDynamic, leafNames[i], leafParentSegments[i], value);
                    }
                    continue;
                }

                String[] parentSegments = leafParentSegments[i];

                if (mapper instanceof FieldMapper fieldMapper) {
                    parseField(fieldMapper, value, context, parentSegments);

                    // Handle copy_to
                    List<String> copyToTargets = resolved.leafCopyToTargets()[i];
                    if (copyToTargets != null) {
                        handleCopyTo(copyToTargets, value, context, mappingLookup);
                    }
                } else if (mapper instanceof ObjectMapper objectMapper) {
                    parseObject(objectMapper, value, context, parentSegments, xContentType);
                }
            }

            // Process compound field groups
            for (var entry : resolved.compoundGroups().entrySet()) {
                BatchMappingsResolver.CompoundFieldGroup group = entry.getValue();
                List<Object> buffer = compoundBuffers.get(entry.getKey());
                if (buffer.isEmpty()) continue;

                XContentParser compoundParser = buildCompoundParser(group, buffer);
                if (compoundParser == null) continue;

                String[] parentSegments = compoundGroupParentSegments.get(entry.getKey());
                setPathForField(parentSegments, context.path());
                try {
                    compoundParser.nextToken(); // Position at START_OBJECT
                    context.setParser(compoundParser);
                    group.parentMapper().parse(context);
                } finally {
                    resetPath(parentSegments.length, context.path());
                    compoundParser.close();
                }
            }

            // Metadata postParse
            for (MetadataFieldMapper metadataMapper : metadataFieldMappers) {
                metadataMapper.postParse(context);
            }

            // Build ParsedDocument — include dynamic mapping update if any
            CompressedXContent dynamicUpdate = DocumentParser.createDynamicUpdate(context);

            results.add(new ParsedDocument(
                context.version(),
                context.seqID(),
                context.id(),
                context.routing(),
                context.reorderParentAndGetDocs(),
                new BytesArray("{\"marker\":true}"),
                source.getXContentType(),
                dynamicUpdate,
                XContentMeteringParserDecorator.UNKNOWN_SIZE
            ) {
                @Override
                public String documentDescription() {
                    IdFieldMapper idMapper = (IdFieldMapper) mappingLookup.getMapping().getMetadataMapperByName(IdFieldMapper.NAME);
                    return idMapper.documentDescription(this);
                }
            });
        }

        return results;
    }

    /**
     * Handles an unmapped field based on the effective dynamic setting.
     * Mirrors the behavior of {@link DocumentParser#parseDynamicValue}.
     */
    private static void handleUnmappedField(
        BatchDocumentParserContext context,
        ObjectMapper.Dynamic effectiveDynamic,
        String leafName,
        String[] parentSegments,
        Object value
    ) throws IOException {
        if (effectiveDynamic == ObjectMapper.Dynamic.STRICT) {
            String parentPath = parentSegments.length > 0 ? String.join(".", parentSegments) : "";
            throw new StrictDynamicMappingException(XContentLocation.UNKNOWN, parentPath, leafName);
        }

        setPathForField(parentSegments, context.path());
        try {
            String fullPath = context.path().pathAsText(leafName);

            if (effectiveDynamic == ObjectMapper.Dynamic.FALSE) {
                // Track for synthetic source, but don't index
                if (context.canAddIgnoredField()) {
                    XContentParser fieldParser = ObjectValueXContentParser.forValue(value);
                    fieldParser.nextToken();
                    context.setParser(fieldParser);
                    context.addIgnoredField(
                        IgnoredSourceFieldMapper.NameValue.fromContext(context, fullPath, XContentDataHelper.encodeToken(fieldParser))
                    );
                }
                return;
            }

            // dynamic=TRUE or dynamic=RUNTIME: create dynamic mapper and parse
            XContentParser fieldParser = ObjectValueXContentParser.forValue(value);
            fieldParser.nextToken();
            context.setParser(fieldParser);

            // For RUNTIME with synthetic source, track the value
            if (effectiveDynamic == ObjectMapper.Dynamic.RUNTIME && context.canAddIgnoredField()) {
                // Need a separate parser for tracking since createDynamicFieldFromValue consumes
                XContentParser trackingParser = ObjectValueXContentParser.forValue(value);
                trackingParser.nextToken();
                context.addIgnoredField(
                    IgnoredSourceFieldMapper.NameValue.fromContext(
                        context,
                        fullPath,
                        XContentDataHelper.encodeToken(trackingParser)
                    )
                );
            }

            effectiveDynamic.getDynamicFieldsBuilder().createDynamicFieldFromValue(context, leafName);
        } finally {
            resetPath(parentSegments.length, context.path());
        }
    }

    /**
     * Adds an ignored field value for synthetic source tracking.
     * Used when a field is under a disabled object and the value should be
     * preserved in synthetic _source even though it's not indexed.
     */
    private static void addIgnoredFieldForSyntheticSource(BatchDocumentParserContext context, String fullPath, Object value)
        throws IOException {
        if (context.canAddIgnoredField()) {
            XContentParser fieldParser = ObjectValueXContentParser.forValue(value);
            fieldParser.nextToken();
            context.setParser(fieldParser);
            context.addIgnoredField(
                new IgnoredSourceFieldMapper.NameValue(fullPath, 0, XContentDataHelper.encodeToken(fieldParser), context.doc())
            );
        }
    }

    private static void parseField(FieldMapper fieldMapper, Object value, BatchDocumentParserContext context, String[] parentSegments)
        throws IOException {
        setPathForField(parentSegments, context.path());
        try {
            if (value instanceof List<?> list) {
                parseListField(fieldMapper, list, context);
            } else {
                XContentParser fieldParser = ObjectValueXContentParser.forValue(value);
                fieldParser.nextToken();
                context.setParser(fieldParser);
                fieldMapper.parse(context);
            }
        } finally {
            resetPath(parentSegments.length, context.path());
        }
    }

    private static void parseListField(FieldMapper fieldMapper, List<?> list, BatchDocumentParserContext context) throws IOException {
        XContentParser arrayParser = ObjectValueXContentParser.forValue(list);
        try {
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
        } finally {
            arrayParser.close();
        }
    }

    private static void parseObject(
        ObjectMapper objectMapper,
        Object value,
        BatchDocumentParserContext context,
        String[] parentSegments,
        XContentType xContentType
    ) throws IOException {
        setPathForField(parentSegments, context.path());
        try {
            XContentParser objectParser = ObjectValueXContentParser.forValue(value);
            try {
                objectParser.nextToken();
                DocumentParserContext subContext = context.switchParser(objectParser);
                DocumentParser.parseObjectOrNested(subContext.createChildContext(objectMapper));
            } finally {
                objectParser.close();
            }
        } finally {
            resetPath(parentSegments.length, context.path());
        }
    }

    private static void handleCopyTo(
        List<String> targets,
        Object value,
        BatchDocumentParserContext context,
        MappingLookup mappingLookup
    ) throws IOException {
        for (String target : targets) {
            // Skip inference fields
            Mapper targetMapper = mappingLookup.getMapper(target);
            if (targetMapper instanceof InferenceFieldMapper) continue;

            // Create a fresh parser for each copy_to target (the original is consumed)
            XContentParser freshParser = ObjectValueXContentParser.forValue(value);
            freshParser.nextToken(); // position on value token

            context.setParser(freshParser);

            // Find target document — walk up to root for top-level copy_to targets
            LuceneDocument targetDoc = context.doc();

            DocumentParserContext copyToContext = context.createCopyToContext(target, targetDoc);
            DocumentParser.innerParseObject(copyToContext);
        }
    }

    private static void bufferCompoundSubField(
        int leafIndex,
        Object value,
        Map<String, BatchMappingsResolver.CompoundFieldGroup> compoundGroups,
        Map<String, List<Object>> compoundBuffers
    ) {
        for (var entry : compoundGroups.entrySet()) {
            BatchMappingsResolver.CompoundFieldGroup group = entry.getValue();
            int idx = group.columnIndices().indexOf(leafIndex);
            if (idx >= 0) {
                List<Object> buffer = compoundBuffers.get(entry.getKey());
                while (buffer.size() <= idx) {
                    buffer.add(null);
                }
                buffer.set(idx, value);
                break;
            }
        }
    }

    private static XContentParser buildCompoundParser(BatchMappingsResolver.CompoundFieldGroup group, List<Object> buffer) {
        // For aggregate_metric_double: expect "sum" (double) and "value_count" (long)
        Double sum = null;
        Long valueCount = null;
        for (int j = 0; j < group.subFieldNames().size(); j++) {
            Object val = j < buffer.size() ? buffer.get(j) : null;
            if (val == null) continue;
            switch (group.subFieldNames().get(j)) {
                case "sum" -> sum = ((Number) val).doubleValue();
                case "value_count" -> valueCount = ((Number) val).longValue();
            }
        }
        if (sum != null && valueCount != null) {
            return CompoundFieldXContentParser.forAggregateMetricDouble(sum, valueCount);
        }
        return null;
    }

    private static final String[] EMPTY_SEGMENTS = new String[0];

    private static String[] computeParentSegments(String fieldPath) {
        int lastDot = fieldPath.lastIndexOf('.');
        if (lastDot < 0) return EMPTY_SEGMENTS;
        return fieldPath.substring(0, lastDot).split("\\.");
    }

    private static String computeLeafName(String fieldPath) {
        int lastDot = fieldPath.lastIndexOf('.');
        return lastDot < 0 ? fieldPath : fieldPath.substring(lastDot + 1);
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
     * A DocumentParserContext implementation for batch parsing with swappable parser.
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
            Map<String, List<RuntimeField>> dynamicRuntimeFields,
            int fieldCountHint
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
            this.parser = RowValueXContentParser.forNullValue();
            this.document = new LuceneDocument(fieldCountHint);
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
