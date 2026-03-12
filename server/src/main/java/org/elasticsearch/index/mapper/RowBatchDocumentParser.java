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
     * Checks if a field path is a sub-field of a compound field mapper (e.g., "metrics.X.sum"
     * is a sub-field of compound parent "metrics.X" which is an aggregate_metric_double).
     * Returns the compound parent info if found, or null if not a compound sub-field.
     */
    static CompoundFieldInfo resolveCompoundParent(String fieldPath, MappingLookup mappingLookup) {
        int dotPos = fieldPath.length();
        while (true) {
            dotPos = fieldPath.lastIndexOf('.', dotPos - 1);
            if (dotPos < 0) return null;
            String parentPath = fieldPath.substring(0, dotPos);
            Mapper parent = resolveMapper(parentPath, mappingLookup);
            if (parent instanceof FieldMapper fm && fm.isCompoundField()) {
                String subPath = fieldPath.substring(dotPos + 1);
                return new CompoundFieldInfo(parentPath, fm, subPath);
            }
        }
    }

    record CompoundFieldInfo(String parentPath, FieldMapper parentMapper, String subFieldPath) {}

    /**
     * Group of sub-field columns belonging to one compound parent field.
     * During row iteration, sub-field values are buffered here and then
     * processed together after the main column loop.
     */
    static final class CompoundFieldGroup {
        final String parentPath;
        final FieldMapper parentMapper;
        final String[] parentSegments;
        final List<String> subFieldPaths = new ArrayList<>();
        final List<Integer> columnIndices = new ArrayList<>();
        // Per-row buffered values (indexed by subFieldPaths position)
        final List<Object> bufferedValues = new ArrayList<>();

        CompoundFieldGroup(String parentPath, FieldMapper parentMapper, String[] parentSegments) {
            this.parentPath = parentPath;
            this.parentMapper = parentMapper;
            this.parentSegments = parentSegments;
        }

        void addSubField(String subFieldPath, int columnIndex) {
            subFieldPaths.add(subFieldPath);
            columnIndices.add(columnIndex);
        }

        void clearBuffers() {
            bufferedValues.clear();
        }
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
        // Check if this is a sub-field of an already-mapped compound field (e.g., metrics.X.sum
        // under a mapped metrics.X of type aggregate_metric_double). In this case the parent
        // mapper handles parsing, so no dynamic mapping is needed.
        if (resolveCompoundParent(fieldPath, mappingLookup) != null) {
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
     * Builds a dynamic mapping update from unmapped batch columns by inspecting the RowType of each
     * dynamic column and using the standard dynamic template/field resolution logic.
     *
     * @param rowBatch       the row-oriented document batch
     * @param dynamicColumns indices of columns that require dynamic mapping
     * @param indexRequests  the index requests (needed for dynamic templates)
     * @param mappingLookup  the current mapping lookup
     * @return a CompressedXContent mapping update, or null if no dynamic mappings are needed
     */
    public CompressedXContent buildDynamicMappingUpdate(
        RowDocumentBatch rowBatch,
        List<Integer> dynamicColumns,
        List<IndexRequest> indexRequests,
        MappingLookup mappingLookup
    ) {
        if (dynamicColumns.isEmpty()) {
            return null;
        }

        final DocBatchSchema schema = rowBatch.schema();

        // Merge dynamic templates and template params from ALL IndexRequests.
        // Different documents in an OTEL-style batch may have different metric fields,
        // each with their own dynamic template assignments. We need the union of all templates
        // to correctly map every field in the batch schema.
        IndexRequest firstRequest = indexRequests.get(0);
        XContentType xContentType = firstRequest.getContentType();
        if (xContentType == null) {
            xContentType = XContentType.JSON;
        }

        Map<String, String> mergedDynamicTemplates = new HashMap<>();
        Map<String, Map<String, String>> mergedDynamicTemplateParams = new HashMap<>();
        for (IndexRequest ir : indexRequests) {
            if (ir.getDynamicTemplates() != null) {
                mergedDynamicTemplates.putAll(ir.getDynamicTemplates());
            }
            if (ir.getDynamicTemplateParams() != null) {
                mergedDynamicTemplateParams.putAll(ir.getDynamicTemplateParams());
            }
        }

        // Create a SourceToParse with merged dynamic templates from all requests
        SourceToParse source = new SourceToParse(
            firstRequest.id(),
            BytesArray.EMPTY,
            xContentType,
            firstRequest.routing(),
            mergedDynamicTemplates,
            mergedDynamicTemplateParams,
            false,
            XContentMeteringParserDecorator.NOOP,
            firstRequest.tsid()
        );

        // Create shared collections for the context
        Set<String> copyToFields = mappingLookup.fieldTypesLookup().getCopyToDestinationFields();
        Map<String, List<Mapper.Builder>> dynamicMappers = new HashMap<>();
        Map<String, ObjectMapper.Builder> dynamicObjectMappers = new HashMap<>();
        Map<String, List<RuntimeField>> dynamicRuntimeFields = new HashMap<>();

        BatchDocumentParserContext context = new BatchDocumentParserContext(
            mappingLookup,
            mappingParserContext,
            source,
            copyToFields,
            dynamicMappers,
            dynamicObjectMappers,
            dynamicRuntimeFields,
            dynamicColumns.size() + 2
        );

        // Track compound parent paths that have already been processed so we create
        // only one dynamic mapping entry per compound parent, not one per sub-field.
        Set<String> processedCompoundParents = new java.util.HashSet<>();

        for (int col : dynamicColumns) {
            String fieldName = schema.getColumnName(col);

            // Check if this is a compound sub-field (e.g., metrics.X.sum).
            // If so, find the parent path and use its dynamic template to create the mapping.
            String compoundParentPath = findCompoundDynamicParent(fieldName, indexRequests);
            if (compoundParentPath != null) {
                if (processedCompoundParents.add(compoundParentPath)) {
                    // Process this compound parent once
                    FieldPathInfo parentPathInfo = computeFieldPathInfo(compoundParentPath, mappingLookup);
                    String[] parentSegments = parentPathInfo.parentSegments();
                    String leafName = parentPathInfo.leafName();
                    ObjectMapper.Dynamic dynamic = getEffectiveDynamic(compoundParentPath, mappingLookup);

                    setPathForField(parentSegments, context.path());
                    try {
                        // Use OBJECT match type since the parent is an object-like field
                        DynamicTemplate.XContentFieldType matchType = DynamicTemplate.XContentFieldType.OBJECT;
                        if (dynamic == ObjectMapper.Dynamic.TRUE) {
                            createDynamicFieldForColumn(context, leafName, matchType, RowType.BINARY);
                        } else if (dynamic == ObjectMapper.Dynamic.RUNTIME) {
                            createRuntimeDynamicFieldForColumn(context, leafName, matchType);
                        }
                    } finally {
                        resetPath(parentSegments.length, context.path());
                    }
                }
                continue;
            }

            FieldPathInfo pathInfo = computeFieldPathInfo(fieldName, mappingLookup);
            String[] parentSegments = pathInfo.parentSegments();
            String leafName = pathInfo.leafName();

            ObjectMapper.Dynamic dynamic = getEffectiveDynamic(fieldName, mappingLookup);

            // Determine the RowType for this column by scanning the batch for the first non-null value
            byte valueType = findFirstNonNullType(rowBatch, col);
            if (valueType == RowType.NULL) {
                continue; // all null — skip this column
            }

            // Set path context for parent segments
            setPathForField(parentSegments, context.path());
            try {
                // Map RowType to XContentFieldType and create dynamic field
                DynamicTemplate.XContentFieldType matchType = rowTypeToXContentFieldType(valueType);

                if (dynamic == ObjectMapper.Dynamic.TRUE) {
                    // Create concrete dynamic field using template or default
                    createDynamicFieldForColumn(context, leafName, matchType, valueType);
                } else if (dynamic == ObjectMapper.Dynamic.RUNTIME) {
                    // Create runtime dynamic field using template or default
                    createRuntimeDynamicFieldForColumn(context, leafName, matchType);
                }
            } finally {
                resetPath(parentSegments.length, context.path());
            }
        }

        return DocumentParser.createDynamicUpdate(context);
    }

    /**
     * Checks if a field path is a sub-field of a compound parent that has a dynamic template.
     * For example, "metrics.X.sum" might have parent "metrics.X" with a "summary" dynamic template.
     * Returns the parent path if found, or null.
     */
    private static String findCompoundDynamicParent(String fieldPath, List<IndexRequest> indexRequests) {
        if (indexRequests.isEmpty()) return null;

        // Check all IndexRequests since different documents may have different dynamic template assignments
        int dotPos = fieldPath.length();
        while (true) {
            dotPos = fieldPath.lastIndexOf('.', dotPos - 1);
            if (dotPos < 0) return null;
            String parentPath = fieldPath.substring(0, dotPos);
            for (IndexRequest ir : indexRequests) {
                Map<String, String> dynamicTemplates = ir.getDynamicTemplates();
                if (dynamicTemplates != null && dynamicTemplates.containsKey(parentPath)) {
                    return parentPath;
                }
            }
        }
    }

    /**
     * Scans a batch column for the first non-null value and returns its RowType base type.
     */
    private static byte findFirstNonNullType(RowDocumentBatch rowBatch, int targetCol) {
        for (int doc = 0; doc < rowBatch.docCount(); doc++) {
            DocBatchRowReader reader = rowBatch.getRowReader(doc);
            DocBatchRowIterator iter = reader.iterator();
            while (iter.next()) {
                if (iter.column() == targetCol && !iter.isNull()) {
                    return iter.baseType();
                }
            }
        }
        return RowType.NULL;
    }

    /**
     * Maps a RowType base type to an XContentFieldType for dynamic template matching.
     */
    private static DynamicTemplate.XContentFieldType rowTypeToXContentFieldType(byte rowType) {
        return switch (rowType) {
            case RowType.STRING -> DynamicTemplate.XContentFieldType.STRING;
            case RowType.LONG -> DynamicTemplate.XContentFieldType.LONG;
            case RowType.DOUBLE -> DynamicTemplate.XContentFieldType.DOUBLE;
            case RowType.TRUE, RowType.FALSE -> DynamicTemplate.XContentFieldType.BOOLEAN;
            case RowType.BINARY -> DynamicTemplate.XContentFieldType.BINARY;
            case RowType.XCONTENT_ARRAY, RowType.ARRAY -> DynamicTemplate.XContentFieldType.STRING; // arrays default to string
            default -> DynamicTemplate.XContentFieldType.STRING;
        };
    }

    /**
     * Creates a concrete dynamic field (dynamic=true) for a batch column, using dynamic templates if available.
     */
    private static void createDynamicFieldForColumn(
        BatchDocumentParserContext context,
        String leafName,
        DynamicTemplate.XContentFieldType matchType,
        byte valueType
    ) {
        // Check for matching dynamic template first
        DynamicTemplate dynamicTemplate = context.findDynamicTemplate(leafName, matchType);
        if (dynamicTemplate != null) {
            String dynamicType = dynamicTemplate.isRuntimeMapping()
                ? matchType.defaultRuntimeMappingType()
                : matchType.defaultMappingType();
            String mappingType = dynamicTemplate.mappingType(dynamicType);
            Map<String, Object> mapping = dynamicTemplate.mappingForName(leafName, dynamicType, context.getDynamicTemplateParams(leafName));
            String fullFieldName = context.path().pathAsText(leafName);
            if (dynamicTemplate.isRuntimeMapping()) {
                MappingParserContext parserContext = context.dynamicTemplateParserContext(null);
                RuntimeField.Parser parser = parserContext.runtimeFieldParser(mappingType);
                RuntimeField.Builder builder = parser.parse(fullFieldName, mapping, parserContext);
                context.addDynamicRuntimeField(builder.createRuntimeField(parserContext));
            } else {
                MappingParserContext parserContext = context.dynamicTemplateParserContext(null);
                Mapper.TypeParser typeParser = parserContext.typeParser(mappingType);
                Mapper.Builder builder = typeParser.parse(leafName, mapping, parserContext);
                context.addDynamicMapper(builder, fullFieldName);
            }
            context.markFieldAsAppliedFromTemplate(fullFieldName);
        } else {
            // No template — use default dynamic field creation
            String fullFieldName = context.path().pathAsText(leafName);
            Mapper.Builder builder = createDefaultDynamicFieldBuilder(context, leafName, valueType);
            if (builder != null) {
                context.addDynamicMapper(builder, fullFieldName);
            }
        }
    }

    /**
     * Creates a runtime dynamic field (dynamic=runtime) for a batch column.
     */
    private static void createRuntimeDynamicFieldForColumn(
        BatchDocumentParserContext context,
        String leafName,
        DynamicTemplate.XContentFieldType matchType
    ) {
        // Check for matching dynamic template first
        DynamicTemplate dynamicTemplate = context.findDynamicTemplate(leafName, matchType);
        if (dynamicTemplate != null) {
            String dynamicType = dynamicTemplate.isRuntimeMapping()
                ? matchType.defaultRuntimeMappingType()
                : matchType.defaultMappingType();
            String mappingType = dynamicTemplate.mappingType(dynamicType);
            Map<String, Object> mapping = dynamicTemplate.mappingForName(leafName, dynamicType, context.getDynamicTemplateParams(leafName));
            String fullFieldName = context.path().pathAsText(leafName);
            if (dynamicTemplate.isRuntimeMapping()) {
                MappingParserContext parserContext = context.dynamicTemplateParserContext(null);
                RuntimeField.Parser parser = parserContext.runtimeFieldParser(mappingType);
                RuntimeField.Builder builder = parser.parse(fullFieldName, mapping, parserContext);
                context.addDynamicRuntimeField(builder.createRuntimeField(parserContext));
            } else {
                MappingParserContext parserContext = context.dynamicTemplateParserContext(null);
                Mapper.TypeParser typeParser = parserContext.typeParser(mappingType);
                Mapper.Builder builder = typeParser.parse(leafName, mapping, parserContext);
                context.addDynamicMapper(builder, fullFieldName);
            }
            context.markFieldAsAppliedFromTemplate(fullFieldName);
        } else {
            // No template — create runtime field using source-only types
            String fullName = context.path().pathAsText(leafName);
            RuntimeField runtimeField = switch (matchType) {
                case STRING -> KeywordScriptFieldType.sourceOnly(fullName);
                case LONG -> LongScriptFieldType.sourceOnly(fullName);
                case DOUBLE -> DoubleScriptFieldType.sourceOnly(fullName);
                case BOOLEAN -> BooleanScriptFieldType.sourceOnly(fullName);
                default -> KeywordScriptFieldType.sourceOnly(fullName);
            };
            context.addDynamicRuntimeField(runtimeField);
        }
    }

    /**
     * Creates a default mapper builder for a concrete dynamic field based on the RowType.
     */
    private static Mapper.Builder createDefaultDynamicFieldBuilder(BatchDocumentParserContext context, String leafName, byte valueType) {
        var indexSettings = context.indexSettings();
        return switch (valueType) {
            case RowType.STRING -> {
                MapperBuilderContext mapperBuilderContext = context.createDynamicMapperBuilderContext();
                if (mapperBuilderContext.parentObjectContainsDimensions()) {
                    yield new KeywordFieldMapper.Builder(leafName, indexSettings);
                } else {
                    yield new TextFieldMapper.Builder(leafName, indexSettings, context.indexAnalyzers(), false).addMultiField(
                        new KeywordFieldMapper.Builder("keyword", indexSettings, true).ignoreAbove(256)
                    );
                }
            }
            case RowType.LONG -> new NumberFieldMapper.Builder(
                leafName,
                NumberFieldMapper.NumberType.LONG,
                org.elasticsearch.script.ScriptCompiler.NONE,
                indexSettings
            );
            case RowType.DOUBLE -> new NumberFieldMapper.Builder(
                leafName,
                NumberFieldMapper.NumberType.FLOAT,
                org.elasticsearch.script.ScriptCompiler.NONE,
                indexSettings
            );
            case RowType.TRUE, RowType.FALSE -> new BooleanFieldMapper.Builder(
                leafName,
                org.elasticsearch.script.ScriptCompiler.NONE,
                indexSettings
            );
            case RowType.BINARY -> new BinaryFieldMapper.Builder(leafName, SourceFieldMapper.isSynthetic(indexSettings));
            default -> null;
        };
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

        // Pre-compute shared collections. The dynamic mapper collections are shared across
        // document contexts intentionally — they accumulate any dynamic mappers encountered
        // during binary blob parsing. createDynamicUpdate is NOT called per-document (see below),
        // so the shared-map cascade that caused O(N) expensive mapping serializations is gone.
        final Set<String> sharedCopyToFields = mappingLookup.fieldTypesLookup().getCopyToDestinationFields();
        final Map<String, List<Mapper.Builder>> sharedDynamicMappers = new HashMap<>();
        final Map<String, ObjectMapper.Builder> sharedDynamicObjectMappers = new HashMap<>();
        final Map<String, List<RuntimeField>> sharedDynamicRuntimeFields = new HashMap<>();

        final SourceToParse[] sources = new SourceToParse[docCount];
        final BatchDocumentParserContext[] contexts = new BatchDocumentParserContext[docCount];
        final ParsedDocument[] results = new ParsedDocument[docCount];
        final Exception[] exceptions = new Exception[docCount];

        // Pre-resolve mappers once per column and validate strict dynamic mappings upfront.
        // Also detect compound sub-fields (e.g., metrics.X.sum where metrics.X is aggregate_metric_double).
        final Mapper[] columnMappers = new Mapper[schema.columnCount()];
        final String[][] columnParentSegments = new String[schema.columnCount()][];
        final boolean[] isCompoundSubField = new boolean[schema.columnCount()];
        final Map<String, CompoundFieldGroup> compoundGroups = new HashMap<>();
        for (int col = 0; col < schema.columnCount(); col++) {
            String fieldName = schema.getColumnName(col);
            columnMappers[col] = resolveMapper(fieldName, mappingLookup);
            columnParentSegments[col] = computeFieldPathInfo(fieldName, mappingLookup).parentSegments();
            if (columnMappers[col] == null && mappingLookup.getFieldType(fieldName) == null) {
                // Check if this is a compound sub-field
                CompoundFieldInfo compoundInfo = resolveCompoundParent(fieldName, mappingLookup);
                if (compoundInfo != null) {
                    isCompoundSubField[col] = true;
                    CompoundFieldGroup group = compoundGroups.computeIfAbsent(
                        compoundInfo.parentPath(),
                        k -> new CompoundFieldGroup(
                            compoundInfo.parentPath(),
                            compoundInfo.parentMapper(),
                            computeFieldPathInfo(compoundInfo.parentPath(), mappingLookup).parentSegments()
                        )
                    );
                    group.addSubField(compoundInfo.subFieldPath(), col);
                } else {
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
        }

        int fieldCountHint = columnMappers.length;
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
                    indexRequest.getDynamicTemplates(),
                    indexRequest.getDynamicTemplateParams(),
                    false,
                    XContentMeteringParserDecorator.NOOP,
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
                    sharedDynamicRuntimeFields,
                    fieldCountHint + 2 // TODO: Unsure
                );

                // Step 3: metadata preParse
                for (MetadataFieldMapper metadataMapper : metadataFieldMappers) {
                    metadataMapper.preParse(contexts[i]);
                }

                // Step 4: Get row iterator and parse fields
                int rowIndex = indexRequest.batchRowIndex() >= 0 ? indexRequest.batchRowIndex() : i;
                DocBatchRowReader rowReader = rowBatch.getRowReader(rowIndex);
                DocBatchRowIterator rowIterator = rowReader.iterator();

                // Clear compound group buffers for this document
                for (CompoundFieldGroup group : compoundGroups.values()) {
                    group.clearBuffers();
                }

                while (rowIterator.next()) {
                    if (exceptions[i] != null) break;
                    if (rowIterator.isNull()) continue;

                    int col = rowIterator.column();

                    // Buffer compound sub-field values instead of parsing them directly
                    if (isCompoundSubField[col]) {
                        try {
                            bufferCompoundSubFieldValue(rowIterator, col, compoundGroups);
                        } catch (Exception e) {
                            exceptions[i] = e;
                        }
                        continue;
                    }

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

                // Process compound field groups after the main column loop
                for (CompoundFieldGroup group : compoundGroups.values()) {
                    if (group.bufferedValues.isEmpty()) continue;
                    try {
                        parseCompoundFieldGroup(group, contexts[i]);
                    } catch (Exception e) {
                        exceptions[i] = e;
                        break;
                    }
                }

                if (exceptions[i] != null) continue;

                // Step 5: metadata postParse
                for (MetadataFieldMapper metadataMapper : metadataFieldMappers) {
                    metadataMapper.postParse(contexts[i]);
                }

                // Step 6: Build ParsedDocument
                // Dynamic mapping updates are intentionally NOT produced here. The batch path
                // pre-handles dynamic mappings in performRowBatchOnPrimary before calling
                // parseRowBatch. Even if parseBinaryObjectForDocument encounters unmapped fields
                // and creates temporary mappers in the context, calling createDynamicUpdate would
                // build and compress the entire mapping for every document — expensive work that
                // performRowBatchOnPrimary never uses (it ignores ParsedDocument.dynamicUpdate).
                BatchDocumentParserContext ctx = contexts[i];

                results[i] = new ParsedDocument(
                    ctx.version(),
                    ctx.seqID(),
                    ctx.id(),
                    ctx.routing(),
                    ctx.reorderParentAndGetDocs(),
                    new BytesArray("{\"marker\":true}"),
                    sources[i].getXContentType(),
                    null,
                    XContentMeteringParserDecorator.UNKNOWN_SIZE
                ) {
                    @Override
                    public String documentDescription() {
                        IdFieldMapper idMapper = (IdFieldMapper) mappingLookup.getMapping().getMetadataMapperByName(IdFieldMapper.NAME);
                        return idMapper.documentDescription(this);
                    }
                };
                fieldCountHint = Math.max(fieldCountHint, ctx.document.getFields().size());
            } catch (Exception e) {
                exceptions[i] = e;
            }
        }

        return new BatchResult(results, exceptions);
    }

    /**
     * Buffers a compound sub-field value from the current iterator position into the appropriate
     * CompoundFieldGroup. The value is read based on the RowType and stored for later processing.
     */
    private static void bufferCompoundSubFieldValue(
        DocBatchRowIterator iterator,
        int col,
        Map<String, CompoundFieldGroup> compoundGroups
    ) {
        byte baseType = iterator.baseType();
        Object value = switch (baseType) {
            case RowType.LONG -> iterator.rowLongValue();
            case RowType.DOUBLE -> iterator.rowDoubleValue();
            case RowType.STRING -> iterator.stringValue();
            case RowType.TRUE -> true;
            case RowType.FALSE -> false;
            default -> null;
        };
        if (value == null) return;

        // Find which group this column belongs to
        for (CompoundFieldGroup group : compoundGroups.values()) {
            int idx = group.columnIndices.indexOf(col);
            if (idx >= 0) {
                // Ensure the bufferedValues list is big enough
                while (group.bufferedValues.size() <= idx) {
                    group.bufferedValues.add(null);
                }
                group.bufferedValues.set(idx, value);
                break;
            }
        }
    }

    /**
     * Processes a compound field group by building a synthetic XContentParser from the buffered
     * sub-field values and calling the parent field mapper's parse method.
     */
    private static void parseCompoundFieldGroup(CompoundFieldGroup group, BatchDocumentParserContext context) throws IOException {
        XContentParser compoundParser = buildCompoundParser(group);
        if (compoundParser == null) return;

        setPathForField(group.parentSegments, context.path());
        try {
            compoundParser.nextToken(); // Position at START_OBJECT
            context.setParser(compoundParser);
            group.parentMapper.parse(context);
        } finally {
            resetPath(group.parentSegments.length, context.path());
            compoundParser.close();
        }
    }

    /**
     * Builds a CompoundFieldXContentParser from the buffered values in a compound group.
     * Currently supports aggregate_metric_double; extend for histogram/exp_histogram later.
     */
    private static XContentParser buildCompoundParser(CompoundFieldGroup group) {
        // For aggregate_metric_double: expect "sum" (double) and "value_count" (long)
        Double sum = null;
        Long valueCount = null;
        for (int j = 0; j < group.subFieldPaths.size(); j++) {
            Object val = j < group.bufferedValues.size() ? group.bufferedValues.get(j) : null;
            if (val == null) continue;
            switch (group.subFieldPaths.get(j)) {
                case "sum" -> sum = ((Number) val).doubleValue();
                case "value_count" -> valueCount = ((Number) val).longValue();
            }
        }
        if (sum != null && valueCount != null) {
            return CompoundFieldXContentParser.forAggregateMetricDouble(sum, valueCount);
        }
        return null;
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

    /**
     * Computes the parent path segments for a field, respecting the object mapper hierarchy.
     * For objects with {@code subobjects: DISABLED} (e.g. passthrough objects), dots after
     * that object are part of the field name, not object separators.
     *
     * @return record containing the parent segments and the leaf field name
     */
    static FieldPathInfo computeFieldPathInfo(String fieldPath, MappingLookup mappingLookup) {
        int lastDot = fieldPath.lastIndexOf('.');
        if (lastDot < 0) {
            return new FieldPathInfo(EMPTY_SEGMENTS, fieldPath);
        }

        // Walk the dot-separated segments, checking for subobjects: DISABLED
        String[] allSegments = fieldPath.split("\\.");
        StringBuilder prefix = new StringBuilder();
        for (int i = 0; i < allSegments.length - 1; i++) {
            if (i > 0) prefix.append('.');
            prefix.append(allSegments[i]);
            ObjectMapper objectMapper = mappingLookup.objectMappers().get(prefix.toString());
            if (objectMapper != null && objectMapper.subobjects() == ObjectMapper.Subobjects.DISABLED) {
                // Everything after this object is the leaf field name
                String[] parentSegments = new String[i + 1];
                System.arraycopy(allSegments, 0, parentSegments, 0, i + 1);
                // Leaf is the remaining segments joined by dots
                StringBuilder leaf = new StringBuilder(allSegments[i + 1]);
                for (int j = i + 2; j < allSegments.length; j++) {
                    leaf.append('.').append(allSegments[j]);
                }
                return new FieldPathInfo(parentSegments, leaf.toString());
            }
        }

        // No subobjects: DISABLED found — split at the last dot (default behavior)
        String[] parentSegments = new String[allSegments.length - 1];
        System.arraycopy(allSegments, 0, parentSegments, 0, allSegments.length - 1);
        return new FieldPathInfo(parentSegments, allSegments[allSegments.length - 1]);
    }

    record FieldPathInfo(String[] parentSegments, String leafName) {}

    /**
     * Legacy overload for contexts where mapping lookup is not available.
     */
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
            // Create a no-op parser as default. The real parser is set per-field via setParser().
            // forNullValue() provides a minimal parser that satisfies the non-null requirement
            // for metadata preParse/postParse calls.
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
