/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.eicf.EicfLuceneColumns;
import org.elasticsearch.eirf.EirfType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.BatchDocumentParserContext;
import org.elasticsearch.index.mapper.BinaryDocValuesSyntheticFieldLoader;
import org.elasticsearch.index.mapper.ColumnBatchBuilder;
import org.elasticsearch.index.mapper.CompositeSyntheticFieldLoader;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.StringStoredFieldFieldLoader;
import org.elasticsearch.index.mapper.TextParams;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.sourcebatch.SourceColumn;
import org.elasticsearch.sourcebatch.SourceColumnCursor;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A {@link FieldMapper} for full-text log fields that internally splits text into a low cardinality template component
 * and high cardinality argument component. Separating these pieces allows the template component to be highly compressed.
 */
public class PatternTextFieldMapper extends FieldMapper {

    private static final NamedAnalyzer STANDARD_ANALYZER = new NamedAnalyzer("standard", AnalyzerScope.GLOBAL, new StandardAnalyzer());

    /**
     * A setting that indicates that pattern text fields should disable templating, usually because there is
     * no valid enterprise license.
     */
    public static final Setting<Boolean> DISABLE_TEMPLATING_SETTING = Setting.boolSetting(
        "index.mapping.pattern_text.disable_templating",
        false,
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex
    );

    public static class Defaults {
        public static final FieldType FIELD_TYPE_DOCS;
        public static final FieldType FIELD_TYPE_POSITIONS;

        static {
            final FieldType ft = new FieldType();
            ft.setTokenized(true);
            ft.setStored(false);
            ft.setStoreTermVectors(false);
            ft.setOmitNorms(true);
            ft.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE_DOCS = freezeAndDeduplicateFieldType(ft);
        }

        static {
            final FieldType ft = new FieldType();
            ft.setTokenized(true);
            ft.setStored(false);
            ft.setStoreTermVectors(false);
            ft.setOmitNorms(true);
            ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            FIELD_TYPE_POSITIONS = freezeAndDeduplicateFieldType(ft);
        }
    }

    public static class Builder extends TextFamilyBuilder {

        private final IndexSettings indexSettings;
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<String> indexOptions = patternTextIndexOptions(m -> ((PatternTextFieldMapper) m).indexOptions);
        private final Parameter<NamedAnalyzer> analyzer;
        private final Parameter<Boolean> disableTemplating;
        private final IndexVersion indexCreatedVersion;
        private final boolean useBinaryDocValuesForRawText;

        public Builder(String name, MappingParserContext context) {
            this(
                name,
                context.indexVersionCreated(),
                context.getIndexSettings(),
                context.isWithinMultiField(),
                useBinaryDocValuesForRawText(context.getIndexSettings())
            );
        }

        public Builder(
            String name,
            IndexVersion indexCreatedVersion,
            IndexSettings indexSettings,
            boolean isWithinMultiField,
            boolean useBinaryDocValuesForRawText
        ) {
            super(name, indexCreatedVersion, isWithinMultiField);
            this.indexSettings = indexSettings;
            this.analyzer = analyzerParam(name, m -> ((PatternTextFieldMapper) m).analyzer);
            this.disableTemplating = disableTemplatingParameter(indexSettings);
            this.indexCreatedVersion = indexCreatedVersion;
            this.useBinaryDocValuesForRawText = useBinaryDocValuesForRawText;
        }

        private boolean useBinaryDocValuesForArgsColumn() {
            return indexCreatedVersion.onOrAfter(IndexVersions.PATTERN_TEXT_ARGS_IN_BINARY_DOC_VALUES);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { meta, indexOptions, analyzer, disableTemplating };
        }

        private PatternTextFieldType buildFieldType(FieldType fieldType, MapperBuilderContext context) {
            NamedAnalyzer analyzer = this.analyzer.get();
            TextSearchInfo tsi = new TextSearchInfo(fieldType, null, analyzer, analyzer);
            return new PatternTextFieldType(
                context.buildFullName(leafName()),
                tsi,
                analyzer,
                disableTemplating.getValue(),
                meta.getValue(),
                context.isSourceSynthetic(),
                isWithinMultiField(),
                useBinaryDocValuesForArgsColumn(),
                useBinaryDocValuesForRawText
            );
        }

        private static FieldType buildLuceneFieldType(Supplier<String> indexOptionSupplier) {
            var indexOptions = TextParams.toIndexOptions(true, indexOptionSupplier.get());
            return indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS ? Defaults.FIELD_TYPE_POSITIONS : Defaults.FIELD_TYPE_DOCS;
        }

        private static Parameter<String> patternTextIndexOptions(Function<FieldMapper, String> initializer) {
            return Parameter.stringParam("index_options", false, initializer, "docs").addValidator(v -> {
                switch (v) {
                    case "positions":
                    case "docs":
                        return;
                    default:
                        throw new MapperParsingException(
                            "Unknown value [" + v + "] for field [index_options] - accepted values are [positions, docs]"
                        );
                }
            });
        }

        private static Parameter<NamedAnalyzer> analyzerParam(String name, Function<FieldMapper, NamedAnalyzer> initializer) {
            return new Parameter<>("analyzer", false, () -> DelimiterAnalyzer.INSTANCE, (n, c, o) -> {
                String analyzerName = o.toString();
                switch (analyzerName) {
                    case "standard":
                        return STANDARD_ANALYZER;
                    case "delimiter":
                        return DelimiterAnalyzer.INSTANCE;
                    default:
                        throw new IllegalArgumentException(
                            "unsupported analyzer [" + analyzerName + "] for field [" + name + "], supported analyzers are [standard, log]"
                        );
                }
            }, initializer, (b, n, v) -> b.field(n, v.name()), NamedAnalyzer::name);
        }

        /**
         * A parameter that indicates the pattern_text mapper should disable templating, usually
         * because there is no valid enterprise license.
         * <p>
         * The parameter should only be explicitly enabled or left unset. When left unset, it defaults to the value determined from the
         * associated index setting, which is set from the current license status.
         */
        private static Parameter<Boolean> disableTemplatingParameter(IndexSettings indexSettings) {
            boolean forceDisable = DISABLE_TEMPLATING_SETTING.get(indexSettings.getSettings());
            return Parameter.boolParam(
                "disable_templating",
                false,
                m -> ((PatternTextFieldMapper) m).fieldType().disableTemplating(),
                forceDisable
            ).addValidator(value -> {
                if (value == false && forceDisable) {
                    throw new MapperParsingException(
                        "value [false] for mapping parameter [disable_templating] contradicts value [true] for index setting ["
                            + DISABLE_TEMPLATING_SETTING.getKey()
                            + "]"
                    );
                }
            }).setSerializerCheck((includeDefaults, isConfigured, value) -> includeDefaults || isConfigured || value);
        }

        @Override
        public String contentType() {
            return PatternTextFieldType.CONTENT_TYPE;
        }

        @Override
        public PatternTextFieldMapper build(MapperBuilderContext context) {
            FieldType fieldType = buildLuceneFieldType(indexOptions);
            PatternTextFieldType patternTextFieldType = buildFieldType(fieldType, context);
            BuilderParams builderParams = builderParams(this, context);
            var templateIdMapper = KeywordFieldMapper.Builder.buildWithDocValuesSkipper(
                patternTextFieldType.templateIdFieldName(leafName()),
                indexSettings,
                isWithinMultiField()
                // Enforce LOW cardinality even if cardinality defaults to HIGH:
            ).indexed(false).docValues(DocValuesParameter.Values.Cardinality.LOW).build(context);
            return new PatternTextFieldMapper(leafName(), fieldType, patternTextFieldType, builderParams, this, templateIdMapper);
        }
    }

    public static final TypeParser PARSER = new TypeParser(Builder::new);

    private final IndexVersion indexCreatedVersion;
    private final NamedAnalyzer analyzer;
    private final IndexSettings indexSettings;
    private final String indexOptions;
    private final FieldType fieldType;
    private final KeywordFieldMapper templateIdMapper;
    private final boolean useBinaryDocValueArgs;
    private final boolean useBinaryDocValuesForRawText;

    private PatternTextFieldMapper(
        String simpleName,
        FieldType fieldType,
        PatternTextFieldType mappedFieldType,
        BuilderParams builderParams,
        Builder builder,
        KeywordFieldMapper templateIdMapper
    ) {
        super(simpleName, mappedFieldType, builderParams);
        assert mappedFieldType.getTextSearchInfo().isTokenized();
        assert mappedFieldType.hasDocValues() == false;
        this.fieldType = fieldType;
        this.indexCreatedVersion = builder.indexCreatedVersion();
        this.analyzer = builder.analyzer.get();
        this.indexSettings = builder.indexSettings;
        this.indexOptions = builder.indexOptions.getValue();
        this.templateIdMapper = templateIdMapper;
        this.useBinaryDocValueArgs = builder.useBinaryDocValuesForArgsColumn();
        this.useBinaryDocValuesForRawText = builder.useBinaryDocValuesForRawText;
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), analyzer);
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), indexCreatedVersion, indexSettings, fieldType().isWithinMultiField(), useBinaryDocValuesForRawText)
            .init(this);
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> mappers = new ArrayList<>();
        Iterator<Mapper> m = super.iterator();
        while (m.hasNext()) {
            mappers.add(m.next());
        }
        mappers.add(templateIdMapper);
        return mappers.iterator();
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        final String value = context.parser().textOrNull();
        if (value == null) {
            return;
        }

        var existingValue = context.doc().getField(fieldType().name());
        if (existingValue != null) {
            throw new IllegalArgumentException("Multiple values are not allowed for field [" + fieldType().name() + "].");
        }

        // Add index on original value
        context.doc().add(new Field(fieldType().name(), value, fieldType));

        if (fieldType().disableTemplating()) {
            storePatternAsRawText(context, value);
            return;
        }

        // Parse template and args
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(value);

        // Add template_id doc_values
        context.doc().add(templateIdMapper.buildKeywordField(new BytesRef(parts.templateId())));

        if (parts.useBinaryDocValuesForRawText()) {
            storePatternAsRawText(context, value);
        } else {
            // Add template doc_values
            context.doc().add(new SortedSetDocValuesField(fieldType().templateFieldName(), new BytesRef(parts.template())));

            // Add args Info
            String argsInfoEncoded = Arg.encodeInfo(parts.argsInfo());
            context.doc().add(new SortedSetDocValuesField(fieldType().argsInfoFieldName(), new BytesRef(argsInfoEncoded)));

            // Add args doc_values
            if (parts.args().isEmpty() == false) {
                String remainingArgs = Arg.encodeRemainingArgs(parts);
                if (useBinaryDocValueArgs) {
                    context.doc().add(new BinaryDocValuesField(fieldType().argsFieldName(), new BytesRef(remainingArgs)));
                } else {
                    context.doc().add(new SortedSetDocValuesField(fieldType().argsFieldName(), new BytesRef(remainingArgs)));
                }
            }
        }
    }

    @Override
    public boolean supportsBatchIndexing() {
        // Mirror the keyword/number/date gates: scripts, copy_to and multi-fields pull in behavior the
        // columnar batch path does not reproduce, so only plain pattern_text fields are eligible. Both the
        // templating and stored-fallback paths are reproduced in mapColumnBatch below.
        return hasScript() == false && copyTo().copyToFields().isEmpty() && multiFields().iterator().hasNext() == false;
    }

    @Override
    public void mapColumnBatch(SourceColumn column, BatchDocumentParserContext[] contexts, ColumnBatchBuilder out) {
        final int docCount = column.docCount();

        // Primary inverted field: wrap the raw Body string column directly (zero-copy for an EICF string
        // column). Lucene applies the configured analyzer lazily per document during the row-pass inversion
        // (BinaryColumnAdapter.tokenStream -> analyzer.tokenStream), exactly mirroring the row document model
        // where parseCreateField stores the string in a tokenized Field and IndexingChain inverts it.
        out.addColumn(EicfLuceneColumns.toBinaryColumn(column, fieldType().name(), fieldType));

        if (fieldType().disableTemplating()) {
            // Templating disabled: only the raw-text stored column is produced (see parseCreateField).
            final EicfLuceneColumns.BinaryColumnBuilder stored = EicfLuceneColumns.binaryColumnBuilder(
                docCount,
                fieldType().storedNamed(),
                BinaryDocValuesField.TYPE
            );
            final SourceColumnCursor cursor = column.cursor();
            while (cursor.advance()) {
                if (cursor.type() == EirfType.STRING) {
                    requireBinaryDocValuesForRawText();
                    final XContentString.UTF8Bytes utf8 = cursor.stringValue().bytes();
                    stored.addBytes(utf8.bytes(), utf8.offset(), utf8.length());
                } else {
                    stored.addAbsent();
                }
            }
            if (stored.isEmpty() == false) {
                out.addColumn(stored.build());
            }
            return;
        }

        // Templating enabled: split each value into the template/args sub-columns, mirroring parseCreateField.
        // Each sub-column packs its computed values into one contiguous buffer for cache-friendly indexing.
        final IndexableFieldType templateIdFieldType = templateIdMapper.buildKeywordField(new BytesRef()).fieldType();
        final EicfLuceneColumns.BinaryColumnBuilder templateId = EicfLuceneColumns.binaryColumnBuilder(
            docCount,
            fieldType().templateIdFieldName(),
            templateIdFieldType
        );
        final EicfLuceneColumns.BinaryColumnBuilder template = EicfLuceneColumns.binaryColumnBuilder(
            docCount,
            fieldType().templateFieldName(),
            SortedSetDocValuesField.TYPE
        );
        final EicfLuceneColumns.BinaryColumnBuilder argsInfo = EicfLuceneColumns.binaryColumnBuilder(
            docCount,
            fieldType().argsInfoFieldName(),
            SortedSetDocValuesField.TYPE
        );
        final EicfLuceneColumns.BinaryColumnBuilder args = EicfLuceneColumns.binaryColumnBuilder(
            docCount,
            fieldType().argsFieldName(),
            useBinaryDocValueArgs ? BinaryDocValuesField.TYPE : SortedSetDocValuesField.TYPE
        );
        final EicfLuceneColumns.BinaryColumnBuilder stored = EicfLuceneColumns.binaryColumnBuilder(
            docCount,
            fieldType().storedNamed(),
            BinaryDocValuesField.TYPE
        );

        final PatternTextValueProcessor.Utf8SplitScratch scratch = new PatternTextValueProcessor.Utf8SplitScratch();
        final SourceColumnCursor cursor = column.cursor();
        try {
            while (cursor.advance()) {
                if (cursor.type() != EirfType.STRING) {
                    templateId.addAbsent();
                    template.addAbsent();
                    argsInfo.addAbsent();
                    args.addAbsent();
                    stored.addAbsent();
                    continue;
                }
                final XContentString.UTF8Bytes utf8 = cursor.stringValue().bytes();
                final byte[] src = utf8.bytes();
                final int srcOff = utf8.offset();
                final int srcLen = utf8.length();
                if (srcLen > PatternTextValueProcessor.MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE) {
                    // Rare large value: the row path truncates by char length, so defer to the String path
                    // (which handles both the truncated-but-not-exceeded and exceeded sub-cases) rather than
                    // re-deriving the truncation byte offset here. A value with byteLen <= the limit always has
                    // charLen <= the limit (UTF-16 length <= UTF-8 byte length), so the hot path below never
                    // needs to consider truncation.
                    mapDocViaStringPath(cursor.stringValue().string(), templateId, template, argsInfo, args, stored);
                    continue;
                }

                // Hot path: split directly on the UTF-8 bytes and compose each sub-column value straight into
                // the page-backed builder arenas, allocating no String, token array, or intermediate buffer.
                PatternTextValueProcessor.splitUtf8(src, srcOff, srcLen, scratch);

                final int idLen = scratch.encodeTemplateId();
                templateId.addBytes(scratch.b64, 0, idLen);
                stored.addAbsent();
                template.addBytes(scratch.template, 0, scratch.templateLen);
                final int infoLen = scratch.encodeArgsInfo();
                argsInfo.addBytes(scratch.b64, 0, infoLen);
                if (scratch.argCount > 0) {
                    // Compose the space-joined args (matching Arg.encodeRemainingArgs) directly into the arena.
                    for (int a = 0; a < scratch.argCount; a++) {
                        if (a > 0) {
                            args.appendBytes(SPACE_BYTES, 0, 1);
                        }
                        args.appendBytes(src, scratch.argOff[a], scratch.argLen[a]);
                    }
                    args.commitValue();
                } else {
                    args.addAbsent();
                }
            }
        } catch (IOException e) {
            // Only the String fallback path calls Arg.encodeInfo, which writes to a pre-sized in-memory buffer
            // and does not realistically fail; surface any failure so ShardBatchMapper falls back to the
            // row-major path rather than dropping data.
            throw new UncheckedIOException(e);
        }

        if (templateId.isEmpty() == false) {
            out.addColumn(templateId.build());
        }
        if (template.isEmpty() == false) {
            out.addColumn(template.build());
        }
        if (argsInfo.isEmpty() == false) {
            out.addColumn(argsInfo.build());
        }
        if (args.isEmpty() == false) {
            out.addColumn(args.build());
        }
        if (stored.isEmpty() == false) {
            out.addColumn(stored.build());
        }
    }

    /** Single-space separator for the joined args column (matches {@code Arg.encodeRemainingArgs}). */
    private static final byte[] SPACE_BYTES = { ' ' };

    /**
     * Maps one document through the String-based {@link PatternTextValueProcessor#split} path, mirroring
     * {@code parseCreateField}. Used only for values longer than {@link PatternTextValueProcessor#MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE}
     * bytes, where the row path's char-based truncation must be reproduced exactly.
     */
    private void mapDocViaStringPath(
        String value,
        EicfLuceneColumns.BinaryColumnBuilder templateId,
        EicfLuceneColumns.BinaryColumnBuilder template,
        EicfLuceneColumns.BinaryColumnBuilder argsInfo,
        EicfLuceneColumns.BinaryColumnBuilder args,
        EicfLuceneColumns.BinaryColumnBuilder stored
    ) throws IOException {
        final PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(value);
        templateId.addString(parts.templateId());
        if (parts.useBinaryDocValuesForRawText()) {
            requireBinaryDocValuesForRawText();
            stored.addString(value);
            template.addAbsent();
            argsInfo.addAbsent();
            args.addAbsent();
        } else {
            stored.addAbsent();
            template.addString(parts.template());
            argsInfo.addString(Arg.encodeInfo(parts.argsInfo()));
            if (parts.args().isEmpty() == false) {
                args.addString(Arg.encodeRemainingArgs(parts));
            } else {
                args.addAbsent();
            }
        }
    }

    /**
     * The legacy raw-text path stores values in a {@link StoredField} rather than binary doc values; the
     * columnar batch path does not emit stored fields, so signal a fallback to the row-major path. Not
     * reachable for a fresh time-series/columnar index, where {@link #useBinaryDocValuesForRawText} is true.
     */
    private void requireBinaryDocValuesForRawText() {
        if (useBinaryDocValuesForRawText == false) {
            throw new UnsupportedOperationException("pattern_text columnar batch indexing requires binary-doc-values raw-text storage");
        }
    }

    /**
     * Store the value as a raw text field, without analyzing it. This can happen when templating is disabled or when the value is too long
     * to be analyzed.
     *
     * Values may be stored in binary doc values or in stored fields, both of which don't have the same length limitations as regular doc
     * values do.
     */
    private void storePatternAsRawText(DocumentParserContext context, final String value) {
        if (useBinaryDocValuesForRawText) {
            context.doc().add(new BinaryDocValuesField(fieldType().storedNamed(), new BytesRef(value)));
        } else {
            // for bwc, store in stored fields
            context.doc().add(new StoredField(fieldType().storedNamed(), new BytesRef(value)));
        }
    }

    private static boolean useBinaryDocValuesForRawText(IndexSettings indexSettings) {
        return indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.STORE_PATTERN_TEXT_FIELDS_IN_BINARY_DOC_VALUES)
            && indexSettings.useTimeSeriesDocValuesFormat();
    }

    @Override
    protected String contentType() {
        return PatternTextFieldType.CONTENT_TYPE;
    }

    @Override
    public PatternTextFieldType fieldType() {
        return (PatternTextFieldType) super.fieldType();
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        return new SyntheticSourceSupport.Native(this::getSyntheticFieldLoader);
    }

    private SourceLoader.SyntheticFieldLoader getSyntheticFieldLoader() {
        if (fieldType().disableTemplating()) {
            if (useBinaryDocValuesForRawText) {
                return new BinaryDocValuesSyntheticFieldLoader(fieldType().storedNamed()) {
                    @Override
                    protected void writeValue(XContentBuilder b, BytesRef value) throws IOException {
                        // pattern text fields are not multi-valued, so there is no special encoding here unlike other fields that use
                        // binary doc values. As a result, we don't need to much and this function remains simple
                        b.field(leafName(), value.utf8ToString());
                    }
                };
            }

            return new StringStoredFieldFieldLoader(fieldType().storedNamed(), fieldType().name(), leafName()) {
                @Override
                protected void write(XContentBuilder b, Object value) throws IOException {
                    b.value(((BytesRef) value).utf8ToString());
                }
            };
        }

        return new CompositeSyntheticFieldLoader(
            leafName(),
            fullPath(),
            new PatternTextSyntheticFieldLoaderLayer(
                fieldType().name(),
                leafReader -> PatternTextFallbackDocValues.fromEnabledPatternText(leafReader, fieldType())
            )
        );
    }

    NamedAnalyzer getAnalyzer() {
        return analyzer;
    }
}
