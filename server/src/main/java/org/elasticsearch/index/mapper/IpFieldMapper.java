/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.network.CIDRUtils;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.escf.EscfColumnBuilder;
import org.elasticsearch.escf.EscfColumnBuilder.CollisionPolicy;
import org.elasticsearch.escf.EscfColumnData;
import org.elasticsearch.escf.EscfColumnKind;
import org.elasticsearch.escf.EscfColumnTransforms;
import org.elasticsearch.escf.LuceneBinaryColumn;
import org.elasticsearch.escf.LuceneLongColumn;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.BytesBinaryIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryMultiSeparateCountBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromOrdsBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMaxBytesRefsFromBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMaxBytesRefsFromOrdsBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMinBytesRefsFromBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMinBytesRefsFromOrdsBlockLoader;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.queries.ScanningBinaryDocValuesRangeQuery;
import org.elasticsearch.lucene.queries.XSortedSetDocValuesRangeQuery;
import org.elasticsearch.script.IpFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.field.IpDocValuesField;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.FieldValues;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import java.net.InetAddress;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import static org.elasticsearch.index.mapper.FieldArrayContext.getOffsetsFieldName;
import static org.elasticsearch.index.mapper.FieldMapper.Parameter.useTimeSeriesDocValuesSkippers;
import static org.elasticsearch.index.mapper.IpPrefixAutomatonUtil.buildIpPrefixAutomaton;

/**
 * A {@link FieldMapper} for ip addresses.
 */
public class IpFieldMapper extends FieldMapper {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(IpFieldMapper.class);

    public static final String CONTENT_TYPE = "ip";

    private static IpFieldMapper toType(FieldMapper in) {
        return (IpFieldMapper) in;
    }

    public static final class Builder extends FieldMapper.DimensionBuilder {

        private final Parameter<Boolean> indexed;
        private final DocValuesParameter docValuesParameters;
        private final Parameter<Boolean> stored = Parameter.storeParam(m -> toType(m).stored, false);

        private final Parameter<Boolean> ignoreMalformed;
        private final Parameter<String> nullValue = Parameter.stringParam("null_value", false, m -> toType(m).nullValueAsString, null)
            .acceptsNull();

        private final Parameter<Script> script = Parameter.scriptParam(m -> toType(m).script);
        private final Parameter<OnScriptError> onScriptErrorParam = Parameter.onScriptErrorParam(
            m -> toType(m).builderParams.onScriptError(),
            script
        );

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<Boolean> dimension;

        private final ScriptCompiler scriptCompiler;
        private final IndexSettings indexSettings;

        private boolean arrayOrderBinaryDocValues;

        public Builder(String name, ScriptCompiler scriptCompiler, IndexSettings indexSettings) {
            super(name);
            this.indexSettings = indexSettings;
            this.scriptCompiler = Objects.requireNonNull(scriptCompiler);
            this.ignoreMalformed = Parameter.boolParam(
                "ignore_malformed",
                true,
                m -> toType(m).ignoreMalformed,
                IGNORE_MALFORMED_SETTING.get(indexSettings.getSettings())
            );
            this.script.precludesParameters(nullValue, ignoreMalformed);

            this.docValuesParameters = DocValuesParameter.of(
                DocValuesParameter.defaultValues(
                    indexSettings,
                    DocValuesParameter.Values.ENABLED_LOW_CARDINALITY,
                    DocValuesParameter.Values.Cardinality.HIGH
                ),
                m -> toType(m).docValuesParameters(),
                indexSettings.getMode().isStrictColumnar()
            );

            this.dimension = TimeSeriesParams.dimensionParam(m -> toType(m).dimension, () -> docValuesParameters.get().enabled());
            this.indexed = Parameter.indexParam(m -> toType(m).indexed, indexSettings, dimension);
            addScriptValidation(script, indexed, () -> docValuesParameters.getValue().enabled());
        }

        Builder nullValue(String nullValue) {
            this.nullValue.setValue(nullValue);
            return this;
        }

        public Builder dimension(boolean dimension) {
            this.dimension.setValue(dimension);
            return this;
        }

        private InetAddress parseNullValue() {
            String nullValueAsString = nullValue.getValue();
            if (nullValueAsString == null) {
                return null;
            }
            try {
                return InetAddresses.forString(nullValueAsString);
            } catch (Exception e) {
                if (indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.V_8_0_0)) {
                    throw new MapperParsingException("Error parsing [null_value] on field [" + leafName() + "]: " + e.getMessage(), e);
                } else {
                    DEPRECATION_LOGGER.warn(
                        DeprecationCategory.MAPPINGS,
                        "ip_mapper_null_field",
                        "Error parsing ["
                            + nullValue.getValue()
                            + "] as IP in [null_value] on field ["
                            + leafName()
                            + "]); [null_value] will be ignored"
                    );
                    return null;
                }
            }
        }

        private FieldValues<InetAddress> scriptValues() {
            if (this.script.get() == null) {
                return null;
            }
            IpFieldScript.Factory factory = scriptCompiler.compile(this.script.get(), IpFieldScript.CONTEXT);
            return factory == null
                ? null
                : (lookup, ctx, doc, consumer) -> factory.newFactory(leafName(), script.get().getParams(), lookup, OnScriptError.FAIL)
                    .newInstance(ctx)
                    .runForDoc(doc, consumer);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] {
                indexed,
                docValuesParameters,
                stored,
                ignoreMalformed,
                nullValue,
                script,
                onScriptErrorParam,
                meta,
                dimension };
        }

        private IndexType indexType() {
            if (indexSettings.getIndexVersionCreated().isLegacyIndexVersion()) {
                return docValuesParameters.get().enabled() ? IndexType.archivedPoints() : IndexType.NONE;
            }
            if (usesBinaryDocValues()) {
                // Disable skippers if using binary doc values
                return IndexType.points(indexed.get(), true);
            }
            if (useTimeSeriesDocValuesSkippers(indexSettings, dimension.get())) {
                return IndexType.skippers();
            }
            if (indexed.get() == false && docValuesParameters.get().enabled()) {
                if (indexSettings.useDocValuesSkipper()
                    && indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.STANDARD_INDEXES_USE_SKIPPERS)) {
                    return IndexType.skippers();
                }
            }
            return IndexType.points(indexed.get(), docValuesParameters.get().enabled());
        }

        @Override
        public String contentType() {
            return CONTENT_TYPE;
        }

        private boolean usesBinaryDocValues() {
            var docValuesParams = docValuesParameters.getValue();
            return docValuesParams.enabled() && docValuesParams.cardinality() == DocValuesParameter.Values.Cardinality.HIGH;
        }

        @Override
        public IpFieldMapper build(MapperBuilderContext context) {
            if (inheritDimensionParameterFromParentObject(context)) {
                dimension.setValue(true);
            }
            hasScript = script.get() != null;
            onScriptError = onScriptErrorParam.getValue();

            String offsetsFieldName = getOffsetsFieldName(
                context,
                indexSettings.sourceKeepMode(),
                docValuesParameters.getValue().enabled(),
                stored.getValue(),
                this,
                indexSettings.getIndexVersionCreated(),
                IndexVersions.SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_IP,
                indexSettings.getMode().isStrictColumnar(),
                docValuesParameters.getValue().multiValue()
            );
            // High-cardinality (binary doc values) fields in strict columnar mode store their values in document order directly in the
            // binary doc values (ArrayOrderInlineNull) instead of recording a sidecar .offsets field; low-cardinality fields keep offsets.
            if (offsetsFieldName != null && usesBinaryDocValues() && indexSettings.getMode().isStrictColumnar()) {
                arrayOrderBinaryDocValues = true;
                offsetsFieldName = null;
            }
            boolean readInArrayOrder = offsetsFieldName != null
                && docValuesParameters.getValue().multiValue()
                && indexSettings.getMode().isStrictColumnar();
            return new IpFieldMapper(
                leafName(),
                new IpFieldType(
                    context.buildFullName(leafName()),
                    indexType(),
                    stored.getValue(),
                    parseNullValue(),
                    scriptValues(),
                    meta.getValue(),
                    dimension.getValue(),
                    context.isSourceSynthetic(),
                    usesBinaryDocValues(),
                    readInArrayOrder,
                    arrayOrderBinaryDocValues,
                    indexSettings.getIndexVersionCreated(),
                    docValuesParameters.getValue()
                ),
                builderParams(this, context),
                context.isSourceSynthetic(),
                this,
                offsetsFieldName
            );
        }

    }

    public static final TypeParser PARSER = createTypeParserWithLegacySupport(
        (n, c) -> new Builder(n, c.scriptCompiler(), c.getIndexSettings())
    );

    public static final class IpFieldType extends SimpleMappedFieldType {

        private final InetAddress nullValue;
        private final FieldValues<InetAddress> scriptValues;
        private final boolean isDimension;
        private final boolean isSyntheticSource;
        private final boolean hasPoints;
        private final boolean usesBinaryDocValues;
        private final boolean readInArrayOrder;
        private final boolean useArrayOrderBinaryDocValues;
        private final IndexVersion indexVersion;
        private final DocValuesParameter.Values docValuesParams;

        public IpFieldType(
            String name,
            IndexType indexType,
            boolean stored,
            InetAddress nullValue,
            FieldValues<InetAddress> scriptValues,
            Map<String, String> meta,
            boolean isDimension,
            boolean isSyntheticSource,
            boolean usesBinaryDocValues,
            boolean readInArrayOrder,
            boolean useArrayOrderBinaryDocValues,
            IndexVersion indexVersion,
            DocValuesParameter.Values docValuesParams
        ) {
            super(name, indexType, stored, meta);
            this.nullValue = nullValue;
            this.scriptValues = scriptValues;
            this.isDimension = isDimension;
            this.isSyntheticSource = isSyntheticSource;
            this.hasPoints = indexType.hasPoints();
            this.usesBinaryDocValues = usesBinaryDocValues;
            this.readInArrayOrder = readInArrayOrder;
            this.useArrayOrderBinaryDocValues = useArrayOrderBinaryDocValues;
            this.indexVersion = indexVersion;
            this.docValuesParams = docValuesParams;
        }

        public IpFieldType(String name) {
            this(name, true, true);
        }

        public IpFieldType(String name, boolean isIndexed) {
            this(name, isIndexed, true);
        }

        public IpFieldType(String name, boolean isIndexed, boolean hasDocValues) {
            this(
                name,
                IndexType.points(isIndexed, hasDocValues),
                false,
                null,
                null,
                Collections.emptyMap(),
                false,
                false,
                false,
                false,
                false,
                IndexVersion.current(),
                null
            );
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean isSearchable() {
            return hasPoints || hasDocValues();
        }

        @Override
        public TextSearchInfo getTextSearchInfo() {
            return TextSearchInfo.SIMPLE_MATCH_WITHOUT_TERMS;
        }

        @Override
        public boolean mayExistInIndex(SearchExecutionContext context) {
            return context.fieldExistsInIndex(name());
        }

        @Override
        public boolean isDimension() {
            return isDimension;
        }

        @Override
        public boolean hasScriptValues() {
            return scriptValues != null;
        }

        public boolean usesBinaryDocValues() {
            return usesBinaryDocValues;
        }

        public boolean usesArrayOrderBinaryDocValues() {
            return useArrayOrderBinaryDocValues;
        }

        /** Which framing a reader of this field's binary doc values has to decode. */
        private BinaryDocValuesFormat binaryFormat() {
            return useArrayOrderBinaryDocValues ? BinaryDocValuesFormat.ARRAY_ORDER_INLINE_NULL : BinaryDocValuesFormat.SEPARATE_COUNT;
        }

        private static InetAddress parse(Object value) {
            if (value instanceof InetAddress) {
                return (InetAddress) value;
            } else {
                if (value instanceof BytesRef) {
                    value = ((BytesRef) value).utf8ToString();
                }
                return InetAddresses.forString(value.toString());
            }
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            if (scriptValues != null) {
                return FieldValues.valueFetcher(scriptValues, v -> InetAddresses.toAddrString((InetAddress) v), context);
            }
            return new SourceValueFetcher(name(), context, nullValue) {
                @Override
                protected Object parseSourceValue(Object value) {
                    InetAddress address;
                    if (value instanceof InetAddress) {
                        address = (InetAddress) value;
                    } else {
                        address = InetAddresses.forString(value.toString());
                    }
                    return InetAddresses.toAddrString(address);
                }
            };
        }

        @Override
        public Query termQuery(Object value, @Nullable SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            Query query;
            if (value instanceof InetAddress) {
                query = InetAddressPoint.newExactQuery(name(), (InetAddress) value);
            } else {
                if (value instanceof BytesRef) {
                    value = ((BytesRef) value).utf8ToString();
                }
                String term = value.toString();
                if (term.contains("/")) {
                    final Tuple<InetAddress, Integer> cidr = InetAddresses.parseCidr(term);
                    query = InetAddressPoint.newPrefixQuery(name(), cidr.v1(), cidr.v2());
                } else {
                    InetAddress address = InetAddresses.forString(term);
                    query = InetAddressPoint.newExactQuery(name(), address);
                }
            }
            if (hasPoints) {
                if (hasDocValues()) {
                    return convertToIndexOrDocValuesQuery(query, usesBinaryDocValues, useArrayOrderBinaryDocValues, context);
                }
                return query;
            } else {
                return convertToDocValuesQuery(query, usesBinaryDocValues, useArrayOrderBinaryDocValues, context);
            }
        }

        static Query convertToIndexOrDocValuesQuery(
            Query query,
            boolean usesBinaryDocValues,
            boolean arrayOrderInlineNull,
            SearchExecutionContext context
        ) {
            assert query instanceof PointRangeQuery;
            return new IndexOrDocValuesQuery(query, convertToDocValuesQuery(query, usesBinaryDocValues, arrayOrderInlineNull, context));
        }

        static Query convertToDocValuesQuery(
            Query query,
            boolean usesBinaryDocValues,
            boolean arrayOrderInlineNull,
            SearchExecutionContext context
        ) {
            assert query instanceof PointRangeQuery;
            PointRangeQuery pointRangeQuery = (PointRangeQuery) query;

            final String field = pointRangeQuery.getField();
            final BytesRef lower = new BytesRef(pointRangeQuery.getLowerPoint());
            final BytesRef upper = new BytesRef(pointRangeQuery.getUpperPoint());

            if (usesBinaryDocValues) {
                return new ScanningBinaryDocValuesRangeQuery(
                    field,
                    lower,
                    upper,
                    arrayOrderInlineNull ? BinaryDocValuesFormat.ARRAY_ORDER_INLINE_NULL : BinaryDocValuesFormat.SEPARATE_COUNT
                );
            } else {
                return XSortedSetDocValuesRangeQuery.newSlowRangeQuery(field, lower, upper, true, true);
            }
        }

        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
            failIfNotIndexedNorDocValuesFallback(context);
            if (hasPoints == false) {
                return super.termsQuery(values, context);
            }
            InetAddress[] addresses = new InetAddress[values.size()];
            int i = 0;
            for (Object value : values) {
                InetAddress address;
                if (value instanceof InetAddress) {
                    address = (InetAddress) value;
                } else {
                    if (value instanceof BytesRef) {
                        value = ((BytesRef) value).utf8ToString();
                    }
                    if (value.toString().contains("/")) {
                        // the `terms` query contains some prefix queries, so we cannot create a set query
                        // and need to fall back to a disjunction of `term` queries
                        return super.termsQuery(values, context);
                    }
                    address = InetAddresses.forString(value.toString());
                }
                addresses[i++] = address;
            }
            return InetAddressPoint.newSetQuery(name(), addresses);
        }

        @Override
        public Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            SearchExecutionContext context
        ) {
            failIfNotIndexedNorDocValuesFallback(context);
            return rangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, (lower, upper) -> {
                Query query = InetAddressPoint.newRangeQuery(name(), lower, upper);
                if (hasPoints) {
                    if (hasDocValues()) {
                        return new IndexOrDocValuesQuery(
                            query,
                            convertToDocValuesQuery(query, usesBinaryDocValues, useArrayOrderBinaryDocValues, context)
                        );
                    } else {
                        return query;
                    }
                } else {
                    return convertToDocValuesQuery(query, usesBinaryDocValues, useArrayOrderBinaryDocValues, context);
                }
            });
        }

        /**
         * Processes query bounds into {@code long}s and delegates the
         * provided {@code builder} to build a range query.
         */
        public static Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            BiFunction<InetAddress, InetAddress, Query> builder
        ) {
            InetAddress lower;
            if (lowerTerm == null) {
                lower = InetAddressPoint.MIN_VALUE;
            } else {
                lower = parse(lowerTerm);
                if (includeLower == false) {
                    if (lower.equals(InetAddressPoint.MAX_VALUE)) {
                        return Queries.NO_DOCS_INSTANCE;
                    }
                    lower = InetAddressPoint.nextUp(lower);
                }
            }

            InetAddress upper;
            if (upperTerm == null) {
                upper = InetAddressPoint.MAX_VALUE;
            } else {
                upper = parse(upperTerm);
                if (includeUpper == false) {
                    if (upper.equals(InetAddressPoint.MIN_VALUE)) {
                        return Queries.NO_DOCS_INSTANCE;
                    }
                    upper = InetAddressPoint.nextDown(upper);
                }
            }

            return builder.apply(lower, upper);
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            if (hasDocValues() && (blContext.fieldExtractPreference() != FieldExtractPreference.STORED || isSyntheticSource)) {
                BlockLoaderFunctionConfig cfg = blContext.blockLoaderFunctionConfig();
                if (cfg == null) {
                    if (usesBinaryDocValues) {
                        if (docValuesParams != null && docValuesParams.multiValue() == false) {
                            // Single-valued binary doc values are written as plain (no separate counts column), so read them as plain.
                            return new BytesRefsFromBinaryBlockLoader(name());
                        }
                        return new BytesRefsFromBinaryMultiSeparateCountBlockLoader(name(), binaryFormat());
                    } else {
                        return new BytesRefsFromOrdsBlockLoader(name(), blContext.ordinalsByteSize(), readInArrayOrder);
                    }
                }
                BinaryDocValuesFormat binaryFormat = binaryFormat();
                return switch (cfg.function()) {
                    case MV_MAX -> usesBinaryDocValues
                        ? new MvMaxBytesRefsFromBinaryBlockLoader(name(), binaryFormat)
                        : new MvMaxBytesRefsFromOrdsBlockLoader(name(), blContext.ordinalsByteSize());
                    case MV_MIN -> usesBinaryDocValues
                        ? new MvMinBytesRefsFromBinaryBlockLoader(name(), binaryFormat)
                        : new MvMinBytesRefsFromOrdsBlockLoader(name(), blContext.ordinalsByteSize());
                    default -> throw new UnsupportedOperationException("unknown fusion config [" + cfg.function() + "]");
                };
            }
            if (blContext.blockLoaderFunctionConfig() != null) {
                throw new UnsupportedOperationException("function fusing only supported for doc values");
            }
            if (isStored()) {
                return new BlockStoredFieldsReader.BytesFromBytesRefsBlockLoader(name());
            }

            // columnar_stored pre-builds _source as a single blob; skip the per-field fallback loader.
            // Multi fields don't have fallback synthetic source.
            if (isSyntheticSource && blContext.mappingLookup().isSourceColumnarStored() == false && blContext.parentField(name()) == null) {
                return blockLoaderFromFallbackSyntheticSource(blContext);
            }
            // see #indexValue
            BlockSourceReader.LeafIteratorLookup lookup = hasDocValues() == false && hasPoints
                ? BlockSourceReader.lookupFromFieldNames(blContext.fieldNames(), name())
                : BlockSourceReader.lookupMatchingAll();
            return new BlockSourceReader.IpsBlockLoader(sourceValueFetcher(blContext), lookup);
        }

        @Override
        public boolean supportsBlockLoaderConfig(BlockLoaderFunctionConfig config, FieldExtractPreference preference) {
            if (hasDocValues() && (preference != FieldExtractPreference.STORED || isSyntheticSource)) {
                return switch (config.function()) {
                    case MV_MAX, MV_MIN -> true;
                    default -> false;
                };
            }
            return false;
        }

        private BlockLoader blockLoaderFromFallbackSyntheticSource(BlockLoaderContext blContext) {
            var reader = new IpFallbackSyntheticSourceReader(nullValue);
            return new FallbackSyntheticSourceBlockLoader(
                reader,
                name(),
                IgnoredSourceFieldMapper.ignoredSourceFormat(blContext.indexSettings())
            ) {
                @Override
                public Builder builder(BlockFactory factory, int expectedCount) {
                    return factory.bytesRefs(expectedCount);
                }
            };
        }

        private SourceValueFetcher sourceValueFetcher(BlockLoaderContext blContext) {
            return new SourceValueFetcher(blContext.sourcePaths(name()), nullValue, blContext.indexSettings().getIgnoredSourceFormat()) {
                @Override
                public InetAddress parseSourceValue(Object value) {
                    return parse(value);
                }
            };
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            if (usesBinaryDocValues) {
                // Binary doc values carry no sorted-set ordinals, so ordinals-based fielddata would silently read nothing; read the binary
                // column directly (array-order aware when values are stored in document order via ArrayOrderInlineNull).
                return new BytesBinaryIndexFieldData.Builder(
                    name(),
                    CoreValuesSourceType.IP,
                    IpDocValuesField::new,
                    indexVersion,
                    binaryFormat()
                );
            }
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), CoreValuesSourceType.IP, IpDocValuesField::new);
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            return DocValueFormat.IP.format((BytesRef) value);
        }

        @Override
        public DocValueFormat docValueFormat(@Nullable String format, ZoneId timeZone) {
            checkNoFormat(format);
            checkNoTimeZone(timeZone);
            return DocValueFormat.IP;
        }

        @Override
        public TermsEnum getTerms(IndexReader reader, String prefix, boolean caseInsensitive, String searchAfter) throws IOException {

            Terms terms = null;
            // terms_enum for ip only works if doc values are enabled
            if (hasDocValues()) {
                terms = SortedSetDocValuesTerms.getTerms(reader, name());
            }
            if (terms == null) {
                // Field does not exist on this shard.
                return null;
            }
            BytesRef searchBytes = searchAfter == null ? null : new BytesRef(InetAddressPoint.encode(InetAddress.getByName(searchAfter)));
            CompiledAutomaton prefixAutomaton = buildIpPrefixAutomaton(prefix);

            if (prefixAutomaton.type == CompiledAutomaton.AUTOMATON_TYPE.ALL) {
                TermsEnum result = terms.iterator();
                if (searchAfter != null) {
                    result = new SearchAfterTermsEnum(result, searchBytes);
                }
                return result;
            }
            return terms.intersect(prefixAutomaton, searchBytes);
        }
    }

    private final boolean indexed;
    private final DocValuesParameter.Values docValuesParameters;
    private final DocValuesFieldFactory dvFactory;
    private final boolean stored;
    private final boolean ignoreMalformed;
    private final boolean storeIgnored;
    private final boolean dimension;
    private final boolean writeDimensionRouting;

    private final InetAddress nullValue;
    private final String nullValueAsString;

    private final IndexSettings indexSettings;
    private final Script script;
    private final FieldValues<InetAddress> scriptValues;
    private final ScriptCompiler scriptCompiler;

    private final String offsetsFieldName;

    private IpFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        BuilderParams builderParams,
        boolean storeIgnored,
        Builder builder,
        String offsetsFieldName
    ) {
        super(simpleName, mappedFieldType, builderParams);
        this.indexed = builder.indexed.getValue();
        this.docValuesParameters = builder.docValuesParameters.getValue();
        this.dvFactory = new DocValuesFieldFactory(
            docValuesParameters.multiValue(),
            fieldType().indexType.hasDocValuesSkipper(),
            builder.indexSettings.getIndexVersionCreated()
        );
        this.stored = builder.stored.getValue();
        this.ignoreMalformed = builder.ignoreMalformed.getValue();
        this.nullValue = builder.parseNullValue();
        this.nullValueAsString = builder.nullValue.getValue();
        this.script = builder.script.get();
        this.scriptValues = builder.scriptValues();
        this.scriptCompiler = builder.scriptCompiler;
        this.dimension = builder.dimension.getValue();
        this.writeDimensionRouting = this.dimension
            && builder.indexSettings.getIndexRouting() instanceof IndexRouting.ExtractFromSource efs
            && efs.extractDimensionsWhileMapping();
        this.storeIgnored = storeIgnored;
        this.offsetsFieldName = offsetsFieldName;
        this.indexSettings = builder.indexSettings;
    }

    @Override
    public boolean ignoreMalformed() {
        return ignoreMalformed;
    }

    public DocValuesParameter.Values docValuesParameters() {
        return docValuesParameters;
    }

    @Override
    protected boolean shouldEnforceSingleValue(XContentParser.Token token) {
        return docValuesParameters.multiValue() == false && (token != XContentParser.Token.VALUE_NULL || nullValue != null);
    }

    @Override
    protected DocValuesParameter.Values.OnFailure onFailureBehavior() {
        return docValuesParameters.onFailure();
    }

    @Override
    public boolean isNullable() {
        return docValuesParameters.nullability() || nullValueAsString != null;
    }

    @Override
    public IpFieldType fieldType() {
        return (IpFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return fieldType().typeName();
    }

    @Override
    public boolean supportsColumnarParse(IndexSettings indexSettings) {
        // Columnar support requires strict-columnar or TSDB mode. Both binary-dv (BINARY/ArrayOrder) and
        // SORTED_SET (TSDB default) encodings are supported; see supportsColumnarDocValues().
        return (indexSettings.getMode().isStrictColumnar() || indexSettings.getMode().isTsdb())
            && supportsColumnarDocValues()
            && stored == false
            && hasScript() == false
            && copyTo().copyToFields().isEmpty()
            && multiFields().iterator().hasNext() == false
            // Use writeDimensionRouting rather than isDimension(): under ForIndexDimensions the
            // coordinating node computes the tsid and writeDimensionRouting is false, so the
            // dimension side-channel write in parseCreateField is skipped and columnar is safe.
            && (fieldType().isDimension() == false || writeDimensionRouting == false)
            && indexSettings.getIndexVersionCreated().isLegacyIndexVersion() == false;
    }

    /**
     * Returns true when this ip field's doc-values encoding is supported on the columnar batch path.
     * <p>Accepts:
     * <ul>
     *   <li>Binary doc-values — array-order (multi_value=true, ArrayOrderInlineNull blob + .counts sidecar)
     *       or single-valued (multi_value=false) encoding.</li>
     *   <li>SORTED_SET doc-values — the TSDB default, optionally with a RANGE skip index.</li>
     * </ul>
     * <p>Rejects:
     * <ul>
     *   <li>{@code doc_values: false} — the row path would also emit a point (and a {@code _field_names}
     *       entry for {@code index: true}) that the columnar path cannot replicate without doc values.</li>
     *   <li>{@code offsetsFieldName != null} — the source-keep {@code arrays} path writes an offsets
     *       sidecar that the columnar path does not yet support.</li>
     * </ul>
     */
    private boolean supportsColumnarDocValues() {
        // doc_values=false: on the row path, indexValue still emits a point and a _field_names entry when
        // index=true. The columnar path cannot replicate that without doc values to anchor it.
        if (fieldType().hasDocValues() == false) {
            return false;
        }

        // source_keep_mode=arrays produces an offsets sidecar (see FieldArrayContext.getOffsetsFieldName).
        // Silently omitting it would corrupt synthetic source.
        if (offsetsFieldName != null) {
            return false;
        }

        if (fieldType().usesBinaryDocValues()) {
            if (fieldType().usesArrayOrderBinaryDocValues()) {
                return true;
            }
            // Only support single valued when not ArrayOrderBinaryDocValues
            return docValuesParameters.multiValue() == false;
        }

        // SORTED_SET (low-cardinality / TSDB default): supported unconditionally — the DV type and
        // DocValuesSkipIndexType are resolved at emission time via hasDocValuesSkipper().
        return true;
    }

    // Field-type constants for columnar batch emission.
    //
    // These must be the *exact same objects* that the row path puts in the document; Lucene's IndexWriter
    // latches (docValuesType, DocValuesSkipIndexType) per field name for the writer's lifetime, and mixing
    // different types for one field throws IllegalArgumentException.
    //
    // SORTED_SET_DV_FIELD_TYPE — SortedSetDocValuesField.TYPE (skip index = NONE).
    // SORTED_SET_DV_INDEXED_TYPE — the private INDEXED_TYPE (skip index = RANGE), obtained via a sentinel
    // call because SortedSetDocValuesField.INDEXED_TYPE is private.
    // IP_POINT_FIELD_TYPE — ESInetAddressPoint.TYPE (1 dim × 16 bytes; no doc values, no index options).
    //
    // Do NOT hand-roll these constants — that is precisely the mistake that caused the keyword skip-index bug.
    private static final IndexableFieldType SORTED_SET_DV_FIELD_TYPE = SortedSetDocValuesField.TYPE;
    private static final IndexableFieldType SORTED_SET_DV_INDEXED_FIELD_TYPE = SortedSetDocValuesField.indexedField(
        "_sentinel",
        new BytesRef()
    ).fieldType();
    private static final IndexableFieldType IP_POINT_FIELD_TYPE = ESInetAddressPoint.TYPE;

    private static EscfColumnBuilder mergeStringColumn(BatchMappingContext ctx) {
        // TODO: Need to wire the data up to be released when the BatchMappingContext is released. This work is in progress.
        EscfColumnBuilder b = new EscfColumnBuilder(CollisionPolicy.MERGE, ctx.recycler());
        b.lockScalar(EscfColumnKind.STRING);
        return b;
    }

    private static EscfColumnBuilder mergeLongColumn(BatchMappingContext ctx) {
        // TODO: Need to wire the data up to be released when the BatchMappingContext is released. This work is in progress.
        EscfColumnBuilder b = new EscfColumnBuilder(CollisionPolicy.MERGE, ctx.recycler());
        b.lockScalar(EscfColumnKind.LONG);
        return b;
    }

    /**
     * Encodes a single IP address from its UTF-8 bytesref form to the 16-byte InetAddressPoint sortable format.
     * Wraps {@link IllegalArgumentException} (malformed IP) in {@link UnsupportedOperationException} so that
     * {@code ShardBatchMapper} falls back to the row path, which handles {@code ignore_malformed} correctly.
     */
    private static BytesRef encodeIp(BytesRef utf8) {
        try {
            return new BytesRef(InetAddresses.encodeAsIpv6(utf8.bytes, utf8.offset, utf8.length));
        } catch (IllegalArgumentException e) {
            throw new UnsupportedOperationException("mapColumnBatch: malformed IP address [" + utf8.utf8ToString() + "]", e);
        }
    }

    @Override
    public void mapColumnBatch(BatchMappingContext ctx, EscfColumn source) {
        // The gate (supportsColumnarParse) blocks doc_values=false fields, so this is always true here.
        // Keeping it as an assert rather than a silent return prevents silent _field_names and points omissions.
        assert fieldType().hasDocValues() : "mapColumnBatch called with doc_values disabled — blocked by the gate";

        if (fieldType().usesBinaryDocValues()) {
            if (fieldType().usesArrayOrderBinaryDocValues()) {
                mapColumnBatchArrayOrder(ctx, source);
            } else {
                mapColumnBatchSingleValue(ctx, source);
            }
        } else {
            mapColumnBatchSortedSet(ctx, source);
        }
    }

    private void mapColumnBatchArrayOrder(BatchMappingContext ctx, EscfColumn source) {
        final int docCount = ctx.docCount();
        // retainValues=false: each value is encoded and appended to the document blob before the cursor
        // advances, so no value has to outlive the nextDoc() that moves past it.
        final ObjectTupleCursor<BytesRef> cursor = EscfColumnTransforms.utf8Cursor(source, false);
        // TODO: make the batch return these column builders to wire up recycling
        final EscfColumnBuilder binaryDvs = mergeStringColumn(ctx);
        final EscfColumnBuilder dvCounts = mergeLongColumn(ctx);
        // Points need a separate builder: the DV blob is length-prefix-packed (not 16-byte-fixed),
        // so the binaryDvs data cannot double as a points source. CollisionPolicy.MERGE preserves
        // duplicates (multiple setString on the same doc → ARRAY cell), matching the row path where
        // doc.add(address) is called once per value in document order without deduplication.
        final boolean emitPoints = fieldType().indexType.hasPoints();
        final EscfColumnBuilder pointsBuilder = emitPoints ? mergeStringColumn(ctx) : null;
        // The 16-byte null-value substitute, or null when no null_value is configured.
        final BytesRef nullValueEncoded = nullValue != null ? new BytesRef(CIDRUtils.encode(nullValue.getAddress())) : null;

        int currentDoc = -1;
        // Each document's slots are appended into docBlob as they are read; the finished blob is handed
        // to binaryDvs.setString, which copies it out immediately, so the buffer is free to be rewritten.
        final BytesRefBuilder docBlob = new BytesRefBuilder();
        int pos = 0;
        int docSlotCount = 0;
        // True when the current doc has at least one non-null slot; gates binary dv blob emission.
        boolean hasNonNull = false;

        while (true) {
            final int nextDoc = cursor.nextDoc();
            if (nextDoc != currentDoc) {
                // Flush the completed doc's elements.
                // All-null docs write counts (matching ArrayOrderInlineNull.recordNull) but no blob.
                if (docSlotCount > 0) {
                    dvCounts.setLong(currentDoc, docSlotCount);
                    if (hasNonNull) {
                        // All IP values are exactly InetAddressPoint.BYTES (16) bytes, so a single non-null
                        // slot is stored raw (no length prefix). Drop the prefix by starting after it.
                        final int length = docSlotCount == 1 ? InetAddressPoint.BYTES : pos;
                        binaryDvs.setString(currentDoc, docBlob.bytes(), pos - length, length);
                    }
                    pos = 0;
                    docSlotCount = 0;
                    hasNonNull = false;
                }
                if (nextDoc == DocIdSetIterator.NO_MORE_DOCS) {
                    break;
                }
                currentDoc = nextDoc;
            }

            BytesRef utf8Value = cursor.value();

            // Explicit JSON null: apply null_value substitution if configured; otherwise record a
            // null doc-values slot (no ignore check), mirroring the row-path's
            // ArrayOrderInlineNull.recordNull for an absent value with no null_value.
            if (utf8Value == null) {
                if (nullValueEncoded != null) {
                    pos = MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.appendSlot(docBlob, pos, nullValueEncoded);
                    docSlotCount++;
                    hasNonNull = true;
                    // null_value also emits a point on the row path (indexValue is called with the
                    // null-value ESInetAddressPoint), so mirror that here.
                    if (pointsBuilder != null) {
                        pointsBuilder.setString(currentDoc, nullValueEncoded);
                    }
                } else {
                    pos = MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.appendSlot(docBlob, pos, null);
                    docSlotCount++;
                    // hasNonNull stays false: null slots do not produce a binary dv blob or a point.
                    // No point is emitted (row-path parity: indexValue is not called for null without null_value).
                }
                continue;
            }

            // encodeIp throws UnsupportedOperationException on malformed input, which makes
            // ShardBatchMapper fall back to the row path for the whole batch.
            final BytesRef encoded = encodeIp(utf8Value);
            pos = MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.appendSlot(docBlob, pos, encoded);
            docSlotCount++;
            hasNonNull = true;
            if (pointsBuilder != null) {
                // One point per value in document order; no deduplication (row-path parity: doc.add(address)
                // is called for every non-null value without sorting or dedup).
                pointsBuilder.setString(currentDoc, encoded);
            }
            // TODO: Implement ignore malformed.
        }

        // Attach output columns. Binary-dv blob and counts are each emitted independently.
        // All-null docs emit counts but no binary blob, so binaryDvs and dvCounts are decoupled.
        if (binaryDvs.isEmpty() == false) {
            ctx.addColumn(LuceneBinaryColumn.of(binaryDvs.finish(docCount), fieldType().name(), CustomDocValuesField.TYPE));
        }
        if (dvCounts.isEmpty() == false) {
            ctx.addColumn(LuceneLongColumn.counts(dvCounts.finish(docCount), fieldType().name()));
        }
        // Points are emitted as a separate column (FEATURE_POINTS only, no doc-values bit).
        // Two columns for one field name are legal: IndexingChain.processBatch only rejects overlapping
        // featureMask bits; FEATURE_DOCVALUES and FEATURE_POINTS are disjoint.
        if (pointsBuilder != null && pointsBuilder.isEmpty() == false) {
            ctx.addColumn(LuceneBinaryColumn.of(pointsBuilder.finish(docCount), fieldType().name(), IP_POINT_FIELD_TYPE));
        }
    }

    private void mapColumnBatchSingleValue(BatchMappingContext ctx, EscfColumn source) {
        final int docCount = ctx.docCount();
        // retainValues=false: every value is consumed within one loop iteration, before the cursor advances.
        final ObjectTupleCursor<BytesRef> cursor = EscfColumnTransforms.utf8Cursor(source, false);
        // IP always re-encodes (no zero-copy shortcut), so the values builder is unconditional.
        final EscfColumnBuilder values = mergeStringColumn(ctx);
        // The 16-byte null-value substitute, or null when no null_value is configured.
        final BytesRef nullValueEncoded = nullValue != null ? new BytesRef(CIDRUtils.encode(nullValue.getAddress())) : null;

        int currentDoc = -1;
        boolean valueSeenThisDoc = false;
        while (true) {
            final int nextDoc = cursor.nextDoc();
            if (nextDoc == DocIdSetIterator.NO_MORE_DOCS) {
                break;
            }
            if (nextDoc != currentDoc) {
                currentDoc = nextDoc;
                valueSeenThisDoc = false;
            }
            BytesRef utf8Value = cursor.value();
            if (utf8Value == null) {
                if (nullValueEncoded != null) {
                    // substitute, fall through to normal processing
                    values.setString(currentDoc, nullValueEncoded);
                    valueSeenThisDoc = true;
                }
                // else null without null_value -> absent (row-path parity)
                continue;
            }

            if (valueSeenThisDoc) {
                // multi_value=false violation: bail so ShardBatchMapper falls back to the row path,
                // which raises the correct per-doc error (on_failure=FAIL).
                // TODO: move to external method validation.
                throw new UnsupportedOperationException(
                    "mapColumnBatch: multi_value=false field [" + fullPath() + "] has more than one value for doc [" + currentDoc + "]"
                );
            }
            valueSeenThisDoc = true;

            // encodeIp throws UnsupportedOperationException on malformed input, which makes
            // ShardBatchMapper fall back to the row path for the whole batch.
            values.setString(currentDoc, encodeIp(utf8Value));
        }

        // Emit a single plain BinaryDocValuesField column (no .counts sidecar), matching
        // DocValuesFieldFactory.addBinaryField's isSingleValued() branch.
        // The same EscfColumnData is reused for the points column when indexed: the finished data is
        // already 16-byte-fixed (one per doc, absent docs skipped), which is exactly what InetAddressPoint
        // needs. Two LuceneBinaryColumn instances sharing one EscfColumnData are safe — each builds its
        // own cursor internally. Release the buffer once, not once per column.
        // TODO: Need to wire the data up to be released when the BatchMappingContext is released.
        if (values.isEmpty() == false) {
            final EscfColumnData data = values.finish(docCount);
            ctx.addColumn(LuceneBinaryColumn.of(data, fieldType().name(), BinaryDocValuesField.TYPE));
            if (fieldType().indexType.hasPoints()) {
                ctx.addColumn(LuceneBinaryColumn.of(data, fieldType().name(), IP_POINT_FIELD_TYPE));
            }
        }
    }

    /**
     * Columnar batch emission for SORTED_SET doc-values ip fields (low cardinality / TSDB default).
     *
     * <p>In TSDB every ip field resolves to {@link IndexType#skippers()} — SORTED_SET with a RANGE skip
     * index — regardless of whether {@code index: true} is set, because
     * {@link FieldMapper.Parameter#useTimeSeriesDocValuesSkippers} is checked before {@code indexed}
     * in {@link IpFieldMapper.Builder#indexType()}. Points are therefore unreachable in TSDB; note
     * that the {@code emitPoints} branch below is exercised only when the skipper is explicitly disabled
     * ({@code index.mapping.use_doc_values_skipper: false}) so that {@code indexed} drives the decision.
     *
     * <p>The frozen field type is derived from Lucene via a sentinel
     * ({@link SortedSetDocValuesField#indexedField}) rather than hand-rolled so that the
     * {@code DocValuesSkipIndexType} is always byte-for-byte identical to what the row path writes.
     * Mixing RANGE (row) and NONE (columnar) within one {@code IndexWriter} session throws
     * {@link IllegalArgumentException}; the compat harness also compares the frozen type by value.
     *
     * <p>Per-value handling: {@code null} → {@code null_value} substitution if configured, else absent
     * (row-path parity). Malformed input causes {@code encodeIp} to throw
     * {@link UnsupportedOperationException}, triggering a whole-batch row-path fallback; per-field
     * {@code ignore_malformed} is not yet handled on the columnar path.
     */
    private void mapColumnBatchSortedSet(BatchMappingContext ctx, EscfColumn source) {
        final int docCount = ctx.docCount();
        // retainValues=false: every value is consumed within one loop iteration, before the cursor advances.
        final ObjectTupleCursor<BytesRef> cursor = EscfColumnTransforms.utf8Cursor(source, false);
        // CollisionPolicy.MERGE: repeated setString on the same doc promotes the cell to an ARRAY,
        // producing one tuple per element. Lucene deduplicates and sorts at write time, matching the
        // row path which calls doc.add(new SortedSetDocValuesField(name, value)) once per element.
        final EscfColumnBuilder values = mergeStringColumn(ctx);
        final BytesRef nullValueEncoded = nullValue != null ? new BytesRef(CIDRUtils.encode(nullValue.getAddress())) : null;

        int doc;
        while ((doc = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            BytesRef utf8Value = cursor.value();
            if (utf8Value == null) {
                if (nullValueEncoded != null) {
                    values.setString(doc, nullValueEncoded);  // substitute, fall through
                } else {
                    continue;  // null without null_value -> absent (row-path parity)
                }
            } else {
                // encodeIp throws UnsupportedOperationException on malformed input → whole-batch fallback.
                values.setString(doc, encodeIp(utf8Value));
                // TODO: Implement ignore malformed.
            }
        }

        if (values.isEmpty() == false) {
            // Select the DV field type that matches the row path: SORTED_SET + RANGE when the skip-index
            // feature is active, plain SORTED_SET otherwise. DocValuesFieldFactory.addSortedField (the row
            // path) branches on the same hasSkipper predicate.
            final IndexableFieldType dvType = fieldType().indexType.hasDocValuesSkipper()
                ? SORTED_SET_DV_INDEXED_FIELD_TYPE
                : SORTED_SET_DV_FIELD_TYPE;
            // Note: points and skippers never co-occur for ip. IndexType.skippers() has hasPoints()==false;
            // IndexType.points() has hasDocValuesSkipper()==false. So IP_POINT_FIELD_TYPE is only emitted
            // alongside SORTED_SET_DV_FIELD_TYPE (no skipper), never alongside SORTED_SET_DV_INDEXED_FIELD_TYPE.
            final EscfColumnData data = values.finish(docCount);
            ctx.addColumn(LuceneBinaryColumn.of(data, fieldType().name(), dvType));
            if (fieldType().indexType.hasPoints()) {
                ctx.addColumn(LuceneBinaryColumn.of(data, fieldType().name(), IP_POINT_FIELD_TYPE));
            }
        }
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        ESInetAddressPoint address;
        XContentString value = context.parser().optimizedTextOrNull();
        try {
            address = value == null
                ? nullValue == null ? null : new ESInetAddressPoint(fieldType().name(), nullValue)
                : new ESInetAddressPoint(fieldType().name(), value);
        } catch (IllegalArgumentException e) {
            if (ignoreMalformed) {
                context.addIgnoredField(fieldType().name());
                if (storeIgnored) {
                    // Save a copy of the field so synthetic source can load it
                    FallbackPostMapper.capture(context, fullPath(), FallbackPostMapper.Reason.MALFORMED);
                }
                return;
            } else {
                throw e;
            }
        }
        if (address != null) {
            indexValue(context, address);
        }
        if (fieldType().usesArrayOrderBinaryDocValues()) {
            // In-order path: non-null values are recorded in indexValue (in document order); here we record null slots so their position
            // is preserved. Non-null values record no slot here as they are already recorded above.
            if (address == null) {
                MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.recordNull(context.doc(), fieldType().name());
            }
        } else if (FieldArrayContext.shouldRecordOffsets(context, offsetsFieldName, docValuesParameters.multiValue())) {
            if (address != null) {
                BytesRef sortableValue = address.binaryValue();
                context.getOffSetContext().recordOffset(offsetsFieldName, sortableValue);
            } else {
                context.getOffSetContext().recordNull(offsetsFieldName);
            }
        }
    }

    private void indexValue(DocumentParserContext context, ESInetAddressPoint address) {
        if (writeDimensionRouting) {
            context.getRoutingFields().addIp(fieldType().name(), address.getInetAddress());
        }
        LuceneDocument doc = context.doc();
        if (fieldType().indexType.hasPoints()) {
            doc.add(address);
        }
        if (fieldType().indexType.hasDocValues()) {
            if (fieldType().usesBinaryDocValues()) {
                assert fieldType().indexType.hasDocValuesSkipper() == false : "skippers are not supported for binary doc values";
                if (fieldType().usesArrayOrderBinaryDocValues()) {
                    // In-order path: write the value into the field's own binary doc-values column directly, in document order with nulls.
                    if (context.isPartOfArray() == false) {
                        MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.recordSingleValue(
                            doc,
                            fieldType().name(),
                            address.binaryValue()
                        );
                    } else {
                        MultiValuedBinaryDocValuesField.ArrayOrderInlineNull.recordValue(doc, fieldType().name(), address.binaryValue());
                    }
                } else {
                    dvFactory.addBinaryField(
                        doc,
                        fieldType().name(),
                        address.binaryValue(),
                        MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
                    );
                }
            } else {
                dvFactory.addSortedField(doc, fieldType().name(), address.binaryValue());
            }
        } else if (stored || indexed) {
            context.addToFieldNames(fieldType().name());
        }
        if (stored) {
            doc.add(new StoredField(fieldType().name(), address.binaryValue()));
        }
    }

    @Override
    protected void indexScriptValues(
        SearchLookup searchLookup,
        LeafReaderContext readerContext,
        int doc,
        DocumentParserContext documentParserContext
    ) {
        this.scriptValues.valuesForDoc(
            searchLookup,
            readerContext,
            doc,
            value -> indexValue(documentParserContext, new ESInetAddressPoint(fieldType().name(), value))
        );
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName(), scriptCompiler, indexSettings).dimension(dimension).init(this);
    }

    @Override
    public void doValidate(MappingLookup lookup) {
        if (dimension && null != lookup.nestedLookup().getNestedParent(fullPath())) {
            throw new IllegalArgumentException(
                TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM + " can't be configured in nested field [" + fullPath() + "]"
            );
        }
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        if (docValuesParameters.enabled()) {
            return new SyntheticSourceSupport.Native(() -> {
                var layers = new ArrayList<CompositeSyntheticFieldLoader.Layer>(2);
                if (fieldType().usesBinaryDocValues() == false) {
                    if (offsetsFieldName != null) {
                        layers.add(
                            new SortedSetWithOffsetsDocValuesSyntheticFieldLoaderLayer(fullPath(), offsetsFieldName, IpFieldMapper::convert)
                        );
                    } else {
                        layers.add(new SortedSetDocValuesSyntheticFieldLoaderLayer(fullPath()) {
                            @Override
                            protected BytesRef convert(BytesRef value) {
                                return IpFieldMapper.convert(value);
                            }

                            @Override
                            protected BytesRef preserve(BytesRef value) {
                                // No need to copy because convert has made a deep copy
                                return value;
                            }
                        });
                    }
                } else {
                    if (fieldType().usesArrayOrderBinaryDocValues()) {
                        layers.add(new ArrayOrderBinaryDocValuesSyntheticFieldLoaderLayer(fullPath(), IpFieldMapper::convert));
                    } else {
                        layers.add(new BinaryDocValuesSyntheticFieldLoaderLayer(fullPath(), indexSettings.getIndexVersionCreated()) {
                            @Override
                            protected void writeValue(XContentBuilder b, BytesRef value) throws IOException {
                                BytesRef converted = IpFieldMapper.convert(value);
                                b.utf8Value(converted.bytes, converted.offset, converted.length);
                            }
                        });
                    }
                }

                if (ignoreMalformed) {
                    layers.add(CompositeSyntheticFieldLoader.malformedValuesLayer(fullPath(), indexSettings.getIndexVersionCreated()));
                }
                if (onFailureColumnEnabled()) {
                    layers.add(CompositeSyntheticFieldLoader.onFailureValuesLayer(fullPath(), indexSettings.getIndexVersionCreated()));
                }
                return new CompositeSyntheticFieldLoader(leafName(), fullPath(), layers);
            });
        }

        return super.syntheticSourceSupport();
    }

    static BytesRef convert(BytesRef value) {
        byte[] bytes = Arrays.copyOfRange(value.bytes, value.offset, value.offset + value.length);
        return new BytesRef(NetworkAddress.format(InetAddressPoint.decode(bytes)));
    }

    @Override
    public String getOffsetFieldName() {
        return offsetsFieldName;
    }

    @Override
    public boolean storesArrayValuesInOrder() {
        return fieldType().usesArrayOrderBinaryDocValues();
    }
}
