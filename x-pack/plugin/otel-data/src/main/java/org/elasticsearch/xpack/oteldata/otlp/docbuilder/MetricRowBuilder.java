/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import io.opentelemetry.proto.resource.v1.Resource;

import com.google.protobuf.ByteString;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.DocumentBatchRowBuilder;
import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.BufferedMurmur3Hasher;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPoint;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.ExponentialHistogramConverter;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TDigestConverter;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Builds metric documents directly into a {@link DocumentBatchRowBuilder},
 * bypassing XContent serialization. Mirrors the logic of {@link MetricDocumentBuilder}
 * and {@link OTelDocumentBuilder} but writes to row-format columns.
 */
public class MetricRowBuilder {

    private final DocumentBatchRowBuilder rowBuilder;
    private final BufferedByteStringAccessor byteStringAccessor;
    private final MappingHints defaultMappingHints;
    private final BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0);
    private final ExponentialHistogramConverter.BucketBuffer scratch = new ExponentialHistogramConverter.BucketBuffer();

    public MetricRowBuilder(
        DocumentBatchRowBuilder rowBuilder,
        BufferedByteStringAccessor byteStringAccessor,
        MappingHints defaultMappingHints
    ) {
        this.rowBuilder = rowBuilder;
        this.byteStringAccessor = byteStringAccessor;
        this.defaultMappingHints = defaultMappingHints;
    }

    /**
     * Builds a metric document row for the given data point group.
     * The caller must have already called {@link DocumentBatchRowBuilder#startDocument()}.
     *
     * @return the TSID BytesRef for this document
     */
    public BytesRef buildMetricRow(
        DataPointGroupingContext.DataPointGroup dataPointGroup,
        Map<String, String> dynamicTemplates,
        Map<String, Map<String, String>> dynamicTemplateParams
    ) throws IOException {
        List<DataPoint> dataPoints = dataPointGroup.dataPoints();

        // @timestamp
        rowBuilder.setLong("@timestamp", TimeUnit.NANOSECONDS.toMillis(dataPointGroup.getTimestampUnixNano()), false);

        // start_timestamp
        if (dataPointGroup.getStartTimestampUnixNano() != 0) {
            rowBuilder.setLong("start_timestamp", TimeUnit.NANOSECONDS.toMillis(dataPointGroup.getStartTimestampUnixNano()), false);
        }

        // Resource fields
        buildResource(dataPointGroup.resource(), dataPointGroup.resourceSchemaUrl());

        // Data stream fields
        buildDataStream(dataPointGroup.targetIndex());

        // Scope fields
        buildScope(dataPointGroup.scope(), dataPointGroup.scopeSchemaUrl());

        // Attributes
        buildAttributes(dataPointGroup.dataPointAttributes());

        // unit
        if (Strings.hasLength(dataPointGroup.unit())) {
            rowBuilder.setString("unit", dataPointGroup.unit(), false);
        }

        // _metric_names_hash
        String metricNamesHash = dataPointGroup.getMetricNamesHash(hasher);
        rowBuilder.setString("_metric_names_hash", metricNamesHash, false);

        // Metric values
        long docCount = 0;
        for (int i = 0, dataPointsSize = dataPoints.size(); i < dataPointsSize; i++) {
            DataPoint dataPoint = dataPoints.get(i);
            MappingHints mappingHints = defaultMappingHints.withConfigFromAttributes(dataPoint.getAttributes());
            buildMetricValue(dataPoint, mappingHints);

            String dynamicTemplate = dataPoint.getDynamicTemplate(mappingHints);
            if (dynamicTemplate != null) {
                String metricFieldPath = "metrics." + dataPoint.getMetricName();
                dynamicTemplates.put(metricFieldPath, dynamicTemplate);
                if (dataPointGroup.unit() != null && dataPointGroup.unit().isEmpty() == false) {
                    dynamicTemplateParams.put(metricFieldPath, Map.of("unit", dataPointGroup.unit()));
                }
            }
            if (mappingHints.docCount()) {
                docCount = dataPoint.getDocCount();
            }
        }

        if (docCount > 0) {
            rowBuilder.setLong("_doc_count", docCount, false);
        }

        TsidBuilder tsidBuilder = dataPointGroup.tsidBuilder();
        tsidBuilder.addStringDimension("_metric_names_hash", metricNamesHash);
        return tsidBuilder.buildTsid();
    }

    private void buildResource(Resource resource, ByteString schemaUrl) {
        setByteStringIfNotEmpty("resource.schema_url", schemaUrl, true);
        int droppedCount = resource.getDroppedAttributesCount();
        if (droppedCount > 0) {
            rowBuilder.setLong("resource.dropped_attributes_count", droppedCount, true);
        }
        buildKeyValueAttributes("resource.attributes.", resource.getAttributesList());
    }

    private void buildDataStream(TargetIndex targetIndex) {
        if (targetIndex.isDataStream() == false) {
            return;
        }
        rowBuilder.setString("data_stream.type", targetIndex.type(), true);
        rowBuilder.setString("data_stream.dataset", targetIndex.dataset(), true);
        rowBuilder.setString("data_stream.namespace", targetIndex.namespace(), true);
    }

    private void buildScope(InstrumentationScope scope, ByteString schemaUrl) {
        setByteStringIfNotEmpty("scope.schema_url", schemaUrl, true);
        setByteStringIfNotEmpty("scope.name", scope.getNameBytes(), true);
        setByteStringIfNotEmpty("scope.version", scope.getVersionBytes(), true);
        int droppedCount = scope.getDroppedAttributesCount();
        if (droppedCount > 0) {
            rowBuilder.setLong("scope.dropped_attributes_count", droppedCount, true);
        }
        buildKeyValueAttributes("scope.attributes.", scope.getAttributesList());
    }

    private void buildAttributes(List<KeyValue> attributes) {
        buildKeyValueAttributes("attributes.", attributes);
    }

    private void buildKeyValueAttributes(String prefix, List<KeyValue> attributes) {
        for (int i = 0, size = attributes.size(); i < size; i++) {
            KeyValue attribute = attributes.get(i);
            String key = attribute.getKey();
            if (OTelDocumentBuilder.isIgnoredAttribute(key) == false) {
                setAnyValue(prefix + key, attribute.getValue());
            }
        }
    }

    private void setAnyValue(String path, AnyValue value) {
        switch (value.getValueCase()) {
            case STRING_VALUE -> {
                ByteString bs = value.getStringValueBytes();
                byte[] buf = byteStringAccessor.toBytes(bs);
                rowBuilder.setString(path, buf, 0, bs.size(), true);
            }
            case BOOL_VALUE -> rowBuilder.setBoolean(path, value.getBoolValue(), true);
            case INT_VALUE -> rowBuilder.setLong(path, value.getIntValue(), true);
            case DOUBLE_VALUE -> rowBuilder.setDouble(path, value.getDoubleValue(), true);
            case ARRAY_VALUE -> {
                // Serialize array as XContent JSON bytes
                try {
                    BytesReference arrayBytes = serializeArrayAsXContent(value);
                    rowBuilder.setXContentArray(path, arrayBytes, true);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to serialize array value for " + path, e);
                }
            }
            case KVLIST_VALUE -> {
                // Flatten key-value list with dot-prefix
                List<KeyValue> kvList = value.getKvlistValue().getValuesList();
                for (int i = 0, size = kvList.size(); i < size; i++) {
                    KeyValue kv = kvList.get(i);
                    setAnyValue(path + "." + kv.getKey(), kv.getValue());
                }
            }
            default -> throw new IllegalArgumentException("Unsupported attribute value type: " + value.getValueCase());
        }
    }

    private BytesReference serializeArrayAsXContent(AnyValue arrayValue) throws IOException {
        try (XContentBuilder xBuilder = XContentFactory.contentBuilder(XContentType.JSON)) {
            xBuilder.startArray();
            List<AnyValue> values = arrayValue.getArrayValue().getValuesList();
            for (int i = 0, size = values.size(); i < size; i++) {
                writeAnyValueToXContent(xBuilder, values.get(i));
            }
            xBuilder.endArray();
            return BytesReference.bytes(xBuilder);
        }
    }

    private void writeAnyValueToXContent(XContentBuilder builder, AnyValue value) throws IOException {
        switch (value.getValueCase()) {
            case STRING_VALUE -> byteStringAccessor.utf8Value(builder, value.getStringValueBytes());
            case BOOL_VALUE -> builder.value(value.getBoolValue());
            case INT_VALUE -> builder.value(value.getIntValue());
            case DOUBLE_VALUE -> builder.value(value.getDoubleValue());
            case ARRAY_VALUE -> {
                builder.startArray();
                List<AnyValue> values = value.getArrayValue().getValuesList();
                for (int i = 0, size = values.size(); i < size; i++) {
                    writeAnyValueToXContent(builder, values.get(i));
                }
                builder.endArray();
            }
            default -> throw new IllegalArgumentException("Unsupported attribute value type: " + value.getValueCase());
        }
    }

    private void buildMetricValue(DataPoint dataPoint, MappingHints mappingHints) throws IOException {
        if (dataPoint instanceof DataPoint.Number numberDp) {
            buildNumberMetricValue(numberDp);
        } else if (dataPoint instanceof DataPoint.ExponentialHistogram expHistDp) {
            buildExponentialHistogramMetricValue(expHistDp, mappingHints);
        } else if (dataPoint instanceof DataPoint.Histogram histDp) {
            buildHistogramMetricValue(histDp, mappingHints);
        } else if (dataPoint instanceof DataPoint.Summary summaryDp) {
            buildSummaryMetricValue(summaryDp);
        }
    }

    private void buildNumberMetricValue(DataPoint.Number numberDp) {
        NumberDataPoint dp = numberDp.dataPoint();
        String metricPath = "metrics." + numberDp.getMetricName();
        switch (dp.getValueCase()) {
            case AS_DOUBLE -> rowBuilder.setDouble(metricPath, dp.getAsDouble(), true);
            case AS_INT -> rowBuilder.setLong(metricPath, dp.getAsInt(), true);
        }
    }

    private void buildExponentialHistogramMetricValue(DataPoint.ExponentialHistogram expHistDp, MappingHints mappingHints)
        throws IOException {
        String metricPath = "metrics." + expHistDp.getMetricName();
        ExponentialHistogramDataPoint dp = expHistDp.dataPoint();
        switch (mappingHints.histogramMapping()) {
            case AGGREGATE_METRIC_DOUBLE -> buildAggregateMetricDouble(metricPath, dp.getSum(), dp.getCount());
            case TDIGEST -> buildExpHistTDigest(metricPath, dp);
            case EXPONENTIAL_HISTOGRAM -> buildExpHistAsExpHist(metricPath, dp);
        }
    }

    private void buildHistogramMetricValue(DataPoint.Histogram histDp, MappingHints mappingHints) throws IOException {
        String metricPath = "metrics." + histDp.getMetricName();
        HistogramDataPoint dp = histDp.dataPoint();
        switch (mappingHints.histogramMapping()) {
            case AGGREGATE_METRIC_DOUBLE -> buildAggregateMetricDouble(metricPath, dp.getSum(), dp.getCount());
            case TDIGEST -> buildExplicitHistTDigest(metricPath, dp);
            case EXPONENTIAL_HISTOGRAM -> buildExplicitHistAsExpHist(metricPath, dp);
        }
    }

    private void buildSummaryMetricValue(DataPoint.Summary summaryDp) throws IOException {
        SummaryDataPoint dp = summaryDp.dataPoint();
        String metricPath = "metrics." + summaryDp.getMetricName();
        buildAggregateMetricDouble(metricPath, dp.getSum(), dp.getCount());
    }

    private void buildAggregateMetricDouble(String metricPath, double sum, long valueCount) throws IOException {
        BytesReference aggBytes;
        try (XContentBuilder xb = XContentFactory.contentBuilder(XContentType.JSON)) {
            xb.startObject();
            xb.field("sum", sum);
            xb.field("value_count", valueCount);
            xb.endObject();
            aggBytes = BytesReference.bytes(xb);
        }
        rowBuilder.setBinary(metricPath, aggBytes, true);
    }

    /**
     * Builds a TDigest representation of an exponential histogram as a single XContent object.
     */
    private void buildExpHistTDigest(String metricPath, ExponentialHistogramDataPoint dp) throws IOException {
        BytesReference histBytes;
        try (XContentBuilder xb = XContentFactory.contentBuilder(XContentType.JSON)) {
            xb.startObject();
            xb.startArray("counts");
            TDigestConverter.counts(dp, xb::value);
            xb.endArray();
            xb.startArray("values");
            TDigestConverter.centroidValues(dp, xb::value);
            xb.endArray();
            xb.endObject();
            histBytes = BytesReference.bytes(xb);
        }
        rowBuilder.setBinary(metricPath, histBytes, true);
    }

    /**
     * Builds a TDigest representation of an explicit bucket histogram as a single XContent object.
     */
    private void buildExplicitHistTDigest(String metricPath, HistogramDataPoint dp) throws IOException {
        BytesReference histBytes;
        try (XContentBuilder xb = XContentFactory.contentBuilder(XContentType.JSON)) {
            xb.startObject();
            xb.startArray("counts");
            TDigestConverter.counts(dp, xb::value);
            xb.endArray();
            xb.startArray("values");
            TDigestConverter.centroidValues(dp, xb::value);
            xb.endArray();
            xb.endObject();
            histBytes = BytesReference.bytes(xb);
        }
        rowBuilder.setBinary(metricPath, histBytes, true);
    }

    /**
     * Builds an OTLP exponential histogram as row columns.
     * Complex nested structure serialized as XContent for histogram-specific sub-fields.
     */
    private void buildExpHistAsExpHist(String metricPath, ExponentialHistogramDataPoint dp) throws IOException {
        // Use XContent to serialize the whole exponential histogram value, then store as XCONTENT_ARRAY
        // This is the simplest approach since exponential histograms have deeply nested structures
        BytesReference histBytes;
        try (XContentBuilder xb = XContentFactory.contentBuilder(XContentType.JSON)) {
            ExponentialHistogramConverter.buildExponentialHistogram(dp, xb);
            histBytes = BytesReference.bytes(xb);
        }
        rowBuilder.setBinary(metricPath, histBytes, true);
    }

    /**
     * Builds an explicit bucket histogram converted to exponential histogram format as row columns.
     */
    private void buildExplicitHistAsExpHist(String metricPath, HistogramDataPoint dp) throws IOException {
        BytesReference histBytes;
        try (XContentBuilder xb = XContentFactory.contentBuilder(XContentType.JSON)) {
            ExponentialHistogramConverter.buildExponentialHistogram(dp, xb, scratch);
            histBytes = BytesReference.bytes(xb);
        }
        rowBuilder.setBinary(metricPath, histBytes, true);
    }

    private void setByteStringIfNotEmpty(String path, ByteString value, boolean fromObject) {
        if (value != null && value.isEmpty() == false) {
            byte[] buf = byteStringAccessor.toBytes(value);
            rowBuilder.setString(path, buf, 0, value.size(), fromObject);
        }
    }
}
