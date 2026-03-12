/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsPartialSuccess;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.DocumentBatchRowBuilder;
import org.elasticsearch.action.bulk.RowDocumentBatch;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.oteldata.OTelPlugin;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MetricRowBuilder;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Transport action for handling OpenTelemetry Protocol (OTLP) Metrics requests.
 * This action processes the incoming metrics data, groups data points, and invokes the
 * appropriate Elasticsearch bulk indexing operations to store the metrics.
 * It also handles the response according to the OpenTelemetry Protocol specifications,
 * including success, partial success responses, and errors due to bad data or server errors.
 *
 * @see <a href="https://opentelemetry.io/docs/specs/otlp">OTLP Specification</a>
 */
public class OTLPMetricsTransportAction extends AbstractOTLPTransportAction {

    public static final String NAME = "indices:data/write/otlp/metrics";
    public static final ActionType<OTLPActionResponse> TYPE = new ActionType<>(NAME);

    // visible for testing
    volatile MappingHints defaultMappingHints;

    @Inject
    public OTLPMetricsTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client,
        ClusterService clusterService
    ) {
        super(NAME, transportService, actionFilters, threadPool, client);
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        defaultMappingHints = MappingHints.fromSettings(clusterSettings.get(OTelPlugin.USE_EXPONENTIAL_HISTOGRAM_FIELD_TYPE));
        clusterSettings.addSettingsUpdateConsumer(OTelPlugin.USE_EXPONENTIAL_HISTOGRAM_FIELD_TYPE, histogramFieldTypeSetting -> {
            defaultMappingHints = MappingHints.fromSettings(histogramFieldTypeSetting);
        });
    }

    @Override
    protected ProcessingContext prepareBulkRequest(OTLPActionRequest request, BulkRequestBuilder bulkRequestBuilder) throws IOException {
        BufferedByteStringAccessor byteStringAccessor = new BufferedByteStringAccessor();
        DataPointGroupingContext context = new DataPointGroupingContext(byteStringAccessor);
        var metricsServiceRequest = ExportMetricsServiceRequest.parseFrom(request.getRequest().streamInput());
        context.groupDataPoints(metricsServiceRequest);
        if (context.totalDataPoints() == 0) {
            return context;
        }

        DocumentBatchRowBuilder rowBuilder = new DocumentBatchRowBuilder();
        MetricRowBuilder metricRowBuilder = new MetricRowBuilder(rowBuilder, byteStringAccessor, defaultMappingHints);
        List<IndexRequest> indexRequests = new ArrayList<>();

        context.consume(dataPointGroup -> {
            rowBuilder.startDocument();
            var dynamicTemplates = Maps.<String, String>newHashMapWithExpectedSize(dataPointGroup.dataPoints().size());
            var dynamicTemplateParams = Maps.<String, Map<String, String>>newHashMapWithExpectedSize(dataPointGroup.dataPoints().size());
            BytesRef tsid = metricRowBuilder.buildMetricRow(dataPointGroup, dynamicTemplates, dynamicTemplateParams);
            rowBuilder.endDocument();

            long timestampMillis = TimeUnit.NANOSECONDS.toMillis(dataPointGroup.getTimestampUnixNano());
            IndexRequest indexRequest = new IndexRequest(dataPointGroup.targetIndex().index()).opType(DocWriteRequest.OpType.CREATE)
                .setRequireDataStream(true)
                .source("{}", XContentType.CBOR)
                .tsid(tsid)
                .setIncludeSourceOnError(false)
                .setDynamicTemplates(dynamicTemplates)
                .setDynamicTemplateParams(dynamicTemplateParams);
            indexRequest.setRawTimestamp(timestampMillis);
            indexRequests.add(indexRequest);
            bulkRequestBuilder.add(indexRequest);
        });

        // Build the batch and wire it up
        RowDocumentBatch batch = rowBuilder.build();
        for (int i = 0; i < indexRequests.size(); i++) {
            indexRequests.get(i).setBatchRowIndex(i);
            indexRequests.get(i).setBatchRef(batch);
        }
        bulkRequestBuilder.setRowDocumentBatch(batch);

        for (int i = 0; i < indexRequests.size(); i++) {
            IndexRequest ir = indexRequests.get(i);
        }

        return context;
    }

    @Override
    protected ExportMetricsServiceResponse responseWithRejectedDataPoints(int rejectedDataPoints, String message) {
        ExportMetricsPartialSuccess partialSuccess = ExportMetricsPartialSuccess.newBuilder()
            .setRejectedDataPoints(rejectedDataPoints)
            .setErrorMessage(message)
            .build();
        return ExportMetricsServiceResponse.newBuilder().setPartialSuccess(partialSuccess).build();
    }
}
