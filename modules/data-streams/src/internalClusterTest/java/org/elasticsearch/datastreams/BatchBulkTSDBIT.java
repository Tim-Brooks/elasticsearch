/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.ShardBatchIndexer;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.junit.ClassRule;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 1)
public class BatchBulkTSDBIT extends ESIntegTestCase {

    @ClassRule
    public static TestRule snapshotBuildRule = (base, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            assumeTrue("batch indexing requires snapshot builds", Build.current().isSnapshot());
            base.evaluate();
        }
    };

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true)
            .build();
    }

    private String findCoordinatingNode() {
        for (String nodeName : internalCluster().getNodeNames()) {
            if (internalCluster().clusterService(nodeName).localNode().canContainData() == false
                && internalCluster().clusterService(nodeName).localNode().isMasterNode() == false) {
                return nodeName;
            }
        }
        return internalCluster().getNodeNames()[internalCluster().getNodeNames().length - 1];
    }

    private void createTsdbTemplate(String dataStreamName) throws IOException {
        // Use a long dimension as a minimal control: a time_series index with one numeric
        // dimension exercises the core TSDB+columnar path without relying on keyword SortedSet support.
        String mapping = """
            {
                "properties": {
                    "@timestamp": {
                        "type": "date"
                    },
                    "series_id": {
                        "type": "long",
                        "time_series_dimension": true
                    }
                }
            }
            """;
        var request = new TransportPutComposableIndexTemplateAction.Request(dataStreamName + "-template");
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStreamName + "*"))
                .template(
                    new Template(
                        Settings.builder()
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 0)
                            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                            .build(),
                        CompressedXContent.fromJSON(mapping),
                        null
                    )
                )
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request));
    }

    public void testTimestampOnlyTsdbColumnarBatchMode() throws IOException {
        String dataStreamName = "test-tsdb-batch-ds";
        createTsdbTemplate(dataStreamName);

        var createRequest = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createRequest).actionGet());
        ensureGreen(dataStreamName);

        String coordinatingNode = findCoordinatingNode();
        int numDocs = randomIntBetween(10, 50);
        // Use current time so documents fall inside the backing index's auto-computed time range.
        Instant baseTime = Instant.now();

        // Warm-up bulk: the first batch may fall back to the row path while the mapping is being
        // established. Send it without MockLog assertions so the mapping settles.
        BulkRequest warmUp = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            warmUp.add(
                new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE)
                    .source(Map.of("@timestamp", baseTime.minusSeconds(numDocs - i).toEpochMilli(), "series_id", 1L))
            );
        }
        BulkResponse warmUpResponse = client(coordinatingNode).bulk(warmUp).actionGet();
        assertNoFailures(warmUpResponse);
        assertThat(warmUpResponse.getItems().length, equalTo(numDocs));

        // Main bulk: mapping is established; this batch must go through the columnar path.
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(
                new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE)
                    .source(Map.of("@timestamp", baseTime.plusSeconds(i).toEpochMilli(), "series_id", 1L))
            );
        }

        final Logger batchLogger = LogManager.getLogger(ShardBatchIndexer.class);
        final Logger resolverLogger = LogManager.getLogger(ShardBatchMapper.class);
        final Level origBatchLevel = batchLogger.getLevel();
        final Level origResolverLevel = resolverLogger.getLevel();
        Loggers.setLevel(batchLogger, Level.TRACE);
        Loggers.setLevel(resolverLogger, Level.DEBUG);
        try (var mockLog = MockLog.capture(ShardBatchIndexer.class, ShardBatchMapper.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "tsdb columnar batch indexed on primary",
                    ShardBatchIndexer.class.getName(),
                    Level.TRACE,
                    "batch indexed * operations on primary shard *"
                )
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no columnar fallback", ShardBatchMapper.class.getName(), Level.DEBUG, "*disabled*")
            );

            BulkResponse bulkResponse = client(coordinatingNode).bulk(bulkRequest).actionGet();
            assertNoFailures(bulkResponse);
            assertThat(bulkResponse.getItems().length, equalTo(numDocs));

            mockLog.assertAllExpectationsMatched();
        } finally {
            Loggers.setLevel(batchLogger, origBatchLevel);
            Loggers.setLevel(resolverLogger, origResolverLevel);
        }

        refresh(dataStreamName);

        // Both warm-up and main bulk docs must be visible.
        assertResponse(prepareSearch(dataStreamName).setSize(0).setTrackTotalHits(true), response -> {
            assertNoFailures(response);
            assertThat(response.getHits().getTotalHits().value(), equalTo((long) numDocs * 2));
        });
    }

    /**
     * Creates an OTel-metrics-shaped composable index template that exercises the keyword dimension
     * columnar path. The template mirrors the structure resolved by the {@code tsdb-metricsgen} Rally
     * benchmark (metrics-otel@template + otel@mappings + metrics-otel@mappings + ecs-tsdb@mappings),
     * but uses only server-available types (no constant_keyword, version, aggregate_metric_double).
     *
     * <p>Key structural choices that match the benchmark:
     * <ul>
     *   <li>Root {@code dynamic: false} with {@code dynamic: true} only on the passthrough subtrees.</li>
     *   <li>Dimension keywords ({@code unit}, {@code temporality}, {@code _metric_names_hash},
     *       {@code scope.name}) + passthrough objects that dynamically map attribute strings as keyword
     *       dimensions via the {@code all_strings_to_keywords} dynamic template.</li>
     *   <li>No explicit {@code routing_path}: {@code DataStreamIndexSettingsProvider} derives
     *       {@code index.dimensions} because no OTel dynamic template sets {@code time_series_dimension}.
     *       This is the routing precondition required for columnar batch mode.</li>
     *   <li>Per-document dynamic templates for metric fields (counter/gauge long/double).</li>
     * </ul>
     *
     * <p>Note: {@code resource.attributes.host.ip} is an ip dimension exercising the SORTED_SET +
     * RANGE-skip-index columnar path (TSDB default). Points are unreachable in TSDB because
     * {@code useTimeSeriesDocValuesSkippers} is checked before {@code indexed}; adding
     * {@code "index": true} to the template would be a silently-vacuous test.
     */
    private void createOtelShapedTsdbTemplate(String dataStreamName) throws IOException {
        String mapping = """
            {
                "dynamic": false,
                "date_detection": false,
                "properties": {
                    "@timestamp": { "type": "date" },
                    "start_timestamp": { "type": "date" },
                    "_metric_names_hash": { "type": "keyword", "time_series_dimension": true },
                    "unit": { "type": "keyword", "time_series_dimension": true, "ignore_above": 1024 },
                    "temporality": { "type": "keyword", "time_series_dimension": true },
                    "metrics": { "type": "passthrough", "dynamic": true, "priority": 10 },
                    "attributes": {
                        "type": "passthrough", "dynamic": true,
                        "priority": 20, "time_series_dimension": true
                    },
                    "scope": {
                        "properties": {
                            "name": { "type": "keyword", "time_series_dimension": true, "ignore_above": 1024 },
                            "attributes": {
                                "type": "passthrough", "dynamic": true,
                                "priority": 30, "time_series_dimension": true
                            }
                        }
                    },
                    "resource": {
                        "properties": {
                            "attributes": {
                                "type": "passthrough", "dynamic": true,
                                "priority": 40, "time_series_dimension": true
                            }
                        }
                    }
                },
                "dynamic_templates": [
                    { "ecs_ip": {
                        "match_mapping_type": "string",
                        "path_match": ["ip", "*.ip", "*_ip"],
                        "mapping": { "type": "ip" }
                    }},
                    { "all_strings_to_keywords": {
                        "match_mapping_type": "string",
                        "mapping": { "type": "keyword", "ignore_above": 1024 }
                    }},
                    { "counter_long":   { "mapping": { "type": "long",   "time_series_metric": "counter" } } },
                    { "gauge_long":     { "mapping": { "type": "long",   "time_series_metric": "gauge"   } } },
                    { "counter_double": { "mapping": { "type": "double", "time_series_metric": "counter" } } },
                    { "gauge_double":   { "mapping": { "type": "double", "time_series_metric": "gauge"   } } }
                ]
            }
            """;
        var request = new TransportPutComposableIndexTemplateAction.Request(dataStreamName + "-template");
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStreamName + "*"))
                .template(
                    new Template(
                        Settings.builder()
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 0)
                            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                            .put("index.mapping.ignore_malformed", true)
                            .put("index.mapping.total_fields.limit", 10000)
                            .build(),
                        CompressedXContent.fromJSON(mapping),
                        null
                    )
                )
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request));
    }

    /** Build a hostmetrics-shaped source doc: one metric with string attributes as dimensions. */
    private static Map<String, Object> otelMetricsDoc(long timestampMillis) {
        return Map.of(
            "@timestamp",
            timestampMillis,
            "_metric_names_hash",
            "system.cpu.time",
            "unit",
            "s",
            "temporality",
            "cumulative",
            "resource",
            Map.of("attributes", Map.of("host.name", "host-0", "os.type", "linux", "host.ip", "10.0.0.1")),
            "scope",
            Map.of("name", "otel-receiver"),
            "attributes",
            Map.of("cpu", "cpu0", "state", "idle"),
            "metrics",
            Map.of("system.cpu.time", 12345.6)
        );
    }

    /**
     * Asserts that the routing precondition for columnar batch mode holds: {@code index.dimensions}
     * is set (coordinator computes the tsid, so {@code ForIndexDimensions} is used and every mapper's
     * {@code writeDimensionRouting} is false) and {@code index.routing_path} is empty.
     */
    private void assertIndexDimensionsRouting(String dataStreamName) {
        GetIndexResponse response = safeGet(indicesAdmin().getIndex(new GetIndexRequest(TEST_REQUEST_TIMEOUT).indices(dataStreamName)));
        assertThat("expected exactly one backing index", response.getIndices().length, equalTo(1));
        Settings settings = response.getSettings().get(response.getIndices()[0]);
        assertThat(
            "index.dimensions must be set (ForIndexDimensions routing precondition for columnar batch)",
            IndexMetadata.INDEX_DIMENSIONS.get(settings),
            not(empty())
        );
        assertThat(
            "index.routing_path must be empty when index.dimensions is used",
            IndexMetadata.INDEX_ROUTING_PATH.get(settings),
            empty()
        );
    }

    /**
     * Replicates the essential structure of the {@code tsdb-metricsgen} Rally benchmark: an OTel-shaped
     * TSDB data stream with keyword dimensions under passthrough objects, per-document dynamic templates
     * for metric types, and synthetic source — and asserts that the second bulk (after mappings are
     * established by the warm-up) goes through the columnar batch path without any fallback.
     */
    public void testOtelMetricsShapedTsdbColumnarBatchMode() throws IOException {
        String dataStreamName = "test-otel-metrics-tsdb-batch";
        createOtelShapedTsdbTemplate(dataStreamName);

        var createRequest = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createRequest).actionGet());
        ensureGreen(dataStreamName);

        // Assert the routing precondition: provider derived index.dimensions (not index.routing_path),
        // so ForIndexDimensions is used and every mapper's writeDimensionRouting is false.
        assertIndexDimensionsRouting(dataStreamName);

        String coordinatingNode = findCoordinatingNode();
        int numDocs = randomIntBetween(10, 50);
        Instant baseTime = Instant.now();

        // Warm-up bulk: introduces all fields (dimensions + metric) so the mapping is fully established.
        // The first bulk falls back to the row path for any unmapped leaves encountered for the first time.
        BulkRequest warmUp = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            warmUp.add(
                new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE)
                    .source(otelMetricsDoc(baseTime.minusSeconds(numDocs - i).toEpochMilli()))
                    .setDynamicTemplates(Map.of("metrics.system.cpu.time", "counter_double"))
            );
        }
        BulkResponse warmUpResponse = client(coordinatingNode).bulk(warmUp).actionGet();
        assertNoFailures(warmUpResponse);
        assertThat(warmUpResponse.getItems().length, equalTo(numDocs));

        // Main bulk: all fields are already mapped; this batch must go through the columnar path.
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(
                new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE)
                    .source(otelMetricsDoc(baseTime.plusSeconds(i).toEpochMilli()))
                    .setDynamicTemplates(Map.of("metrics.system.cpu.time", "counter_double"))
            );
        }

        final Logger batchLogger = LogManager.getLogger(ShardBatchIndexer.class);
        final Logger resolverLogger = LogManager.getLogger(ShardBatchMapper.class);
        final Level origBatchLevel = batchLogger.getLevel();
        final Level origResolverLevel = resolverLogger.getLevel();
        Loggers.setLevel(batchLogger, Level.TRACE);
        Loggers.setLevel(resolverLogger, Level.DEBUG);
        try (var mockLog = MockLog.capture(ShardBatchIndexer.class, ShardBatchMapper.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "otel-tsdb columnar batch indexed on primary",
                    ShardBatchIndexer.class.getName(),
                    Level.TRACE,
                    "batch indexed * operations on primary shard *"
                )
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no columnar fallback after warm-up",
                    ShardBatchMapper.class.getName(),
                    Level.DEBUG,
                    "*disabled*"
                )
            );

            BulkResponse bulkResponse = client(coordinatingNode).bulk(bulkRequest).actionGet();
            assertNoFailures(bulkResponse);
            assertThat(bulkResponse.getItems().length, equalTo(numDocs));

            mockLog.assertAllExpectationsMatched();
        } finally {
            Loggers.setLevel(batchLogger, origBatchLevel);
            Loggers.setLevel(resolverLogger, origResolverLevel);
        }

        refresh(dataStreamName);

        // Both warm-up and main bulk docs must be visible.
        assertResponse(prepareSearch(dataStreamName).setSize(0).setTrackTotalHits(true), response -> {
            assertNoFailures(response);
            assertThat(response.getHits().getTotalHits().value(), equalTo((long) numDocs * 2));
        });
    }

    /**
     * Pins the warm-up-then-batch contract from the request: "Dynamic mappings don't work in batch
     * mode but they will establish a set of mappings and then it will work in batch mode after the
     * first few requests."
     *
     * <p>Bulk #1 introduces metric {@code a} → falls back because of an unmapped leaf.
     * Bulk #2 introduces metric {@code b} → falls back for that new leaf.
     * Bulk #3 sends only metrics {@code a} and {@code b} → goes through the columnar path.
     */
    public void testOtelMetricsShapedTsdbBatchesAfterDynamicMappingSettles() throws IOException {
        String dataStreamName = "test-otel-metrics-tsdb-dynamic";
        createOtelShapedTsdbTemplate(dataStreamName);

        var createRequest = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createRequest).actionGet());
        ensureGreen(dataStreamName);

        String coordinatingNode = findCoordinatingNode();
        Instant baseTime = Instant.now();

        // Build a doc with a custom metric field name, varying timestamp and metric name.
        // The attribute dimensions remain constant so all docs are in the same series.
        int batch = 0;

        // Bulk #1: introduces metrics.cpu_time_a (new field → dynamic mapping → row path for this field)
        BulkRequest bulk1 = new BulkRequest();
        for (int i = 0; i < 10; i++) {
            Map<String, Object> doc = Map.of(
                "@timestamp",
                baseTime.minusSeconds(600 - batch * 200L + i).toEpochMilli(),
                "_metric_names_hash",
                "cpu_time_a",
                "unit",
                "s",
                "temporality",
                "cumulative",
                "resource",
                Map.of("attributes", Map.of("host.name", "host-0", "os.type", "linux")),
                "scope",
                Map.of("name", "otel-receiver"),
                "attributes",
                Map.of("cpu", "cpu0", "state", "idle"),
                "metrics",
                Map.of("cpu_time_a", 1.0)
            );
            bulk1.add(
                new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE)
                    .source(doc)
                    .setDynamicTemplates(Map.of("metrics.cpu_time_a", "counter_double"))
            );
        }
        assertNoFailures(client(coordinatingNode).bulk(bulk1).actionGet());
        batch++;

        // Bulk #2: introduces metrics.cpu_time_b (another new field → falls back for that doc)
        BulkRequest bulk2 = new BulkRequest();
        for (int i = 0; i < 10; i++) {
            Map<String, Object> doc = Map.of(
                "@timestamp",
                baseTime.minusSeconds(400 - batch * 200L + i).toEpochMilli(),
                "_metric_names_hash",
                "cpu_time_b",
                "unit",
                "s",
                "temporality",
                "cumulative",
                "resource",
                Map.of("attributes", Map.of("host.name", "host-0", "os.type", "linux")),
                "scope",
                Map.of("name", "otel-receiver"),
                "attributes",
                Map.of("cpu", "cpu0", "state", "idle"),
                "metrics",
                Map.of("cpu_time_b", 2.0)
            );
            bulk2.add(
                new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE)
                    .source(doc)
                    .setDynamicTemplates(Map.of("metrics.cpu_time_b", "counter_double"))
            );
        }
        assertNoFailures(client(coordinatingNode).bulk(bulk2).actionGet());
        batch++;

        // Bulk #3: both cpu_time_a and cpu_time_b are already mapped; must go through the columnar path.
        BulkRequest bulk3 = new BulkRequest();
        for (int i = 0; i < 10; i++) {
            Map<String, Object> doc = Map.of(
                "@timestamp",
                baseTime.plusSeconds(i + batch * 200L).toEpochMilli(),
                "_metric_names_hash",
                i % 2 == 0 ? "cpu_time_a" : "cpu_time_b",
                "unit",
                "s",
                "temporality",
                "cumulative",
                "resource",
                Map.of("attributes", Map.of("host.name", "host-0", "os.type", "linux")),
                "scope",
                Map.of("name", "otel-receiver"),
                "attributes",
                Map.of("cpu", "cpu0", "state", "idle"),
                "metrics",
                Map.of(i % 2 == 0 ? "cpu_time_a" : "cpu_time_b", 3.0)
            );
            String metricName = i % 2 == 0 ? "metrics.cpu_time_a" : "metrics.cpu_time_b";
            String templateName = i % 2 == 0 ? "counter_double" : "counter_double";
            bulk3.add(
                new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE)
                    .source(doc)
                    .setDynamicTemplates(Map.of(metricName, templateName))
            );
        }

        final Logger batchLogger = LogManager.getLogger(ShardBatchIndexer.class);
        final Logger resolverLogger = LogManager.getLogger(ShardBatchMapper.class);
        final Level origBatchLevel = batchLogger.getLevel();
        final Level origResolverLevel = resolverLogger.getLevel();
        Loggers.setLevel(batchLogger, Level.TRACE);
        Loggers.setLevel(resolverLogger, Level.DEBUG);
        try (var mockLog = MockLog.capture(ShardBatchIndexer.class, ShardBatchMapper.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "dynamic-settled tsdb columnar batch indexed on primary",
                    ShardBatchIndexer.class.getName(),
                    Level.TRACE,
                    "batch indexed * operations on primary shard *"
                )
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no columnar fallback after mapping settled",
                    ShardBatchMapper.class.getName(),
                    Level.DEBUG,
                    "*disabled*"
                )
            );

            assertNoFailures(client(coordinatingNode).bulk(bulk3).actionGet());
            mockLog.assertAllExpectationsMatched();
        } finally {
            Loggers.setLevel(batchLogger, origBatchLevel);
            Loggers.setLevel(resolverLogger, origResolverLevel);
        }

        refresh(dataStreamName);

        // All 30 docs (bulk1 + bulk2 + bulk3) must be visible.
        assertResponse(prepareSearch(dataStreamName).setSize(0).setTrackTotalHits(true), response -> {
            assertNoFailures(response);
            assertThat(response.getHits().getTotalHits().value(), equalTo(30L));
        });
    }
}
