/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.apache.logging.log4j.Level;
import org.elasticsearch.Build;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.ShardBatchIndexer;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assume.assumeTrue;

/**
 * Exercises the columnar bulk batch-indexing fast path for the "textbench" scenario: a
 * {@code logsdb_columnar} OTEL-logs index sorted on {@code ServiceName, Body.template_id, @timestamp},
 * combining {@code keyword}, {@code byte}, {@code date_nanos} and — the field this test primarily
 * covers — {@code pattern_text} ({@code Body}).
 *
 * <p>The {@code flattened} fields from the real textbench template ({@code ResourceAttributes},
 * {@code ScopeAttributes}, {@code LogAttributes}) are intentionally commented out: {@code flattened}
 * columnar batch support is a follow-up and will get its own dedicated test.
 *
 * <p>A {@link MockLog} expectation asserts that {@code pattern_text} does not force the batch to fall
 * back to the row-major path, i.e. that the {@code pattern_text} mapper participates in the columnar path.
 */
public class TextBenchBatchIndexingIT extends ESSingleNodeTestCase {

    private static final String[] SERVICES = { "frontend", "cart", "checkout" };

    // A mix of templates with and without numeric arguments, to exercise pattern_text template/args extraction.
    private static final String[] BODIES = {
        "Failed to place order",      // no args
        "User 42 logged in",          // one arg
        "Request completed in 128 ms", // one arg
        "Connection reset by peer" };  // no args

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(InternalSettingsPlugin.class, XPackPlugin.class, LogsDBPlugin.class, DataStreamsPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(ShardBatchIndexer.BATCH_INDEXING.getKey(), true)
            .put("cluster.logsdb.enabled", "true")
            .put("cluster.logsdb_columnar.enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled())
            // A trial license grants the enterprise features pattern_text templating requires.
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            .build();
    }

    public void testTextBenchScenarioViaBatchMode() throws Exception {
        assumeTrue("batch indexing requires snapshot builds", Build.current().isSnapshot());
        assumeTrue("columnar index mode feature flag must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());

        final String index = "otel_logs-textbench";
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.mode", "logsdb_columnar")
                        .putList("index.sort.field", "ServiceName", "Body.template_id", "@timestamp")
                        .putList("index.sort.order", "asc", "asc", "desc")
                )
                .setMapping(textBenchMapping())
        );
        ensureGreen(index);

        final int numDocs = 60;
        int expectedOrderMatches = 0;
        final BulkRequest bulkRequest = new BulkRequest();
        Instant timestamp = Instant.parse("2025-09-23T00:00:00Z");
        for (int i = 0; i < numDocs; i++) {
            final String body = BODIES[i % BODIES.length];
            if (body.contains("order")) {
                expectedOrderMatches++;
            }
            bulkRequest.add(new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE).id("doc-" + i).source(doc(i, timestamp, body)));
            timestamp = timestamp.plusSeconds(1);
        }

        // The MockLog expectations prove pattern_text went through the columnar batch path: a mapper that did
        // not support batch indexing would log "does not support batch indexing" (resolveMappers), and any
        // assembly failure would log "failed to assemble column batch" (mapColumnBatch). Neither must fire.
        try (var mockLog = MockLog.capture(ShardBatchMapper.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "pattern_text stays on the columnar path",
                    ShardBatchMapper.class.getCanonicalName(),
                    Level.DEBUG,
                    "*does not support batch indexing*"
                )
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no columnar fallback",
                    ShardBatchMapper.class.getCanonicalName(),
                    Level.WARN,
                    "*failed to assemble column batch*"
                )
            );
            final BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
            assertNoFailures(bulkResponse);
            mockLog.assertAllExpectationsMatched();
        }

        indicesAdmin().prepareRefresh(index).get();
        // Force merge to a single segment: this flushes with the index sort on Body.template_id, validating the
        // pattern_text template_id doc-value column is correct and sortable. A broken column fails the shard here.
        assertNoFailures(indicesAdmin().forceMerge(new ForceMergeRequest(index).maxNumSegments(1)).actionGet());

        // Total docs round-trip.
        assertResponse(client().search(new SearchRequest(index).source(new SearchSourceBuilder().size(0).trackTotalHits(true))), r -> {
            assertNoFailures(r);
            assertThat(r.getHits().getTotalHits().value(), equalTo((long) numDocs));
        });

        // Full-text match on the analyzed Body (pattern_text primary inverted column): proves Lucene applied
        // the field analyzer to the BinaryColumn during the row-pass inversion.
        final int expectedOrder = expectedOrderMatches;
        assertResponse(
            client().search(
                new SearchRequest(index).source(
                    new SearchSourceBuilder().query(QueryBuilders.matchQuery("Body", "order")).size(0).trackTotalHits(true)
                )
            ),
            r -> {
                assertNoFailures(r);
                assertThat(r.getHits().getTotalHits().value(), equalTo((long) expectedOrder));
            }
        );

        // Sorted search on the index-sort key succeeds and returns every doc.
        assertResponse(
            client().search(
                new SearchRequest(index).source(
                    new SearchSourceBuilder().sort("ServiceName", SortOrder.ASC).size(numDocs).trackTotalHits(true)
                )
            ),
            r -> {
                assertNoFailures(r);
                assertThat(r.getHits().getHits().length, equalTo(numDocs));
            }
        );

        // Synthetic-source reconstruction via GET: Body (template + args columns), the keyword and byte scalars,
        // and @timestamp must all round-trip from the columnar doc values.
        final GetResponse getResponse = client().get(new GetRequest(index).id("doc-1")).actionGet();
        assertTrue(getResponse.isExists());
        final Map<String, Object> source = getResponse.getSourceAsMap();
        assertThat(source.get("Body"), equalTo(BODIES[1 % BODIES.length]));
        assertThat(source.get("ServiceName"), equalTo(SERVICES[1 % SERVICES.length]));
        assertThat(((Number) source.get("SeverityNumber")).intValue(), equalTo(1 % 24));
        assertThat(source.get("TraceId"), equalTo("trace-1"));
        assertThat(source.get("@timestamp"), notNullValue());

        // Populated flattened fields reconstruct from the columnar keyed doc values. We assert the
        // non-dotted LogAttributes.userId value exactly (dotted keys reconstruct into nested objects, which
        // we don't pin here) and that ResourceAttributes comes back as a non-empty object.
        assertThat(source.get("ResourceAttributes"), instanceOf(Map.class));
        assertThat(((Map<?, ?>) source.get("ResourceAttributes")).isEmpty(), equalTo(false));
        assertThat(source.get("LogAttributes"), instanceOf(Map.class));
        assertThat(((Map<?, ?>) source.get("LogAttributes")).get("userId"), equalTo("u-1"));
        // The empty ScopeAttributes object is treated as absent and is not reconstructed.
        assertThat(source.get("ScopeAttributes"), nullValue());
    }

    private XContentBuilder doc(int i, Instant timestamp, String body) throws IOException {
        XContentBuilder b = JsonXContent.contentBuilder();
        b.startObject();
        b.field("@timestamp", timestamp.toString());
        b.field("TraceId", "trace-" + i);
        b.field("SpanId", "span-" + i);
        b.field("TraceFlags", i % 2);
        b.field("SeverityText", (i % 2 == 0) ? "error" : "info");
        b.field("SeverityNumber", i % 24);
        b.field("ServiceName", SERVICES[i % SERVICES.length]);
        b.field("Body", body);
        b.field("ResourceSchemaUrl", "");
        b.field("ScopeSchemaUrl", "");
        b.field("ScopeName", "node-logger");
        b.field("ScopeVersion", "");
        // Flattened fields, faithful to textbench: ResourceAttributes/LogAttributes are populated, while
        // ScopeAttributes is an empty object {} (the EICF encoder treats an empty object as absent, so it
        // stays on the columnar path and simply isn't reconstructed in synthetic source).
        b.field("ResourceAttributes", Map.of("host.name", "host-" + i, "k8s.namespace.name", "otel-demo", "os.type", "linux"));
        b.field("ScopeAttributes", Map.of());
        b.field("LogAttributes", Map.of("userId", "u-" + i, "error.code", "13"));
        b.endObject();
        return b;
    }

    private static XContentBuilder textBenchMapping() throws IOException {
        XContentBuilder mapping = JsonXContent.contentBuilder();
        mapping.startObject();
        {
            mapping.field("dynamic", "strict");
            mapping.startObject("properties");
            {
                mapping.startObject("@timestamp").field("type", "date_nanos").endObject();
                mapping.startObject("TraceId").field("type", "keyword").endObject();
                mapping.startObject("SpanId").field("type", "keyword").endObject();
                mapping.startObject("TraceFlags").field("type", "byte").endObject();
                mapping.startObject("SeverityText").field("type", "keyword").endObject();
                mapping.startObject("SeverityNumber").field("type", "byte").endObject();
                mapping.startObject("ServiceName").field("type", "keyword").endObject();
                mapping.startObject("Body").field("type", "pattern_text").endObject();
                mapping.startObject("ResourceSchemaUrl").field("type", "keyword").endObject();
                mapping.startObject("ScopeSchemaUrl").field("type", "keyword").endObject();
                mapping.startObject("ScopeName").field("type", "keyword").endObject();
                mapping.startObject("ScopeVersion").field("type", "keyword").endObject();
                mapping.startObject("ResourceAttributes").field("type", "flattened").endObject();
                mapping.startObject("ScopeAttributes").field("type", "flattened").endObject();
                mapping.startObject("LogAttributes").field("type", "flattened").endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        return mapping;
    }
}
