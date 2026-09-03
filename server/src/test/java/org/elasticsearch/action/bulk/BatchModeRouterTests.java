/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.escf.EscfColumnKind;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.sourcebatch.LeafSink;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceRow;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class BatchModeRouterTests extends ESTestCase {

    /** Data stream name used by tests that exercise the single-backing-index TSDB path. */
    private static final String DATA_STREAM = "metrics-app-default";
    /** Fixed epoch for backing index names so tests do not depend on wall-clock time. */
    private static final long EPOCH_MILLIS = 1704067200000L; // 2024-01-01T00:00:00Z

    private static final String GEN_1_START = "2024-01-01T00:00:00Z";
    private static final String GEN_1_END = "2024-06-01T00:00:00Z";

    private static final Instant IN_GEN_1 = Instant.parse("2024-03-01T00:00:00Z");

    /** Builds a plain {@link IndexMetadata} with no routing path (Unpartitioned strategy). */
    private static IndexMetadata plainMetadata(String name, int shards) {
        return IndexMetadata.builder(name).settings(indexSettings(name).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)).build();
    }

    /**
     * Builds a TSDB backing index whose routing strategy is
     * {@link IndexRouting.ExtractFromSource.ForIndexDimensions}: time_series mode plus a non-empty
     * {@code index.dimensions}, which is what selects that strategy in
     * {@link IndexRouting#fromIndexMetadata}.
     */
    private static IndexMetadata tsdbBackingIndex(int generation, int shards, String start, String end) {
        String name = DataStream.getDefaultBackingIndexName(DATA_STREAM, generation, EPOCH_MILLIS);
        return IndexMetadata.builder(name)
            .settings(
                indexSettings(name).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
                    .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
                    .put(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim")
                    .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), start)
                    .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), end)
            )
            .build();
    }

    private static Settings.Builder indexSettings(String indexName) {
        return Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_INDEX_UUID, indexName + "-uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
    }

    private static ProjectMetadata project(IndexMetadata... indices) {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(ProjectId.DEFAULT);
        for (IndexMetadata index : indices) {
            builder.put(index, false);
        }
        return builder.build();
    }

    /** A time series data stream over the given backing indices, in generation order. */
    private static ProjectMetadata projectWithDataStream(IndexMetadata... backingIndices) {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(ProjectId.DEFAULT);
        List<Index> indices = new ArrayList<>();
        for (IndexMetadata index : backingIndices) {
            builder.put(index, false);
            indices.add(index.getIndex());
        }
        builder.put(
            DataStream.builder(DATA_STREAM, indices).setGeneration(backingIndices.length).setIndexMode(IndexMode.TIME_SERIES).build()
        );
        return builder.build();
    }

    /** The documents backing a batch, kept so tests can assert the rows survived the scatter unchanged. */
    private record Docs(EscfBatch batch, List<BytesReference> sources) {}

    /** Builds a batch of {@code n} rows, each {@code {"dim": "d<i>", "val": i}}. */
    private static Docs buildDocs(int n) throws IOException {
        List<BytesReference> sources = new ArrayList<>(n);
        try (EscfEncoder encoder = new EscfEncoder()) {
            for (int i = 0; i < n; i++) {
                XContentBuilder doc = JsonXContent.contentBuilder();
                doc.startObject();
                doc.field("dim", "d" + i);
                doc.field("val", (long) i);
                doc.endObject();
                BytesReference source = BytesReference.bytes(doc);
                sources.add(source);
                encoder.parseToScratch(source, XContentType.JSON, LeafSink.NO_OP);
                encoder.commitScratchTo(0);
            }
            return new Docs(encoder.buildPartition(0), sources);
        }
    }

    private static EscfBatch buildBatch(int n) throws IOException {
        return buildDocs(n).batch();
    }

    /**
     * Builds sourceless {@link IndexRequest}s referencing rows {@code 0..numDocs-1} of {@code batch}
     * and attaches the batch under {@code batchKey} — the name the requests target, which is what
     * the router keys on.
     */
    private static BulkRequest buildBulkRequest(String batchKey, EscfBatch batch, int numDocs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(rowRequest(batchKey, batch, i));
        }
        bulkRequest.setPreBuiltBatches(Map.of(batchKey, batch));
        return bulkRequest;
    }

    private static IndexRequest rowRequest(String indexName, EscfBatch batch, int row) {
        IndexRequest request = new IndexRequest(indexName).id("doc-" + row).opType(DocWriteRequest.OpType.INDEX);
        request.indexSource().setSourceRow(batch, row, XContentType.JSON);
        return request;
    }

    /** A row-bearing request for a TSDB data stream: create-only, with the timestamp and tsid pre-computed. */
    private static IndexRequest tsdbRowRequest(String indexName, EscfBatch batch, int row, Instant timestamp) {
        IndexRequest request = new IndexRequest(indexName).opType(DocWriteRequest.OpType.CREATE);
        request.indexSource().setSourceRow(batch, row, XContentType.JSON);
        // The source is empty, so both of these must be supplied by the batch producer.
        request.setTimeSeriesTimestamp(timestamp);
        request.tsid(new BytesRef("tsid-" + row));
        return request;
    }

    /** Marker for {@link #buildTimestampBatch}: omit {@code @timestamp} from this row entirely. */
    private static final Object ABSENT = new Object();

    /**
     * Builds a batch whose rows are {@code {"@timestamp": <value>, "dim": "d<i>"}}. A {@code null} entry writes an
     * explicit JSON null; {@link #ABSENT} omits the field. The value types drive which ESCF column kind the
     * encoder produces — uniform longs give a LONG column, uniform strings a STRING column, and a mix a UNION.
     */
    private static EscfBatch buildTimestampBatch(Object... timestamps) throws IOException {
        try (EscfEncoder encoder = new EscfEncoder()) {
            for (int i = 0; i < timestamps.length; i++) {
                XContentBuilder doc = JsonXContent.contentBuilder();
                doc.startObject();
                Object timestamp = timestamps[i];
                if (timestamp == null) {
                    doc.nullField(DataStream.TIMESTAMP_FIELD_NAME);
                } else if (timestamp != ABSENT) {
                    doc.field(DataStream.TIMESTAMP_FIELD_NAME, timestamp);
                }
                doc.field("dim", "d" + i);
                doc.endObject();
                encoder.parseToScratch(BytesReference.bytes(doc), XContentType.JSON, LeafSink.NO_OP);
                encoder.commitScratchTo(0);
            }
            return encoder.buildPartition(0);
        }
    }

    /**
     * A row-bearing TSDB request with a pre-computed tsid but <b>no</b> timestamp — the case the timestamp tests
     * below cover, where the timestamp must come from the batch's own column.
     */
    private static IndexRequest tsdbRowRequestNoTimestamp(EscfBatch batch, int row) {
        IndexRequest request = new IndexRequest(DATA_STREAM).opType(DocWriteRequest.OpType.CREATE);
        request.indexSource().setSourceRow(batch, row, XContentType.JSON);
        request.tsid(new BytesRef("tsid-" + row));
        return request;
    }

    private static BulkRequest tsdbBulkNoTimestamps(EscfBatch batch) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int row = 0; row < batch.docCount(); row++) {
            bulkRequest.add(tsdbRowRequestNoTimestamp(batch, row));
        }
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));
        return bulkRequest;
    }

    private static EscfColumn timestampColumn(EscfBatch batch) {
        return batch.column(batch.schema().findLeaf(DataStream.TIMESTAMP_FIELD_NAME, 0));
    }

    /** Runs only the timestamp pre-pass and returns the cached raw timestamp of each row. */
    private static List<Object> cachedRawTimestamps(EscfBatch batch) {
        BulkRequest bulkRequest = tsdbBulkNoTimestamps(batch);
        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        router.cacheRawTimestamps(bulkRequest.requests);
        List<Object> raw = new ArrayList<>();
        for (DocWriteRequest<?> request : bulkRequest.requests) {
            raw.add(((IndexRequest) request).getRawTimestamp());
        }
        router.close();
        return raw;
    }

    /**
     * Mirror of {@link BulkOperation}'s shard grouping loop: caches raw timestamps from the batch,
     * resolves the concrete write index and routing for each item, then delegates to
     * {@link BatchModeRouter#route} (which records the item for deferred assignment), and finally
     * resolves all shard assignments via {@link BatchModeRouter#buildGrouping}.
     *
     * @param skipRows rows to drop before routing, standing in for items that fail validation in the
     *                 real loop
     */
    private static Map<ShardId, List<BulkItemRequest>> routeAll(
        BatchModeRouter router,
        BulkRequest bulkRequest,
        ProjectMetadata project,
        Set<Integer> skipRows
    ) {
        router.cacheRawTimestamps(bulkRequest.requests);
        int slot = 0;
        for (DocWriteRequest<?> docWriteRequest : bulkRequest.requests) {
            IndexRequest request = (IndexRequest) docWriteRequest;
            BulkItemRequest item = new BulkItemRequest(slot++, request);
            if (skipRows.contains(request.indexSource().rowIndex())) {
                continue;
            }
            IndexAbstraction abstraction = project.getIndicesLookup().get(request.index());
            Index concreteIndex = request.getConcreteWriteIndex(abstraction, project);
            IndexRouting routing = IndexRouting.fromIndexMetadata(project.getIndexSafe(concreteIndex));
            router.route(item, request, abstraction, concreteIndex, routing, project);
        }
        return router.buildGrouping((failedItem, e) -> { throw new AssertionError("unexpected routing failure: " + e.getMessage(), e); });
    }

    private static Map<ShardId, List<BulkItemRequest>> routeAll(BatchModeRouter router, BulkRequest bulkRequest, ProjectMetadata project) {
        return routeAll(router, bulkRequest, project, Set.of());
    }

    /** Asserts every shard's items map 1:1 and in order onto its batch's rows. */
    private static void assertShardsAligned(Map<ShardId, List<BulkItemRequest>> requestsByShard, Map<ShardId, SourceBatch> shardBatches) {
        BatchModeRouter.validateBatchAlignment(requestsByShard, shardBatches);
        for (Map.Entry<ShardId, List<BulkItemRequest>> entry : requestsByShard.entrySet()) {
            SourceBatch shardBatch = shardBatches.get(entry.getKey());
            assertThat("no batch for shard " + entry.getKey(), shardBatch, notNullValue());
            assertThat("row count for shard " + entry.getKey(), shardBatch.docCount(), equalTo(entry.getValue().size()));
            assertTrue("rows not aligned for shard " + entry.getKey(), BulkShardBatch.rowsAlignWithItems(shardBatch, entry.getValue()));
        }
    }

    private static Map<String, Object> asMap(BytesReference source) {
        return XContentHelper.convertToMap(source, false, XContentType.JSON).v2();
    }

    public void testCreateReturnsNullWhenNoBatches() {
        assertThat(BatchModeRouter.create(new BulkRequest(), true), nullValue());
    }

    public void testCreateReturnsNullWhenEmptyBatchMap() {
        BulkRequest request = new BulkRequest();
        request.setPreBuiltBatches(Map.of());
        assertThat(BatchModeRouter.create(request, true), nullValue());
    }

    /**
     * A {@link SourceBatch} that is not an {@link EscfBatch} is unreachable through any production
     * code path today, so a stub is the only way to exercise the guard. It throws from every method
     * to make it obvious if anything other than the {@code instanceof} check ever touches it.
     */
    private static class NotAnEscfBatch implements SourceBatch {
        @Override
        public int docCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SourceSchema schema() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int columnCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytesReference data() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SourceRow row(int docIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SourceBatch slice(int from, int to) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long ramBytesUsed() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }

    public void testCreateThrowsForNonEscfBatch() {
        BulkRequest request = new BulkRequest();
        request.setPreBuiltBatches(Map.of("myindex", new NotAnEscfBatch()));
        var e = expectThrows(IllegalArgumentException.class, () -> BatchModeRouter.create(request, true));
        assertThat(e.getMessage(), containsString("must be an EscfBatch"));
    }

    /**
     * Step-1 limit: exactly one pre-built batch per bulk. A second batch name triggers an immediate
     * rejection at create time with a message pointing to the upcoming follow-up.
     */
    public void testRejectsMultipleBatches() throws IOException {
        EscfBatch batchA = buildBatch(1);
        EscfBatch batchB = buildBatch(1);
        BulkRequest request = new BulkRequest();
        request.setPreBuiltBatches(Map.of("index-a", batchA, "index-b", batchB));
        var e = expectThrows(IllegalArgumentException.class, () -> BatchModeRouter.create(request, true));
        assertThat(e.getMessage(), containsString("at most one is supported in step 1"));
    }

    public void testSingleShardAllRowsRouted() throws IOException {
        int numDocs = randomIntBetween(3, 20);
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);
        IndexMetadata md = plainMetadata("myindex", 1);
        ProjectMetadata project = project(md);

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        assertThat(router, notNullValue());
        var requestsByShard = routeAll(router, bulkRequest, project);
        Map<ShardId, SourceBatch> result = router.shardBatches();

        assertThat(result.size(), equalTo(1));
        SourceBatch shardBatch = result.get(new ShardId(md.getIndex(), 0));
        assertThat(shardBatch, notNullValue());
        assertThat(shardBatch.docCount(), equalTo(numDocs));
        assertShardsAligned(requestsByShard, result);
        router.close();
    }

    public void testMultiShardRowsAlignWithItems() throws IOException {
        int numDocs = randomIntBetween(10, 50);
        int numShards = randomIntBetween(2, 5);
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);
        IndexMetadata md = plainMetadata("myindex", numShards);
        ProjectMetadata project = project(md);

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var requestsByShard = routeAll(router, bulkRequest, project);
        Map<ShardId, SourceBatch> result = router.shardBatches();

        assertShardsAligned(requestsByShard, result);
        assertThat(result.size(), equalTo(requestsByShard.size()));
        router.close();
    }

    /**
     * If some (but not all) rows are dropped before routing, {@code shardBatches()} must fail
     * rather than silently produce a misaligned batch. Discard-bucket support will be added in a
     * follow-up.
     */
    public void testThrowsWhenSomeRowsDropped() throws IOException {
        int numDocs = randomIntBetween(3, 20);
        int numShards = randomIntBetween(1, 4);
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);
        ProjectMetadata project = project(plainMetadata("myindex", numShards));

        // Drop between 1 and numDocs-1 rows so that routedCount > 0 and < docCount.
        int dropCount = randomIntBetween(1, numDocs - 1);
        Set<Integer> dropped = new HashSet<>();
        while (dropped.size() < dropCount) {
            dropped.add(randomIntBetween(0, numDocs - 1));
        }
        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var e = expectThrows(IllegalStateException.class, () -> routeAll(router, bulkRequest, project, dropped));
        assertThat(e.getMessage(), containsString("not yet supported"));
        router.close();
    }

    public void testAllRowsDroppedProducesNoBatches() throws IOException {
        EscfBatch batch = buildBatch(5);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, 5);
        ProjectMetadata project = project(plainMetadata("myindex", 2));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var requestsByShard = routeAll(router, bulkRequest, project, Set.of(0, 1, 2, 3, 4));
        assertTrue(requestsByShard.isEmpty());
        assertTrue(router.shardBatches().isEmpty());
        router.close();
    }

    /**
     * The failure-store redirect pass re-enters {@code executeBulkRequestsByShard}, so
     * {@code shardBatches()} can be called a second time. It must not re-scatter: the first call's
     * batches are already attached to in-flight shard requests and their items already point at
     * shard-local rows.
     */
    public void testSecondShardBatchesCallIsANoOp() throws IOException {
        int numDocs = 12;
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);
        ProjectMetadata project = project(plainMetadata("myindex", 3));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var requestsByShard = routeAll(router, bulkRequest, project);
        Map<ShardId, SourceBatch> first = router.shardBatches();
        assertFalse(first.isEmpty());

        List<Integer> rowsAfterFirst = bulkRequest.requests.stream().map(r -> ((IndexRequest) r).indexSource().rowIndex()).toList();
        assertThat(router.shardBatches(), equalTo(Map.of()));
        List<Integer> rowsAfterSecond = bulkRequest.requests.stream().map(r -> ((IndexRequest) r).indexSource().rowIndex()).toList();
        assertThat(rowsAfterSecond, equalTo(rowsAfterFirst));
        assertShardsAligned(requestsByShard, first);
        router.close();
    }

    public void testRejectsItemWithoutSourceRow() throws IOException {
        EscfBatch batch = buildBatch(1);
        // Item has inline source — no source-row reference.
        IndexRequest request = new IndexRequest("myindex").id("doc-0").source(new HashMap<>());
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(request);
        bulkRequest.setPreBuiltBatches(Map.of("myindex", batch));
        var e = expectThrows(IllegalArgumentException.class, () -> BatchModeRouter.create(bulkRequest, true));
        assertThat(e.getMessage(), containsString("must carry a source-row reference"));
    }

    public void testRejectsRowBearingItemWithNoBatchForItsName() throws IOException {
        EscfBatch batch = buildBatch(1);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setPreBuiltBatches(Map.of("myindex", batch));
        IndexMetadata other = plainMetadata("otherindex", 1);
        ProjectMetadata project = project(plainMetadata("myindex", 1), other);
        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);

        // Carries a row but targets a name with no batch — e.g. because something rewrote _index.
        IndexRequest request = rowRequest("otherindex", batch, 0);
        IndexAbstraction ia = project.getIndicesLookup().get(request.index());
        BulkItemRequest item = new BulkItemRequest(0, request);
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> router.route(item, request, ia, other.getIndex(), IndexRouting.fromIndexMetadata(other), project)
        );
        assertThat(e.getMessage(), containsString("no pre-built batch was supplied under that name"));
        router.close();
    }

    public void testRejectsInlineItemForAnUnbatchedName() throws IOException {
        EscfBatch batch = buildBatch(1);
        // Inline source in a bulk that carries batches: its shard's rows could not line up with its items.
        IndexRequest request = new IndexRequest("otherindex").id("doc-0").source(new HashMap<>());
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(request);
        bulkRequest.setPreBuiltBatches(Map.of("myindex", batch));
        var e = expectThrows(IllegalArgumentException.class, () -> BatchModeRouter.create(bulkRequest, true));
        assertThat(e.getMessage(), containsString("must carry a source-row reference"));
    }

    public void testRejectsNonIndexRequestItem() throws IOException {
        EscfBatch batch = buildBatch(1);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new DeleteRequest("myindex", "doc-0"));
        bulkRequest.setPreBuiltBatches(Map.of("myindex", batch));
        var e = expectThrows(IllegalArgumentException.class, () -> BatchModeRouter.create(bulkRequest, true));
        assertThat(e.getMessage(), containsString("cannot be mixed with pre-built source batches"));
    }

    /**
     * Step-1 limit: one batch may only resolve to one concrete write index. This is the restriction
     * that prevents using pre-built batches with TSDB data streams spanning two backing indices.
     * Support for multi-index fan-out will be added in a follow-up.
     */
    public void testRejectsSecondConcreteIndex() throws IOException {
        // Two plain indices; the items target the same batch name but resolve to different
        // concrete write indices because the metadata has no alias pointing both to the same one.
        EscfBatch batch = buildBatch(2);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(rowRequest("myindex", batch, 0));
        bulkRequest.add(rowRequest("myindex", batch, 1));
        bulkRequest.setPreBuiltBatches(Map.of("myindex", batch));

        IndexMetadata mdA = plainMetadata("myindex", 1);
        // Simulate a second concrete index by using a different Index object (different UUID).
        Index concreteA = mdA.getIndex();
        IndexMetadata mdB = IndexMetadata.builder("myindex-alt")
            .settings(indexSettings("myindex-alt").put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1))
            .build();
        Index concreteB = mdB.getIndex();
        ProjectMetadata project = project(mdA, mdB);

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        IndexRouting routingA = IndexRouting.fromIndexMetadata(mdA);
        // Resolve the abstraction for "myindex" — both items target the same name.
        IndexAbstraction ia = project.getIndicesLookup().get("myindex");
        IndexRequest first = (IndexRequest) bulkRequest.requests.get(0);
        BulkItemRequest itemA = new BulkItemRequest(0, first);
        router.route(itemA, first, ia, concreteA, routingA, project);

        // The second item is artificially routed to a different concrete index — must be rejected.
        IndexRequest second = (IndexRequest) bulkRequest.requests.get(1);
        IndexRouting routingB = IndexRouting.fromIndexMetadata(mdB);
        BulkItemRequest itemB = new BulkItemRequest(1, second);
        var e = expectThrows(IllegalArgumentException.class, () -> router.route(itemB, second, ia, concreteB, routingB, project));
        assertThat(e.getMessage(), containsString("not yet supported"));
        router.close();
    }

    public void testRejectsNonMonotonicRowIndex() throws IOException {
        EscfBatch batch = buildBatch(3);
        BulkRequest bulkRequest = new BulkRequest();
        // Items in reverse row order — should fail on the second item.
        for (int i = 2; i >= 0; i--) {
            bulkRequest.add(rowRequest("myindex", batch, i));
        }
        bulkRequest.setPreBuiltBatches(Map.of("myindex", batch));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        IndexMetadata md = plainMetadata("myindex", 1);
        ProjectMetadata project = project(md);
        IndexRouting routing = IndexRouting.fromIndexMetadata(md);
        IndexAbstraction ia = project.getIndicesLookup().get("myindex");

        IndexRequest first = (IndexRequest) bulkRequest.requests.get(0);
        BulkItemRequest item0 = new BulkItemRequest(0, first);
        router.route(item0, first, ia, md.getIndex(), routing, project);

        IndexRequest second = (IndexRequest) bulkRequest.requests.get(1);
        BulkItemRequest item1 = new BulkItemRequest(1, second);
        var e = expectThrows(IllegalArgumentException.class, () -> router.route(item1, second, ia, md.getIndex(), routing, project));
        assertThat(e.getMessage(), containsString("not strictly greater"));
        router.close();
    }

    /**
     * A pre-built batch without any pre-computed tsids should have its tsids computed by the
     * columnar calculator in {@link BatchModeRouter#buildGrouping}, matching what the row-path
     * extractor would produce from the same source.
     */
    public void testProvidedBatchWithoutTsidComputesTsidViaColumnarCalculator() throws IOException {
        IndexMetadata md = tsdbBackingIndex(1, 1, GEN_1_START, GEN_1_END);
        IndexRouting routing = IndexRouting.fromIndexMetadata(md);
        assertThat(routing, instanceOf(IndexRouting.ExtractFromSource.ForIndexDimensions.class));
        IndexRouting.ExtractFromSource.ForIndexDimensions dims = (IndexRouting.ExtractFromSource.ForIndexDimensions) routing;
        ProjectMetadata project = projectWithDataStream(md);

        Docs docs = buildDocs(1);
        IndexRequest request = new IndexRequest(DATA_STREAM).opType(DocWriteRequest.OpType.CREATE);
        request.indexSource().setSourceRow(docs.batch(), 0, XContentType.JSON);
        request.setTimeSeriesTimestamp(IN_GEN_1);
        // Intentionally no tsid: the columnar calculator must compute it.
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(request);
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, docs.batch()));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var requestsByShard = routeAll(router, bulkRequest, project);
        assertThat("tsid must be computed by columnar path", request.tsid(), notNullValue());
        // Verify parity with the row-path extractor.
        BytesRef expected = dims.buildTsid(XContentType.JSON, docs.sources().get(0));
        assertThat(request.tsid(), equalTo(expected));
        assertFalse("grouping must be non-empty", requestsByShard.isEmpty());
        router.close();
    }

    /**
     * A uniform numeric {@code @timestamp} column: every row's epoch-millis value is cached as a {@link Long},
     * which is the only numeric type {@code DataStream#getTimeSeriesTimestamp} accepts.
     */
    public void testCachesRawTimestampFromLongColumn() throws IOException {
        long first = IN_GEN_1.toEpochMilli();
        long second = IN_GEN_1.plusMillis(1234).toEpochMilli();
        assertThat(cachedRawTimestamps(buildTimestampBatch(first, second)), equalTo(List.of(first, second)));
    }

    /**
     * A uniform string {@code @timestamp} column is cached verbatim as a {@link String}, leaving the parse to
     * {@code DataStream}'s own formatter. Covers all three formats that formatter accepts, including the
     * nanosecond form, which the default date field formatter would reject.
     */
    public void testCachesRawTimestampFromStringColumn() throws IOException {
        List<Object> raw = cachedRawTimestamps(
            buildTimestampBatch("2024-03-01T00:00:00Z", "2024-03-01T00:00:00.123456789Z", "1709251200000")
        );
        assertThat(raw, equalTo(List.of("2024-03-01T00:00:00Z", "2024-03-01T00:00:00.123456789Z", "1709251200000")));
    }

    /**
     * Column shapes the cursor walk does not accept are skipped wholesale, leaving every row uncached so the
     * existing resolution reports the failure per item inside the grouping loop. Mixed value types and explicit
     * nulls both produce a UNION column, which has no single typed cursor; a multi-valued field produces ARRAY;
     * and a floating point value produces DOUBLE, which the source parser would truncate rather than accept.
     */
    public void testSkipsColumnKindsThatHaveNoTimestampCursor() throws IOException {
        assertUncachedWithKind(EscfColumnKind.UNION, buildTimestampBatch(IN_GEN_1.toEpochMilli(), "2024-03-01T00:00:00Z"));
        assertUncachedWithKind(EscfColumnKind.UNION, buildTimestampBatch(IN_GEN_1.toEpochMilli(), null));
        assertUncachedWithKind(EscfColumnKind.ARRAY, buildTimestampBatch(List.of(1L, 2L), List.of(3L, 4L)));
        assertUncachedWithKind(EscfColumnKind.DOUBLE, buildTimestampBatch(1.5d, 2.5d));
    }

    private static void assertUncachedWithKind(byte expectedKind, EscfBatch batch) {
        assertThat(timestampColumn(batch).kind(), equalTo(expectedKind));
        for (Object value : cachedRawTimestamps(batch)) {
            assertThat(value, nullValue());
        }
    }

    /**
     * A cursor visits only the rows that are present, so it cannot be kept in step with the request list when
     * some rows are missing {@code @timestamp}. Such a column is skipped entirely rather than walked row by row —
     * caching a subset would be correct but is not worth a second code path, and the rows that do resolve would
     * anyway be joined by failures from the rows that do not.
     */
    public void testSkipsSparseTimestampColumn() throws IOException {
        EscfBatch batch = buildTimestampBatch(IN_GEN_1.toEpochMilli(), ABSENT, IN_GEN_1.toEpochMilli());
        assertFalse("a row without @timestamp must leave the column sparse", timestampColumn(batch).isDense());
        assertThat(cachedRawTimestamps(batch), equalTo(Arrays.asList(null, null, null)));
    }

    /**
     * The walk pairs request {@code i} with row {@code i}, so it must not run when the two are not the same
     * length — otherwise a request could be handed another document's timestamp.
     */
    public void testSkipsWhenRequestCountDiffersFromRowCount() throws IOException {
        EscfBatch batch = buildTimestampBatch(IN_GEN_1.toEpochMilli(), IN_GEN_1.toEpochMilli(), IN_GEN_1.toEpochMilli());
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(tsdbRowRequestNoTimestamp(batch, 0));
        bulkRequest.add(tsdbRowRequestNoTimestamp(batch, 1));
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        router.cacheRawTimestamps(bulkRequest.requests);
        for (DocWriteRequest<?> request : bulkRequest.requests) {
            assertThat(((IndexRequest) request).getRawTimestamp(), nullValue());
        }
        router.close();
    }

    /**
     * Same reason as above, one level finer: a request whose row index is not its own position must not pick up
     * the timestamp of whichever document happens to sit at that position.
     */
    public void testSkipsRequestPointingAtAnotherRow() throws IOException {
        long first = IN_GEN_1.toEpochMilli();
        long second = IN_GEN_1.plusSeconds(60).toEpochMilli();
        EscfBatch batch = buildTimestampBatch(first, second);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(tsdbRowRequestNoTimestamp(batch, 1)); // points at row 1 from position 0
        bulkRequest.add(tsdbRowRequestNoTimestamp(batch, 1));
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        router.cacheRawTimestamps(bulkRequest.requests);

        assertThat("must not take row 0's timestamp", ((IndexRequest) bulkRequest.requests.get(0)).getRawTimestamp(), nullValue());
        assertThat(((IndexRequest) bulkRequest.requests.get(1)).getRawTimestamp(), equalTo(second));
        router.close();
    }

    /** A batch with no {@code @timestamp} column at all is a no-op, not a failure. */
    public void testNoTimestampColumnIsANoOp() throws IOException {
        assertThat(cachedRawTimestamps(buildBatch(3)), equalTo(Arrays.asList(null, null, null)));
    }

    /**
     * The pre-pass must not touch a request the producer already resolved: {@code setRawTimestamp} asserts it is
     * only ever set once, so overwriting would trip that assertion.
     */
    public void testDoesNotOverwriteProducerSuppliedTimestamp() throws IOException {
        EscfBatch batch = buildTimestampBatch(IN_GEN_1.toEpochMilli(), IN_GEN_1.toEpochMilli());
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(tsdbRowRequest(DATA_STREAM, batch, 0, IN_GEN_1)); // producer set timeSeriesTimestamp
        IndexRequest preSetRaw = tsdbRowRequestNoTimestamp(batch, 1);
        preSetRaw.setRawTimestamp("2024-04-01T00:00:00Z");
        bulkRequest.add(preSetRaw);
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        router.cacheRawTimestamps(bulkRequest.requests);

        assertThat(((IndexRequest) bulkRequest.requests.get(0)).getRawTimestamp(), nullValue());
        assertThat(((IndexRequest) bulkRequest.requests.get(0)).getTimeSeriesTimestamp(), equalTo(IN_GEN_1));
        assertThat(preSetRaw.getRawTimestamp(), equalTo("2024-04-01T00:00:00Z"));
        router.close();
    }

    /**
     * End to end: a TSDB bulk whose items carry no producer-supplied timestamp still resolves to the correct
     * backing index, because the write index is selected from the value read out of the batch's column. Before
     * this worked, every item failed — the items hold no inline source for the source parser to read.
     */
    public void testResolvesTsdbWriteIndexFromTimestampColumn() throws IOException {
        IndexMetadata md = tsdbBackingIndex(1, 1, GEN_1_START, GEN_1_END);
        ProjectMetadata project = projectWithDataStream(md);

        // Both column kinds the scan understands must produce the same write index for the same instant.
        EscfBatch batch = randomBoolean()
            ? buildTimestampBatch(IN_GEN_1.toEpochMilli(), IN_GEN_1.toEpochMilli())
            : buildTimestampBatch(IN_GEN_1.toString(), IN_GEN_1.toString());
        BulkRequest bulkRequest = tsdbBulkNoTimestamps(batch);

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var requestsByShard = routeAll(router, bulkRequest, project);

        assertThat(requestsByShard.keySet(), equalTo(Set.of(new ShardId(md.getIndex(), 0))));
        for (DocWriteRequest<?> request : bulkRequest.requests) {
            // getConcreteWriteIndex resolves and memoizes the canonical (second-truncated) instant.
            assertThat(((IndexRequest) request).getTimeSeriesTimestamp(), equalTo(IN_GEN_1));
        }
        assertShardsAligned(requestsByShard, router.shardBatches());
        router.close();
    }

    /**
     * The column value really is consulted per row: with two generations covering different windows, a batch of
     * rows that all fall in generation 2 routes to generation 2, not to the newest-write-index default.
     */
    public void testTimestampColumnSelectsOlderGeneration() throws IOException {
        IndexMetadata gen1 = tsdbBackingIndex(1, 1, "2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z");
        IndexMetadata gen2 = tsdbBackingIndex(2, 1, "2024-02-01T00:00:00Z", "2024-03-01T00:00:00Z");
        ProjectMetadata project = projectWithDataStream(gen1, gen2);

        Instant inGen1 = Instant.parse("2024-01-15T00:00:00Z");
        EscfBatch batch = buildTimestampBatch(inGen1.toEpochMilli(), inGen1.toEpochMilli());
        BulkRequest bulkRequest = tsdbBulkNoTimestamps(batch);

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var requestsByShard = routeAll(router, bulkRequest, project);

        assertThat(requestsByShard.keySet(), equalTo(Set.of(new ShardId(gen1.getIndex(), 0))));
        router.close();
    }

    /**
     * Per-row resolution means a batch straddling a rollover boundary now legitimately resolves to two backing
     * indices, which the router does not yet support. Pinned here so the follow-up that adds multi-index batches
     * has a failing case to turn green.
     */
    public void testTimestampColumnSpanningTwoGenerationsIsRejected() throws IOException {
        IndexMetadata gen1 = tsdbBackingIndex(1, 1, "2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z");
        IndexMetadata gen2 = tsdbBackingIndex(2, 1, "2024-02-01T00:00:00Z", "2024-03-01T00:00:00Z");
        ProjectMetadata project = projectWithDataStream(gen1, gen2);

        EscfBatch batch = buildTimestampBatch(
            Instant.parse("2024-01-15T00:00:00Z").toEpochMilli(),
            Instant.parse("2024-02-15T00:00:00Z").toEpochMilli()
        );
        BulkRequest bulkRequest = tsdbBulkNoTimestamps(batch);

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var e = expectThrows(IllegalArgumentException.class, () -> routeAll(router, bulkRequest, project));
        assertThat(e.getMessage(), containsString("batches spanning multiple concrete indices"));
        router.close();
    }

    public void testForIndexDimensionsWithTsidSucceeds() throws IOException {
        IndexMetadata md = tsdbBackingIndex(1, 1, GEN_1_START, GEN_1_END);
        ProjectMetadata project = projectWithDataStream(md);

        EscfBatch batch = buildBatch(1);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(tsdbRowRequest(DATA_STREAM, batch, 0, IN_GEN_1));
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var requestsByShard = routeAll(router, bulkRequest, project);
        assertShardsAligned(requestsByShard, router.shardBatches());
        router.close();
    }

    public void testSingleShardPassthroughHandsSourceBatchThrough() throws IOException {
        int numDocs = randomIntBetween(3, 20);
        Docs docs = buildDocs(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", docs.batch(), numDocs);
        IndexMetadata md = plainMetadata("myindex", 1);
        ProjectMetadata project = project(md);

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var requestsByShard = routeAll(router, bulkRequest, project);
        Map<ShardId, SourceBatch> result = router.shardBatches();

        assertThat(result.size(), equalTo(1));
        assertSame(docs.batch(), result.get(new ShardId(md.getIndex(), 0)));
        assertShardsAligned(requestsByShard, result);

        // The items were never re-pointed, so each one still materializes the document it was built from.
        for (int i = 0; i < numDocs; i++) {
            IndexRequest request = (IndexRequest) bulkRequest.requests.get(i);
            assertThat(request.indexSource().rowIndex(), equalTo(i));
            request.indexSource().ensureInlineSource();
            assertThat("row " + i + " content", asMap(request.indexSource().bytes()), equalTo(asMap(docs.sources().get(i))));
        }
        router.close();
    }

    /**
     * Even a single-shard index throws when a row is dropped, because the passthrough fast path
     * requires all rows to be present. The throw now occurs in {@link BatchModeRouter#buildGrouping}
     * which is called at the end of {@link #routeAll}.
     */
    public void testSingleShardWithDroppedRowThrows() throws IOException {
        int numDocs = randomIntBetween(2, 20);
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);
        IndexMetadata md = plainMetadata("myindex", 1);
        ProjectMetadata project = project(md);

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        int drop = randomIntBetween(0, numDocs - 1);
        var e = expectThrows(IllegalStateException.class, () -> routeAll(router, bulkRequest, project, Set.of(drop)));
        assertThat(e.getMessage(), containsString("not yet supported"));
        router.close();
    }

    /** More than one shard means the rows genuinely have to be split, whatever they happened to route to. */
    public void testMultiShardDoesNotPassThrough() throws IOException {
        int numDocs = randomIntBetween(10, 50);
        EscfBatch batch = buildBatch(numDocs);
        BulkRequest bulkRequest = buildBulkRequest("myindex", batch, numDocs);
        ProjectMetadata project = project(plainMetadata("myindex", randomIntBetween(2, 5)));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var requestsByShard = routeAll(router, bulkRequest, project);
        Map<ShardId, SourceBatch> result = router.shardBatches();

        for (Map.Entry<ShardId, SourceBatch> entry : result.entrySet()) {
            assertNotSame("shard " + entry.getKey() + " was handed the whole batch", batch, entry.getValue());
        }
        assertShardsAligned(requestsByShard, result);
        router.close();
    }

    /**
     * A mixed pre-built batch (some items with pre-set tsid, some without) must be rejected.
     * The all-or-none rule is enforced by
     * {@link IndexRouting.ExtractFromSource.ForIndexDimensions#indexShard(IndexRequest[], SourceBatch)}
     * during {@link BatchModeRouter#buildGrouping}.
     */
    public void testMixedTsidBatchIsRejected() throws IOException {
        IndexMetadata md = tsdbBackingIndex(1, 1, GEN_1_START, GEN_1_END);
        ProjectMetadata project = projectWithDataStream(md);

        EscfBatch batch = buildBatch(2);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(tsdbRowRequest(DATA_STREAM, batch, 0, IN_GEN_1)); // has tsid
        IndexRequest withoutTsid = new IndexRequest(DATA_STREAM).opType(DocWriteRequest.OpType.CREATE);
        withoutTsid.indexSource().setSourceRow(batch, 1, XContentType.JSON);
        withoutTsid.setTimeSeriesTimestamp(IN_GEN_1); // but no tsid
        bulkRequest.add(withoutTsid);
        bulkRequest.setPreBuiltBatches(Map.of(DATA_STREAM, batch));

        // Route all items — the mixed-tsid detection fires in buildGrouping() when the columnar
        // trio runs.
        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        var e = expectThrows(IllegalArgumentException.class, () -> routeAll(router, bulkRequest, project));
        assertThat(e.getMessage(), containsString("Batch tsid consistency violation"));
        router.close();
    }

    /** A bulk with a single batch still checks the name every item targets, not just the first. */
    public void testSingleBatchStillValidatesNameAfterFirstItem() throws IOException {
        EscfBatch batch = buildBatch(2);
        IndexMetadata md = plainMetadata("myindex", 1);
        IndexMetadata other = plainMetadata("otherindex", 1);
        ProjectMetadata project = project(md, other);

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(rowRequest("myindex", batch, 0));
        bulkRequest.setPreBuiltBatches(Map.of("myindex", batch));

        BatchModeRouter router = BatchModeRouter.create(bulkRequest, true);
        IndexRequest first = (IndexRequest) bulkRequest.requests.get(0);
        IndexRouting routing = IndexRouting.fromIndexMetadata(md);
        IndexAbstraction iaFirst = project.getIndicesLookup().get(first.index());
        BulkItemRequest item0 = new BulkItemRequest(0, first);
        router.route(item0, first, iaFirst, md.getIndex(), routing, project);

        IndexRequest rewritten = rowRequest("otherindex", batch, 1);
        IndexAbstraction iaOther = project.getIndicesLookup().get(rewritten.index());
        BulkItemRequest item1 = new BulkItemRequest(1, rewritten);
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> router.route(item1, rewritten, iaOther, other.getIndex(), IndexRouting.fromIndexMetadata(other), project)
        );
        assertThat(e.getMessage(), containsString("no pre-built batch was supplied under that name"));
        router.close();
    }

    public void testValidateRejectsRowBearingItemWithNoBatch() throws IOException {
        EscfBatch batch = buildBatch(1);
        ShardId shardId = new ShardId(new Index("myindex", "myindex-uuid"), 0);
        var requestsByShard = Map.of(shardId, List.of(new BulkItemRequest(0, rowRequest("myindex", batch, 0))));

        var e = expectThrows(IllegalStateException.class, () -> BatchModeRouter.validateBatchAlignment(requestsByShard, Map.of()));
        assertThat(e.getMessage(), containsString("would be indexed with an empty source"));
    }

    public void testValidateRejectsRowCountMismatch() throws IOException {
        EscfBatch batch = buildBatch(2);
        ShardId shardId = new ShardId(new Index("myindex", "myindex-uuid"), 0);
        // Three items for a two-row batch.
        List<BulkItemRequest> items = List.of(
            new BulkItemRequest(0, rowRequest("myindex", batch, 0)),
            new BulkItemRequest(1, rowRequest("myindex", batch, 1)),
            new BulkItemRequest(2, rowRequest("myindex", batch, 1))
        );
        var e = expectThrows(
            IllegalStateException.class,
            () -> BatchModeRouter.validateBatchAlignment(Map.of(shardId, items), Map.of(shardId, batch))
        );
        assertThat(e.getMessage(), containsString("does not align with its items"));
    }

    public void testValidatePassesForInlineSourceItems() {
        ShardId shardId = new ShardId(new Index("myindex", "myindex-uuid"), 0);
        var items = List.of(new BulkItemRequest(0, new IndexRequest("myindex").source(new HashMap<>())));
        BatchModeRouter.validateBatchAlignment(Map.of(shardId, items), Map.of());
    }
}
