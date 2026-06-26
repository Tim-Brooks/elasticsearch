/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.document.column.BinaryColumn;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.ColumnBatch;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.eicf.EicfBatch;
import org.elasticsearch.eicf.EicfEncoder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper.BatchMapperResolution;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * Parse-time tests for the columnar batch-mapping fast path: drives {@link ShardBatchMapper#mapColumnBatch}
 * directly with an EICF batch and inspects the assembled Lucene {@link ColumnBatch} (field columns +
 * metadata columns). Only {@code NumberFieldMapper} (+ metadata) is supported for now; keyword/text/
 * boolean/ip/date cases are commented out until re-added. Heterogeneous (UNION) numeric columns —
 * string-into-number and explicit nulls — are converted on the columnar path (see
 * {@link #testNumberColumnConvertsMixedTypes()}).
 */
public class ShardBatchMapperParseTests extends IndexShardTestCase {

    private static final Settings SYNTHETIC_SOURCE_SETTINGS = indexSettings(IndexVersion.current(), 1, 0).put(
        "index.mapping.source.mode",
        "synthetic"
    ).build();

    private static final Settings STORED_SOURCE_SETTINGS = indexSettings(IndexVersion.current(), 1, 0).build();

    // Columnar mode gives flattened its binary doc-values / doc-values-only configuration.
    private static final Settings COLUMNAR_SETTINGS = indexSettings(IndexVersion.current(), 1, 0).put(
        IndexSettings.MODE.getKey(),
        IndexMode.COLUMNAR.name()
    ).build();

    private IndexShard newShardWithMapping(String mapping, Settings settings) throws IOException {
        IndexMetadata md = IndexMetadata.builder("index").putMapping(mapping).settings(settings).primaryTerm(0, 1).build();
        IndexShard shard = newShard(new ShardId(md.getIndex(), 0), true, "n1", md, null);
        recoverShardFromStore(shard);
        return shard;
    }

    private static BulkItemRequest[] items(int count) {
        BulkItemRequest[] items = new BulkItemRequest[count];
        for (int i = 0; i < count; i++) {
            items[i] = new BulkItemRequest(i, new IndexRequest("index").id("id-" + i));
        }
        return items;
    }

    /** Drives the columnar mapping and returns the assembled ColumnBatch (full range). */
    private ColumnBatch mapToColumnBatch(IndexShard shard, EicfBatch batch, BulkItemRequest[] items) {
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(batch.schema(), shard.mapperService().mappingLookup());
        assertNotNull("expected batch path to support this mapping", resolution);
        List<Engine.Index> ops = ShardBatchMapper.mapColumnBatch(items, batch, shard, 0, resolution);
        assertNotNull("mapColumnBatch returned null (fallback signal)", ops);
        assertThat(ops, hasSize(items.length));
        for (int i = 0; i < items.length; i++) {
            assertThat(ops.get(i).id(), equalTo("id-" + i));
        }
        assertNotNull("a column batch provider must be attached", batch.columnBatchProvider());
        return batch.columnBatchProvider().columnBatch(0, batch.docCount());
    }

    private static Map<String, Column> columnsByName(ColumnBatch columnBatch) {
        Map<String, Column> byName = new HashMap<>();
        for (Column c : columnBatch.columns()) {
            byName.put(c.name(), c);
        }
        return byName;
    }

    private static long[] readLongs(LongColumn column, int count) {
        long[] values = new long[count];
        LongTupleCursor cursor = column.tuples();
        for (int d = 0; d < count; d++) {
            assertEquals(d, cursor.nextDoc());
            values[d] = cursor.longValue();
        }
        return values;
    }

    private static BytesRef[] readBinaries(BinaryColumn column, int count) {
        BytesRef[] values = new BytesRef[count];
        ObjectTupleCursor<BytesRef> cursor = column.tuples();
        for (int d = 0; d < count; d++) {
            assertEquals(d, cursor.nextDoc());
            values[d] = BytesRef.deepCopyOf(cursor.value());
        }
        return values;
    }

    public void testNumericColumns() throws Exception {
        String mapping = """
            {
              "properties": {
                "value": { "type": "long" },
                "score": { "type": "double" }
              }
            }""";
        IndexShard shard = newShardWithMapping(mapping, SYNTHETIC_SOURCE_SETTINGS);

        int numDocs = 3;
        List<BytesReference> sources = new java.util.ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            sources.add(new BytesArray("{\"value\":" + (100 + i) + ",\"score\":" + (1.5 + i) + "}"));
        }

        try (EicfBatch batch = EicfEncoder.encode(sources, XContentType.JSON)) {
            ColumnBatch columnBatch = mapToColumnBatch(shard, batch, items(numDocs));
            assertEquals(numDocs, columnBatch.numDocs());
            Map<String, Column> cols = columnsByName(columnBatch);

            Column valueCol = cols.get("value");
            assertNotNull("value column must be present", valueCol);
            assertTrue(valueCol instanceof LongColumn);
            assertEquals(LongColumn.NumericKind.LONG, ((LongColumn) valueCol).numericKind());
            long[] values = readLongs((LongColumn) valueCol, numDocs);
            for (int i = 0; i < numDocs; i++) {
                assertEquals(100L + i, values[i]);
            }

            Column scoreCol = cols.get("score");
            assertNotNull("score column must be present", scoreCol);
            assertEquals(LongColumn.NumericKind.DOUBLE, ((LongColumn) scoreCol).numericKind());
            long[] scores = readLongs((LongColumn) scoreCol, numDocs);
            for (int i = 0; i < numDocs; i++) {
                assertEquals("score column must carry the sortable-long encoding", NumericUtils.doubleToSortableLong(1.5 + i), scores[i]);
            }
        }

        closeShards(shard);
    }

    public void testMetadataColumns() throws Exception {
        String mapping = """
            {
              "properties": {
                "value": { "type": "long" }
              }
            }""";
        // Stored source so SourceFieldMapper contributes a stored _source column.
        IndexShard shard = newShardWithMapping(mapping, STORED_SOURCE_SETTINGS);

        int numDocs = 2;
        List<BytesReference> sources = List.of(new BytesArray("{\"value\":7}"), new BytesArray("{\"value\":8}"));

        try (EicfBatch batch = EicfEncoder.encode(sources, XContentType.JSON)) {
            ColumnBatch columnBatch = mapToColumnBatch(shard, batch, items(numDocs));
            Map<String, Column> cols = columnsByName(columnBatch);

            // _id: indexed+stored binary column carrying the encoded ids.
            Column idCol = cols.get(IdFieldMapper.NAME);
            assertNotNull("_id column must be present", idCol);
            assertTrue(idCol instanceof BinaryColumn);
            BytesRef[] ids = readBinaries((BinaryColumn) idCol, numDocs);
            for (int i = 0; i < numDocs; i++) {
                assertThat(ids[i], equalTo(Uid.encodeId("id-" + i)));
            }

            // Engine-assigned metadata columns are registered (mutable, filled by the engine later).
            assertNotNull("_seq_no column must be present", cols.get(SeqNoFieldMapper.NAME));
            assertNotNull("_primary_term column must be present", cols.get(SeqNoFieldMapper.PRIMARY_TERM_NAME));
            assertNotNull("_version column must be present", cols.get(VersionFieldMapper.NAME));

            // _source: stored binary column with the (reconstructed) document source.
            Column sourceCol = cols.get(SourceFieldMapper.NAME);
            assertNotNull("_source column must be present in stored-source mode", sourceCol);
            assertTrue(sourceCol instanceof BinaryColumn);
            BytesRef[] srcs = readBinaries((BinaryColumn) sourceCol, numDocs);
            assertThat("reconstructed _source must be non-empty", srcs[0], not(equalTo(new BytesRef())));
            assertThat(srcs[0].length, greaterThan(0));
        }

        closeShards(shard);
    }

    public void testNumberColumnConvertsMixedTypes() throws Exception {
        String mapping = """
            {
              "properties": {
                "value": { "type": "long" }
              }
            }""";
        IndexShard shard = newShardWithMapping(mapping, SYNTHETIC_SOURCE_SETTINGS);

        // A heterogeneous "value" column promotes to UNION: number, numeric string, explicit null,
        // unparseable string. The batch must stay columnar — the convertible docs are indexed and the
        // null / unparseable docs are left absent (rather than falling back to the row-major path).
        List<BytesReference> sources = List.of(
            new BytesArray("{\"value\":10}"),
            new BytesArray("{\"value\":\"20\"}"),
            new BytesArray("{\"value\":null}"),
            new BytesArray("{\"value\":\"not-a-number\"}")
        );
        int numDocs = sources.size();

        try (EicfBatch batch = EicfEncoder.encode(sources, XContentType.JSON)) {
            ColumnBatch columnBatch = mapToColumnBatch(shard, batch, items(numDocs));
            Map<String, Column> cols = columnsByName(columnBatch);

            Column valueCol = cols.get("value");
            assertNotNull("value column must be present (no fallback)", valueCol);
            assertTrue(valueCol instanceof LongColumn);
            assertEquals(Column.Density.SPARSE, valueCol.density());

            LongTupleCursor cursor = ((LongColumn) valueCol).tuples();
            assertEquals(0, cursor.nextDoc());
            assertEquals(10L, cursor.longValue());
            assertEquals("numeric string is parsed and indexed", 1, cursor.nextDoc());
            assertEquals(20L, cursor.longValue());
            assertEquals("null and unparseable string leave the field absent", DocIdSetIterator.NO_MORE_DOCS, cursor.nextDoc());
        }

        closeShards(shard);
    }

    public void testKeywordColumnIsSortedSet() throws Exception {
        String mapping = """
            {
              "properties": {
                "s": { "type": "keyword", "index": false }
              }
            }""";
        IndexShard shard = newShardWithMapping(mapping, SYNTHETIC_SOURCE_SETTINGS);

        List<BytesReference> sources = List.of(new BytesArray("{\"s\":\"alpha\"}"), new BytesArray("{\"s\":\"beta\"}"));
        try (EicfBatch batch = EicfEncoder.encode(sources, XContentType.JSON)) {
            ColumnBatch columnBatch = mapToColumnBatch(shard, batch, items(sources.size()));
            Column s = columnsByName(columnBatch).get("s");
            assertNotNull("keyword column must be present (no fallback)", s);
            assertTrue(s instanceof BinaryColumn);
            assertEquals(DocValuesType.SORTED_SET, s.fieldType().docValuesType());
            BytesRef[] values = readBinaries((BinaryColumn) s, sources.size());
            assertEquals("alpha", values[0].utf8ToString());
            assertEquals("beta", values[1].utf8ToString());
        }

        closeShards(shard);
    }

    public void testKeywordHighCardinalityEmitsBinaryAndCounts() throws Exception {
        assumeTrue(
            "extended doc_values params feature flag must be enabled",
            FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled()
        );
        String mapping = """
            {
              "properties": {
                "s": { "type": "keyword", "index": false, "doc_values": { "cardinality": "high" } }
              }
            }""";
        IndexShard shard = newShardWithMapping(mapping, SYNTHETIC_SOURCE_SETTINGS);

        List<BytesReference> sources = List.of(new BytesArray("{\"s\":\"alpha\"}"), new BytesArray("{\"s\":\"beta\"}"));
        try (EicfBatch batch = EicfEncoder.encode(sources, XContentType.JSON)) {
            ColumnBatch columnBatch = mapToColumnBatch(shard, batch, items(sources.size()));
            Map<String, Column> cols = columnsByName(columnBatch);

            // High-cardinality keyword stores the value as BINARY doc values...
            Column value = cols.get("s");
            assertNotNull("keyword value column must be present", value);
            assertTrue(value instanceof BinaryColumn);
            assertEquals(DocValuesType.BINARY, value.fieldType().docValuesType());
            BytesRef[] values = readBinaries((BinaryColumn) value, sources.size());
            assertEquals("alpha", values[0].utf8ToString());
            assertEquals("beta", values[1].utf8ToString());

            // ...plus a companion <name>.counts numeric field carrying the per-document value count.
            Column counts = cols.get("s.counts");
            assertNotNull("keyword counts column must be present", counts);
            assertTrue(counts instanceof LongColumn);
            assertEquals(DocValuesType.NUMERIC, counts.fieldType().docValuesType());
            assertArrayEquals(new long[] { 1L, 1L }, readLongs((LongColumn) counts, sources.size()));
        }

        closeShards(shard);
    }

    public void testDateColumnParsesStrings() throws Exception {
        String mapping = """
            {
              "properties": {
                "d": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss", "index": false }
              }
            }""";
        IndexShard shard = newShardWithMapping(mapping, SYNTHETIC_SOURCE_SETTINGS);

        List<BytesReference> sources = List.of(
            new BytesArray("{\"d\":\"2013-07-15 03:39:17\"}"),
            new BytesArray("{\"d\":\"2020-01-02 00:00:00\"}")
        );
        try (EicfBatch batch = EicfEncoder.encode(sources, XContentType.JSON)) {
            Column d = columnsByName(mapToColumnBatch(shard, batch, items(sources.size()))).get("d");
            assertNotNull("date column must be present (no fallback)", d);
            assertTrue(d instanceof LongColumn);
            assertEquals(LongColumn.NumericKind.LONG, ((LongColumn) d).numericKind());
            // multi-value default → SORTED_NUMERIC (matches the row path and index sorting)
            assertEquals(DocValuesType.SORTED_NUMERIC, d.fieldType().docValuesType());
            long[] values = readLongs((LongColumn) d, sources.size());
            assertEquals(Instant.parse("2013-07-15T03:39:17Z").toEpochMilli(), values[0]);
            assertEquals(Instant.parse("2020-01-02T00:00:00Z").toEpochMilli(), values[1]);
        }

        closeShards(shard);
    }

    public void testFlattenedColumnGroupEmitsKeyedAndRootColumns() throws Exception {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        String mapping = """
            {
              "properties": {
                "attrs": { "type": "flattened" }
              }
            }""";
        IndexShard shard = newShardWithMapping(mapping, COLUMNAR_SETTINGS);

        // doc0 has two keys (multi-value SeparateCount encoding), doc1 a single key (raw encoding),
        // doc2 has no attrs at all (absent in every column). Note: an empty flattened object {} is not
        // encodable in EICF's columnar format, so the absent case uses a document without the field.
        List<BytesReference> sources = List.of(
            new BytesArray("{\"attrs\":{\"host.name\":\"h1\",\"os.type\":\"linux\"}}"),
            new BytesArray("{\"attrs\":{\"host.name\":\"h2\"}}"),
            new BytesArray("{}")
        );
        int numDocs = sources.size();

        try (EicfBatch batch = EicfEncoder.encode(sources, XContentType.JSON)) {
            ColumnBatch columnBatch = mapToColumnBatch(shard, batch, items(numDocs));
            Map<String, Column> cols = columnsByName(columnBatch);

            // Keyed sub-field: BINARY values + NUMERIC .counts companion (MultiValuedBinaryDocValuesField SeparateCount).
            Column keyed = cols.get("attrs._keyed");
            assertNotNull("attrs._keyed column must be present", keyed);
            assertTrue(keyed instanceof BinaryColumn);
            assertEquals(DocValuesType.BINARY, keyed.fieldType().docValuesType());
            Column keyedCounts = cols.get("attrs._keyed.counts");
            assertNotNull("attrs._keyed.counts column must be present", keyedCounts);
            assertEquals(DocValuesType.NUMERIC, keyedCounts.fieldType().docValuesType());
            // doc2 (empty object) is absent in every column.
            assertEquals(Column.Density.SPARSE, keyed.density());

            // Sorted-unique "key\0value" entries; doc2 absent (cursor skips it).
            assertSeparateCount(
                (BinaryColumn) keyed,
                (LongColumn) keyedCounts,
                List.of(List.of("host.name\0h1", "os.type\0linux"), List.of("host.name\0h2"))
            );

            // In (strict) columnar mode the flattened field is doc-values-only with indexed=false, so the
            // root field carries no doc values (hasRootDocValues=false): only the keyed columns are emitted.
            assertNull("root value column must not be present in columnar mode", cols.get("attrs"));
            assertNull("root counts column must not be present in columnar mode", cols.get("attrs.counts"));
        }

        closeShards(shard);
    }

    /**
     * Asserts a SeparateCount values+counts column pair decodes to {@code expectedByDoc} for the present
     * documents in order, and that no further documents are present (trailing docs are absent).
     */
    private static void assertSeparateCount(BinaryColumn values, LongColumn counts, List<List<String>> expectedByDoc) {
        ObjectTupleCursor<BytesRef> valueCursor = values.tuples();
        LongTupleCursor countCursor = counts.tuples();
        for (List<String> expected : expectedByDoc) {
            int doc = valueCursor.nextDoc();
            assertThat(doc, not(equalTo(DocIdSetIterator.NO_MORE_DOCS)));
            assertEquals("values and counts must cover the same documents", doc, countCursor.nextDoc());
            int count = (int) countCursor.longValue();
            assertEquals(expected.size(), count);
            assertEquals(expected, decode(BytesRef.deepCopyOf(valueCursor.value()), count));
        }
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, valueCursor.nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, countCursor.nextDoc());
    }

    /** Decodes a SeparateCount binary doc value: raw bytes for a single value, else {@code [VInt len][bytes]…}. */
    private static List<String> decode(BytesRef packed, int count) {
        if (count == 1) {
            return List.of(packed.utf8ToString());
        }
        List<String> out = new ArrayList<>(count);
        ByteArrayDataInput in = new ByteArrayDataInput(packed.bytes, packed.offset, packed.length);
        for (int i = 0; i < count; i++) {
            int len = in.readVInt();
            int pos = in.getPosition();
            out.add(new String(packed.bytes, pos, len, StandardCharsets.UTF_8));
            in.setPosition(pos + len);
        }
        return out;
    }

    // TODO columnar: re-enable / rewrite for the columnar path when these mappers support batch indexing.
    // - testBooleanMapper / testIpMapper / testIpMapperIgnoreMalformed / testTextMapper: types not supported.
    // - testParseMappingsSyntheticSourceAndIgnored: _ignored / synthetic ignore_above handling is a follow-up.
}
