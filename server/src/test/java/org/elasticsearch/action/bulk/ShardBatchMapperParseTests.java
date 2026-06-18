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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.eicf.EicfBatch;
import org.elasticsearch.eicf.EicfEncoder;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
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
 * boolean/ip/date and string-into-number cases are commented out until re-added.
 */
public class ShardBatchMapperParseTests extends IndexShardTestCase {

    private static final Settings SYNTHETIC_SOURCE_SETTINGS = indexSettings(IndexVersion.current(), 1, 0).put(
        "index.mapping.source.mode",
        "synthetic"
    ).build();

    private static final Settings STORED_SOURCE_SETTINGS = indexSettings(IndexVersion.current(), 1, 0).build();

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

    // TODO columnar: re-enable / rewrite for the columnar path when these mappers support batch indexing.
    // - testSupportedMapperTypes (date + keyword): keyword/date not supported yet.
    // - testIgnoreAboveOnKeywordDoesNotFail, testKeyword*: keyword not supported.
    // - testNumberMapperReceivesStringValue: string-into-long produces a UNION column -> fallback (not columnar).
    // - testBooleanMapper / testIpMapper / testIpMapperIgnoreMalformed / testTextMapper: types not supported.
    // - testNullValuesAreSkipped: an explicit null promotes a numeric column to UNION -> fallback (not columnar).
    // - testParseMappingsSyntheticSourceAndIgnored: _ignored / synthetic ignore_above handling is a follow-up.
}
