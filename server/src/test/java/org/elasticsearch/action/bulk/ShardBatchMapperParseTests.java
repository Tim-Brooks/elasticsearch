/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineBatch;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper.BatchMapperResolution;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

/**
 * Parse-time tests for the batch-mapping fast path: drives {@link ShardBatchMapper} directly and
 * verifies the resulting columnar output. Engine indexing is intentionally not exercised here —
 * those interactions are covered by {@code ShardBatchIndexer} tests; this file's job is to lock
 * down the mapper's columnar parsing contract.
 */
public class ShardBatchMapperParseTests extends IndexShardTestCase {

    /**
     * COLUMNAR mode with synthetic recovery source. Synthetic recovery satisfies
     * {@link org.elasticsearch.index.mapper.SourceFieldMapper#supportsColumnarParse} (only a size
     * estimate is stored, not the full source), while keeping recovery source enabled so that
     * {@code RecoverySourceHandler} can open a changes snapshot for replica recovery.
     */
    private static final Settings COLUMNAR_SETTINGS = indexSettings(IndexVersion.current(), 1, 0).put(
        IndexSettings.MODE.getKey(),
        IndexMode.COLUMNAR.getName()
    ).put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true).build();

    private static final Settings STORED_SOURCE_SETTINGS = indexSettings(IndexVersion.current(), 1, 0).build();

    private IndexShard newShardWithMapping(String mapping, Settings settings) throws IOException {
        IndexMetadata md = IndexMetadata.builder("index").putMapping(mapping).settings(settings).primaryTerm(0, 1).build();
        IndexShard shard = newShard(new ShardId(md.getIndex(), 0), true, "n1", md, null);
        recoverShardFromStore(shard);
        return shard;
    }

    private static IndexRequest indexRequest(String id) {
        return new IndexRequest("index").id(id);
    }

    /** Builds a single-document JSON {@link BytesReference} from alternating name/value pairs. */
    private static BytesReference doc(Object... kvPairs) throws IOException {
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject();
            for (int i = 0; i < kvPairs.length; i += 2) {
                b.field((String) kvPairs[i], kvPairs[i + 1]);
            }
            b.endObject();
            return BytesReference.bytes(b);
        }
    }

    /**
     * Calls {@code resolveMappers} then {@code mapColumnBatch} over the full batch (no chunking).
     * Returns the {@link EngineBatch}, or {@code null} if the columnar path was not taken.
     */
    private static EngineBatch mapBatch(IndexShard shard, BulkItemRequest[] items, SourceBatch batch) {
        final BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(
            batch.schema(),
            shard.mapperService().mappingLookup(),
            shard.indexSettings()
        );
        if (resolution == null) {
            return null;
        }
        return ShardBatchMapper.mapColumnBatch(items, batch, shard, 0, items.length, resolution, Engine.Operation.Origin.PRIMARY);
    }

    /**
     * Verifies that {@code mapColumnBatch} produces a non-null result for a simple keyword mapping
     * in COLUMNAR mode, confirming the columnar path is taken end-to-end.
     */
    public void testParseMappingsAddsMetadataFields() throws IOException {
        final String mapping = """
            {
              "dynamic": "strict",
              "properties": {
                "f": { "type": "keyword" }
              }
            }""";

        IndexShard shard = newShardWithMapping(mapping, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")) };
            try (SourceBatch batch = EscfEncoder.encode(List.of(doc("f", "hello")), XContentType.JSON)) {
                EngineBatch result = mapBatch(shard, items, batch);
                assertNotNull("expected columnar path to succeed", result);

                final MappedColumns mc = result.columns();
                mc.fillPrimaryTerm(1L);
                mc.setSeqNo(0, 10L);
                mc.setVersion(0, 1L);

                final MappedColumns.RowCursor cursor = mc.rowCursor();
                cursor.advance();
                final List<IndexableField> fields = cursor.fields();

                assertTrue("_id should be present", fields.stream().anyMatch(f -> "_id".equals(f.name())));
                assertTrue("_seq_no should be present", fields.stream().anyMatch(f -> "_seq_no".equals(f.name())));
                assertTrue("_primary_term should be present", fields.stream().anyMatch(f -> "_primary_term".equals(f.name())));
                assertTrue("_version should be present", fields.stream().anyMatch(f -> "_version".equals(f.name())));
                // Keyword field "f" should also appear in the binary doc-values column.
                assertTrue("keyword field f should be present", fields.stream().anyMatch(f -> "f".equals(f.name())));
            }
        } finally {
            closeShards(shard);
        }
    }

    /**
     * Verifies that a keyword value exceeding {@code ignore_above} does not crash the columnar path
     * and causes the field name to appear in the {@code _ignored} column.
     */
    public void testIgnoreAboveOnKeywordDoesNotFail() throws IOException {
        final String mapping = """
            {
              "dynamic": "strict",
              "properties": {
                "f": { "type": "keyword", "ignore_above": 5 }
              }
            }""";

        IndexShard shard = newShardWithMapping(mapping, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")) };
            // "toolong" is 7 chars, exceeds ignore_above=5.
            try (SourceBatch batch = EscfEncoder.encode(List.of(doc("f", "toolong")), XContentType.JSON)) {
                EngineBatch result = mapBatch(shard, items, batch);
                assertNotNull("expected columnar path to succeed with ignore_above exceeded", result);

                final MappedColumns mc = result.columns();
                mc.fillPrimaryTerm(1L);
                mc.setSeqNo(0, 1L);
                mc.setVersion(0, 1L);

                final MappedColumns.RowCursor cursor = mc.rowCursor();
                cursor.advance();
                final List<IndexableField> fields = cursor.fields();

                // LuceneBinaryColumn stores field names as BytesRef, so check binaryValue(), not stringValue().
                final BytesRef fRef = new BytesRef("f");
                assertTrue(
                    "_ignored should contain field name f",
                    fields.stream().anyMatch(fld -> "_ignored".equals(fld.name()) && fRef.equals(fld.binaryValue()))
                );
                // The ignored value should not land in the binary doc-values column.
                assertFalse(
                    "f binary DV should be absent when value exceeds ignore_above",
                    fields.stream().anyMatch(fld -> "f".equals(fld.name()) && fld.binaryValue() != null)
                );
            }
        } finally {
            closeShards(shard);
        }
    }

    /**
     * Verifies that an explicit JSON null for a keyword field produces a null slot in the
     * doc-values encoding — no term, no binary value, and no crash.
     */
    public void testNullValuesAreSkipped() throws IOException {
        final String mapping = """
            {
              "dynamic": "strict",
              "properties": {
                "f": { "type": "keyword" }
              }
            }""";

        IndexShard shard = newShardWithMapping(mapping, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")), new BulkItemRequest(1, indexRequest("doc2")) };
            try (
                SourceBatch batch = EscfEncoder.encode(
                    List.of(new BytesArray("{\"f\":\"hello\"}"), new BytesArray("{\"f\":null}")),
                    XContentType.JSON
                )
            ) {
                EngineBatch result = mapBatch(shard, items, batch);
                assertNotNull("expected columnar path to succeed", result);

                final MappedColumns mc = result.columns();
                mc.fillPrimaryTerm(1L);
                for (int i = 0; i < 2; i++) {
                    mc.setSeqNo(i, i + 1L);
                    mc.setVersion(i, 1L);
                }

                final MappedColumns.RowCursor cursor = mc.rowCursor();

                // Doc 0 has a real value — binary DV for "f" should be present.
                cursor.advance();
                List<IndexableField> doc0Fields = cursor.fields();
                assertTrue(
                    "doc0: f binary DV should be present for non-null value",
                    doc0Fields.stream().anyMatch(fld -> "f".equals(fld.name()) && fld.binaryValue() != null)
                );

                // Doc 1 has an explicit null — no binary DV blob for "f" (null slot, no value).
                cursor.advance();
                List<IndexableField> doc1Fields = cursor.fields();
                assertFalse(
                    "doc1: f binary DV should be absent for explicit null",
                    doc1Fields.stream().anyMatch(fld -> "f".equals(fld.name()) && fld.binaryValue() != null)
                );
            }
        } finally {
            closeShards(shard);
        }
    }

    // TODO(columnar): bring back once the corresponding field mappers support columnar parsing:
    // - testSupportedMapperTypes (date, long, double — keyword already covered above)
    // - testNumberMapperReceivesStringValue (long/double with a string source value)
    // - testParseMappingsSyntheticSourceAndIgnored
    // - testBooleanMapper
    // - testIpMapper
    // - testIpMapperIgnoreMalformed
    // - testTextMapper

    private static final String FLATTENED_MAPPING = """
        {
          "dynamic": "strict",
          "properties": {
            "flat": { "type": "flattened" }
          }
        }""";

    /**
     * The core flattened-field regression test: a batch containing a flattened field's subkeys must
     * take the columnar path (non-null result) and emit the {@code flat._keyed} and
     * {@code flat._keyed.counts} output columns. Before the column-group logic was added,
     * {@code resolveMappers} would see an unmapped leaf ({@code flat.key1}) whose field type was
     * non-null (FlattenedFieldType is a DynamicFieldType), classify it as a runtime-field shadow, and
     * return null, forcing every flattened-field batch onto the row path.
     */
    public void testFlattenedFieldProducesKeyedColumns() throws IOException {
        IndexShard shard = newShardWithMapping(FLATTENED_MAPPING, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")) };
            try (
                SourceBatch batch = EscfEncoder.encode(
                    List.of(new BytesArray("{\"flat\":{\"key1\":\"a\",\"key2\":\"b\"}}")),
                    XContentType.JSON
                )
            ) {
                EngineBatch result = mapBatch(shard, items, batch);
                assertNotNull("flattened field must take the columnar path", result);

                final MappedColumns mc = result.columns();
                mc.fillPrimaryTerm(1L);
                mc.setSeqNo(0, 1L);
                mc.setVersion(0, 1L);

                final MappedColumns.RowCursor cursor = mc.rowCursor();
                cursor.advance();
                final List<IndexableField> fields = cursor.fields();

                // The keyed doc-values blob must be present.
                assertTrue(
                    "flat._keyed binary DV must be present",
                    fields.stream().anyMatch(f -> "flat._keyed".equals(f.name()) && f.binaryValue() != null)
                );
                // The counts column must report 2 slots (one per key).
                assertTrue(
                    "flat._keyed.counts must report 2",
                    fields.stream().anyMatch(f -> "flat._keyed.counts".equals(f.name()) && Long.valueOf(2L).equals(f.numericValue()))
                );
            }
        } finally {
            closeShards(shard);
        }
    }

    /** A flattened field and a keyword field in the same batch each emit their respective columns. */
    public void testFlattenedAndKeywordInSameBatch() throws IOException {
        final String mapping = """
            {
              "dynamic": "strict",
              "properties": {
                "host": { "type": "keyword" },
                "flat": { "type": "flattened" }
              }
            }""";

        IndexShard shard = newShardWithMapping(mapping, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")), new BulkItemRequest(1, indexRequest("doc2")) };
            try (
                SourceBatch batch = EscfEncoder.encode(
                    List.of(
                        new BytesArray("{\"host\":\"h1\",\"flat\":{\"k\":\"v1\"}}"),
                        new BytesArray("{\"host\":\"h2\",\"flat\":{\"k\":\"v2\"}}")
                    ),
                    XContentType.JSON
                )
            ) {
                EngineBatch result = mapBatch(shard, items, batch);
                assertNotNull("expected columnar path to succeed", result);

                final MappedColumns mc = result.columns();
                mc.fillPrimaryTerm(1L);
                for (int i = 0; i < 2; i++) {
                    mc.setSeqNo(i, i + 1L);
                    mc.setVersion(i, 1L);
                }

                final MappedColumns.RowCursor cursor = mc.rowCursor();
                for (int doc = 0; doc < 2; doc++) {
                    cursor.advance();
                    final List<IndexableField> fields = cursor.fields();
                    assertTrue(
                        "doc" + doc + ": host binary DV must be present",
                        fields.stream().anyMatch(f -> "host".equals(f.name()) && f.binaryValue() != null)
                    );
                    assertTrue(
                        "doc" + doc + ": flat._keyed must be present",
                        fields.stream().anyMatch(f -> "flat._keyed".equals(f.name()) && f.binaryValue() != null)
                    );
                }
            }
        } finally {
            closeShards(shard);
        }
    }

    /**
     * A {@code null} flattened value goes through {@code FlattenedFieldMapper#mapColumnBatch} (the own-path
     * leaf path), which asserts it is all-null-or-empty-object and emits nothing. The batch must
     * still succeed (non-null result) and produce no {@code flat._keyed} field.
     */
    public void testFlattenedNullValueEmitsNoKeyedColumn() throws IOException {
        IndexShard shard = newShardWithMapping(FLATTENED_MAPPING, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")) };
            try (SourceBatch batch = EscfEncoder.encode(List.of(new BytesArray("{\"flat\":null}")), XContentType.JSON)) {
                EngineBatch result = mapBatch(shard, items, batch);
                assertNotNull("null flattened value must not disable the columnar path", result);

                final MappedColumns mc = result.columns();
                mc.fillPrimaryTerm(1L);
                mc.setSeqNo(0, 1L);
                mc.setVersion(0, 1L);

                final MappedColumns.RowCursor cursor = mc.rowCursor();
                cursor.advance();
                final List<IndexableField> fields = cursor.fields();
                assertFalse(
                    "null flattened value must not emit a flat._keyed field",
                    fields.stream().anyMatch(f -> f.name().startsWith("flat._keyed"))
                );
            }
        } finally {
            closeShards(shard);
        }
    }

    /**
     * A mixed batch: first doc has {@code "flat":null} (own-path leaf path), second has
     * {@code "flat":{"k":"v"}} (group path). Both docs must be processed without a fallback, and
     * only the second doc emits a {@code flat._keyed} field.
     */
    public void testFlattenedMixedNullAndKeyedDocInSameBatch() throws IOException {
        IndexShard shard = newShardWithMapping(FLATTENED_MAPPING, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = { new BulkItemRequest(0, indexRequest("doc1")), new BulkItemRequest(1, indexRequest("doc2")) };
            try (
                SourceBatch batch = EscfEncoder.encode(
                    List.of(new BytesArray("{\"flat\":null}"), new BytesArray("{\"flat\":{\"k\":\"v\"}}")),
                    XContentType.JSON
                )
            ) {
                EngineBatch result = mapBatch(shard, items, batch);
                assertNotNull("mixed null+keyed batch must take the columnar path", result);

                final MappedColumns mc = result.columns();
                mc.fillPrimaryTerm(1L);
                for (int i = 0; i < 2; i++) {
                    mc.setSeqNo(i, i + 1L);
                    mc.setVersion(i, 1L);
                }

                final MappedColumns.RowCursor cursor = mc.rowCursor();

                // Doc 0: null → no keyed field.
                cursor.advance();
                assertFalse(
                    "doc0 (null): must not emit flat._keyed",
                    cursor.fields().stream().anyMatch(f -> f.name().startsWith("flat._keyed"))
                );

                // Doc 1: {k:v} → keyed field with count 1.
                cursor.advance();
                final List<IndexableField> doc1Fields = cursor.fields();
                assertTrue(
                    "doc1: flat._keyed must be present",
                    doc1Fields.stream().anyMatch(f -> "flat._keyed".equals(f.name()) && f.binaryValue() != null)
                );
                assertTrue(
                    "doc1: flat._keyed.counts must be 1",
                    doc1Fields.stream().anyMatch(f -> "flat._keyed.counts".equals(f.name()) && Long.valueOf(1L).equals(f.numericValue()))
                );
            }
        } finally {
            closeShards(shard);
        }
    }

    /**
     * Verifies that {@code resolveMappers} is computed once per batch and the column indexes it
     * records remain valid when {@code mapColumnBatch} is called on different chunk slices.
     * {@link org.elasticsearch.escf.EscfBatch#slice} preserves column ordering, so leaf indexes
     * computed at resolve time are stable.
     */
    public void testFlattenedGroupAcrossChunks() throws IOException {
        IndexShard shard = newShardWithMapping(FLATTENED_MAPPING, COLUMNAR_SETTINGS);
        try {
            final BulkItemRequest[] items = {
                new BulkItemRequest(0, indexRequest("doc1")),
                new BulkItemRequest(1, indexRequest("doc2")),
                new BulkItemRequest(2, indexRequest("doc3")),
                new BulkItemRequest(3, indexRequest("doc4")) };
            try (
                SourceBatch batch = EscfEncoder.encode(
                    List.of(
                        new BytesArray("{\"flat\":{\"k\":\"v1\"}}"),
                        new BytesArray("{\"flat\":{\"k\":\"v2\"}}"),
                        new BytesArray("{\"flat\":{\"k\":\"v3\"}}"),
                        new BytesArray("{\"flat\":{\"k\":\"v4\"}}")
                    ),
                    XContentType.JSON
                )
            ) {
                // Resolve once, then map two chunks separately.
                final BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(
                    batch.schema(),
                    shard.mapperService().mappingLookup(),
                    shard.indexSettings()
                );
                assertNotNull(resolution);

                final EngineBatch chunk0 = ShardBatchMapper.mapColumnBatch(
                    items,
                    batch,
                    shard,
                    0,
                    2,
                    resolution,
                    Engine.Operation.Origin.PRIMARY
                );
                final EngineBatch chunk1 = ShardBatchMapper.mapColumnBatch(
                    items,
                    batch,
                    shard,
                    2,
                    4,
                    resolution,
                    Engine.Operation.Origin.PRIMARY
                );
                assertNotNull("chunk 0 must succeed", chunk0);
                assertNotNull("chunk 1 must succeed", chunk1);

                for (EngineBatch chunk : new EngineBatch[] { chunk0, chunk1 }) {
                    final MappedColumns mc = chunk.columns();
                    mc.fillPrimaryTerm(1L);
                    for (int i = 0; i < 2; i++) {
                        mc.setSeqNo(i, i + 1L);
                        mc.setVersion(i, 1L);
                    }
                    final MappedColumns.RowCursor cursor = mc.rowCursor();
                    for (int doc = 0; doc < 2; doc++) {
                        cursor.advance();
                        assertTrue(
                            "chunk doc " + doc + ": flat._keyed must be present",
                            cursor.fields().stream().anyMatch(f -> "flat._keyed".equals(f.name()) && f.binaryValue() != null)
                        );
                    }
                }
            }
        } finally {
            closeShards(shard);
        }
    }
}
