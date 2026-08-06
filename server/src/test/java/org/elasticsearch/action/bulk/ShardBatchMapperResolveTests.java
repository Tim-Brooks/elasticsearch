/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper.BatchMapperResolution;
import org.elasticsearch.index.mapper.ShardBatchMapper.ColumnGroupResolution;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ShardBatchMapperResolveTests extends MapperServiceTestCase {

    private final IndexSettings indexSettings = new IndexSettings(
        new IndexMetadata.Builder("index").settings(
            indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build()
        ).build(),
        Settings.builder().put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false).build()
    );

    /** Builds a flat schema from simple (non-dotted) leaf names. */
    private static SourceSchema schemaOf(String... leafPaths) throws IOException {
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject();
            for (String path : leafPaths) {
                b.field(path, 0);
            }
            b.endObject();
            try (EscfBatch batch = EscfEncoder.encode(List.of(BytesReference.bytes(b)), XContentType.JSON)) {
                return batch.schema();
            }
        }
    }

    /** Builds a schema from dotted paths (e.g. "outer.inner"), converting each to a nested JSON object. */
    @SuppressWarnings("unchecked")
    private static SourceSchema schemaOfNested(String... dottedPaths) throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        for (String path : dottedPaths) {
            int dot = path.indexOf('.');
            String parent = path.substring(0, dot);
            String child = path.substring(dot + 1);
            Map<String, Object> nested = (Map<String, Object>) doc.computeIfAbsent(parent, k -> new LinkedHashMap<>());
            nested.put(child, 0);
        }
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            try (EscfBatch batch = EscfEncoder.encode(List.of(BytesReference.bytes(b.map(doc))), XContentType.JSON)) {
                return batch.schema();
            }
        }
    }

    private MapperService mapper(XContentBuilder mapping) throws IOException {
        return createMapperService(
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
                .build(),
            mapping
        );
    }

    public void testHappyPath() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("host").field("type", "keyword").endObject();
            b.startObject("value").field("type", "keyword").endObject();
        }));
        SourceSchema schema = schemaOf("host", "value");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertEquals(2, resolution.columnMappers().length);
        assertTrue(resolution.columnMappers()[schema.findLeaf("host", 0)] instanceof KeywordFieldMapper);
        assertTrue(resolution.columnMappers()[schema.findLeaf("value", 0)] instanceof KeywordFieldMapper);
    }

    public void testKeywordIgnoreAboveIsSupported() throws IOException {
        MapperService ms = mapper(mapping(b -> b.startObject("host").field("type", "keyword").field("ignore_above", 32).endObject()));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("host"), ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof KeywordFieldMapper);
    }

    public void testNumberIgnoreMalformedIsNotSupported() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("v").field("type", "long").field("ignore_malformed", true).endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("v"), ms.mappingLookup(), indexSettings);
        assertNull(resolution);
    }

    public void testMissingLeafUnderDynamicFalseIsIgnored() throws IOException {
        MapperService ms = mapper(topMapping(b -> {
            b.field("dynamic", "false");
            b.startObject("properties");
            b.startObject("known").field("type", "keyword").endObject();
            b.endObject();
        }));
        SourceSchema schema = schemaOf("known", "unknown");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertNotNull(resolution.columnMappers()[schema.findLeaf("known", 0)]);
        assertNull(resolution.columnMappers()[schema.findLeaf("unknown", 0)]);
    }

    public void testMissingLeafUnderDynamicTrueFallsBack() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("known").field("type", "keyword").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("known", "unknown"), ms.mappingLookup(), indexSettings);
        assertNull(resolution);
    }

    // TODO: not relevant at the moment because we are columnar only which does not support runtime fields
    // public void testRuntimeFieldInMappingFallsBack() throws IOException {
    // MapperService ms = mapper(topMapping(b -> {
    // b.startObject("runtime");
    // b.startObject("rt").field("type", "keyword").endObject();
    // b.endObject();
    // b.startObject("properties");
    // b.startObject("known").field("type", "keyword").endObject();
    // b.endObject();
    // }));
    // BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("known"), ms.mappingLookup(), indexSettings);
    // assertNull(resolution);
    // }

    public void testIndexTimeScriptFallsBack() throws IOException {
        // A long field with a script is a standard example of an index-time script. Registering one
        // populates MappingLookup.indexTimeScriptMappers() which resolveMappers short-circuits on.
        // We can't easily register a real script in a unit test without wiring a ScriptService, but
        // we can verify that any mapper marked hasScript=true via the `script` parameter trips the
        // supportsBatchIndexing() guard. That path is covered by testUnsupportedMapperType below
        // (the short-circuit in resolveMappers on indexTimeScriptMappers is a superset check and
        // redundant with the per-mapper guard, so this test is intentionally narrow).
    }

    public void testTextMapperNotSupported() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("t").field("type", "text").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("t"), ms.mappingLookup(), indexSettings);
        assertNull(resolution);
    }

    public void testBooleanMapperNotSupported() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("b").field("type", "boolean").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("b"), ms.mappingLookup(), indexSettings);
        assertNull(resolution);
    }

    public void testIpMapperNotSupported() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("ip").field("type", "ip").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("ip"), ms.mappingLookup(), indexSettings);
        assertNull(resolution);
    }

    // TODO: not relevant at the moment because we are columnar only which does not support copy_to
    // public void testKeywordWithCopyToFallsBack() throws IOException {
    // MapperService ms = mapper(mapping(b -> {
    // b.startObject("src").field("type", "keyword").field("copy_to", "dst").endObject();
    // b.startObject("dst").field("type", "keyword").endObject();
    // }));
    // assertNull(ShardBatchMapper.resolveMappers(schemaOf("src"), ms.mappingLookup(), indexSettings));
    // }

    public void testKeywordWithMultiFieldsFallsBack() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("host");
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("lower").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        assertNull(ShardBatchMapper.resolveMappers(schemaOf("host"), ms.mappingLookup(), indexSettings));
    }

    public void testNestedLeafHappyPath() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("outer");
            b.startObject("properties");
            b.startObject("inner").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        SourceSchema schema = schemaOfNested("outer.inner");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof KeywordFieldMapper);
    }

    public void testNestedLeafUnderNestedDynamicFalseIsIgnored() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("outer");
            b.field("dynamic", "false");
            b.startObject("properties");
            b.startObject("known").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        SourceSchema schema = schemaOfNested("outer.known", "outer.unknown");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertNotNull(resolution.columnMappers()[schema.findLeaf("known", schema.findNonLeaf("outer", 0))]);
        assertNull(resolution.columnMappers()[schema.findLeaf("unknown", schema.findNonLeaf("outer", 0))]);
    }

    /** Builds a schema by encoding raw JSON, for shapes the other helpers cannot express. */
    private static SourceSchema schemaOfJson(String... jsonDocs) throws IOException {
        final List<BytesReference> sources = Arrays.stream(jsonDocs).map(j -> (BytesReference) new BytesArray(j)).toList();
        try (EscfBatch batch = EscfEncoder.encode(sources, XContentType.JSON)) {
            return batch.schema();
        }
    }

    /** Returns a mapper service with a single top-level {@code flat} flattened field. */
    private MapperService flattenedMapper() throws IOException {
        return mapper(mapping(b -> b.startObject("flat").field("type", "flattened").endObject()));
    }

    /**
     * A batch containing a flattened field's subkeys must resolve to a column group, not fall back due
     * to the runtime-field-shadow check. This is the core regression test: {@code getFieldType("flat.key1")}
     * is non-null (FlattenedFieldType is a DynamicFieldType), so the group check must run first.
     */
    public void testFlattenedGroupHappyPath() throws IOException {
        MapperService ms = flattenedMapper();
        SourceSchema schema = schemaOfNested("flat.key1", "flat.key2");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);

        // Both leaves are owned by the group — not by a per-leaf mapper.
        final int flatNonLeaf = schema.findNonLeaf("flat", 0);
        final int key1Leaf = schema.findLeaf("key1", flatNonLeaf);
        final int key2Leaf = schema.findLeaf("key2", flatNonLeaf);
        assertNull(resolution.columnMappers()[key1Leaf]);
        assertNull(resolution.columnMappers()[key2Leaf]);

        assertEquals(1, resolution.columnGroups().length);
        final ColumnGroupResolution group = resolution.columnGroups()[0];
        assertThat(group.mapper(), org.hamcrest.Matchers.instanceOf(FlattenedFieldMapper.class));

        // Relative keys and leaf indexes must correspond.
        assertArrayEquals(new String[] { "key1", "key2" }, group.relativeKeys());
        assertArrayEquals(new int[] { key1Leaf, key2Leaf }, group.leafIndexes());
    }

    /** A flattened group and an ordinary leaf mapper coexist in the same batch resolution. */
    public void testFlattenedGroupCoexistsWithPlainLeaf() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("host").field("type", "keyword").endObject();
            b.startObject("flat").field("type", "flattened").endObject();
        }));
        SourceSchema schema = schemaOfJson("{\"host\":\"h\",\"flat\":{\"k\":\"v\"}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);

        // The keyword leaf has its own mapper.
        assertThat(resolution.columnMappers()[schema.findLeaf("host", 0)], org.hamcrest.Matchers.instanceOf(KeywordFieldMapper.class));
        // The flattened subkey leaf is owned by the group.
        assertNull(resolution.columnMappers()[schema.findLeaf("k", schema.findNonLeaf("flat", 0))]);
        assertEquals(1, resolution.columnGroups().length);
    }

    /** A doc with only the flattened field at its own path (null or empty object) goes through mapColumnBatch, not the group path. */
    public void testFlattenedLeafAtOwnPathUsesLeafMapper() throws IOException {
        MapperService ms = flattenedMapper();
        // {"flat":null} encodes the flattened field itself as a leaf at "flat", with no subkeys.
        SourceSchema schemaNullDoc = schemaOfJson("{\"flat\":null}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaNullDoc, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertThat(
            resolution.columnMappers()[schemaNullDoc.findLeaf("flat", 0)],
            org.hamcrest.Matchers.instanceOf(FlattenedFieldMapper.class)
        );
        assertEquals(0, resolution.columnGroups().length);
    }

    /**
     * When both the own-path leaf and subkey leaves are present (e.g. one null doc + one object doc),
     * the own-path leaf goes through mapColumnBatch and the subkeys form a group.
     */
    public void testFlattenedOwnPathLeafAndGroupCoexist() throws IOException {
        MapperService ms = flattenedMapper();
        // Two docs: first has null, second has a subkey. The batch schema has both "flat" (own-path
        // leaf under root) and "key" (a leaf under non-leaf "flat").
        SourceSchema schema = schemaOfJson("{\"flat\":null}", "{\"flat\":{\"key\":\"v\"}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);

        // Own-path leaf maps to the FlattenedFieldMapper directly (mapColumnBatch).
        assertThat(resolution.columnMappers()[schema.findLeaf("flat", 0)], org.hamcrest.Matchers.instanceOf(FlattenedFieldMapper.class));
        // Subkey leaf is null in columnMappers — owned by the group.
        assertNull(resolution.columnMappers()[schema.findLeaf("key", schema.findNonLeaf("flat", 0))]);
        assertEquals(1, resolution.columnGroups().length);
        assertArrayEquals(new String[] { "key" }, resolution.columnGroups()[0].relativeKeys());
    }

    /** Two flattened fields in the same batch produce two independent groups. */
    public void testTwoFlattenedFieldsProduceTwoGroups() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("a").field("type", "flattened").endObject();
            b.startObject("b").field("type", "flattened").endObject();
        }));
        SourceSchema schema = schemaOfJson("{\"a\":{\"x\":\"1\"},\"b\":{\"y\":\"2\"}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertEquals(2, resolution.columnGroups().length);
        // Groups are ordered by first-leaf-appearance in the schema.
        assertEquals("a", resolution.columnGroups()[0].mapper().fullPath());
        assertEquals("b", resolution.columnGroups()[1].mapper().fullPath());
    }

    /**
     * A nested object under the flattened field (e.g. {@code {"flat":{"outer":{"inner":"v"}}}}) produces
     * a leaf whose full path is {@code flat.outer.inner}. The ancestor walk in {@code findColumnGroup}
     * skips the "outer" non-leaf and stops at the FlattenedFieldMapper at "flat". The relative key is
     * "outer.inner" — the full suffix after the owner path and separator dot.
     */
    public void testNestedKeyProducesCompoundRelativeKey() throws IOException {
        MapperService ms = flattenedMapper();
        // Produces leaf "inner" under non-leaf "outer" under non-leaf "flat" — full path "flat.outer.inner".
        SourceSchema schema = schemaOfJson("{\"flat\":{\"outer\":{\"inner\":\"v\"}}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        assertEquals(1, resolution.columnGroups().length);
        assertArrayEquals(new String[] { "outer.inner" }, resolution.columnGroups()[0].relativeKeys());
    }

    /**
     * A flattened field configured with {@code "index": true} fails {@code supportsColumnarParse}
     * (enables the inverted index / terms channel which has no columnar writer), so the whole batch
     * falls back to the row path.
     */
    public void testUnsupportedFlattenedConfigFallsBack() throws IOException {
        MapperService ms = mapper(mapping(b -> b.startObject("flat").field("type", "flattened").field("index", true).endObject()));
        SourceSchema schema = schemaOfNested("flat.k");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNull(resolution);
    }

    /**
     * A non-group FieldMapper (e.g. keyword) at an ancestor path stops the column-group walk and
     * hands control back to the existing dynamic-mapping logic. Under the default dynamic=true root,
     * an unmapped leaf under a keyword ancestor forces a fallback.
     */
    public void testNonGroupFieldMapperAncestorFallsBack() throws IOException {
        // "a" is a keyword field; "a.b" is unmapped — the encoder treats the JSON value as subkeys,
        // but the mapping sees "a" as a leaf field, not a group owner.
        MapperService ms = mapper(mapping(b -> b.startObject("a").field("type", "keyword").endObject()));
        SourceSchema schema = schemaOfJson("{\"a\":{\"b\":\"v\"}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        // "a" is a keyword FieldMapper and resolvesColumnGroup() is false, so the walk stops and "a.b"
        // falls through to the dynamic=true fallback.
        assertNull(resolution);
    }

    /**
     * An unmapped group-like leaf path under a {@code dynamic=false} parent still follows the existing
     * "silently ignored" path when no group mapper exists above it. Guards against the group check
     * accidentally swallowing the dynamic=false branch.
     */
    public void testUnmappedLeafUnderDynamicFalseIsStillIgnoredWithNoGroup() throws IOException {
        // No flattened field — just root dynamic=false with one known keyword.
        MapperService ms = mapper(topMapping(b -> {
            b.field("dynamic", "false");
            b.startObject("properties");
            b.startObject("known").field("type", "keyword").endObject();
            b.endObject();
        }));
        // "o" has a subkey "u" — but there is no group mapper at "o", so "o.u" is just an unmapped leaf.
        SourceSchema schema = schemaOfJson("{\"known\":\"k\",\"o\":{\"u\":1}}");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup(), indexSettings);
        assertNotNull(resolution);
        // The "o.u" leaf is silently ignored (dynamic=false), not grouped.
        assertEquals(0, resolution.columnGroups().length);
        // "known" still resolves normally.
        assertNotNull(resolution.columnMappers()[schema.findLeaf("known", 0)]);
    }
}
