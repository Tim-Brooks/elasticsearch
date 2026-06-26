/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.eirf.EirfBatch;
import org.elasticsearch.eirf.EirfRowBuilder;
import org.elasticsearch.eirf.EirfSchema;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper.BatchMapperResolution;
import org.elasticsearch.index.mapper.ShardBatchMapper.ColumnGroup;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Resolve-time tests for the columnar batch path. After the columnar migration only
 * {@link NumberFieldMapper} (and the metadata mappers) support batch indexing; keyword/text/boolean/
 * ip/date are reverted and their happy-path resolve cases are commented out until re-added.
 *
 * <p>{@code resolveMappers} only inspects the schema, so the schema is still built with an
 * {@link EirfRowBuilder} (format-agnostic) here.
 */
public class ShardBatchMapperResolveTests extends MapperServiceTestCase {

    /** Build a schema with the given (name) leaves by driving an EirfRowBuilder. */
    private static EirfSchema schemaOf(String... leafNames) {
        try (EirfRowBuilder b = new EirfRowBuilder()) {
            b.startDocument();
            for (String name : leafNames) {
                // Any value shape works — resolveMappers only looks at the schema, not the value.
                b.setLong(name, 0L);
            }
            b.endDocument();
            try (EirfBatch batch = b.build()) {
                return batch.schema();
            }
        }
    }

    private MapperService mapper(XContentBuilder mapping) throws IOException {
        return createMapperService(mapping);
    }

    public void testNumericHappyPath() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("value").field("type", "long").endObject();
            b.startObject("score").field("type", "double").endObject();
        }));
        EirfSchema schema = schemaOf("value", "score");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup());
        assertNotNull(resolution);
        assertEquals(2, resolution.columnMappers().length);
        assertTrue(resolution.columnMappers()[schema.findLeaf("value", 0)] instanceof NumberFieldMapper);
        assertTrue(resolution.columnMappers()[schema.findLeaf("score", 0)] instanceof NumberFieldMapper);
    }

    public void testNumberIgnoreMalformedIsSupported() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("v").field("type", "long").field("ignore_malformed", true).endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("v"), ms.mappingLookup());
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof NumberFieldMapper);
    }

    public void testMissingLeafUnderDynamicFalseIsIgnored() throws IOException {
        MapperService ms = mapper(topMapping(b -> {
            b.field("dynamic", "false");
            b.startObject("properties");
            b.startObject("known").field("type", "long").endObject();
            b.endObject();
        }));
        EirfSchema schema = schemaOf("known", "unknown");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup());
        assertNotNull(resolution);
        assertNotNull(resolution.columnMappers()[schema.findLeaf("known", 0)]);
        assertNull(resolution.columnMappers()[schema.findLeaf("unknown", 0)]);
    }

    public void testMissingLeafUnderDynamicTrueFallsBack() throws IOException {
        MapperService ms = mapper(mapping(b -> { b.startObject("known").field("type", "long").endObject(); }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("known", "unknown"), ms.mappingLookup());
        assertNull(resolution);
    }

    public void testRuntimeFieldInMappingFallsBack() throws IOException {
        MapperService ms = mapper(topMapping(b -> {
            b.startObject("runtime");
            b.startObject("rt").field("type", "keyword").endObject();
            b.endObject();
            b.startObject("properties");
            b.startObject("known").field("type", "long").endObject();
            b.endObject();
        }));
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schemaOf("known"), ms.mappingLookup());
        assertNull(resolution);
    }

    public void testNumberWithCopyToFallsBack() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("src").field("type", "long").field("copy_to", "dst").endObject();
            b.startObject("dst").field("type", "long").endObject();
        }));
        assertNull(ShardBatchMapper.resolveMappers(schemaOf("src"), ms.mappingLookup()));
    }

    public void testNestedLeafHappyPath() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("outer");
            b.startObject("properties");
            b.startObject("inner").field("type", "long").endObject();
            b.endObject();
            b.endObject();
        }));
        EirfSchema schema = schemaOf("outer.inner");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup());
        assertNotNull(resolution);
        assertTrue(resolution.columnMappers()[0] instanceof NumberFieldMapper);
    }

    public void testNestedLeafUnderNestedDynamicFalseIsIgnored() throws IOException {
        MapperService ms = mapper(mapping(b -> {
            b.startObject("outer");
            b.field("dynamic", "false");
            b.startObject("properties");
            b.startObject("known").field("type", "long").endObject();
            b.endObject();
            b.endObject();
        }));
        EirfSchema schema = schemaOf("outer.known", "outer.unknown");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup());
        assertNotNull(resolution);
        assertNotNull(resolution.columnMappers()[schema.findLeaf("known", schema.findNonLeaf("outer", 0))]);
        assertNull(resolution.columnMappers()[schema.findLeaf("unknown", schema.findNonLeaf("outer", 0))]);
    }

    private static Settings columnarSettings() {
        return Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.name()).build();
    }

    public void testFlattenedGroupResolvesToParent() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        MapperService ms = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject("attrs").field("type", "flattened").endObject())
        );
        // EICF explodes the flattened object into a leaf per key; none has its own mapper, so they must
        // resolve as a group against the flattened parent.
        EirfSchema schema = schemaOf("attrs.host.name", "attrs.os.type");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup());
        assertNotNull(resolution);
        assertEquals("the child leaves belong to a group, not a leaf mapper", 1, resolution.groups().size());
        ColumnGroup group = resolution.groups().getFirst();
        assertTrue(group.mapper() instanceof FlattenedFieldMapper);
        assertEquals(2, group.leafIndices().length);

        Set<String> relativeKeys = new HashSet<>(Set.of(group.relativeKeys()));
        assertEquals(Set.of("host.name", "os.type"), relativeKeys);

        // The grouped leaves must not also be handed to a leaf mapper.
        for (int leaf : group.leafIndices()) {
            assertNull(resolution.columnMappers()[leaf]);
        }
    }

    public void testFlattenedEmptyObjectResolvesAsLeaf() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        MapperService ms = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject("attrs").field("type", "flattened").endObject())
        );
        // An empty flattened object is encoded by EICF as a single leaf at the field path, which resolves
        // directly to the flattened mapper (handled by its leaf mapColumnBatch as a no-op).
        EirfSchema schema = schemaOf("attrs");
        BatchMapperResolution resolution = ShardBatchMapper.resolveMappers(schema, ms.mappingLookup());
        assertNotNull(resolution);
        assertTrue(resolution.groups().isEmpty());
        assertTrue(resolution.columnMappers()[schema.findLeaf("attrs", 0)] instanceof FlattenedFieldMapper);
    }

    public void testFlattenedNonColumnarFallsBack() throws IOException {
        // Without the binary/columnar doc-values configuration the flattened mapper does not support batch
        // indexing, so its child leaves cannot be grouped and the batch falls back to the row-major path.
        MapperService ms = mapper(mapping(b -> b.startObject("attrs").field("type", "flattened").endObject()));
        assertNull(ShardBatchMapper.resolveMappers(schemaOf("attrs.host.name"), ms.mappingLookup()));
    }

    // TODO columnar: re-enable when keyword supports columnar batch indexing.
    // public void testKeywordIgnoreAboveIsSupported() { ... }
    // public void testKeywordWithMultiFieldsFallsBack() { ... }
    // TODO columnar: re-enable when text supports columnar batch indexing.
    // public void testTextMapperHappyPath() { ... }
    // public void testTextMapperWithIndexPrefixesFallsBack() { ... }
    // public void testTextMapperWithIndexPhrasesFallsBack() { ... }
    // public void testTextMapperWithFielddataIsSupported() { ... }
    // TODO columnar: re-enable when boolean supports columnar batch indexing.
    // public void testBooleanMapperHappyPath() { ... }
    // public void testBooleanIgnoreMalformedIsSupported() { ... }
    // TODO columnar: re-enable when ip supports columnar batch indexing.
    // public void testIpMapperHappyPath() { ... }
    // public void testIpIgnoreMalformedIsSupported() { ... }
    // TODO columnar: re-enable when date supports columnar batch indexing.
}
