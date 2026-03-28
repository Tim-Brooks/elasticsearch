/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.IndexMode;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class BatchMappingsResolverTests extends MapperServiceTestCase {

    private static class MockCompoundFieldMapper extends MockFieldMapper {
        MockCompoundFieldMapper(String fullName) {
            super(fullName);
        }

        @Override
        public boolean isCompoundField() {
            return true;
        }
    }

    private static MappingLookup createMappingLookup(List<FieldMapper> fieldMappers, List<ObjectMapper> objectMappers) {
        RootObjectMapper.Builder builder = new RootObjectMapper.Builder("_doc", ObjectMapper.Defaults.SUBOBJECTS);
        Mapping mapping = new Mapping(
            builder.build(MapperBuilderContext.root(false, false)),
            new MetadataFieldMapper[0],
            Collections.emptyMap()
        );
        return MappingLookup.fromMappers(mapping, fieldMappers, objectMappers, IndexMode.STANDARD);
    }

    public void testSimpleFlatFields() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("status").field("type", "keyword").endObject();
            b.startObject("count").field("type", "integer").endObject();
        }));
        MappingLookup lookup = mapperService.mappingLookup();

        EIRFSchema.Builder builder = new EIRFSchema.Builder();
        builder.appendLeafColumn("status", -1);
        builder.appendLeafColumn("count", -1);
        EIRFSchema schema = builder.build();

        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);

        assertEquals(0, resolved.parentFullPaths().length);
        assertEquals(2, resolved.leafFullPaths().length);
        assertEquals("status", resolved.leafFullPaths()[0]);
        assertEquals("count", resolved.leafFullPaths()[1]);
        assertThat(resolved.leafMappers()[0], notNullValue());
        assertThat(resolved.leafMappers()[1], notNullValue());
    }

    public void testNestedObjectFields() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("resource").startObject("properties");
            b.startObject("name").field("type", "keyword").endObject();
            b.endObject().endObject();
        }));
        MappingLookup lookup = mapperService.mappingLookup();

        EIRFSchema.Builder builder = new EIRFSchema.Builder();
        int resourceIdx = builder.appendParentColumn("resource", -1);
        builder.appendLeafColumn("name", resourceIdx);
        EIRFSchema schema = builder.build();

        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);

        assertEquals("resource", resolved.parentFullPaths()[0]);
        assertEquals("resource.name", resolved.leafFullPaths()[0]);
        assertThat(resolved.leafMappers()[0], notNullValue());
        assertThat(resolved.leafMappers()[0], instanceOf(KeywordFieldMapper.class));
    }

    public void testSharedParent() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("metrics").startObject("properties");
            b.startObject("cpu").field("type", "float").endObject();
            b.startObject("memory").field("type", "long").endObject();
            b.endObject().endObject();
        }));
        MappingLookup lookup = mapperService.mappingLookup();

        EIRFSchema.Builder builder = new EIRFSchema.Builder();
        int metricsIdx = builder.appendParentColumn("metrics", -1);
        builder.appendLeafColumn("cpu", metricsIdx);
        builder.appendLeafColumn("memory", metricsIdx);
        EIRFSchema schema = builder.build();

        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);

        assertEquals(1, resolved.parentFullPaths().length);
        assertEquals("metrics", resolved.parentFullPaths()[0]);
        assertEquals("metrics.cpu", resolved.leafFullPaths()[0]);
        assertEquals("metrics.memory", resolved.leafFullPaths()[1]);
        assertThat(resolved.leafMappers()[0], notNullValue());
        assertThat(resolved.leafMappers()[1], notNullValue());
    }

    public void testDeepNesting() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("a").startObject("properties");
            b.startObject("b").startObject("properties");
            b.startObject("c").startObject("properties");
            b.startObject("value").field("type", "integer").endObject();
            b.endObject().endObject();
            b.endObject().endObject();
            b.endObject().endObject();
        }));
        MappingLookup lookup = mapperService.mappingLookup();

        EIRFSchema.Builder builder = new EIRFSchema.Builder();
        int aIdx = builder.appendParentColumn("a", -1);
        int bIdx = builder.appendParentColumn("b", aIdx);
        int cIdx = builder.appendParentColumn("c", bIdx);
        builder.appendLeafColumn("value", cIdx);
        EIRFSchema schema = builder.build();

        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);

        assertEquals("a", resolved.parentFullPaths()[0]);
        assertEquals("a.b", resolved.parentFullPaths()[1]);
        assertEquals("a.b.c", resolved.parentFullPaths()[2]);
        assertEquals("a.b.c.value", resolved.leafFullPaths()[0]);
        assertThat(resolved.leafMappers()[0], notNullValue());
    }

    public void testUnmappedLeaf() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("status").field("type", "keyword").endObject();
        }));
        MappingLookup lookup = mapperService.mappingLookup();

        EIRFSchema.Builder builder = new EIRFSchema.Builder();
        builder.appendLeafColumn("status", -1);
        builder.appendLeafColumn("nonexistent", -1);
        EIRFSchema schema = builder.build();

        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);

        assertThat(resolved.leafMappers()[0], notNullValue());
        assertThat(resolved.leafMappers()[1], nullValue());
    }

    public void testUnmappedParentMappedLeaf() throws IOException {
        // Use flattened-style mapping: the full dotted path is mapped directly
        // but the intermediate object may not exist in objectMappers
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("resource.name").field("type", "keyword").endObject();
        }));
        MappingLookup lookup = mapperService.mappingLookup();

        EIRFSchema.Builder builder = new EIRFSchema.Builder();
        int resourceIdx = builder.appendParentColumn("resource", -1);
        builder.appendLeafColumn("name", resourceIdx);
        EIRFSchema schema = builder.build();

        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);

        assertEquals("resource.name", resolved.leafFullPaths()[0]);
        assertThat(resolved.leafMappers()[0], notNullValue());
    }

    public void testMetadataFieldMappersResolved() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("status").field("type", "keyword").endObject();
        }));
        MappingLookup lookup = mapperService.mappingLookup();

        EIRFSchema.Builder builder = new EIRFSchema.Builder();
        builder.appendLeafColumn("status", -1);
        EIRFSchema schema = builder.build();

        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);

        MetadataFieldMapper[] metadataMappers = resolved.metadataFieldMappers();
        assertThat(metadataMappers, notNullValue());
        assertThat(metadataMappers.length, greaterThan(0));

        // Verify the same metadata mappers that MappingLookup's mapping provides
        MetadataFieldMapper[] expected = lookup.getMapping().getSortedMetadataMappers();
        assertArrayEquals(expected, metadataMappers);

        // Verify well-known metadata fields are present (e.g., _id, _source)
        Set<String> metadataNames = Arrays.stream(metadataMappers).map(Mapper::fullPath).collect(Collectors.toSet());
        assertTrue(metadataNames.contains("_source"));
    }

    public void testNoCompoundFields() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("status").field("type", "keyword").endObject();
            b.startObject("count").field("type", "integer").endObject();
        }));
        MappingLookup lookup = mapperService.mappingLookup();

        EIRFSchema.Builder builder = new EIRFSchema.Builder();
        builder.appendLeafColumn("status", -1);
        builder.appendLeafColumn("count", -1);
        EIRFSchema schema = builder.build();

        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);

        assertFalse(resolved.isCompoundSubField()[0]);
        assertFalse(resolved.isCompoundSubField()[1]);
        assertTrue(resolved.compoundGroups().isEmpty());
    }

    public void testUnmappedLeafNoCompoundParent() {
        MappingLookup lookup = createMappingLookup(
            List.of(new MockFieldMapper("status")),
            List.of()
        );

        EIRFSchema.Builder builder = new EIRFSchema.Builder();
        builder.appendLeafColumn("status", -1);
        builder.appendLeafColumn("nonexistent", -1);
        EIRFSchema schema = builder.build();

        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);

        assertFalse(resolved.isCompoundSubField()[0]);
        assertFalse(resolved.isCompoundSubField()[1]);
        assertTrue(resolved.compoundGroups().isEmpty());
    }

    public void testCompoundSubFieldDetected() {
        MappingLookup lookup = createMappingLookup(
            List.of(new MockCompoundFieldMapper("metrics.duration")),
            List.of()
        );

        EIRFSchema.Builder builder = new EIRFSchema.Builder();
        int metricsIdx = builder.appendParentColumn("metrics", -1);
        int durationIdx = builder.appendParentColumn("duration", metricsIdx);
        builder.appendLeafColumn("sum", durationIdx);
        EIRFSchema schema = builder.build();

        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);

        assertTrue(resolved.isCompoundSubField()[0]);
        assertThat(resolved.compoundGroups(), hasKey("metrics.duration"));

        BatchMappingsResolver.CompoundFieldGroup group = resolved.compoundGroups().get("metrics.duration");
        assertEquals("metrics.duration", group.parentPath());
        assertThat(group.parentMapper(), instanceOf(MockCompoundFieldMapper.class));
        assertEquals(List.of("sum"), group.subFieldNames());
        assertEquals(List.of(0), group.columnIndices());
    }

    public void testMultipleSubFieldsShareCompoundParent() {
        MappingLookup lookup = createMappingLookup(
            List.of(new MockCompoundFieldMapper("metrics.duration")),
            List.of()
        );

        EIRFSchema.Builder builder = new EIRFSchema.Builder();
        int metricsIdx = builder.appendParentColumn("metrics", -1);
        int durationIdx = builder.appendParentColumn("duration", metricsIdx);
        builder.appendLeafColumn("sum", durationIdx);
        builder.appendLeafColumn("value_count", durationIdx);
        EIRFSchema schema = builder.build();

        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);

        assertTrue(resolved.isCompoundSubField()[0]);
        assertTrue(resolved.isCompoundSubField()[1]);
        assertEquals(1, resolved.compoundGroups().size());
        assertThat(resolved.compoundGroups(), hasKey("metrics.duration"));

        BatchMappingsResolver.CompoundFieldGroup group = resolved.compoundGroups().get("metrics.duration");
        assertEquals("metrics.duration", group.parentPath());
        assertEquals(List.of("sum", "value_count"), group.subFieldNames());
        assertEquals(List.of(0, 1), group.columnIndices());
    }

    // --- New tests for Steps 1-5 ---

    public void testEffectiveDynamicTrue() throws IOException {
        // Root dynamic defaults to TRUE; unmapped leaf should get TRUE
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("status").field("type", "keyword").endObject();
        }));
        MappingLookup lookup = mapperService.mappingLookup();

        EIRFSchema.Builder builder = new EIRFSchema.Builder();
        builder.appendLeafColumn("status", -1);
        builder.appendLeafColumn("unmapped_field", -1);
        EIRFSchema schema = builder.build();

        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);

        // Mapped leaf — dynamic not resolved
        assertNull(resolved.leafEffectiveDynamic()[0]);
        // Unmapped leaf — should inherit root dynamic (TRUE)
        assertEquals(ObjectMapper.Dynamic.TRUE, resolved.leafEffectiveDynamic()[1]);
    }

    public void testEffectiveDynamicFalse() throws IOException {
        // Parent object with dynamic=false — unmapped child leaf should get FALSE
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("data");
            b.field("dynamic", false);
            b.startObject("properties");
            b.startObject("known").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        MappingLookup lookup = mapperService.mappingLookup();

        EIRFSchema.Builder builder = new EIRFSchema.Builder();
        int dataIdx = builder.appendParentColumn("data", -1);
        builder.appendLeafColumn("known", dataIdx);
        builder.appendLeafColumn("unknown", dataIdx);
        EIRFSchema schema = builder.build();

        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);

        // "data.known" is mapped — no dynamic needed
        assertNull(resolved.leafEffectiveDynamic()[0]);
        // "data.unknown" is unmapped under dynamic=false parent
        assertEquals(ObjectMapper.Dynamic.FALSE, resolved.leafEffectiveDynamic()[1]);
    }

    public void testEffectiveDynamicStrict() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("strict_obj");
            b.field("dynamic", "strict");
            b.startObject("properties");
            b.startObject("defined").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
        MappingLookup lookup = mapperService.mappingLookup();

        EIRFSchema.Builder builder = new EIRFSchema.Builder();
        int objIdx = builder.appendParentColumn("strict_obj", -1);
        builder.appendLeafColumn("undefined_field", objIdx);
        EIRFSchema schema = builder.build();

        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);

        assertEquals(ObjectMapper.Dynamic.STRICT, resolved.leafEffectiveDynamic()[0]);
    }

    public void testEffectiveDynamicInheritance() throws IOException {
        // Root=true, child object=false — child's unmapped leaf gets FALSE
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("parent_obj");
            b.field("dynamic", false);
            b.startObject("properties");
            b.startObject("child_obj").startObject("properties");
            b.startObject("existing").field("type", "keyword").endObject();
            b.endObject().endObject();
            b.endObject();
            b.endObject();
        }));
        MappingLookup lookup = mapperService.mappingLookup();

        EIRFSchema.Builder builder = new EIRFSchema.Builder();
        int parentIdx = builder.appendParentColumn("parent_obj", -1);
        int childIdx = builder.appendParentColumn("child_obj", parentIdx);
        builder.appendLeafColumn("new_field", childIdx);
        // Also add a root-level unmapped field (should get root dynamic = TRUE)
        builder.appendLeafColumn("root_unmapped", -1);
        EIRFSchema schema = builder.build();

        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);

        // child_obj doesn't have explicit dynamic, inherits parent_obj's FALSE
        assertEquals(ObjectMapper.Dynamic.FALSE, resolved.leafEffectiveDynamic()[0]);
        // Root-level unmapped — inherits root dynamic TRUE
        assertEquals(ObjectMapper.Dynamic.TRUE, resolved.leafEffectiveDynamic()[1]);
    }

    public void testRuntimeFieldDetected() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("runtime");
            b.startObject("rt_field").field("type", "keyword").endObject();
            b.endObject();
            b.startObject("properties");
            b.startObject("status").field("type", "keyword").endObject();
            b.endObject();
        }));
        MappingLookup lookup = mapperService.mappingLookup();

        EIRFSchema.Builder builder = new EIRFSchema.Builder();
        builder.appendLeafColumn("status", -1);
        builder.appendLeafColumn("rt_field", -1);
        EIRFSchema schema = builder.build();

        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);

        // "status" is a regular mapped field
        assertFalse(resolved.isRuntimeField()[0]);
        assertThat(resolved.leafMappers()[0], notNullValue());
        // "rt_field" is a runtime field — mapper should be null, isRuntimeField true
        assertTrue(resolved.isRuntimeField()[1]);
        assertThat(resolved.leafMappers()[1], nullValue());
        // Runtime fields should not get effective dynamic resolved
        assertNull(resolved.leafEffectiveDynamic()[1]);
    }

    public void testCopyToTargets() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("title").field("type", "text").field("copy_to", "all_text").endObject();
            b.startObject("body").field("type", "text").field("copy_to", "all_text").endObject();
            b.startObject("all_text").field("type", "text").endObject();
            b.startObject("status").field("type", "keyword").endObject();
        }));
        MappingLookup lookup = mapperService.mappingLookup();

        EIRFSchema.Builder builder = new EIRFSchema.Builder();
        builder.appendLeafColumn("title", -1);
        builder.appendLeafColumn("body", -1);
        builder.appendLeafColumn("all_text", -1);
        builder.appendLeafColumn("status", -1);
        EIRFSchema schema = builder.build();

        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);

        // "title" and "body" have copy_to targets
        assertEquals(List.of("all_text"), resolved.leafCopyToTargets()[0]);
        assertEquals(List.of("all_text"), resolved.leafCopyToTargets()[1]);
        // "all_text" and "status" have no copy_to
        assertNull(resolved.leafCopyToTargets()[2]);
        assertNull(resolved.leafCopyToTargets()[3]);
    }

    public void testDisabledParent() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("disabled_obj");
            b.field("type", "object");
            b.field("enabled", false);
            b.endObject();
            b.startObject("enabled_field").field("type", "keyword").endObject();
        }));
        MappingLookup lookup = mapperService.mappingLookup();

        EIRFSchema.Builder builder = new EIRFSchema.Builder();
        int disabledIdx = builder.appendParentColumn("disabled_obj", -1);
        builder.appendLeafColumn("child", disabledIdx);
        builder.appendLeafColumn("enabled_field", -1);
        EIRFSchema schema = builder.build();

        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);

        // Leaf under disabled parent should be marked as disabled
        assertTrue(resolved.leafParentDisabled()[0]);
        // Root-level field — not disabled
        assertFalse(resolved.leafParentDisabled()[1]);
    }

    public void testDisabledGrandparent() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("grandparent");
            b.field("type", "object");
            b.field("enabled", false);
            b.endObject();
        }));
        MappingLookup lookup = mapperService.mappingLookup();

        EIRFSchema.Builder builder = new EIRFSchema.Builder();
        int gpIdx = builder.appendParentColumn("grandparent", -1);
        int parentIdx = builder.appendParentColumn("parent", gpIdx);
        builder.appendLeafColumn("value", parentIdx);
        EIRFSchema schema = builder.build();

        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);

        // Disabled at grandparent propagates to leaf
        assertTrue(resolved.leafParentDisabled()[0]);
    }

    public void testParentObjectMappersResolved() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("metrics").startObject("properties");
            b.startObject("cpu").field("type", "float").endObject();
            b.endObject().endObject();
        }));
        MappingLookup lookup = mapperService.mappingLookup();

        EIRFSchema.Builder builder = new EIRFSchema.Builder();
        int metricsIdx = builder.appendParentColumn("metrics", -1);
        builder.appendLeafColumn("cpu", metricsIdx);
        EIRFSchema schema = builder.build();

        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);

        assertEquals(1, resolved.parentObjectMappers().length);
        assertThat(resolved.parentObjectMappers()[0], notNullValue());
        assertThat(resolved.parentObjectMappers()[0], instanceOf(ObjectMapper.class));
        assertEquals("metrics", resolved.parentObjectMappers()[0].fullPath());
    }

    public void testParentObjectMapperNullForUnmappedParent() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("status").field("type", "keyword").endObject();
        }));
        MappingLookup lookup = mapperService.mappingLookup();

        EIRFSchema.Builder builder = new EIRFSchema.Builder();
        int unmappedIdx = builder.appendParentColumn("nonexistent_parent", -1);
        builder.appendLeafColumn("child", unmappedIdx);
        EIRFSchema schema = builder.build();

        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);

        assertNull(resolved.parentObjectMappers()[0]);
    }
}
