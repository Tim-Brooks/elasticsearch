/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class BatchDocumentParserTests extends MapperServiceTestCase {

    private ParsedDocument parseViaStandardPath(MapperService mapperService, XContentBuilder docBuilder) throws IOException {
        BytesReference bytes = BytesReference.bytes(docBuilder);
        SourceToParse source = new SourceToParse("1", bytes, XContentType.JSON);
        return mapperService.documentMapper().parse(source);
    }

    private List<ParsedDocument> parseViaBatchPath(
        MapperService mapperService,
        EIRFSchema schema,
        List<List<Object>> rows,
        List<IndexRequest> indexRequests
    ) throws IOException {
        MappingLookup lookup = mapperService.mappingLookup();
        BatchMappingsResolver.Resolved resolved = BatchMappingsResolver.resolve(schema, lookup);
        BatchDocumentParser parser = new BatchDocumentParser(mapperService.parserContext());
        BatchDocumentParser.RowIterator rowIterator = new BatchDocumentParser.RowIterator() {
            @Override
            public int size() {
                return rows.size();
            }

            @Override
            public IndexRequest indexRequest(int row) {
                return indexRequests.get(row);
            }

            @Override
            public List<Object> values(int row) {
                return rows.get(row);
            }
        };
        return parser.parse(resolved, rowIterator, lookup);
    }

    private IndexRequest defaultIndexRequest() {
        IndexRequest ir = new IndexRequest("test");
        ir.id("1");
        return ir;
    }

    private void assertEquivalentDocuments(ParsedDocument expected, ParsedDocument actual) {
        LuceneDocument expectedRoot = expected.rootDoc();
        LuceneDocument actualRoot = actual.rootDoc();

        // Collect field names from both, ignoring metadata-only fields that differ by path
        Set<String> expectedFieldNames = new HashSet<>();
        for (IndexableField field : expectedRoot.getFields()) {
            expectedFieldNames.add(field.name());
        }
        Set<String> actualFieldNames = new HashSet<>();
        for (IndexableField field : actualRoot.getFields()) {
            actualFieldNames.add(field.name());
        }

        // Check all expected indexed/stored fields are present in actual
        for (String fieldName : expectedFieldNames) {
            // Skip source and version metadata that differ between paths
            if (fieldName.equals("_source") || fieldName.equals("_version")) continue;
            assertTrue("Missing field in batch-parsed doc: " + fieldName, actualFieldNames.contains(fieldName));

            // Compare stored field values
            List<IndexableField> expectedFields = expectedRoot.getFields(fieldName);
            List<IndexableField> actualFields = actualRoot.getFields(fieldName);
            assertThat(
                "Field count mismatch for " + fieldName,
                actualFields.size(),
                equalTo(expectedFields.size())
            );

            for (int i = 0; i < expectedFields.size(); i++) {
                if (expectedFields.get(i).stringValue() != null) {
                    assertThat(
                        "String value mismatch for " + fieldName + "[" + i + "]",
                        actualFields.get(i).stringValue(),
                        equalTo(expectedFields.get(i).stringValue())
                    );
                }
                if (expectedFields.get(i).numericValue() != null) {
                    assertThat(
                        "Numeric value mismatch for " + fieldName + "[" + i + "]",
                        actualFields.get(i).numericValue(),
                        equalTo(expectedFields.get(i).numericValue())
                    );
                }
            }
        }
    }

    public void testSimpleKeywordAndInteger() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("status").field("type", "keyword").endObject();
            b.startObject("count").field("type", "integer").endObject();
        }));

        // Standard path
        ParsedDocument expected = parseViaStandardPath(mapperService, jsonBuilder().startObject()
            .field("status", "active")
            .field("count", 42)
            .endObject());

        // Batch path
        EIRFSchema.Builder schemaBuilder = new EIRFSchema.Builder();
        schemaBuilder.appendLeafColumn("status", -1);
        schemaBuilder.appendLeafColumn("count", -1);
        EIRFSchema schema = schemaBuilder.build();

        List<Object> row = Arrays.asList("active", 42);
        List<ParsedDocument> results = parseViaBatchPath(
            mapperService, schema, List.of(row), List.of(defaultIndexRequest())
        );

        assertThat(results.size(), equalTo(1));
        assertEquivalentDocuments(expected, results.get(0));
    }

    public void testNestedObjectPath() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("resource").startObject("properties");
            b.startObject("name").field("type", "keyword").endObject();
            b.endObject().endObject();
        }));

        ParsedDocument expected = parseViaStandardPath(mapperService, jsonBuilder().startObject()
            .startObject("resource").field("name", "my-host").endObject()
            .endObject());

        EIRFSchema.Builder schemaBuilder = new EIRFSchema.Builder();
        int resourceIdx = schemaBuilder.appendParentColumn("resource", -1);
        schemaBuilder.appendLeafColumn("name", resourceIdx);
        EIRFSchema schema = schemaBuilder.build();

        List<Object> row = List.of("my-host");
        List<ParsedDocument> results = parseViaBatchPath(
            mapperService, schema, List.of(row), List.of(defaultIndexRequest())
        );

        assertThat(results.size(), equalTo(1));
        assertEquivalentDocuments(expected, results.get(0));
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

        ParsedDocument expected = parseViaStandardPath(mapperService, jsonBuilder().startObject()
            .startObject("a").startObject("b").startObject("c")
            .field("value", 99)
            .endObject().endObject().endObject()
            .endObject());

        EIRFSchema.Builder schemaBuilder = new EIRFSchema.Builder();
        int aIdx = schemaBuilder.appendParentColumn("a", -1);
        int bIdx = schemaBuilder.appendParentColumn("b", aIdx);
        int cIdx = schemaBuilder.appendParentColumn("c", bIdx);
        schemaBuilder.appendLeafColumn("value", cIdx);
        EIRFSchema schema = schemaBuilder.build();

        List<Object> row = List.of(99);
        List<ParsedDocument> results = parseViaBatchPath(
            mapperService, schema, List.of(row), List.of(defaultIndexRequest())
        );

        assertThat(results.size(), equalTo(1));
        assertEquivalentDocuments(expected, results.get(0));
    }

    public void testCopyToSingleTarget() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("title").field("type", "keyword").field("copy_to", "all_fields").endObject();
            b.startObject("all_fields").field("type", "text").endObject();
        }));

        ParsedDocument expected = parseViaStandardPath(mapperService, jsonBuilder().startObject()
            .field("title", "hello")
            .endObject());

        EIRFSchema.Builder schemaBuilder = new EIRFSchema.Builder();
        schemaBuilder.appendLeafColumn("title", -1);
        EIRFSchema schema = schemaBuilder.build();

        List<Object> row = List.of("hello");
        List<ParsedDocument> results = parseViaBatchPath(
            mapperService, schema, List.of(row), List.of(defaultIndexRequest())
        );

        assertThat(results.size(), equalTo(1));
        assertEquivalentDocuments(expected, results.get(0));
    }

    public void testCopyToMultipleTargets() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("title");
            b.field("type", "keyword");
            b.array("copy_to", "target_a", "target_b");
            b.endObject();
            b.startObject("target_a").field("type", "text").endObject();
            b.startObject("target_b").field("type", "keyword").endObject();
        }));

        ParsedDocument expected = parseViaStandardPath(mapperService, jsonBuilder().startObject()
            .field("title", "world")
            .endObject());

        EIRFSchema.Builder schemaBuilder = new EIRFSchema.Builder();
        schemaBuilder.appendLeafColumn("title", -1);
        EIRFSchema schema = schemaBuilder.build();

        List<Object> row = List.of("world");
        List<ParsedDocument> results = parseViaBatchPath(
            mapperService, schema, List.of(row), List.of(defaultIndexRequest())
        );

        assertThat(results.size(), equalTo(1));
        assertEquivalentDocuments(expected, results.get(0));
    }

    public void testDisabledObjectSkipsChildren() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("meta").field("type", "object").field("enabled", false).endObject();
            b.startObject("status").field("type", "keyword").endObject();
        }));

        // Standard path: fields under disabled object produce no indexed fields
        ParsedDocument expected = parseViaStandardPath(mapperService, jsonBuilder().startObject()
            .startObject("meta").field("key", "val").endObject()
            .field("status", "active")
            .endObject());

        // Batch path: meta.key is under disabled object, should be skipped
        EIRFSchema.Builder schemaBuilder = new EIRFSchema.Builder();
        int metaIdx = schemaBuilder.appendParentColumn("meta", -1);
        schemaBuilder.appendLeafColumn("key", metaIdx);
        schemaBuilder.appendLeafColumn("status", -1);
        EIRFSchema schema = schemaBuilder.build();

        List<Object> row = Arrays.asList("val", "active");
        List<ParsedDocument> results = parseViaBatchPath(
            mapperService, schema, List.of(row), List.of(defaultIndexRequest())
        );

        assertThat(results.size(), equalTo(1));
        // The "status" field should be present; "meta.key" should not be indexed
        LuceneDocument root = results.get(0).rootDoc();
        assertThat(root.getFields("status").size(), greaterThan(0));
        assertThat(root.getFields("meta.key").size(), equalTo(0));
    }

    public void testRuntimeFieldNotIndexed() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("properties");
            b.startObject("status").field("type", "keyword").endObject();
            b.endObject();
            b.startObject("runtime");
            b.startObject("computed_field").field("type", "keyword").endObject();
            b.endObject();
        }));

        EIRFSchema.Builder schemaBuilder = new EIRFSchema.Builder();
        schemaBuilder.appendLeafColumn("status", -1);
        schemaBuilder.appendLeafColumn("computed_field", -1);
        EIRFSchema schema = schemaBuilder.build();

        List<Object> row = Arrays.asList("active", "should_be_ignored");
        List<ParsedDocument> results = parseViaBatchPath(
            mapperService, schema, List.of(row), List.of(defaultIndexRequest())
        );

        assertThat(results.size(), equalTo(1));
        LuceneDocument root = results.get(0).rootDoc();
        assertThat(root.getFields("status").size(), greaterThan(0));
        assertThat(root.getFields("computed_field").size(), equalTo(0));
    }

    public void testUnmappedFieldDynamicFalse() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.field("dynamic", "false");
            b.startObject("properties");
            b.startObject("status").field("type", "keyword").endObject();
            b.endObject();
        }));

        EIRFSchema.Builder schemaBuilder = new EIRFSchema.Builder();
        schemaBuilder.appendLeafColumn("status", -1);
        schemaBuilder.appendLeafColumn("unknown_field", -1);
        EIRFSchema schema = schemaBuilder.build();

        List<Object> row = Arrays.asList("active", "some_value");
        List<ParsedDocument> results = parseViaBatchPath(
            mapperService, schema, List.of(row), List.of(defaultIndexRequest())
        );

        assertThat(results.size(), equalTo(1));
        LuceneDocument root = results.get(0).rootDoc();
        assertThat(root.getFields("status").size(), greaterThan(0));
        assertThat(root.getFields("unknown_field").size(), equalTo(0));
    }

    public void testArrayField() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("tags").field("type", "keyword").endObject();
        }));

        ParsedDocument expected = parseViaStandardPath(mapperService, jsonBuilder().startObject()
            .array("tags", "a", "b", "c")
            .endObject());

        EIRFSchema.Builder schemaBuilder = new EIRFSchema.Builder();
        schemaBuilder.appendLeafColumn("tags", -1);
        EIRFSchema schema = schemaBuilder.build();

        List<Object> row = List.of(Arrays.asList("a", "b", "c"));
        List<ParsedDocument> results = parseViaBatchPath(
            mapperService, schema, List.of(row), List.of(defaultIndexRequest())
        );

        assertThat(results.size(), equalTo(1));
        assertEquivalentDocuments(expected, results.get(0));
    }

    public void testNullValueSkipped() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("status").field("type", "keyword").endObject();
            b.startObject("count").field("type", "integer").endObject();
        }));

        // Standard path with only status
        ParsedDocument expected = parseViaStandardPath(mapperService, jsonBuilder().startObject()
            .field("status", "active")
            .endObject());

        EIRFSchema.Builder schemaBuilder = new EIRFSchema.Builder();
        schemaBuilder.appendLeafColumn("status", -1);
        schemaBuilder.appendLeafColumn("count", -1);
        EIRFSchema schema = schemaBuilder.build();

        // null value for count should produce same result as omitting it
        List<Object> row = new ArrayList<>();
        row.add("active");
        row.add(null);
        List<ParsedDocument> results = parseViaBatchPath(
            mapperService, schema, List.of(row), List.of(defaultIndexRequest())
        );

        assertThat(results.size(), equalTo(1));
        assertEquivalentDocuments(expected, results.get(0));
    }

    public void testMultipleDocuments() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("name").field("type", "keyword").endObject();
            b.startObject("value").field("type", "integer").endObject();
        }));

        EIRFSchema.Builder schemaBuilder = new EIRFSchema.Builder();
        schemaBuilder.appendLeafColumn("name", -1);
        schemaBuilder.appendLeafColumn("value", -1);
        EIRFSchema schema = schemaBuilder.build();

        IndexRequest ir1 = new IndexRequest("test").id("1");
        IndexRequest ir2 = new IndexRequest("test").id("2");

        List<List<Object>> rows = List.of(
            Arrays.asList("first", 10),
            Arrays.asList("second", 20)
        );

        List<ParsedDocument> results = parseViaBatchPath(
            mapperService, schema, rows, List.of(ir1, ir2)
        );

        assertThat(results.size(), equalTo(2));

        // Verify first doc
        ParsedDocument expected1 = parseViaStandardPath(mapperService, jsonBuilder().startObject()
            .field("name", "first").field("value", 10).endObject());
        assertEquivalentDocuments(expected1, results.get(0));

        // Verify second doc
        ParsedDocument expected2 = parseViaStandardPath(mapperService, jsonBuilder().startObject()
            .field("name", "second").field("value", 20).endObject());
        assertEquivalentDocuments(expected2, results.get(1));
    }

    public void testTextField() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("body").field("type", "text").endObject();
        }));

        ParsedDocument expected = parseViaStandardPath(mapperService, jsonBuilder().startObject()
            .field("body", "the quick brown fox")
            .endObject());

        EIRFSchema.Builder schemaBuilder = new EIRFSchema.Builder();
        schemaBuilder.appendLeafColumn("body", -1);
        EIRFSchema schema = schemaBuilder.build();

        List<Object> row = List.of("the quick brown fox");
        List<ParsedDocument> results = parseViaBatchPath(
            mapperService, schema, List.of(row), List.of(defaultIndexRequest())
        );

        assertThat(results.size(), equalTo(1));
        assertEquivalentDocuments(expected, results.get(0));
    }

    public void testDynamicTrueCreatesMapper() throws IOException {
        // Default dynamic=true — unmapped fields should get dynamic mappers
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("status").field("type", "keyword").endObject();
        }));

        EIRFSchema.Builder schemaBuilder = new EIRFSchema.Builder();
        schemaBuilder.appendLeafColumn("status", -1);
        schemaBuilder.appendLeafColumn("new_field", -1);
        EIRFSchema schema = schemaBuilder.build();

        List<Object> row = Arrays.asList("active", "dynamic_value");
        List<ParsedDocument> results = parseViaBatchPath(
            mapperService, schema, List.of(row), List.of(defaultIndexRequest())
        );

        assertThat(results.size(), equalTo(1));
        LuceneDocument root = results.get(0).rootDoc();
        // Known field should be present
        assertThat(root.getFields("status").size(), greaterThan(0));
        // Dynamic field should be indexed
        assertThat(root.getFields("new_field").size(), greaterThan(0));
        // Should produce a dynamic mapping update
        assertNotNull(results.get(0).dynamicMappingsUpdate());
    }

    public void testDynamicTrueNumericField() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("status").field("type", "keyword").endObject();
        }));

        EIRFSchema.Builder schemaBuilder = new EIRFSchema.Builder();
        schemaBuilder.appendLeafColumn("status", -1);
        schemaBuilder.appendLeafColumn("count", -1);
        EIRFSchema schema = schemaBuilder.build();

        List<Object> row = Arrays.asList("active", 42L);
        List<ParsedDocument> results = parseViaBatchPath(
            mapperService, schema, List.of(row), List.of(defaultIndexRequest())
        );

        assertThat(results.size(), equalTo(1));
        LuceneDocument root = results.get(0).rootDoc();
        assertThat(root.getFields("status").size(), greaterThan(0));
        // Dynamic long field should be indexed
        assertThat(root.getFields("count").size(), greaterThan(0));
        assertNotNull(results.get(0).dynamicMappingsUpdate());
    }

    public void testDynamicStrictThrows() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.field("dynamic", "strict");
            b.startObject("properties");
            b.startObject("status").field("type", "keyword").endObject();
            b.endObject();
        }));

        EIRFSchema.Builder schemaBuilder = new EIRFSchema.Builder();
        schemaBuilder.appendLeafColumn("status", -1);
        schemaBuilder.appendLeafColumn("unknown_field", -1);
        EIRFSchema schema = schemaBuilder.build();

        List<Object> row = Arrays.asList("active", "some_value");
        expectThrows(StrictDynamicMappingException.class, () -> parseViaBatchPath(
            mapperService, schema, List.of(row), List.of(defaultIndexRequest())
        ));
    }

    public void testDynamicTrueNestedPath() throws IOException {
        // Unmapped field under an existing object with dynamic=true
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("resource").startObject("properties");
            b.startObject("name").field("type", "keyword").endObject();
            b.endObject().endObject();
        }));

        EIRFSchema.Builder schemaBuilder = new EIRFSchema.Builder();
        int resourceIdx = schemaBuilder.appendParentColumn("resource", -1);
        schemaBuilder.appendLeafColumn("name", resourceIdx);
        schemaBuilder.appendLeafColumn("version", resourceIdx);
        EIRFSchema schema = schemaBuilder.build();

        List<Object> row = Arrays.asList("my-host", "1.0");
        List<ParsedDocument> results = parseViaBatchPath(
            mapperService, schema, List.of(row), List.of(defaultIndexRequest())
        );

        assertThat(results.size(), equalTo(1));
        LuceneDocument root = results.get(0).rootDoc();
        assertThat(root.getFields("resource.name").size(), greaterThan(0));
        // Dynamic field under resource should be indexed
        assertThat(root.getFields("resource.version").size(), greaterThan(0));
        assertNotNull(results.get(0).dynamicMappingsUpdate());
    }

    private static XContentBuilder jsonBuilder() throws IOException {
        return JsonXContent.contentBuilder();
    }
}
