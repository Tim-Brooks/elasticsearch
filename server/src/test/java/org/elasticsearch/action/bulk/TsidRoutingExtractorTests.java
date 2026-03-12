/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentString;

import java.nio.charset.StandardCharsets;
import java.util.Set;

public class TsidRoutingExtractorTests extends ESTestCase {

    public void testExactDimensionFieldMatch() {
        TsidRoutingExtractor extractor = new TsidRoutingExtractor(Set.of("scope.name", "unit"));

        extractor.resolveColumn(0, "scope.name");
        extractor.resolveColumn(1, "unit");
        extractor.resolveColumn(2, "not_a_dimension");

        // Verify dimension columns are tracked by extracting from scratch buffers
        IndexRequest request = new IndexRequest("test");
        DocumentBatchRowEncoder.ScratchBuffers scratch = newScratch(3);
        setString(scratch, 0, "my-scope");
        setString(scratch, 1, "ms");
        scratch.typeBytes[2] = RowType.NULL;

        extractor.extractFromScratch(scratch, request);
        assertNotNull(request.tsid());
    }

    public void testWildcardDimensionFieldMatch() {
        TsidRoutingExtractor extractor = new TsidRoutingExtractor(Set.of("attributes.*", "resource.attributes.*"));

        extractor.resolveColumn(0, "attributes.host.name");
        extractor.resolveColumn(1, "resource.attributes.service.name");
        extractor.resolveColumn(2, "metric_name");

        IndexRequest request = new IndexRequest("test");
        DocumentBatchRowEncoder.ScratchBuffers scratch = newScratch(3);
        setString(scratch, 0, "host1");
        setString(scratch, 1, "svc1");
        scratch.typeBytes[2] = RowType.NULL;

        extractor.extractFromScratch(scratch, request);
        assertNotNull("wildcard dimension fields should produce a tsid", request.tsid());
    }

    public void testWildcardDoesNotMatchNonPrefix() {
        TsidRoutingExtractor extractor = new TsidRoutingExtractor(Set.of("attributes.*"));

        extractor.resolveColumn(0, "other.field");
        extractor.resolveColumn(1, "attributesXfoo");

        IndexRequest request = new IndexRequest("test");
        DocumentBatchRowEncoder.ScratchBuffers scratch = newScratch(2);
        scratch.typeBytes[0] = RowType.NULL;
        scratch.typeBytes[1] = RowType.NULL;

        extractor.extractFromScratch(scratch, request);
        assertNull("non-matching fields should not produce a tsid", request.tsid());
    }

    public void testMixedExactAndWildcard() {
        TsidRoutingExtractor extractor = new TsidRoutingExtractor(
            Set.of("scope.name", "scope.attributes.*", "resource.attributes.*", "unit", "_metric_names_hash", "attributes.*")
        );

        extractor.resolveColumn(0, "scope.name");
        extractor.resolveColumn(1, "attributes.http.method");
        extractor.resolveColumn(2, "resource.attributes.service.name");
        extractor.resolveColumn(3, "unrelated_field");

        IndexRequest request = new IndexRequest("test");
        DocumentBatchRowEncoder.ScratchBuffers scratch = newScratch(4);
        setString(scratch, 0, "my-scope");
        setString(scratch, 1, "GET");
        setString(scratch, 2, "my-service");
        scratch.typeBytes[3] = RowType.NULL;

        extractor.extractFromScratch(scratch, request);
        assertNotNull(request.tsid());
    }

    public void testLongDimensionWithWildcard() {
        TsidRoutingExtractor extractor = new TsidRoutingExtractor(Set.of("metrics.*"));

        extractor.resolveColumn(0, "metrics.count");

        IndexRequest request = new IndexRequest("test");
        DocumentBatchRowEncoder.ScratchBuffers scratch = newScratch(1);
        scratch.typeBytes[0] = RowType.LONG;
        ByteUtils.writeLongBE(42L, scratch.fixedData, 0);

        extractor.extractFromScratch(scratch, request);
        assertNotNull(request.tsid());
    }

    public void testIsColumnResolved() {
        TsidRoutingExtractor extractor = new TsidRoutingExtractor(Set.of("attributes.*"));

        assertFalse(extractor.isColumnResolved(0));
        extractor.resolveColumn(0, "attributes.foo");
        assertTrue(extractor.isColumnResolved(0));
        assertFalse(extractor.isColumnResolved(1));
    }

    private static DocumentBatchRowEncoder.ScratchBuffers newScratch(int numColumns) {
        return new DocumentBatchRowEncoder.ScratchBuffers(numColumns);
    }

    private static void setString(DocumentBatchRowEncoder.ScratchBuffers scratch, int colIdx, String value) {
        scratch.typeBytes[colIdx] = RowType.STRING;
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        scratch.varData[colIdx] = new XContentString.UTF8Bytes(bytes, 0, bytes.length);
    }
}
