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
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assume.assumeTrue;

/**
 * Diagnostic: exercises the NORMAL (non-batch) document/row indexing path for a columnar-mode index with
 * {@code doc_values.multi_value: false} and a numeric index sort — the same configuration as the nightly
 * ClickBench benchmark. Batch indexing is deliberately NOT enabled, so this is exactly what the nightly run
 * does. If this passes, the pure row path works with this config (and the batch path must match it); if it
 * fails the same way the batch run does, the config itself is unsupported on this Lucene/ES build.
 */
public class ColumnarIndexSortRowPathIT extends ESIntegTestCase {

    public void testRowPathColumnarSingleValueNumericIndexSort() throws IOException {
        assumeTrue("columnar index mode feature flag must be enabled", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        String index = "row-columnar-sort";
        XContentBuilder mapping = JsonXContent.contentBuilder();
        mapping.startObject();
        {
            mapping.startObject("_doc");
            mapping.field("dynamic", "strict");
            mapping.startObject("properties");
            {
                mapping.startObject("name").field("type", "keyword").endObject();
                mapping.startObject("value").field("type", "long").endObject();
                mapping.startObject("ts").field("type", "date").field("format", "yyyy-MM-dd HH:mm:ss").endObject();
            }
            mapping.endObject();
            mapping.endObject();
        }
        mapping.endObject();
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    org.elasticsearch.common.settings.Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.mode", "columnar")
                        .put("index.mapping.doc_values.multi_value", false)
                        .put("index.seq_no.index_options", "doc_values_only")
                        .putList("index.sort.field", "value", "ts")
                        .putList("index.sort.order", "desc", "asc")
                )
                .setMapping(mapping)
        );
        ensureGreen(index);

        int numDocs = 50;
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(
                new IndexRequest(index).id("d-" + i)
                    .source(
                        "{\"name\":\"n-" + i + "\",\"value\":" + i + ",\"ts\":\"2013-07-15 03:39:" + String.format("%02d", i % 60) + "\"}",
                        XContentType.JSON
                    )
            );
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertNoFailures(bulkResponse);
        // The flush + index sort happens here — the same point the batch path NPEs.
        refresh(index);
        assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(0).setTrackTotalHits(true), r -> {
            assertNoFailures(r);
            assertThat(r.getHits().getTotalHits().value(), equalTo((long) numDocs));
        });
    }
}
