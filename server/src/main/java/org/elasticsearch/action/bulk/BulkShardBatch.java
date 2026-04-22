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
import org.elasticsearch.action.index.IndexSource;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.eirf.EirfBatch;
import org.elasticsearch.eirf.EirfEncoder;
import org.elasticsearch.eirf.EirfRowToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

public class BulkShardBatch implements Writeable {

    private final EirfBatch eirfBatch;

    public BulkShardBatch(EirfBatch eirfBatch) {
        if (eirfBatch == null) {
            throw new IllegalArgumentException("eirfBatch must not be null");
        }
        this.eirfBatch = eirfBatch;
    }

    public BulkShardBatch(StreamInput in) throws IOException {
        this.eirfBatch = new EirfBatch(in.readBytesReference(), () -> {});
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBytesReference(eirfBatch.data());
    }

    public EirfBatch getEirfBatch() {
        return eirfBatch;
    }

    /**
     * Encodes the items of the given {@link BulkShardRequest} into an EIRF batch, replaces each item's inline source with a row
     * reference via {@link IndexSource#setEirfRow(int)}, and attaches the batch to the request.
     */
    public static BulkShardBatch createShardBatch(BulkShardRequest bulkShardRequest) throws IOException {
        BulkItemRequest[] items = bulkShardRequest.items();
        EirfBatch batch;
        // TODO: Pooled eventually
        try (EirfEncoder encoder = new EirfEncoder()) {
            for (BulkItemRequest item : items) {
                IndexRequest indexRequest = (IndexRequest) item.request();
                encoder.addDocument(indexRequest.indexSource().bytes(), indexRequest.getContentType());
            }
            batch = encoder.build();
        }
        for (int i = 0; i < items.length; i++) {
            IndexRequest indexRequest = (IndexRequest) items[i].request();
            indexRequest.indexSource().setEirfRow(i);
        }
        return new BulkShardBatch(batch);
    }

    /**
     * Reverses {@link #createShardBatch(BulkShardRequest)}: for each item that was converted to an EIRF row, serializes that row back
     * into its original content type and restores it as the inline source, then clears the batch from the request. This is used when
     * the target node does not support the bulk shard batch transport version.
     */
    public static void inlineSources(EirfBatch batch, BulkItemRequest[] items) throws IOException {
        for (BulkItemRequest item : items) {
            IndexRequest indexRequest = (IndexRequest) item.request();
            IndexSource indexSource = indexRequest.indexSource();
            if (indexSource.hasEirfRow() == false) {
                continue;
            }
            XContentType contentType = indexSource.contentType();
            try (XContentBuilder xcb = XContentFactory.contentBuilder(contentType)) {
                EirfRowToXContent.writeRow(batch.getRowReader(indexSource.rowIndex()), batch.schema(), xcb);
                indexSource.source(BytesReference.bytes(xcb), contentType);
            }
        }
    }
}
