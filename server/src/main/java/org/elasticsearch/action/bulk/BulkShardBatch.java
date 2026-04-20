/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.eirf.EirfBatch;

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
}