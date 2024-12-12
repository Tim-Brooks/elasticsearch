/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.MergePolicy;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

public class ElasticsearchThreadPoolMergeScheduler extends AbstractThreadPoolMergeScheduler {

    public ElasticsearchThreadPoolMergeScheduler(ThreadPool threadPool) {

    }

    @Override
    protected void dispatchMerge(MergeTask newMergeTask) throws IOException {

    }

    @Override
    protected synchronized String getMergeTaskName(MergeSource mergeSource, MergePolicy.OneMerge merge) {
        // Implement similar to get merge thread name
        return super.getMergeTaskName(mergeSource, merge);
    }
}
