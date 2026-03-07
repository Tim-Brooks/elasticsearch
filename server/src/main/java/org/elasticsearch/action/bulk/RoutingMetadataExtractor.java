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

/**
 * Extracts routing metadata from scratch buffers during batch encoding.
 * Implementations handle different routing strategies (sort-field hash vs TSID).
 */
public interface RoutingMetadataExtractor {

    /**
     * Called when a new column is discovered during encoding.
     * Implementations check whether the column is relevant for routing and track it.
     */
    void resolveColumn(int colIdx, String fieldPath);

    /**
     * Returns true if the given column index has already been resolved.
     */
    boolean isColumnResolved(int colIdx);

    /**
     * After a document is fully flattened, reads routing-relevant values from scratch buffers
     * and sets the computed routing metadata on the request.
     */
    void extractFromScratch(DocumentBatchRowEncoder.ScratchBuffers scratch, IndexRequest request);

    /**
     * Resets per-document state for the next document.
     */
    void resetDocument();
}
