/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import java.util.Arrays;

/**
 * Memoized mapping from leaf column index to dotted path string for a single {@link EirfSchema}.
 *
 * <p>Owns a growable {@code String[]} that lazily computes and caches
 * {@link EirfSchema#getFullPath(int)} results. One instance per schema (i.e. one per
 * {@code IndexAbstraction} in a bulk encoding session). The column index space is stable for the
 * lifetime of the schema — once a column index is resolved, the same string object is returned
 * on every subsequent call.
 */
public final class ColumnPathCache {

    private static final int INITIAL_CAPACITY = 16;

    private String[] paths;

    public ColumnPathCache() {
        this.paths = new String[INITIAL_CAPACITY];
    }

    /**
     * Returns the dotted path for the given leaf column, computing and caching it from
     * {@code schema} on the first call for each column index.
     */
    public String get(int columnIndex, EirfSchema schema) {
        if (columnIndex >= paths.length) {
            int newCap = paths.length;
            while (columnIndex >= newCap) {
                newCap <<= 1;
            }
            paths = Arrays.copyOf(paths, newCap);
        }
        String path = paths[columnIndex];
        if (path == null) {
            path = schema.getFullPath(columnIndex);
            paths[columnIndex] = path;
        }
        return path;
    }
}
