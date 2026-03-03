/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Append-only column name registry for the row-oriented document batch format.
 * Maps column names to integer indices. New columns can only be appended; existing
 * columns are never removed or reordered.
 */
public final class DocBatchSchema {

    private final List<String> names;
    private final Map<String, Integer> nameToIndex;

    /** Creates an empty schema for encoding. */
    public DocBatchSchema() {
        this.names = new ArrayList<>();
        this.nameToIndex = new HashMap<>();
    }

    /** Creates a schema from a pre-existing list of column names (for reading). */
    public DocBatchSchema(List<String> columnNames) {
        this.names = new ArrayList<>(columnNames);
        this.nameToIndex = new HashMap<>(columnNames.size());
        for (int i = 0; i < columnNames.size(); i++) {
            nameToIndex.put(columnNames.get(i), i);
        }
    }

    public int columnCount() {
        return names.size();
    }

    public String getColumnName(int index) {
        return names.get(index);
    }

    public int getColumnIndex(String name) {
        Integer idx = nameToIndex.get(name);
        return idx != null ? idx : -1;
    }

    /**
     * Appends a column if not already present. Idempotent: returns existing index if the name is known.
     */
    public int appendColumn(String name) {
        Integer existing = nameToIndex.get(name);
        if (existing != null) {
            return existing;
        }
        int index = names.size();
        names.add(name);
        nameToIndex.put(name, index);
        return index;
    }
}
