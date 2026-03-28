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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Append-only column name registry for the row-oriented document batch format.
 * Maps column names to integer indices. New columns can only be appended; existing
 * columns are never removed or reordered.
 *
 * A schema may optionally have a "fixed" prefix — a preset list of known fields that
 * are shared across batches to avoid repeatedly re-parsing or appending them per document.
 * Dynamic fields are appended after the fixed fields (indices {@code fixedCount, fixedCount+1, ...}).
 */
public final class DocBatchSchema {

    private final DocBatchSchema fixed;
    private final List<String> names;
    private final Map<String, Integer> nameToIndex;

    /** Creates an empty schema with no fixed fields. */
    public DocBatchSchema() {
        this((DocBatchSchema) null);
    }

    /** Creates a schema seeded with fixed fields from the given fixed schema. Null means no fixed prefix. */
    public DocBatchSchema(DocBatchSchema fixed) {
        this.fixed = fixed;
        this.names = new ArrayList<>();
        this.nameToIndex = new HashMap<>();
    }

    /** Creates a schema from a pre-existing list of column names (for reading). */
    public DocBatchSchema(List<String> columnNames) {
        this.fixed = null;
        this.names = new ArrayList<>(columnNames);
        this.nameToIndex = new HashMap<>(columnNames.size());
        for (int i = 0; i < columnNames.size(); i++) {
            nameToIndex.put(columnNames.get(i), i);
        }
    }

    /** Internal constructor for building a fixed schema. */
    private DocBatchSchema(List<String> names, Map<String, Integer> nameToIndex) {
        this.fixed = null;
        this.names = new ArrayList<>(names);
        this.nameToIndex = new HashMap<>(nameToIndex);
    }

    /**
     * Creates a fixed schema from the given field names. The returned schema can be passed to
     * {@link #DocBatchSchema(DocBatchSchema)} to seed new per-batch schemas without re-appending
     * the known fields each time.
     */
    public static DocBatchSchema fixed(List<String> fieldNames) {
        if (fieldNames.isEmpty()) {
            return null;
        }
        List<String> names = new ArrayList<>(fieldNames.size());
        Map<String, Integer> nameToIndex = new HashMap<>(fieldNames.size());
        for (int i = 0; i < fieldNames.size(); i++) {
            String name = fieldNames.get(i);
            if (nameToIndex.putIfAbsent(name, i) != null) {
                throw new IllegalArgumentException("duplicate fixed field name: " + name);
            }
            names.add(name);
        }
        return new DocBatchSchema(Collections.unmodifiableList(names), Collections.unmodifiableMap(nameToIndex));
    }

    private int fixedColumnCount() {
        return fixed != null ? fixed.columnCount() : 0;
    }

    public int columnCount() {
        return fixedColumnCount() + names.size();
    }

    public String getColumnName(int index) {
        int fc = fixedColumnCount();
        return index < fc ? fixed.getColumnName(index) : names.get(index - fc);
    }

    public int getColumnIndex(String name) {
        if (fixed != null) {
            int columnIndex = fixed.getColumnIndex(name);
            if (columnIndex != -1) {
                return columnIndex;
            }
        }
        Integer idx = nameToIndex.get(name);
        return idx != null ? idx : -1;
    }

    /**
     * Appends a column if not already present. Idempotent: returns existing index if the name is known.
     */
    public int appendColumn(String name) {
        int existing = getColumnIndex(name);
        if (existing != -1) {
            return existing;
        }
        int index = names.size() + fixedColumnCount();
        names.add(name);
        nameToIndex.put(name, index);
        return index;
    }

    public record LeafField(String name, byte[] utf) {

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof LeafField leafField) {
                return name.equals(leafField.name);
            }
            return false;
        }
    }
}
