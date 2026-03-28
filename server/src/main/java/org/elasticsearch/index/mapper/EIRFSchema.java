/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Immutable column schema for a row-oriented EIRF format. Tracks two kinds of columns:
 * <ul>
 *   <li><b>Parent columns</b> — represent object/nested containers in the document tree.</li>
 *   <li><b>Leaf columns</b> — represent terminal field values.</li>
 * </ul>
 * Each column stores a field name and a back-pointer to its parent column index ({@code -1} for root-level columns).
 * Use {@link Builder} to construct instances.
 */
public final class EIRFSchema {

    public record Column(String name, int parentIndex) {}

    private final Column[] parentColumns;
    private final Column[] leafColumns;

    private EIRFSchema(Column[] parentColumns, Column[] leafColumns) {
        this.parentColumns = parentColumns;
        this.leafColumns = leafColumns;
    }

    public int parentColumnCount() {
        return parentColumns.length;
    }

    public Column getParentColumn(int index) {
        return parentColumns[index];
    }

    public int leafColumnCount() {
        return leafColumns.length;
    }

    public Column getLeafColumn(int index) {
        return leafColumns[index];
    }

    public static final class Builder {

        private final List<Column> parentColumns = new ArrayList<>();
        private final List<Column> leafColumns = new ArrayList<>();
        private final Map<Column, Integer> parentLookup = new HashMap<>();
        private final Map<Column, Integer> leafLookup = new HashMap<>();

        public Builder() {}

        /**
         * Appends a parent column if not already present. Returns the index.
         */
        public int appendParentColumn(String name, int parentIndex) {
            Column column = new Column(name, parentIndex);
            Integer existing = parentLookup.get(column);
            if (existing != null) {
                return existing;
            }
            int index = parentColumns.size();
            parentColumns.add(column);
            parentLookup.put(column, index);
            return index;
        }

        /**
         * Appends a leaf column if not already present. Returns the index.
         */
        public int appendLeafColumn(String name, int parentIndex) {
            Column column = new Column(name, parentIndex);
            Integer existing = leafLookup.get(column);
            if (existing != null) {
                return existing;
            }
            int index = leafColumns.size();
            leafColumns.add(column);
            leafLookup.put(column, index);
            return index;
        }

        public EIRFSchema build() {
            return new EIRFSchema(parentColumns.toArray(new Column[0]), leafColumns.toArray(new Column[0]));
        }
    }
}
