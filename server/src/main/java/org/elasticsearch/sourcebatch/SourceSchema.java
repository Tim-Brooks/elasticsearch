/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.apache.lucene.util.UnicodeUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The field schema shared by every row in a {@link SourceBatch}, independent of the physical layout
 * (row-major or column-major) the batch is stored in.
 *
 * <p>Uses a parent-pointer structure with two levels:
 * <ul>
 *   <li><b>Non-leaf fields</b> (objects/containers) form a tree. Index 0 is always the root.</li>
 *   <li><b>Leaf fields</b> (columns in the batch) each point to a parent non-leaf field.</li>
 * </ul>
 *
 * <p>Example for {@code {"user": {"name": "alice"}, "status": "active"}}:
 * <pre>
 * Non-leaf: [root(parent:-1), "user"(parent:0)]
 * Leaf:     ["name"(parent:1), "status"(parent:0)]
 * </pre>
 */
public final class SourceSchema {

    private static final int INITIAL_CAPACITY = 8;
    /** Maximum number of fields per level, constrained by u16 encoding in the batch header. */
    static final int MAX_FIELDS = 65535;

    private final FieldLevel nonLeaves;
    private final FieldLevel leaves;

    /**
     * Creates a new schema with root automatically added as non-leaf index 0.
     */
    public SourceSchema() {
        this.nonLeaves = new FieldLevel(INITIAL_CAPACITY);
        this.leaves = new FieldLevel(INITIAL_CAPACITY);

        // Add root at index 0, self-referential parent
        nonLeaves.append("", 0);
    }

    /**
     * Constructor for reading: builds from pre-parsed non-leaf and leaf arrays.
     */
    public SourceSchema(List<String> nonLeafNames, int[] nonLeafParents, List<String> leafNames, int[] leafParents) {
        this.nonLeaves = new FieldLevel(nonLeafNames, nonLeafParents);
        this.leaves = new FieldLevel(leafNames, leafParents);
    }

    public int nonLeafCount() {
        return nonLeaves.count();
    }

    public String getNonLeafName(int idx) {
        return nonLeaves.getName(idx);
    }

    public int getNonLeafParent(int idx) {
        return nonLeaves.getParent(idx);
    }

    /**
     * Finds a non-leaf field by name and parent index. Returns -1 if not found.
     */
    public int findNonLeaf(String name, int parentIdx) {
        return nonLeaves.find(name, parentIdx);
    }

    /**
     * Appends a non-leaf field if not already present. Idempotent.
     */
    public int appendNonLeaf(String name, int parentIdx) {
        return nonLeaves.append(name, parentIdx);
    }

    /**
     * Returns the number of leaf fields (columns).
     */
    public int leafCount() {
        return leaves.count();
    }

    public String getLeafName(int idx) {
        return leaves.getName(idx);
    }

    public int getLeafParent(int idx) {
        return leaves.getParent(idx);
    }

    /**
     * Finds a leaf field by name and parent non-leaf index. Returns -1 if not found.
     */
    public int findLeaf(String name, int parentIdx) {
        return leaves.find(name, parentIdx);
    }

    /**
     * Appends a leaf field if not already present. Idempotent.
     */
    public int appendLeaf(String name, int parentIdx) {
        return leaves.append(name, parentIdx);
    }

    /**
     * Reconstructs the full dot-separated path for a leaf field by walking parent pointers.
     * For a leaf "name" under non-leaf "user" under root, returns "user.name".
     * For a leaf "status" directly under root, returns "status".
     */
    public String getFullPath(int leafIdx) {
        String leafName = leaves.getName(leafIdx);
        int parentIdx = leaves.getParent(leafIdx);

        if (parentIdx == 0) {
            return leafName;
        }

        StringBuilder sb = new StringBuilder();
        buildNonLeafPath(sb, parentIdx);
        sb.append('.').append(leafName);
        return sb.toString();
    }

    private void buildNonLeafPath(StringBuilder sb, int nonLeafIdx) {
        if (nonLeafIdx == 0) {
            return;
        }
        int parent = nonLeaves.getParent(nonLeafIdx);
        buildNonLeafPath(sb, parent);
        if (sb.isEmpty() == false) {
            sb.append('.');
        }
        sb.append(nonLeaves.getName(nonLeafIdx));
    }

    /**
     * Returns the chain of non-leaf indices from root to the given non-leaf index (inclusive).
     * Root (index 0) is excluded from the result.
     */
    int[] getNonLeafChain(int nonLeafIdx) {
        if (nonLeafIdx == 0) {
            return new int[0];
        }
        int depth = 0;
        int idx = nonLeafIdx;
        while (idx != 0) {
            depth++;
            idx = nonLeaves.getParent(idx);
        }
        int[] chain = new int[depth];
        idx = nonLeafIdx;
        for (int i = depth - 1; i >= 0; i--) {
            chain[i] = idx;
            idx = nonLeaves.getParent(idx);
        }
        return chain;
    }

    /**
     * Holds a parallel name list, parent array, and a flat open-addressed lookup table for one
     * level of schema fields.
     *
     * <p>The lookup table uses four parallel arrays (hashes, parents, names, values) with linear
     * probing. Capacity is always a power of two; empty slots are identified by {@code hashes[i] == 0}
     * (keys that hash to zero are stored as 1 to preserve the sentinel). Load factor is kept below
     * 75% to bound probe lengths. No entry is ever deleted (schema is append-only), so linear
     * probing terminates correctly without tombstones.
     *
     * <p>The probe condition {@code hashes[i] == h && parents[i] == parent && (names[i] == name ||
     * names[i].equals(name))} tries identity first to exploit the fact that both the SIMD parser
     * and Jackson canonicalize field-name {@link String} instances — the same field in the same JVM
     * returns the same reference, so {@code ==} succeeds without calling {@code equals}.
     */
    private static final class FieldLevel {
        public static final int MISSING = -1;
        private final List<String> names;
        private int[] parents;

        private int[] tableHashes;
        private int[] tableParents;
        private String[] tableNames;
        private int[] tableValues;
        private int tableSize;
        private int tableMask;

        FieldLevel(int initialCapacity) {
            this.names = new ArrayList<>();
            this.parents = new int[initialCapacity];
            int cap = tableCap(initialCapacity);
            tableHashes = new int[cap];
            tableParents = new int[cap];
            tableNames = new String[cap];
            tableValues = new int[cap];
            tableMask = cap - 1;
        }

        FieldLevel(List<String> names, int[] parents) {
            this.names = new ArrayList<>(names);
            this.parents = Arrays.copyOf(parents, names.size());
            int n = names.size();
            int cap = tableCap(n);
            tableHashes = new int[cap];
            tableParents = new int[cap];
            tableNames = new String[cap];
            tableValues = new int[cap];
            tableMask = cap - 1;
            for (int i = 0; i < n; i++) {
                rawInsert(hash(parents[i], names.get(i)), parents[i], names.get(i), i);
            }
            tableSize = n;
        }

        int count() {
            return names.size();
        }

        String getName(int idx) {
            return names.get(idx);
        }

        int getParent(int idx) {
            return parents[idx];
        }

        int find(String name, int parentIdx) {
            int h = hash(parentIdx, name);
            int i = h & tableMask;
            while (true) {
                int sh = tableHashes[i];
                if (sh == 0) {
                    return MISSING;
                }
                if (sh == h && tableParents[i] == parentIdx && (tableNames[i] == name || tableNames[i].equals(name))) {
                    return tableValues[i];
                }
                i = (i + 1) & tableMask;
            }
        }

        int append(String name, int parentIdx) {
            int h = hash(parentIdx, name);
            int i = h & tableMask;
            while (true) {
                int sh = tableHashes[i];
                if (sh == 0) {
                    break;
                }
                if (sh == h && tableParents[i] == parentIdx && (tableNames[i] == name || tableNames[i].equals(name))) {
                    return tableValues[i];
                }
                i = (i + 1) & tableMask;
            }
            int index = names.size();
            if (index >= MAX_FIELDS) {
                throw new IllegalStateException("Schema field count exceeds maximum of " + MAX_FIELDS);
            }
            if (UnicodeUtil.calcUTF16toUTF8Length(name, 0, name.length()) > MAX_FIELDS) {
                throw new IllegalStateException("Schema field name exceeds maximum of " + MAX_FIELDS + " bytes: " + name);
            }
            names.add(name);
            if (index >= parents.length) {
                parents = Arrays.copyOf(parents, parents.length << 1);
            }
            parents[index] = parentIdx;
            tableSize++;
            if (tableSize * 4 > tableHashes.length * 3) {
                // Rehash doubles the table and re-inserts all entries (including the new one).
                rehash();
            } else {
                tableHashes[i] = h;
                tableParents[i] = parentIdx;
                tableNames[i] = name;
                tableValues[i] = index;
            }
            return index;
        }

        private void rawInsert(int h, int parent, String name, int value) {
            int i = h & tableMask;
            while (tableHashes[i] != 0) {
                i = (i + 1) & tableMask;
            }
            tableHashes[i] = h;
            tableParents[i] = parent;
            tableNames[i] = name;
            tableValues[i] = value;
        }

        private void rehash() {
            int newCap = tableHashes.length * 2;
            tableHashes = new int[newCap];
            tableParents = new int[newCap];
            tableNames = new String[newCap];
            tableValues = new int[newCap];
            tableMask = newCap - 1;
            int n = names.size();
            for (int idx = 0; idx < n; idx++) {
                rawInsert(hash(parents[idx], names.get(idx)), parents[idx], names.get(idx), idx);
            }
        }

        private static int hash(int parent, String name) {
            int h = (parent * 0x9e3779b9) ^ name.hashCode();
            h ^= h >>> 16;
            return h == 0 ? 1 : h;
        }

        /** Returns a power-of-two table capacity that keeps load below 75% for {@code n} entries. */
        private static int tableCap(int n) {
            int min = Math.max(n * 4 / 3 + 2, 16);
            return Integer.highestOneBit(min - 1) << 1;
        }
    }
}
