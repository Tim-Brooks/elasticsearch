/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema for the Elastic Internal Row Format (EIRF).
 *
 * <p>Uses a parent-pointer structure with two levels:
 * <ul>
 *   <li><b>Non-leaf fields</b> (objects/containers) form a tree. Index 0 is always the root.</li>
 *   <li><b>Leaf fields</b> (columns in row data) each point to a parent non-leaf field.</li>
 * </ul>
 *
 * <p>Example for {@code {"user": {"name": "alice"}, "status": "active"}}:
 * <pre>
 * Non-leaf: [root(parent:-1), "user"(parent:0)]
 * Leaf:     ["name"(parent:1), "status"(parent:0)]
 * </pre>
 */
public final class EirfSchema {

    private static final int INITIAL_CAPACITY = 8;

    // Non-leaf fields (objects)
    private final List<String> nonLeafNames;
    private int[] nonLeafParents;
    private final Map<String, Integer> nonLeafLookup; // "parentIdx/name" -> index

    // Leaf fields (columns)
    private final List<String> leafNames;
    private int[] leafParents;
    private final Map<String, Integer> leafLookup; // "parentIdx/name" -> index

    /**
     * Creates a new schema with root automatically added as non-leaf index 0.
     */
    public EirfSchema() {
        this.nonLeafNames = new ArrayList<>();
        this.nonLeafParents = new int[INITIAL_CAPACITY];
        this.nonLeafLookup = new HashMap<>();

        this.leafNames = new ArrayList<>();
        this.leafParents = new int[INITIAL_CAPACITY];
        this.leafLookup = new HashMap<>();

        // Add root at index 0, self-referential parent
        nonLeafNames.add("");
        nonLeafParents[0] = 0;
        nonLeafLookup.put("0/", 0);
    }

    /**
     * Constructor for reading: builds from pre-parsed non-leaf and leaf arrays.
     */
    EirfSchema(List<String> nonLeafNames, int[] nonLeafParents, List<String> leafNames, int[] leafParents) {
        this.nonLeafNames = new ArrayList<>(nonLeafNames);
        this.nonLeafParents = Arrays.copyOf(nonLeafParents, nonLeafNames.size());
        this.nonLeafLookup = new HashMap<>(nonLeafNames.size());
        for (int i = 0; i < nonLeafNames.size(); i++) {
            nonLeafLookup.put(nonLeafParents[i] + "/" + nonLeafNames.get(i), i);
        }

        this.leafNames = new ArrayList<>(leafNames);
        this.leafParents = Arrays.copyOf(leafParents, leafNames.size());
        this.leafLookup = new HashMap<>(leafNames.size());
        for (int i = 0; i < leafNames.size(); i++) {
            leafLookup.put(leafParents[i] + "/" + leafNames.get(i), i);
        }
    }

    // ---- Non-leaf operations ----

    public int nonLeafCount() {
        return nonLeafNames.size();
    }

    public String getNonLeafName(int idx) {
        return nonLeafNames.get(idx);
    }

    public int getNonLeafParent(int idx) {
        return nonLeafParents[idx];
    }

    /**
     * Finds a non-leaf field by name and parent index. Returns -1 if not found.
     */
    public int findNonLeaf(String name, int parentIdx) {
        Integer idx = nonLeafLookup.get(parentIdx + "/" + name);
        return idx != null ? idx : -1;
    }

    /**
     * Appends a non-leaf field if not already present. Idempotent.
     */
    public int appendNonLeaf(String name, int parentIdx) {
        String key = parentIdx + "/" + name;
        Integer existing = nonLeafLookup.get(key);
        if (existing != null) {
            return existing;
        }
        int index = nonLeafNames.size();
        nonLeafNames.add(name);
        if (index >= nonLeafParents.length) {
            nonLeafParents = Arrays.copyOf(nonLeafParents, nonLeafParents.length << 1);
        }
        nonLeafParents[index] = parentIdx;
        nonLeafLookup.put(key, index);
        return index;
    }

    // ---- Leaf operations ----

    /**
     * Returns the number of leaf fields (columns).
     */
    public int leafCount() {
        return leafNames.size();
    }

    public String getLeafName(int idx) {
        return leafNames.get(idx);
    }

    public int getLeafParent(int idx) {
        return leafParents[idx];
    }

    /**
     * Finds a leaf field by name and parent non-leaf index. Returns -1 if not found.
     */
    public int findLeaf(String name, int parentIdx) {
        Integer idx = leafLookup.get(parentIdx + "/" + name);
        return idx != null ? idx : -1;
    }

    /**
     * Appends a leaf field if not already present. Idempotent.
     */
    public int appendLeaf(String name, int parentIdx) {
        String key = parentIdx + "/" + name;
        Integer existing = leafLookup.get(key);
        if (existing != null) {
            return existing;
        }
        int index = leafNames.size();
        leafNames.add(name);
        if (index >= leafParents.length) {
            leafParents = Arrays.copyOf(leafParents, leafParents.length << 1);
        }
        leafParents[index] = parentIdx;
        leafLookup.put(key, index);
        return index;
    }

    // ---- Path reconstruction ----

    /**
     * Reconstructs the full dot-separated path for a leaf field by walking parent pointers.
     * For a leaf "name" under non-leaf "user" under root, returns "user.name".
     * For a leaf "status" directly under root, returns "status".
     */
    public String getFullPath(int leafIdx) {
        String leafName = leafNames.get(leafIdx);
        int parentIdx = leafParents[leafIdx];

        if (parentIdx == 0) {
            // Direct child of root
            return leafName;
        }

        // Walk up the non-leaf chain collecting names
        StringBuilder sb = new StringBuilder();
        buildNonLeafPath(sb, parentIdx);
        sb.append('.').append(leafName);
        return sb.toString();
    }

    private void buildNonLeafPath(StringBuilder sb, int nonLeafIdx) {
        if (nonLeafIdx == 0) {
            return; // root, nothing to append
        }
        int parent = nonLeafParents[nonLeafIdx];
        buildNonLeafPath(sb, parent);
        if (sb.length() > 0) {
            sb.append('.');
        }
        sb.append(nonLeafNames.get(nonLeafIdx));
    }

    /**
     * Returns the chain of non-leaf indices from root to the given non-leaf index (inclusive).
     * Root (index 0) is excluded from the result.
     */
    int[] getNonLeafChain(int nonLeafIdx) {
        if (nonLeafIdx == 0) {
            return new int[0];
        }
        // Count depth
        int depth = 0;
        int idx = nonLeafIdx;
        while (idx != 0) {
            depth++;
            idx = nonLeafParents[idx];
        }
        int[] chain = new int[depth];
        idx = nonLeafIdx;
        for (int i = depth - 1; i >= 0; i--) {
            chain[i] = idx;
            idx = nonLeafParents[idx];
        }
        return chain;
    }
}
