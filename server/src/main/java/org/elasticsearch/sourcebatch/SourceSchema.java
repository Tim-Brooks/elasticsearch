/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.ObjectIntMap;

import org.apache.lucene.util.ArrayUtil;
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

    /**
     * Temporary A/B scaffolding for benchmarking the sequence-order cache.
     * Set to {@code false} before a JMH fork to measure the baseline without the cache.
     * <b>Remove this field (and all references to it) before shipping.</b>
     */
    public static volatile boolean ORDER_CACHE_ENABLED = true;

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
     * Resets the sequence-order cursor on both field levels so the next row's appends are
     * verified against the positions recorded during previous rows. Must be called at the start
     * of every row (i.e. from {@link org.elasticsearch.escf.EscfRowBuffer#beginRow()}).
     */
    public void beginRow() {
        nonLeaves.resetCursor();
        leaves.resetCursor();
    }

    /**
     * Reconstructs the full dot-separated path for a leaf field by walking parent pointers.
     * For a leaf "name" under non-leaf "user" under root, returns "user.name".
     * For a leaf "status" directly under root, returns "status".
     */
    public String getFullPath(int leafIdx) {
        // TODO: Could consider caching this in some type of field name object.
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

    private record FieldKey(int parentIdx, String name) {}

    /**
     * Holds a parallel name list, parent array, and lookup map for one level of schema fields.
     *
     * <p>Includes a sequence-order identity cache: after the first row has been appended, subsequent
     * rows are checked against the recorded append sequence using reference equality on the field
     * name String. Because field names coming from the simdjson path are canonicalized by
     * the field-name lookup table (which returns the same String instance for a given name across
     * documents), {@code ==} is a reliable fast check. A hit costs one array load and two integer
     * compares — no hashing.
     *
     * <p>A forward probe window ({@link #PROBE_AHEAD} slots) handles the common case where a
     * document omits an optional field. On a full-window miss the cursor jumps past the checked
     * window (by {@code PROBE_AHEAD + 1}) to allow re-alignment on the next call. The miss budget
     * is per-document: each new row resets both the cursor and the miss counter so that
     * heterogeneous batches never permanently disable the cache, and every document starts
     * with the optimistic fast path.
     */
    private static final class FieldLevel {
        public static final int MISSING = -1;

        /** Slots probed past the cursor on a shape-deviation (omitted optional field etc.). */
        private static final int PROBE_AHEAD = 2;
        /**
         * Divergences within a single document before the cache is disabled for that document.
         * Resets to zero at {@link #resetCursor()} so the next document tries again.
         */
        private static final int MISS_BUDGET = 5;

        private final List<String> names;
        private int[] parents;
        private final ObjectIntMap<FieldKey> lookup;

        // ---- Sequence-order cache (benchmarking prototype) ----
        private String[] seqName;
        private int[] seqParent;
        private int[] seqIndex;
        /** High-water mark: number of recorded entries from completed rows. */
        private int seqLen;
        /** Current position within the row being appended; reset by {@link #resetCursor()}. */
        private int cursor;
        /** Divergences seen in the current document; reset per row. */
        private int misses;
        /** Whether the sequence cache is still active for the current document. Reset per row. */
        private boolean seqEnabled;

        FieldLevel(int initialCapacity) {
            this.names = new ArrayList<>();
            this.parents = new int[initialCapacity];
            this.lookup = new ObjectIntHashMap<>(initialCapacity);
            this.seqName = new String[initialCapacity];
            this.seqParent = new int[initialCapacity];
            this.seqIndex = new int[initialCapacity];
            this.seqEnabled = ORDER_CACHE_ENABLED;
        }

        FieldLevel(List<String> names, int[] parents) {
            this.names = new ArrayList<>(names);
            this.parents = Arrays.copyOf(parents, names.size());
            this.lookup = new ObjectIntHashMap<>(names.size());
            for (int i = 0; i < names.size(); i++) {
                lookup.put(new FieldKey(parents[i], names.get(i)), i);
            }
            // Decoded schemas are read-only; sequence cache is not needed.
            this.seqName = new String[0];
            this.seqParent = new int[0];
            this.seqIndex = new int[0];
            this.seqEnabled = false;
        }

        /**
         * Resets per-document state: cursor back to 0, miss counter cleared, cache re-enabled.
         * Called at the start of every row so each document gets a fresh optimistic attempt.
         */
        void resetCursor() {
            cursor = 0;
            misses = 0;
            seqEnabled = ORDER_CACHE_ENABLED;
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
            return lookup.getOrDefault(new FieldKey(parentIdx, name), MISSING);
        }

        int append(String name, int parentIdx) {
            if (seqEnabled) {
                if (cursor < seqLen) {
                    // Verification phase: probe the window for this field using identity equality.
                    int hit = probeSequence(name, parentIdx);
                    if (hit != MISSING) {
                        return hit;
                    }
                    // Full-window miss: advance past the checked window to allow re-alignment,
                    // then count toward the per-document budget. Never overwrite the recorded
                    // sequence — just skip forward so the next probe starts from a fresh position.
                    cursor = Math.min(cursor + PROBE_AHEAD + 1, seqLen);
                    if (++misses >= MISS_BUDGET) {
                        seqEnabled = false;
                    }
                }
                // cursor >= seqLen: learning phase — fall through to the map, then record below.
            }

            // Use a transient key for the lookup so it never escapes this method and stays eligible for scalar
            // replacement on the common hit path.
            int existing = lookup.getOrDefault(new FieldKey(parentIdx, name), MISSING);
            int index = (existing != MISSING) ? existing : insertNew(name, parentIdx);

            // Learning phase only: extend the sequence for fields not yet recorded. Never
            // overwrite an existing entry — only append at seqLen. Skip fields that are already
            // in the schema (existing != MISSING) and were just skipped by cursor advancement;
            // they are already recorded at their correct position earlier in the sequence.
            if (seqEnabled && cursor >= seqLen && existing == MISSING) {
                recordLearning(name, parentIdx, index);
            }
            return index;
        }

        /**
         * Probes {@code [cursor, cursor + PROBE_AHEAD]} for a match using identity equality.
         * On a hit, advances {@code cursor} to {@code p + 1} and returns the cached schema index.
         * On a miss returns {@link #MISSING} without modifying {@code cursor}.
         */
        private int probeSequence(String name, int parentIdx) {
            int end = Math.min(cursor + 1 + PROBE_AHEAD, seqLen);
            for (int p = cursor; p < end; p++) {
                if (seqName[p] == name && seqParent[p] == parentIdx) {
                    assert seqIndex[p] == lookup.getOrDefault(new FieldKey(parentIdx, name), MISSING)
                        : "sequence cache returned wrong index for (" + name + ", parent=" + parentIdx + ")";
                    cursor = p + 1;
                    return seqIndex[p];
                }
            }
            return MISSING;
        }

        /**
         * Appends a new entry at {@code seqLen} and advances both {@code seqLen} and {@code cursor}.
         * Only called in the learning phase ({@code cursor >= seqLen}).
         */
        private void recordLearning(String name, int parentIdx, int index) {
            ensureSeqCapacity(seqLen + 1);
            seqName[seqLen] = name;
            seqParent[seqLen] = parentIdx;
            seqIndex[seqLen] = index;
            seqLen++;
            cursor = seqLen; // cursor tracks the frontier during learning
        }

        private void ensureSeqCapacity(int minCapacity) {
            if (minCapacity <= seqName.length) {
                return;
            }
            int newCap = ArrayUtil.oversize(minCapacity, Integer.BYTES);
            seqName = Arrays.copyOf(seqName, newCap);
            seqParent = Arrays.copyOf(seqParent, newCap);
            seqIndex = Arrays.copyOf(seqIndex, newCap);
        }

        /** Inserts a brand-new (name, parentIdx) pair into the backing structures and returns its index. */
        private int insertNew(String name, int parentIdx) {
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
            lookup.put(new FieldKey(parentIdx, name), index);
            return index;
        }
    }
}
