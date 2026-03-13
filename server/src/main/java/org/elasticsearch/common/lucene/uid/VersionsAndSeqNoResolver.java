/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.uid;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.index.mapper.Uid;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/** Utility class to resolve the Lucene doc ID, version, seqNo and primaryTerms for a given uid. */
public final class VersionsAndSeqNoResolver {

    static final ConcurrentMap<IndexReader.CacheKey, CloseableThreadLocal<PerThreadIDVersionAndSeqNoLookup[]>> lookupStates =
        ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    // Evict this reader from lookupStates once it's closed:
    private static final IndexReader.ClosedListener removeLookupState = key -> {
        CloseableThreadLocal<PerThreadIDVersionAndSeqNoLookup[]> ctl = lookupStates.remove(key);
        if (ctl != null) {
            ctl.close();
        }
    };

    private static PerThreadIDVersionAndSeqNoLookup[] getLookupState(IndexReader reader, boolean loadTimestampRange) throws IOException {
        // We cache on the top level
        // This means cache entries have a shorter lifetime, maybe as low as 1s with the
        // default refresh interval and a steady indexing rate, but on the other hand it
        // proved to be cheaper than having to perform a CHM and a TL get for every segment.
        // See https://github.com/elastic/elasticsearch/pull/19856.
        IndexReader.CacheHelper cacheHelper = reader.getReaderCacheHelper();
        CloseableThreadLocal<PerThreadIDVersionAndSeqNoLookup[]> ctl = lookupStates.get(cacheHelper.getKey());
        if (ctl == null) {
            // First time we are seeing this reader's core; make a new CTL:
            ctl = new CloseableThreadLocal<>();
            CloseableThreadLocal<PerThreadIDVersionAndSeqNoLookup[]> other = lookupStates.putIfAbsent(cacheHelper.getKey(), ctl);
            if (other == null) {
                // Our CTL won, we must remove it when the reader is closed:
                cacheHelper.addClosedListener(removeLookupState);
            } else {
                // Another thread beat us to it: just use their CTL:
                ctl = other;
            }
        }

        PerThreadIDVersionAndSeqNoLookup[] lookupState = ctl.get();
        if (lookupState == null) {
            lookupState = new PerThreadIDVersionAndSeqNoLookup[reader.leaves().size()];
            for (LeafReaderContext leaf : reader.leaves()) {
                lookupState[leaf.ord] = new PerThreadIDVersionAndSeqNoLookup(leaf.reader(), loadTimestampRange);
            }
            ctl.set(lookupState);
        } else {
            if (Assertions.ENABLED) {
                // Ensure cached lookup instances have loaded timestamp range if that was requested
                for (PerThreadIDVersionAndSeqNoLookup lookup : lookupState) {
                    if (lookup.loadedTimestampRange != loadTimestampRange) {
                        throw new AssertionError(
                            "Mismatch between lookup.loadedTimestampRange ["
                                + lookup.loadedTimestampRange
                                + "] and loadTimestampRange ["
                                + loadTimestampRange
                                + "]"
                        );
                    }
                }
            }
        }

        if (lookupState.length != reader.leaves().size()) {
            throw new AssertionError("Mismatched numbers of leaves: " + lookupState.length + " != " + reader.leaves().size());
        }

        return lookupState;
    }

    private VersionsAndSeqNoResolver() {}

    /** Wraps an {@link LeafReaderContext}, a doc ID <b>relative to the context doc base</b> and a version. */
    public static class DocIdAndVersion {
        public final int docId;
        public final long version;
        public final long seqNo;
        public final long primaryTerm;
        public final LeafReader reader;
        public final int docBase;

        public DocIdAndVersion(int docId, long version, long seqNo, long primaryTerm, LeafReader reader, int docBase) {
            this.docId = docId;
            this.version = version;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.reader = reader;
            this.docBase = docBase;
        }
    }

    /** Wraps an {@link LeafReaderContext}, a doc ID <b>relative to the context doc base</b> and a seqNo. */
    public static class DocIdAndSeqNo {
        public final int docId;
        public final long seqNo;
        public final LeafReaderContext context;

        DocIdAndSeqNo(int docId, long seqNo, LeafReaderContext context) {
            this.docId = docId;
            this.seqNo = seqNo;
            this.context = context;
        }
    }

    /**
     * Load the internal doc ID and version for the uid from the reader, returning<ul>
     * <li>null if the uid wasn't found,
     * <li>a doc ID and a version otherwise
     * </ul>
     */
    public static DocIdAndVersion timeSeriesLoadDocIdAndVersion(IndexReader reader, BytesRef term, boolean loadSeqNo) throws IOException {
        PerThreadIDVersionAndSeqNoLookup[] lookups = getLookupState(reader, false);
        List<LeafReaderContext> leaves = reader.leaves();
        // iterate backwards to optimize for the frequently updated documents
        // which are likely to be in the last segments
        for (int i = leaves.size() - 1; i >= 0; i--) {
            final LeafReaderContext leaf = leaves.get(i);
            PerThreadIDVersionAndSeqNoLookup lookup = lookups[leaf.ord];
            DocIdAndVersion result = lookup.lookupVersion(term, loadSeqNo, leaf);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    /**
     * A special variant of loading docid and version in case of time series indices.
     * <p>
     * Makes use of the fact that timestamp is part of the id, the existence of @timestamp field and
     * that segments are sorted by {@link org.elasticsearch.cluster.metadata.DataStream#TIMESERIES_LEAF_READERS_SORTER}.
     * This allows this method to know whether there is no document with the specified id without loading the docid for
     * the specified id.
     *
     * @param reader         The reader load docid, version and seqno from.
     * @param uid            The term that describes the uid of the document to load docid, version and seqno for.
     * @param id             The id that contains the encoded timestamp. The timestamp is used to skip checking the id for entire segments.
     * @param loadSeqNo      Whether to load sequence number from _seq_no doc values field.
     * @param useSyntheticId Whether the id is a synthetic (true) or standard (false ) document id.
     * @return the internal doc ID and version for the specified term from the specified reader or
     *         returning <code>null</code> if no document was found for the specified id
     * @throws IOException In case of an i/o related failure
     */
    public static DocIdAndVersion timeSeriesLoadDocIdAndVersion(
        IndexReader reader,
        BytesRef uid,
        String id,
        boolean loadSeqNo,
        boolean useSyntheticId
    ) throws IOException {
        final long timestamp;
        if (useSyntheticId) {
            assert uid.equals(Uid.encodeId((id)));
            timestamp = TsidExtractingIdFieldMapper.extractTimestampFromSyntheticId(uid);
        } else {
            byte[] idAsBytes = Base64.getUrlDecoder().decode(id);
            timestamp = TsidExtractingIdFieldMapper.extractTimestampFromId(idAsBytes);
        }
        PerThreadIDVersionAndSeqNoLookup[] lookups = getLookupState(reader, true);
        List<LeafReaderContext> leaves = reader.leaves();
        // iterate in default order, the segments should be sorted by DataStream#TIMESERIES_LEAF_READERS_SORTER
        long prevMaxTimestamp = Long.MAX_VALUE;
        for (final LeafReaderContext leaf : leaves) {
            PerThreadIDVersionAndSeqNoLookup lookup = lookups[leaf.ord];
            assert lookup.loadedTimestampRange;
            assert prevMaxTimestamp >= lookup.maxTimestamp;
            if (timestamp < lookup.minTimestamp) {
                continue;
            }
            if (timestamp > lookup.maxTimestamp) {
                return null;
            }
            DocIdAndVersion result = lookup.lookupVersion(uid, loadSeqNo, leaf);
            if (result != null) {
                return result;
            }
            prevMaxTimestamp = lookup.maxTimestamp;
        }
        return null;
    }

    /**
     * Batch version of {@link #timeSeriesLoadDocIdAndVersion(IndexReader, BytesRef, String, boolean, boolean)} for TSDB indices.
     * Uses timestamp-based segment skipping for efficiency.
     */
    public static DocIdAndVersion[] timeSeriesLoadDocIdAndVersions(
        IndexReader reader,
        BytesRef[] uids,
        String[] ids,
        boolean loadSeqNo,
        boolean useSyntheticId
    ) throws IOException {
        final int count = uids.length;
        final DocIdAndVersion[] results = new DocIdAndVersion[count];
        if (count == 0) {
            return results;
        }

        // Extract timestamps from all IDs
        final long[] timestamps = new long[count];
        for (int i = 0; i < count; i++) {
            if (useSyntheticId) {
                timestamps[i] = TsidExtractingIdFieldMapper.extractTimestampFromSyntheticId(uids[i]);
            } else {
                byte[] idAsBytes = Base64.getUrlDecoder().decode(ids[i]);
                timestamps[i] = TsidExtractingIdFieldMapper.extractTimestampFromId(idAsBytes);
            }
        }

        // Sort once upfront by term lexicographic order
        final int[] sortedIndices = sortIndicesByTerm(uids, count);
        final BytesRef[] sortedTerms = new BytesRef[count];
        for (int i = 0; i < count; i++) {
            sortedTerms[i] = uids[sortedIndices[i]];
        }

        PerThreadIDVersionAndSeqNoLookup[] lookups = getLookupState(reader, true);
        List<LeafReaderContext> leaves = reader.leaves();

        // Reusable arrays for per-leaf subsets (drawn from the pre-sorted order)
        int[] subOriginalIndices = new int[count];
        BytesRef[] subTerms = new BytesRef[count];

        for (final LeafReaderContext leaf : leaves) {
            PerThreadIDVersionAndSeqNoLookup lookup = lookups[leaf.ord];
            assert lookup.loadedTimestampRange;

            // Walk the pre-sorted order, picking entries whose timestamp matches this leaf
            int subCount = 0;
            for (int i = 0; i < count; i++) {
                int orig = sortedIndices[i];
                if (results[orig] != null) {
                    continue; // already resolved
                }
                if (timestamps[orig] < lookup.minTimestamp || timestamps[orig] > lookup.maxTimestamp) {
                    continue; // timestamp not in segment range
                }
                subTerms[subCount] = sortedTerms[i];
                subOriginalIndices[subCount] = orig;
                subCount++;
            }
            if (subCount == 0) {
                continue;
            }

            // subTerms is already in sorted order (subset of the globally sorted array)
            lookup.lookupVersions(subTerms, subOriginalIndices, subCount, loadSeqNo, leaf, results);
        }
        return results;
    }

    /**
     * Batch version of {@link #timeSeriesLoadDocIdAndVersion(IndexReader, BytesRef, boolean)} for non-TSDB indices.
     */
    public static DocIdAndVersion[] loadDocIdAndVersions(IndexReader reader, BytesRef[] uids, boolean loadSeqNo) throws IOException {
        final int count = uids.length;
        final DocIdAndVersion[] results = new DocIdAndVersion[count];
        if (count == 0) {
            return results;
        }

        // Sort once by term lexicographic order
        final int[] sortedIndices = sortIndicesByTerm(uids, count);
        final BytesRef[] sortedTerms = new BytesRef[count];
        for (int i = 0; i < count; i++) {
            sortedTerms[i] = uids[sortedIndices[i]];
        }

        PerThreadIDVersionAndSeqNoLookup[] lookups = getLookupState(reader, false);
        List<LeafReaderContext> leaves = reader.leaves();
        // iterate backwards to optimize for frequently updated documents in later segments
        for (int i = leaves.size() - 1; i >= 0; i--) {
            final LeafReaderContext leaf = leaves.get(i);
            PerThreadIDVersionAndSeqNoLookup lookup = lookups[leaf.ord];
            lookup.lookupVersions(sortedTerms, sortedIndices, count, loadSeqNo, leaf, results);
        }
        return results;
    }

    /**
     * Build an index array sorted by the lexicographic order of uids.
     * Uses IntroSorter (no boxing, no allocation beyond the result array).
     */
    private static int[] sortIndicesByTerm(BytesRef[] uids, int count) {
        final int[] indices = new int[count];
        for (int i = 0; i < count; i++) {
            indices[i] = i;
        }
        new IntroSorter() {
            private int pivot;

            @Override
            protected void swap(int i, int j) {
                int tmp = indices[i];
                indices[i] = indices[j];
                indices[j] = tmp;
            }

            @Override
            protected int comparePivot(int j) {
                return uids[pivot].compareTo(uids[indices[j]]);
            }

            @Override
            protected void setPivot(int i) {
                pivot = indices[i];
            }
        }.sort(0, count);
        return indices;
    }

    public static DocIdAndVersion loadDocIdAndVersionUncached(IndexReader reader, BytesRef term, boolean loadSeqNo) throws IOException {
        List<LeafReaderContext> leaves = reader.leaves();
        for (int i = leaves.size() - 1; i >= 0; i--) {
            final LeafReaderContext leaf = leaves.get(i);
            PerThreadIDVersionAndSeqNoLookup lookup = new PerThreadIDVersionAndSeqNoLookup(leaf.reader(), false, false);
            DocIdAndVersion result = lookup.lookupVersion(term, loadSeqNo, leaf);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    /**
     * Loads the internal docId and sequence number of the latest copy for a given uid from the provided reader.
     * The result is either null or the live and latest version of the given uid.
     */
    public static DocIdAndSeqNo loadDocIdAndSeqNo(IndexReader reader, BytesRef term) throws IOException {
        final PerThreadIDVersionAndSeqNoLookup[] lookups = getLookupState(reader, false);
        final List<LeafReaderContext> leaves = reader.leaves();
        // iterate backwards to optimize for the frequently updated documents
        // which are likely to be in the last segments
        for (int i = leaves.size() - 1; i >= 0; i--) {
            final LeafReaderContext leaf = leaves.get(i);
            final PerThreadIDVersionAndSeqNoLookup lookup = lookups[leaf.ord];
            final DocIdAndSeqNo result = lookup.lookupSeqNo(term, leaf);
            if (result != null) {
                return result;
            }
        }
        return null;
    }
}
