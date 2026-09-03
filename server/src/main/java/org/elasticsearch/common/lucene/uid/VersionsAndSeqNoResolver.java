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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.index.mapper.Uid;

import java.io.IOException;
import java.util.Arrays;
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
    public static final class DocIdAndVersion {
        public final int docId;
        public final long version;
        public final long seqNo;
        public final long primaryTerm;
        public final LeafReader reader;
        public final int docBase;

        DocIdAndVersion(int docId, long version, long seqNo, long primaryTerm, LeafReader reader, int docBase) {
            this.docId = docId;
            this.version = version;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.reader = reader;
            this.docBase = docBase;
        }
    }

    /** Wraps an {@link LeafReaderContext}, a doc ID <b>relative to the context doc base</b> and a seqNo. */
    public static final class DocIdAndSeqNo {
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
    public static DocIdAndVersion loadDocIdAndVersion(IndexReader reader, BytesRef term, boolean loadSeqNo) throws IOException {
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
     * Resolves doc ID and version for a batch of UIDs, amortizing reader and seek overhead across the batch.
     * <p>
     * Results are written into {@code results[i]} for each {@code uids[i]}; a null entry means the UID
     * was not found. {@code results} is fully overwritten, and {@code uids} is not modified. UIDs need not
     * be pre-sorted; sorting is done internally.
     * <p>
     * The batch is sorted by UID and then handed to each segment newest-first. Each segment resolves what it
     * can and compacts those entries out of the batch, so later (older) segments only see UIDs that are still
     * unresolved and the newest segment's version naturally wins.
     * <p>
     * This method uses {@code loadTimestampRange = false} and is intended for standard (non-time-series)
     * indices. For time series indices use {@link #timeSeriesBatchLoadDocIdAndVersion} instead.
     */
    public static void batchLoadDocIdAndVersion(IndexReader reader, BytesRef[] uids, boolean[] loadSeqNo, DocIdAndVersion[] results)
        throws IOException {
        final int n = uids.length;
        assert results.length == n && loadSeqNo.length == n;
        Arrays.fill(results, null);
        if (n == 0) {
            return;
        }

        // Working copy of the batch: sorted by UID, then compacted in place as segments resolve entries.
        // originalIndex maps each working position back to the caller's array index.
        final BytesRef[] candidates = uids.clone();
        final int[] originalIndex = new int[n];
        for (int i = 0; i < n; i++) {
            originalIndex[i] = i;
        }
        sortByUid(candidates, originalIndex, null, n);

        final PerThreadIDVersionAndSeqNoLookup[] lookups = getLookupState(reader, false);
        final List<LeafReaderContext> leaves = reader.leaves();
        int count = n;

        // Iterate backwards: the most recently written segment is most likely to contain the current version.
        for (int s = leaves.size() - 1; s >= 0 && count > 0; s--) {
            final LeafReaderContext leaf = leaves.get(s);
            count = lookups[leaf.ord].batchLookupVersion(leaf, candidates, originalIndex, count, loadSeqNo, results);
        }
    }

    /**
     * Resolves doc ID and version for a batch of time series UIDs, amortizing reader and seek overhead across
     * the batch while exploiting per-segment timestamp ranges to skip segments that cannot contain a given UID.
     * <p>
     * Results are written into {@code results[i]} for each {@code uids[i]}; a null entry means the
     * UID was not found. {@code results} is fully overwritten, and {@code uids} is not modified. UIDs need not
     * be pre-sorted; sorting is done internally.
     * <p>
     * Segments are iterated in {@link org.elasticsearch.cluster.metadata.DataStream#TIMESERIES_LEAF_READERS_SORTER}
     * forward order (descending maxTimestamp), so the first segment's maxTimestamp bounds the entire reader. UIDs
     * with a newer timestamp than that cannot exist in any segment, and are filtered out up front — before the
     * batch is sorted and before any working array is allocated. That is the steady state for time series
     * ingestion, where documents arrive with timestamps ahead of everything already searchable, so the common
     * case costs one pass over {@code uids} and nothing else.
     *
     * @param uids          the UID terms to look up
     * @param ids           the document IDs corresponding to each UID; used to extract timestamps
     * @param useSyntheticId true if IDs are synthetic TSDB IDs (timestamp embedded in UID),
     *                       false if they are standard base64-URL-encoded 20-byte IDs
     * @param loadSeqNo     whether to populate seqNo/primaryTerm in each result
     * @param results       out parameter; null entry means not found
     */
    public static void timeSeriesBatchLoadDocIdAndVersion(
        IndexReader reader,
        BytesRef[] uids,
        String[] ids,
        boolean useSyntheticId,
        boolean[] loadSeqNo,
        DocIdAndVersion[] results
    ) throws IOException {
        final int n = uids.length;
        assert results.length == n && loadSeqNo.length == n && ids.length == n;
        Arrays.fill(results, null);
        if (n == 0) {
            return;
        }

        final PerThreadIDVersionAndSeqNoLookup[] lookups = getLookupState(reader, true);
        final List<LeafReaderContext> leaves = reader.leaves();
        if (leaves.isEmpty()) {
            return;
        }

        // Segments without a @timestamp field (no-op or tombstone only) report Long.MAX_VALUE here, which
        // disables the filter rather than wrongly excluding anything.
        final long readerMaxTimestamp = lookups[leaves.get(0).ord].maxTimestamp;

        BytesRef[] candidates = null;
        long[] timestamps = null;
        int[] originalIndex = null;
        int count = 0;
        for (int i = 0; i < n; i++) {
            final long timestamp = extractTimestamp(uids[i], ids[i], useSyntheticId);
            if (timestamp > readerMaxTimestamp) {
                // Newer than every segment: definitively not indexed yet, results[i] stays null.
                continue;
            }
            if (candidates == null) {
                // Sized for the worst case from here on, so this allocates at most once per batch.
                final int capacity = n - i;
                candidates = new BytesRef[capacity];
                timestamps = new long[capacity];
                originalIndex = new int[capacity];
            }
            candidates[count] = uids[i];
            timestamps[count] = timestamp;
            originalIndex[count] = i;
            count++;
        }
        if (count == 0) {
            return;
        }

        sortByUid(candidates, originalIndex, timestamps, count);

        long prevMaxTimestamp = Long.MAX_VALUE;
        for (int s = 0; s < leaves.size() && count > 0; s++) {
            final LeafReaderContext leaf = leaves.get(s);
            final PerThreadIDVersionAndSeqNoLookup lookup = lookups[leaf.ord];
            // Segments without a @timestamp field sort last but report an unbounded range, so they are
            // exempt from the monotonicity the skipping logic relies on.
            final boolean bounded = lookup.maxTimestamp != Long.MAX_VALUE;
            assert bounded == false || prevMaxTimestamp >= lookup.maxTimestamp
                : "segments are not in descending maxTimestamp order: " + prevMaxTimestamp + " < " + lookup.maxTimestamp;
            if (bounded) {
                prevMaxTimestamp = lookup.maxTimestamp;
            }
            count = lookup.timeSeriesBatchLookupVersion(leaf, candidates, timestamps, originalIndex, count, loadSeqNo, results);
        }
    }

    private static long extractTimestamp(BytesRef uid, String id, boolean useSyntheticId) {
        if (useSyntheticId) {
            assert uid.equals(Uid.encodeId(id));
            return TsidExtractingIdFieldMapper.extractTimestampFromSyntheticId(uid);
        }
        if (TsidExtractingIdFieldMapper.isEncodedStandardId(uid)) {
            // Read the timestamp straight out of the encoded uid: no base64 decode, no allocation.
            final long timestamp = TsidExtractingIdFieldMapper.extractTimestampFromEncodedId(uid);
            assert timestamp == TsidExtractingIdFieldMapper.extractTimestampFromId(Base64.getUrlDecoder().decode(id))
                : "in-place timestamp extraction disagrees with the decoded id [" + id + "]";
            return timestamp;
        }
        return TsidExtractingIdFieldMapper.extractTimestampFromId(Base64.getUrlDecoder().decode(id));
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
            assert uid.equals(Uid.encodeId(id));
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
        return loadDocIdAndSeqNo(reader, term, true);
    }

    /**
     * Loads the internal docId and sequence number of the latest copy for a given uid from the provided reader.
     * When {@code loadSeqNo} is false, {@code UNASSIGNED_SEQ_NO} is returned instead of reading the doc value.
     * The result is either null or the live and latest version of the given uid.
     */
    public static DocIdAndSeqNo loadDocIdAndSeqNo(IndexReader reader, BytesRef term, boolean loadSeqNo) throws IOException {
        final PerThreadIDVersionAndSeqNoLookup[] lookups = getLookupState(reader, false);
        final List<LeafReaderContext> leaves = reader.leaves();
        // iterate backwards to optimize for the frequently updated documents
        // which are likely to be in the last segments
        for (int i = leaves.size() - 1; i >= 0; i--) {
            final LeafReaderContext leaf = leaves.get(i);
            final PerThreadIDVersionAndSeqNoLookup lookup = lookups[leaf.ord];
            final DocIdAndSeqNo result = lookup.lookupDocIdAndSeqNo(term, leaf, loadSeqNo);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    /**
     * Sorts the first {@code count} entries of {@code uids} into ascending order, carrying {@code originalIndex}
     * and, when non-null, {@code timestamps} along with it so all three stay aligned.
     * <p>
     * Note that {@link IntroSorter#setPivot} must capture the pivot <i>value</i>, not its slot: the very next
     * thing {@code IntroSorter} does is swap the pivot out of that slot.
     */
    private static void sortByUid(BytesRef[] uids, int[] originalIndex, @Nullable long[] timestamps, int count) {
        if (count < 2) {
            return;
        }
        new IntroSorter() {
            private BytesRef pivot;

            @Override
            protected void setPivot(int i) {
                pivot = uids[i];
            }

            @Override
            protected int comparePivot(int j) {
                return pivot.compareTo(uids[j]);
            }

            @Override
            protected int compare(int i, int j) {
                return uids[i].compareTo(uids[j]);
            }

            @Override
            protected void swap(int i, int j) {
                final BytesRef tmpUid = uids[i];
                uids[i] = uids[j];
                uids[j] = tmpUid;
                final int tmpIndex = originalIndex[i];
                originalIndex[i] = originalIndex[j];
                originalIndex[j] = tmpIndex;
                if (timestamps != null) {
                    final long tmpTimestamp = timestamps[i];
                    timestamps[i] = timestamps[j];
                    timestamps[j] = tmpTimestamp;
                }
            }
        }.sort(0, count);
    }
}
