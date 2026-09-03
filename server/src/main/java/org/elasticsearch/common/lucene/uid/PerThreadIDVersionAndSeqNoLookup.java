/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.uid;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndSeqNo;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;

import java.io.IOException;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/** Utility class to do efficient primary-key (only 1 doc contains the
 *  given term) lookups by segment, re-using the enums.  This class is
 *  not thread safe, so it is the caller's job to create and use one
 *  instance of this per thread.  Do not use this if a term may appear
 *  in more than one document!  It will only return the first one it
 *  finds.
 *  This class uses live docs, so it should be cached based on the
 *  {@link org.apache.lucene.index.IndexReader#getReaderCacheHelper() reader cache helper}
 *  rather than the {@link LeafReader#getCoreCacheHelper() core cache helper}.
 */
final class PerThreadIDVersionAndSeqNoLookup {
    // TODO: do we really need to store all this stuff? some if it might not speed up anything.
    // we keep it around for now, to reduce the amount of e.g. hash lookups by field and stuff

    private final TermsEnum termsEnum;

    /** Reused for iteration (when the term exists) */
    private PostingsEnum docsEnum;

    /**
     * Doc values iterators reused across the hits of a batch lookup, see {@link #readVersionInfo}. Doc values are
     * forward-only, so these are only valid while the doc ids we resolve are non-decreasing; {@link #dvDocId} tracks
     * the furthest doc we advanced to so we know when to start over.
     */
    private LeafReader dvReader;
    private NumericDocValues versionDV;
    private NumericDocValues seqNoDV;
    private NumericDocValues primaryTermDV;
    private int dvDocId = -1;

    /** used for assertions to make sure class usage meets assumptions */
    private final Object readerKey;

    final boolean loadedTimestampRange;
    final long minTimestamp;
    final long maxTimestamp;

    /**
     * Initialize lookup for the provided segment
     */
    PerThreadIDVersionAndSeqNoLookup(LeafReader reader, boolean trackReaderKey, boolean loadTimestampRange) throws IOException {
        final Terms terms = reader.terms(IdFieldMapper.NAME);
        if (terms == null) {
            // If a segment contains only no-ops, it does not have _uid but has both _soft_deletes and _tombstone fields.
            final NumericDocValues softDeletesDV = reader.getNumericDocValues(Lucene.SOFT_DELETES_FIELD);
            final NumericDocValues tombstoneDV = reader.getNumericDocValues(SeqNoFieldMapper.TOMBSTONE_NAME);
            // this is a special case when we pruned away all IDs in a segment since all docs are deleted.
            final boolean allDocsDeleted = (softDeletesDV != null && reader.numDocs() == 0);
            if ((softDeletesDV == null || tombstoneDV == null) && allDocsDeleted == false) {
                throw new IllegalArgumentException(
                    "reader does not have _uid terms but not a no-op segment; "
                        + "_soft_deletes ["
                        + softDeletesDV
                        + "], _tombstone ["
                        + tombstoneDV
                        + "]"
                );
            }
            termsEnum = null;
        } else {
            termsEnum = terms.iterator();
        }
        if (reader.getNumericDocValues(VersionFieldMapper.NAME) == null) {
            throw new IllegalArgumentException("reader misses the [" + VersionFieldMapper.NAME + "] field; _uid terms [" + terms + "]");
        }
        Object readerKey = null;
        assert trackReaderKey ? (readerKey = reader.getCoreCacheHelper().getKey()) != null : readerKey == null;
        this.readerKey = readerKey;

        this.loadedTimestampRange = loadTimestampRange;
        // Also check for the existence of the timestamp field, because sometimes a segment can only contain tombstone documents,
        // which don't have any mapped fields (also not the timestamp field) and just some meta fields like _id, _seq_no etc.
        long minTimestamp = 0;
        long maxTimestamp = Long.MAX_VALUE;
        if (loadTimestampRange) {
            FieldInfo info = reader.getFieldInfos().fieldInfo(DataStream.TIMESTAMP_FIELD_NAME);
            if (info != null) {
                if (info.docValuesSkipIndexType() == DocValuesSkipIndexType.RANGE) {
                    DocValuesSkipper skipper = reader.getDocValuesSkipper(DataStream.TIMESTAMP_FIELD_NAME);
                    assert skipper != null : "no skipper for reader:" + reader + " and parent:" + reader.getContext().parent.reader();
                    minTimestamp = skipper.minValue();
                    maxTimestamp = skipper.maxValue();
                } else {
                    PointValues tsPointValues = reader.getPointValues(DataStream.TIMESTAMP_FIELD_NAME);
                    assert tsPointValues != null
                        : "no timestamp field for reader:" + reader + " and parent:" + reader.getContext().parent.reader();
                    minTimestamp = LongPoint.decodeDimension(tsPointValues.getMinPackedValue(), 0);
                    maxTimestamp = LongPoint.decodeDimension(tsPointValues.getMaxPackedValue(), 0);
                }
            }
        }
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
    }

    PerThreadIDVersionAndSeqNoLookup(LeafReader reader, boolean loadTimestampRange) throws IOException {
        this(reader, true, loadTimestampRange);
    }

    /** Return null if id is not found.
     * We pass the {@link LeafReaderContext} as an argument so that things
     * still work with reader wrappers that hide some documents while still
     * using the same cache key. Otherwise we'd have to disable caching
     * entirely for these readers.
     */
    public DocIdAndVersion lookupVersion(BytesRef id, boolean loadSeqNo, LeafReaderContext context) throws IOException {
        assert readerKey == null || context.reader().getCoreCacheHelper().getKey().equals(readerKey)
            : "context's reader is not the same as the reader class was initialized on.";
        int docID = getDocID(id, context);

        if (docID != DocIdSetIterator.NO_MORE_DOCS) {
            final long seqNo;
            final long term;
            if (loadSeqNo) {
                seqNo = readNumericDocValues(context.reader(), SeqNoFieldMapper.NAME, docID);
                term = readNumericDocValues(context.reader(), SeqNoFieldMapper.PRIMARY_TERM_NAME, docID);
            } else {
                seqNo = UNASSIGNED_SEQ_NO;
                term = UNASSIGNED_PRIMARY_TERM;
            }
            final long version = readNumericDocValues(context.reader(), VersionFieldMapper.NAME, docID);
            return new DocIdAndVersion(docID, version, seqNo, term, context.reader(), context.docBase);
        } else {
            return null;
        }
    }

    /**
     * returns the internal lucene doc id for the given id bytes.
     * {@link DocIdSetIterator#NO_MORE_DOCS} is returned if not found
     * */
    private int getDocID(BytesRef id, LeafReaderContext context) throws IOException {
        // termsEnum can possibly be null here if this leaf contains only no-ops.
        if (termsEnum != null && termsEnum.seekExact(id)) {
            return scanLiveDoc(context.reader().getLiveDocs());
        } else {
            return DocIdSetIterator.NO_MORE_DOCS;
        }
    }

    /**
     * Scans postings for the term the {@link #termsEnum} is currently positioned on and returns the
     * highest live doc ID found, or {@link DocIdSetIterator#NO_MORE_DOCS} if no live doc exists.
     * There may be more than one matching doc ID in the case of nested docs, so we want the last one.
     */
    private int scanLiveDoc(Bits liveDocs) throws IOException {
        int docID = DocIdSetIterator.NO_MORE_DOCS;
        docsEnum = termsEnum.postings(docsEnum, 0);
        for (int d = docsEnum.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = docsEnum.nextDoc()) {
            if (liveDocs != null && liveDocs.get(d) == false) {
                continue;
            }
            docID = d;
        }
        return docID;
    }

    /**
     * Resolves version info for a batch of UIDs against this segment's terms dictionary using
     * {@link TermsEnum#seekExact} per UID.
     * <p>
     * The batch is held in the parallel arrays {@code uids} / {@code originalIndex}, whose first
     * {@code count} entries are the UIDs still looking for a home, in ascending lexicographic order.
     * {@code originalIndex[i]} is the position of {@code uids[i]} in the caller's {@code loadSeqNo} and
     * {@code results} arrays. Sorted order improves terms dictionary cache locality across consecutive
     * lookups, even though the forward-scan amortisation of {@code seekCeil} is not used.
     * <p>
     * UIDs resolved by this segment are written to {@code results} and then <b>compacted out</b> of
     * {@code uids} / {@code originalIndex} in place, preserving sort order. The caller feeds the returned
     * count into the next segment, so each segment only walks the UIDs that are still unresolved rather
     * than rescanning the whole batch. Since the caller visits segments newest-first, the first segment to
     * produce a hit wins, which is the behaviour we want.
     * <p>
     * Using {@code seekExact} rather than {@code seekCeil} matters a great deal here: for time series
     * indices the {@code _id} terms are wrapped in a Bloom filter that only intercepts {@code seekExact}
     * (see {@code DelegatingBloomFilterFieldsProducer}), and even without one it lets the block-tree FST
     * reject absent terms before loading any block from disk. Absent terms are the dominant case for
     * insert-heavy workloads.
     *
     * @return the number of UIDs left unresolved, i.e. the new {@code count} for the next segment
     */
    int batchLookupVersion(
        LeafReaderContext context,
        BytesRef[] uids,
        int[] originalIndex,
        int count,
        boolean[] loadSeqNo,
        DocIdAndVersion[] results
    ) throws IOException {
        if (termsEnum == null) {
            return count;
        }
        assert readerKey == null || context.reader().getCoreCacheHelper().getKey().equals(readerKey)
            : "context's reader is not the same as the reader class was initialized on.";

        final Bits liveDocs = context.reader().getLiveDocs();
        int remaining = 0;

        for (int i = 0; i < count; i++) {
            final BytesRef uid = uids[i];
            if (termsEnum.seekExact(uid)) {
                final int docID = scanLiveDoc(liveDocs);
                if (docID != DocIdSetIterator.NO_MORE_DOCS) {
                    final int original = originalIndex[i];
                    results[original] = readVersionInfo(context, docID, loadSeqNo[original]);
                    continue;
                }
            }
            if (remaining != i) {
                uids[remaining] = uid;
                originalIndex[remaining] = originalIndex[i];
            }
            remaining++;
        }

        return remaining;
    }

    /**
     * Resolves version info for a batch of time series UIDs against this segment's terms dictionary,
     * using the segment's timestamp range to avoid lookups for UIDs that cannot be in this segment.
     * <p>
     * The batch layout is the same as {@link #batchLookupVersion}, with {@code timestamps} carrying the
     * timestamp of each UID, and is likewise compacted in place as entries are resolved. An entry is
     * dropped from the batch when it is found in this segment, and also when its timestamp exceeds
     * {@link #maxTimestamp}: the caller iterates segments in
     * {@link org.elasticsearch.cluster.metadata.DataStream#TIMESERIES_LEAF_READERS_SORTER} order, so every
     * later segment has an even lower maxTimestamp and cannot contain it either. Entries whose timestamp
     * falls below {@link #minTimestamp} are kept, since a later (older) segment may still hold them.
     *
     * @return the number of UIDs left unresolved, i.e. the new {@code count} for the next segment
     */
    int timeSeriesBatchLookupVersion(
        LeafReaderContext context,
        BytesRef[] uids,
        long[] timestamps,
        int[] originalIndex,
        int count,
        boolean[] loadSeqNo,
        DocIdAndVersion[] results
    ) throws IOException {
        assert loadedTimestampRange : "timeSeriesBatchLookupVersion requires loadedTimestampRange=true";
        if (termsEnum == null) {
            return count;
        }
        assert readerKey == null || context.reader().getCoreCacheHelper().getKey().equals(readerKey)
            : "context's reader is not the same as the reader class was initialized on.";

        final Bits liveDocs = context.reader().getLiveDocs();
        int remaining = 0;

        for (int i = 0; i < count; i++) {
            final long ts = timestamps[i];
            if (ts > maxTimestamp) {
                // Newer than any doc in this or any subsequent segment: permanently not found, drop it.
                continue;
            }
            // A timestamp below minTimestamp predates this segment but may land in a later (older) one,
            // so skip the terms lookup but keep the entry in the batch.
            if (ts >= minTimestamp && termsEnum.seekExact(uids[i])) {
                final int docID = scanLiveDoc(liveDocs);
                if (docID != DocIdSetIterator.NO_MORE_DOCS) {
                    final int original = originalIndex[i];
                    results[original] = readVersionInfo(context, docID, loadSeqNo[original]);
                    continue;
                }
            }
            if (remaining != i) {
                uids[remaining] = uids[i];
                timestamps[remaining] = ts;
                originalIndex[remaining] = originalIndex[i];
            }
            remaining++;
        }

        return remaining;
    }

    /**
     * Reads the version (and optionally seqNo/primaryTerm) doc values for a matched doc, reusing the doc values
     * iterators across the hits of a batch where possible.
     * <p>
     * {@link LeafReader#getNumericDocValues} is not free — it hashes the field name and builds a fresh iterator —
     * and the naive version pays it three times per hit. Doc values iterators are forward-only, so the cached ones
     * can only be reused while doc ids do not go backwards; that holds often enough to be worth it because the
     * batch is scanned in {@code _id} order, which for time series indices tracks the index sort closely.
     */
    private DocIdAndVersion readVersionInfo(LeafReaderContext context, int docID, boolean loadSeqNo) throws IOException {
        final LeafReader reader = context.reader();
        if (reader != dvReader || docID < dvDocId) {
            dvReader = reader;
            versionDV = null;
            seqNoDV = null;
            primaryTermDV = null;
        }
        dvDocId = docID;

        if (versionDV == null) {
            versionDV = reader.getNumericDocValues(VersionFieldMapper.NAME);
        }
        final long version = advanceAndGet(versionDV, VersionFieldMapper.NAME, docID);

        final long seqNo;
        final long primaryTerm;
        if (loadSeqNo) {
            if (seqNoDV == null) {
                seqNoDV = reader.getNumericDocValues(SeqNoFieldMapper.NAME);
                primaryTermDV = reader.getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME);
            }
            seqNo = advanceAndGet(seqNoDV, SeqNoFieldMapper.NAME, docID);
            primaryTerm = advanceAndGet(primaryTermDV, SeqNoFieldMapper.PRIMARY_TERM_NAME, docID);
        } else {
            seqNo = UNASSIGNED_SEQ_NO;
            primaryTerm = UNASSIGNED_PRIMARY_TERM;
        }
        return new DocIdAndVersion(docID, version, seqNo, primaryTerm, reader, context.docBase);
    }

    private static long readNumericDocValues(LeafReader reader, String field, int docId) throws IOException {
        return advanceAndGet(reader.getNumericDocValues(field), field, docId);
    }

    private static long advanceAndGet(NumericDocValues dv, String field, int docId) throws IOException {
        if (dv == null || dv.advanceExact(docId) == false) {
            assert false : "document [" + docId + "] does not have docValues for [" + field + "]";
            throw new IllegalStateException("document [" + docId + "] does not have docValues for [" + field + "]");
        }
        return dv.longValue();
    }

    /** Return null if id is not found. */
    DocIdAndSeqNo lookupDocIdAndSeqNo(BytesRef id, LeafReaderContext context, boolean loadSeqNo) throws IOException {
        assert readerKey == null || context.reader().getCoreCacheHelper().getKey().equals(readerKey)
            : "context's reader is not the same as the reader class was initialized on.";
        final int docID = getDocID(id, context);
        if (docID != DocIdSetIterator.NO_MORE_DOCS) {
            final long seqNo = loadSeqNo ? readNumericDocValues(context.reader(), SeqNoFieldMapper.NAME, docID) : UNASSIGNED_SEQ_NO;
            return new DocIdAndSeqNo(docID, seqNo, context);
        } else {
            return null;
        }
    }
}
