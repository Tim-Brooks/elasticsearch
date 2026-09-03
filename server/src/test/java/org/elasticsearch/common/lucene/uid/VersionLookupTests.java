/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.uid;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * test per-segment lookup of version-related data structures
 */
public class VersionLookupTests extends ESTestCase {

    /**
     * test version lookup actually works
     */
    public void testSimple() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        Document doc = new Document();
        doc.add(new StringField(IdFieldMapper.NAME, "6", Field.Store.YES));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 87));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        writer.addDocument(doc);
        writer.addDocument(new Document());
        DirectoryReader reader = DirectoryReader.open(writer);
        LeafReaderContext segment = reader.leaves().getFirst();
        PerThreadIDVersionAndSeqNoLookup lookup = new PerThreadIDVersionAndSeqNoLookup(segment.reader(), false);
        // found doc
        DocIdAndVersion result = lookup.lookupVersion(new BytesRef("6"), randomBoolean(), segment);
        assertNotNull(result);
        assertEquals(87, result.version);
        assertEquals(0, result.docId);
        // not found doc
        assertNull(lookup.lookupVersion(new BytesRef("7"), randomBoolean(), segment));
        // deleted doc
        writer.deleteDocuments(new Term(IdFieldMapper.NAME, "6"));
        reader.close();
        reader = DirectoryReader.open(writer);
        segment = reader.leaves().getFirst();
        lookup = new PerThreadIDVersionAndSeqNoLookup(segment.reader(), false);
        assertNull(lookup.lookupVersion(new BytesRef("6"), randomBoolean(), segment));
        reader.close();
        writer.close();
        dir.close();
    }

    /**
     * test version lookup with two documents matching the ID
     */
    public void testTwoDocuments() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        Document doc = new Document();
        doc.add(new StringField(IdFieldMapper.NAME, "6", Field.Store.YES));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 87));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        writer.addDocument(doc);
        writer.addDocument(doc);
        writer.addDocument(new Document());
        DirectoryReader reader = DirectoryReader.open(writer);
        LeafReaderContext segment = reader.leaves().getFirst();
        PerThreadIDVersionAndSeqNoLookup lookup = new PerThreadIDVersionAndSeqNoLookup(segment.reader(), false);
        // return the last doc when there are duplicates
        DocIdAndVersion result = lookup.lookupVersion(new BytesRef("6"), randomBoolean(), segment);
        assertNotNull(result);
        assertEquals(87, result.version);
        assertEquals(1, result.docId);
        // delete the first doc only
        assertTrue(writer.tryDeleteDocument(reader, 0) >= 0);
        reader.close();
        reader = DirectoryReader.open(writer);
        segment = reader.leaves().getFirst();
        lookup = new PerThreadIDVersionAndSeqNoLookup(segment.reader(), false);
        result = lookup.lookupVersion(new BytesRef("6"), randomBoolean(), segment);
        assertNotNull(result);
        assertEquals(87, result.version);
        assertEquals(1, result.docId);
        // delete both docs
        assertTrue(writer.tryDeleteDocument(reader, 1) >= 0);
        reader.close();
        reader = DirectoryReader.open(writer);
        segment = reader.leaves().getFirst();
        lookup = new PerThreadIDVersionAndSeqNoLookup(segment.reader(), false);
        assertNull(lookup.lookupVersion(new BytesRef("6"), randomBoolean(), segment));
        reader.close();
        writer.close();
        dir.close();
    }

    public void testLoadTimestampRange() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        Document doc = new Document();
        doc.add(new StringField(IdFieldMapper.NAME, "6", Field.Store.YES));
        doc.add(new LongPoint(DataStream.TIMESTAMP_FIELD_NAME, 1_000));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 87));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        writer.addDocument(doc);
        doc = new Document();
        doc.add(new StringField(IdFieldMapper.NAME, "8", Field.Store.YES));
        doc.add(new LongPoint(DataStream.TIMESTAMP_FIELD_NAME, 1_000_000));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 1));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, randomNonNegativeLong()));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, randomLongBetween(1, Long.MAX_VALUE)));
        writer.addDocument(doc);
        DirectoryReader reader = DirectoryReader.open(writer);

        LeafReaderContext segment = reader.leaves().getFirst();
        PerThreadIDVersionAndSeqNoLookup lookup = new PerThreadIDVersionAndSeqNoLookup(segment.reader(), true);
        assertTrue(lookup.loadedTimestampRange);
        assertEquals(1_000L, lookup.minTimestamp);
        assertEquals(1_000_000L, lookup.maxTimestamp);

        lookup = new PerThreadIDVersionAndSeqNoLookup(segment.reader(), false);
        assertFalse(lookup.loadedTimestampRange);
        assertEquals(0L, lookup.minTimestamp);
        assertEquals(Long.MAX_VALUE, lookup.maxTimestamp);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchLookupBasic() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        writer.addDocument(makeDoc("a", 1L, 10L, 1L));
        writer.addDocument(makeDoc("b", 2L, 20L, 1L));
        writer.addDocument(makeDoc("c", 3L, 30L, 1L));
        DirectoryReader reader = DirectoryReader.open(writer);

        DocIdAndVersion[] results = batchLookup(reader, "a", "b", "c");

        assertNotNull(results[0]);
        assertEquals(1L, results[0].version);
        assertNotNull(results[1]);
        assertEquals(2L, results[1].version);
        assertNotNull(results[2]);
        assertEquals(3L, results[2].version);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchLookupPartialMiss() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        writer.addDocument(makeDoc("a", 10L, 1L, 1L));
        writer.addDocument(makeDoc("c", 30L, 3L, 1L));
        DirectoryReader reader = DirectoryReader.open(writer);

        DocIdAndVersion[] results = batchLookup(reader, "a", "b", "c");

        assertNotNull(results[0]);
        assertEquals(10L, results[0].version);
        assertNull(results[1]);
        assertNotNull(results[2]);
        assertEquals(30L, results[2].version);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchLookupMultiSegment() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        writer.addDocument(makeDoc("a", 1L, 1L, 1L));
        writer.addDocument(makeDoc("b", 2L, 2L, 1L));
        writer.flush();
        writer.addDocument(makeDoc("c", 3L, 3L, 1L));
        writer.addDocument(makeDoc("d", 4L, 4L, 1L));
        DirectoryReader reader = DirectoryReader.open(writer);
        assertEquals(2, reader.leaves().size());

        DocIdAndVersion[] results = batchLookup(reader, "a", "b", "c", "d");

        for (int i = 0; i < 4; i++) {
            assertNotNull(results[i]);
            assertEquals((i + 1), results[i].version);
        }

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchLookupNewerSegmentWins() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        writer.addDocument(makeDoc("a", 1L, 1L, 1L));
        writer.flush();
        writer.addDocument(makeDoc("a", 2L, 2L, 1L));
        DirectoryReader reader = DirectoryReader.open(writer);
        assertEquals(2, reader.leaves().size());

        DocIdAndVersion[] results = batchLookup(reader, "a");

        assertNotNull(results[0]);
        assertEquals(2L, results[0].version);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchLookupLoadSeqNoFlag() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        writer.addDocument(makeDoc("a", 5L, 42L, 7L));
        DirectoryReader reader = DirectoryReader.open(writer);

        BytesRef[] uids = { new BytesRef("a") };

        DocIdAndVersion[] withSeqNo = new DocIdAndVersion[1];
        VersionsAndSeqNoResolver.batchLoadDocIdAndVersion(reader, uids, new boolean[] { true }, withSeqNo);
        assertNotNull(withSeqNo[0]);
        assertEquals(5L, withSeqNo[0].version);
        assertEquals(42L, withSeqNo[0].seqNo);
        assertEquals(7L, withSeqNo[0].primaryTerm);

        DocIdAndVersion[] withoutSeqNo = new DocIdAndVersion[1];
        VersionsAndSeqNoResolver.batchLoadDocIdAndVersion(reader, uids, new boolean[] { false }, withoutSeqNo);
        assertNotNull(withoutSeqNo[0]);
        assertEquals(5L, withoutSeqNo[0].version);
        assertEquals(UNASSIGNED_SEQ_NO, withoutSeqNo[0].seqNo);
        assertEquals(UNASSIGNED_PRIMARY_TERM, withoutSeqNo[0].primaryTerm);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchLookupDeletedDoc() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        writer.addDocument(makeDoc("a", 1L, 1L, 1L));
        writer.addDocument(makeDoc("b", 2L, 2L, 1L));
        writer.deleteDocuments(new Term(IdFieldMapper.NAME, "a"));
        DirectoryReader reader = DirectoryReader.open(writer);

        DocIdAndVersion[] results = batchLookup(reader, "a", "b");

        assertNull(results[0]);
        assertNotNull(results[1]);
        assertEquals(2L, results[1].version);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchLookupEmpty() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        writer.addDocument(makeDoc("a", 1L, 1L, 1L));
        DirectoryReader reader = DirectoryReader.open(writer);

        batchLookup(reader); // no exception

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchLookupAllMissing() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        writer.addDocument(makeDoc("a", 1L, 1L, 1L));
        DirectoryReader reader = DirectoryReader.open(writer);

        DocIdAndVersion[] results = batchLookup(reader, "x", "y", "z");

        assertNull(results[0]);
        assertNull(results[1]);
        assertNull(results[2]);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchLookupInterleavedMissAndHit() throws Exception {
        // Segment has "b" and "c" but not "a", so the batch alternates between misses and hits and the
        // unresolved "a" has to survive compaction while "b" and "c" are removed from the batch.
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        writer.addDocument(makeDoc("b", 2L, 20L, 1L));
        writer.addDocument(makeDoc("c", 3L, 30L, 1L));
        DirectoryReader reader = DirectoryReader.open(writer);

        DocIdAndVersion[] results = batchLookup(reader, "a", "b", "c");

        assertNull(results[0]);
        assertNotNull(results[1]);
        assertEquals(2L, results[1].version);
        assertNotNull(results[2]);
        assertEquals(3L, results[2].version);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchTimeSeriesLookupBasic() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        long ts = 1_000_000L;
        String id = makeTsdbId(1, ts);
        writer.addDocument(makeDocWithTimestamp(id, ts, 7L, 42L, 1L));
        DirectoryReader reader = DirectoryReader.open(writer);

        DocIdAndVersion[] results = batchTimeSeriesLookup(reader, id);

        assertNotNull(results[0]);
        assertEquals(7L, results[0].version);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchTimeSeriesLookupSegmentSkipping() throws Exception {
        // Two segments: segment 0 covers ts=2000, segment 1 covers ts=1000.
        // A UID with ts=1000 must be found in segment 1 (ts < minTimestamp of segment 0 → skipped).
        // A UID with ts=2000 must be found in segment 0.
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        long tsNewer = 2_000L;
        long tsOlder = 1_000L;
        String idNewer = makeTsdbId(1, tsNewer);
        String idOlder = makeTsdbId(2, tsOlder);
        writer.addDocument(makeDocWithTimestamp(idNewer, tsNewer, 10L, 1L, 1L));
        writer.flush(); // segment 0: covers [2000, 2000]
        writer.addDocument(makeDocWithTimestamp(idOlder, tsOlder, 20L, 2L, 1L));
        // segment 1: covers [1000, 1000]
        DirectoryReader reader = DirectoryReader.open(writer);
        assertEquals(2, reader.leaves().size());

        DocIdAndVersion[] results = batchTimeSeriesLookup(reader, idNewer, idOlder);

        assertNotNull(results[0]);
        assertEquals(10L, results[0].version);
        assertNotNull(results[1]);
        assertEquals(20L, results[1].version);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchTimeSeriesLookupTimestampTooNew() throws Exception {
        // A UID whose timestamp is newer than the only segment's maxTimestamp is not found.
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        long segmentTs = 1_000L;
        String segmentId = makeTsdbId(1, segmentTs);
        writer.addDocument(makeDocWithTimestamp(segmentId, segmentTs, 5L, 1L, 1L));
        DirectoryReader reader = DirectoryReader.open(writer);

        // ts=2000 is newer than the segment's maxTimestamp=1000 → not found
        String tooNewId = makeTsdbId(2, 2_000L);
        DocIdAndVersion[] results = batchTimeSeriesLookup(reader, tooNewId);
        assertNull(results[0]);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchTimeSeriesLookupMixedTimestamps() throws Exception {
        // Three segments covering different time ranges. UIDs span all ranges plus one missing doc.
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        String idA = makeTsdbId(1, 3_000L);
        String idB = makeTsdbId(2, 2_000L);
        String idC = makeTsdbId(3, 1_000L);
        String idMissing = makeTsdbId(4, 2_000L); // same time range as idB but not indexed
        writer.addDocument(makeDocWithTimestamp(idA, 3_000L, 1L, 1L, 1L));
        writer.flush(); // segment 0: [3000, 3000]
        writer.addDocument(makeDocWithTimestamp(idB, 2_000L, 2L, 2L, 1L));
        writer.flush(); // segment 1: [2000, 2000]
        writer.addDocument(makeDocWithTimestamp(idC, 1_000L, 3L, 3L, 1L));
        // segment 2: [1000, 1000]
        DirectoryReader reader = DirectoryReader.open(writer);
        assertEquals(3, reader.leaves().size());

        DocIdAndVersion[] results = batchTimeSeriesLookup(reader, idA, idB, idC, idMissing);

        assertNotNull(results[0]);
        assertEquals(1L, results[0].version);
        assertNotNull(results[1]);
        assertEquals(2L, results[1].version);
        assertNotNull(results[2]);
        assertEquals(3L, results[2].version);
        assertNull(results[3]); // idMissing not in index

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchTimeSeriesLookupLoadSeqNoFlag() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        long ts = 500L;
        String id = makeTsdbId(1, ts);
        writer.addDocument(makeDocWithTimestamp(id, ts, 9L, 77L, 3L));
        DirectoryReader reader = DirectoryReader.open(writer);

        BytesRef uid = new BytesRef(id);
        DocIdAndVersion[] withSeqNo = new DocIdAndVersion[1];
        VersionsAndSeqNoResolver.timeSeriesBatchLoadDocIdAndVersion(
            reader,
            new BytesRef[] { uid },
            new String[] { id },
            false,
            new boolean[] { true },
            withSeqNo
        );
        assertNotNull(withSeqNo[0]);
        assertEquals(9L, withSeqNo[0].version);
        assertEquals(77L, withSeqNo[0].seqNo);
        assertEquals(3L, withSeqNo[0].primaryTerm);

        DocIdAndVersion[] withoutSeqNo = new DocIdAndVersion[1];
        VersionsAndSeqNoResolver.timeSeriesBatchLoadDocIdAndVersion(
            reader,
            new BytesRef[] { uid },
            new String[] { id },
            false,
            new boolean[] { false },
            withoutSeqNo
        );
        assertNotNull(withoutSeqNo[0]);
        assertEquals(9L, withoutSeqNo[0].version);
        assertEquals(UNASSIGNED_SEQ_NO, withoutSeqNo[0].seqNo);
        assertEquals(UNASSIGNED_PRIMARY_TERM, withoutSeqNo[0].primaryTerm);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchTimeSeriesLookupUnsortedInput() throws Exception {
        // UIDs provided out of lexicographic order — results must align with original positions.
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        long ts = 1_000L;
        String idA = makeTsdbId(0xAA, ts);
        String idB = makeTsdbId(0xBB, ts);
        String idC = makeTsdbId(0xCC, ts);
        writer.addDocument(makeDocWithTimestamp(idA, ts, 1L, 1L, 1L));
        writer.addDocument(makeDocWithTimestamp(idB, ts, 2L, 2L, 1L));
        writer.addDocument(makeDocWithTimestamp(idC, ts, 3L, 3L, 1L));
        DirectoryReader reader = DirectoryReader.open(writer);

        // Provide in reverse order: C, A, B
        DocIdAndVersion[] results = batchTimeSeriesLookup(reader, idC, idA, idB);

        assertNotNull(results[0]);
        assertEquals(3L, results[0].version); // idC
        assertNotNull(results[1]);
        assertEquals(1L, results[1].version); // idA
        assertNotNull(results[2]);
        assertEquals(2L, results[2].version); // idB

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchTimeSeriesLookupUpdateAcrossSegments() throws Exception {
        // Write version 1 alongside an unrelated doc, flush to seal segment 0, then
        // update the same doc (version 2) into segment 1. The unrelated doc keeps segment 0
        // alive after version 1 is deleted, and closing the writer commits the deletion.
        // The lookup must skip the deleted version 1 in segment 0 and return version 2.
        Directory dir = newDirectory();
        long ts = 1_000L;
        String id = makeTsdbId(1, ts);
        String idOther = makeTsdbId(2, ts);
        try (IndexWriter writer = newNoMergeWriter(dir)) {
            writer.addDocument(makeDocWithTimestamp(id, ts, 1L, 1L, 1L));
            writer.addDocument(makeDocWithTimestamp(idOther, ts, 99L, 9L, 1L)); // keeps segment alive
            writer.flush(); // seal segment 0: version 1 + unrelated doc
            writer.updateDocument(new Term(IdFieldMapper.NAME, id), makeDocWithTimestamp(id, ts, 2L, 2L, 1L));
        } // close commits and applies pending deletion of version 1 to segment 0
        DirectoryReader reader = DirectoryReader.open(dir);
        assertEquals(2, reader.leaves().size());

        DocIdAndVersion[] results = batchTimeSeriesLookup(reader, id);

        assertNotNull(results[0]);
        assertEquals(2L, results[0].version);

        reader.close();
        dir.close();
    }

    public void testBatchTimeSeriesLookupDeletedDoc() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        long ts = 1_000L;
        String id = makeTsdbId(1, ts);
        String idOther = makeTsdbId(2, ts);
        writer.addDocument(makeDocWithTimestamp(id, ts, 5L, 1L, 1L));
        writer.addDocument(makeDocWithTimestamp(idOther, ts, 6L, 2L, 1L));
        writer.deleteDocuments(new Term(IdFieldMapper.NAME, id));
        DirectoryReader reader = DirectoryReader.open(writer);

        DocIdAndVersion[] results = batchTimeSeriesLookup(reader, id, idOther);

        assertNull(results[0]);
        assertNotNull(results[1]);
        assertEquals(6L, results[1].version);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchTimeSeriesLookupEmpty() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        long ts = 1_000L;
        String id = makeTsdbId(1, ts);
        writer.addDocument(makeDocWithTimestamp(id, ts, 5L, 1L, 1L));
        DirectoryReader reader = DirectoryReader.open(writer);

        batchTimeSeriesLookup(reader); // no exception

        reader.close();
        writer.close();
        dir.close();
    }

    public void testBatchTimeSeriesLookupTimestampTooOld() throws Exception {
        // A UID whose timestamp is older than every segment's minTimestamp is never found.
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        long segmentTs = 1_000L;
        String segmentId = makeTsdbId(1, segmentTs);
        writer.addDocument(makeDocWithTimestamp(segmentId, segmentTs, 5L, 1L, 1L));
        DirectoryReader reader = DirectoryReader.open(writer);

        // ts=1 predates the only segment's minTimestamp=1000 → not found
        String tooOldId = makeTsdbId(2, 1L);
        DocIdAndVersion[] results = batchTimeSeriesLookup(reader, tooOldId);
        assertNull(results[0]);

        reader.close();
        writer.close();
        dir.close();
    }

    public void testLoadTimestampRangeWithDeleteTombstone() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        var randomSeqNoIndexOptions = randomFrom(SeqNoFieldMapper.SeqNoIndexOptions.values());
        writer.addDocument(ParsedDocument.deleteTombstone(randomSeqNoIndexOptions, "_id").docs().getFirst());
        DirectoryReader reader = DirectoryReader.open(writer);
        LeafReaderContext segment = reader.leaves().getFirst();
        PerThreadIDVersionAndSeqNoLookup lookup = new PerThreadIDVersionAndSeqNoLookup(segment.reader(), true);
        assertTrue(lookup.loadedTimestampRange);
        assertEquals(0L, lookup.minTimestamp);
        assertEquals(Long.MAX_VALUE, lookup.maxTimestamp);
        reader.close();
        writer.close();
        dir.close();
    }

    /**
     * The time series batch lookup reads the timestamp straight out of the {@link Uid#encodeId} form of the id
     * rather than base64 decoding it. Exercise that with real encoded uids, including an id whose first byte
     * forces {@link Uid#encodeId} to prepend an escape byte, which shifts every following byte along.
     */
    public void testBatchTimeSeriesLookupReadsTimestampFromEncodedUid() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);

        // 0xFF as the first byte is >= Uid.BASE64_ESCAPE, so this one is stored with a leading escape byte.
        String escapedId = makeTsdbId(0xFF, 2_000L);
        String plainId = makeTsdbId(1, 1_000L);
        assertEquals(21, Uid.encodeId(escapedId).length);
        assertEquals(20, Uid.encodeId(plainId).length);

        writer.addDocument(makeDocWithEncodedId(escapedId, 2_000L, 10L, 1L, 1L));
        writer.addDocument(makeDocWithEncodedId(plainId, 1_000L, 20L, 2L, 1L));
        DirectoryReader reader = DirectoryReader.open(writer);

        String missingId = makeTsdbId(2, 1_500L); // in range for the segment, but not indexed
        String[] ids = { escapedId, plainId, missingId };
        BytesRef[] uids = new BytesRef[ids.length];
        for (int i = 0; i < ids.length; i++) {
            uids[i] = Uid.encodeId(ids[i]);
        }
        DocIdAndVersion[] results = new DocIdAndVersion[ids.length];
        VersionsAndSeqNoResolver.timeSeriesBatchLoadDocIdAndVersion(reader, uids, ids, false, new boolean[ids.length], results);

        assertNotNull(results[0]);
        assertEquals(10L, results[0].version);
        assertNotNull(results[1]);
        assertEquals(20L, results[1].version);
        assertNull(results[2]);

        reader.close();
        writer.close();
        dir.close();
    }

    /**
     * Documents newer than every segment are filtered out before the batch is sorted, which is the fast path for
     * steady time series ingestion. Check that it agrees with the per-document lookup and that a mixed batch
     * still resolves the documents that do fall inside the reader's range.
     */
    public void testBatchTimeSeriesLookupAllNewerThanReader() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);
        String indexedId = makeTsdbId(1, 1_000L);
        writer.addDocument(makeDocWithTimestamp(indexedId, 1_000L, 5L, 1L, 1L));
        DirectoryReader reader = DirectoryReader.open(writer);

        // Every id is newer than the segment's maxTimestamp of 1000.
        DocIdAndVersion[] allNew = batchTimeSeriesLookup(reader, makeTsdbId(2, 1_001L), makeTsdbId(3, 5_000L), makeTsdbId(4, 9_000L));
        for (DocIdAndVersion result : allNew) {
            assertNull(result);
        }

        DocIdAndVersion[] mixed = batchTimeSeriesLookup(reader, makeTsdbId(5, 9_000L), indexedId, makeTsdbId(6, 8_000L));
        assertNull(mixed[0]);
        assertNotNull(mixed[1]);
        assertEquals(5L, mixed[1].version);
        assertNull(mixed[2]);

        reader.close();
        writer.close();
        dir.close();
    }

    /**
     * Batches are compacted in place as segments resolve entries, and the surviving entries have to stay sorted and
     * aligned with the caller's array. Cross-check a randomized batch against the per-document lookups, which take a
     * completely different path through the same segments.
     */
    public void testBatchLookupMatchesSingleDocumentLookup() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);

        // Give every document a distinct version, seqNo and primaryTerm so that reading the wrong doc's doc values
        // cannot go unnoticed.
        final int segments = randomIntBetween(1, 6);
        final List<String> indexed = new ArrayList<>();
        for (int s = 0; s < segments; s++) {
            for (int d = 0; d < randomIntBetween(1, 10); d++) {
                String id = "id-" + randomAlphaOfLength(8);
                indexed.add(id);
                long n = indexed.size();
                writer.addDocument(makeDoc(id, n, 1000 + n, 2000 + n));
            }
            writer.flush();
        }
        DirectoryReader reader = DirectoryReader.open(writer);

        // A mix of indexed and never-indexed ids, in arbitrary order, with duplicates allowed. loadSeqNo varies per
        // entry so that the cached doc values iterators are created lazily part-way through the batch.
        final int lookupCount = randomIntBetween(1, 40);
        final String[] ids = new String[lookupCount];
        final BytesRef[] uids = new BytesRef[lookupCount];
        final boolean[] loadSeqNo = new boolean[lookupCount];
        for (int i = 0; i < lookupCount; i++) {
            ids[i] = randomBoolean() ? randomFrom(indexed) : "missing-" + randomAlphaOfLength(8);
            uids[i] = new BytesRef(ids[i]);
            loadSeqNo[i] = randomBoolean();
        }

        DocIdAndVersion[] batch = new DocIdAndVersion[lookupCount];
        VersionsAndSeqNoResolver.batchLoadDocIdAndVersion(reader, uids, loadSeqNo, batch);
        assertBatchMatchesSingleLookups(ids, batch, i -> VersionsAndSeqNoResolver.loadDocIdAndVersion(reader, uids[i], loadSeqNo[i]));

        reader.close();
        writer.close();
        dir.close();
    }

    /**
     * The time series counterpart of {@link #testBatchLookupMatchesSingleDocumentLookup}, cross-checked against
     * {@link VersionsAndSeqNoResolver#timeSeriesLoadDocIdAndVersion}. Segments are written newest-first so they are
     * already in the descending maxTimestamp order that {@link DataStream#TIMESERIES_LEAF_READERS_SORTER} produces.
     */
    public void testBatchTimeSeriesLookupMatchesSingleDocumentLookup() throws Exception {
        Directory dir = newDirectory();
        IndexWriter writer = newNoMergeWriter(dir);

        final int segments = randomIntBetween(1, 6);
        final List<String> indexed = new ArrayList<>();
        long timestamp = segments * 1000L;
        for (int s = 0; s < segments; s++) {
            for (int d = 0; d < randomIntBetween(1, 10); d++) {
                long docTimestamp = timestamp + randomIntBetween(0, 999);
                String id = makeTsdbId(randomIntBetween(0, 0xFF), docTimestamp);
                indexed.add(id);
                // Distinct version, seqNo and primaryTerm per document, so a doc values misread cannot go unnoticed.
                long n = indexed.size();
                writer.addDocument(makeDocWithEncodedId(id, docTimestamp, n, 1000 + n, 2000 + n));
            }
            writer.flush();
            timestamp -= 1000L;
        }
        DirectoryReader reader = DirectoryReader.open(writer);

        // Mix ids that are indexed, that predate every segment, and that postdate every segment.
        final int lookupCount = randomIntBetween(1, 40);
        final String[] ids = new String[lookupCount];
        for (int i = 0; i < lookupCount; i++) {
            ids[i] = switch (randomIntBetween(0, 2)) {
                case 0 -> randomFrom(indexed);
                case 1 -> makeTsdbId(randomIntBetween(0, 0xFF), randomLongBetween(segments * 1000L + 1000L, Long.MAX_VALUE / 2));
                case 2 -> makeTsdbId(randomIntBetween(0, 0xFF), randomLongBetween(0, 999));
                default -> throw new AssertionError("unreachable");
            };
        }

        final BytesRef[] uids = new BytesRef[lookupCount];
        final boolean[] loadSeqNo = new boolean[lookupCount];
        for (int i = 0; i < lookupCount; i++) {
            uids[i] = Uid.encodeId(ids[i]);
            loadSeqNo[i] = randomBoolean();
        }

        DocIdAndVersion[] batch = new DocIdAndVersion[lookupCount];
        VersionsAndSeqNoResolver.timeSeriesBatchLoadDocIdAndVersion(reader, uids, ids, false, loadSeqNo, batch);
        assertBatchMatchesSingleLookups(
            ids,
            batch,
            i -> VersionsAndSeqNoResolver.timeSeriesLoadDocIdAndVersion(reader, uids[i], ids[i], loadSeqNo[i], false)
        );

        reader.close();
        writer.close();
        dir.close();
    }

    private interface SingleLookup {
        DocIdAndVersion lookup(int i) throws IOException;
    }

    private static void assertBatchMatchesSingleLookups(String[] ids, DocIdAndVersion[] batch, SingleLookup single) throws IOException {
        for (int i = 0; i < ids.length; i++) {
            DocIdAndVersion expected = single.lookup(i);
            if (expected == null) {
                assertNull("expected no result for [" + ids[i] + "]", batch[i]);
                continue;
            }
            assertNotNull("expected a result for [" + ids[i] + "]", batch[i]);
            assertEquals(ids[i], expected.docId, batch[i].docId);
            assertEquals(ids[i], expected.docBase, batch[i].docBase);
            assertEquals(ids[i], expected.version, batch[i].version);
            assertEquals(ids[i], expected.seqNo, batch[i].seqNo);
            assertEquals(ids[i], expected.primaryTerm, batch[i].primaryTerm);
        }
    }

    private static IndexWriter newNoMergeWriter(Directory dir) throws IOException {
        return new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER).setMergePolicy(NoMergePolicy.INSTANCE));
    }

    private static DocIdAndVersion[] batchLookup(DirectoryReader reader, String... ids) throws IOException {
        BytesRef[] uids = new BytesRef[ids.length];
        for (int i = 0; i < ids.length; i++) {
            uids[i] = new BytesRef(ids[i]);
        }
        DocIdAndVersion[] results = new DocIdAndVersion[ids.length];
        VersionsAndSeqNoResolver.batchLoadDocIdAndVersion(reader, uids, new boolean[ids.length], results);
        return results;
    }

    private static DocIdAndVersion[] batchTimeSeriesLookup(DirectoryReader reader, String... ids) throws IOException {
        BytesRef[] uids = new BytesRef[ids.length];
        for (int i = 0; i < ids.length; i++) {
            uids[i] = new BytesRef(ids[i]);
        }
        DocIdAndVersion[] results = new DocIdAndVersion[ids.length];
        VersionsAndSeqNoResolver.timeSeriesBatchLoadDocIdAndVersion(reader, uids, ids, false, new boolean[ids.length], results);
        return results;
    }

    /**
     * Builds a base64-URL-encoded 20-byte TSDB-style document ID with the given timestamp.
     * The timestamp is stored big-endian at bytes 12-19, matching the non-synthetic TSDB ID
     * format read by {@link org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper#extractTimestampFromId}.
     */
    private static String makeTsdbId(int discriminator, long timestamp) {
        byte[] idBytes = new byte[20];
        idBytes[0] = (byte) discriminator;
        ByteBuffer.wrap(idBytes).putLong(12, timestamp);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(idBytes);
    }

    private static Document makeDoc(String id, long version, long seqNo, long primaryTerm) {
        Document doc = new Document();
        doc.add(new StringField(IdFieldMapper.NAME, id, Field.Store.YES));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, version));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, seqNo));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, primaryTerm));
        return doc;
    }

    private static Document makeDocWithTimestamp(String id, long timestamp, long version, long seqNo, long primaryTerm) {
        Document doc = new Document();
        doc.add(new StringField(IdFieldMapper.NAME, id, Field.Store.YES));
        doc.add(new LongPoint(DataStream.TIMESTAMP_FIELD_NAME, timestamp));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, version));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, seqNo));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, primaryTerm));
        return doc;
    }

    /**
     * As {@link #makeDocWithTimestamp}, but indexing the {@link Uid#encodeId} form of the id the way the real
     * indexing path does, so that lookups can be driven with encoded uids.
     */
    private static Document makeDocWithEncodedId(String id, long timestamp, long version, long seqNo, long primaryTerm) {
        Document doc = new Document();
        doc.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(id), Field.Store.YES));
        doc.add(new LongPoint(DataStream.TIMESTAMP_FIELD_NAME, timestamp));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, version));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, seqNo));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, primaryTerm));
        return doc;
    }
}
