/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.compress;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.FutureObjects;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;

public class EsLZ4 {

    private EsLZ4() {}

    static final int MEMORY_USAGE = 14;
    static final int MIN_MATCH = 4; // minimum length of a match
    static final int MAX_DISTANCE = 1 << 16; // maximum distance of a reference
    static final int LAST_LITERALS = 5; // the last 5 bytes must be encoded as literals
    static final int HASH_LOG_HC = 15; // log size of the dictionary for compressHC
    static final int HASH_TABLE_SIZE_HC = 1 << HASH_LOG_HC;


    private static int hash(int i, int hashBits) {
        return (i * -1640531535) >>> (32 - hashBits);
    }

    private static int hashHC(int i) {
        return hash(i, HASH_LOG_HC);
    }

    private static int readInt(byte[] buf, int i) {
        return ((buf[i] & 0xFF) << 24) | ((buf[i+1] & 0xFF) << 16) | ((buf[i+2] & 0xFF) << 8) | (buf[i+3] & 0xFF);
    }

    private static int commonBytes(byte[] b, int o1, int o2, int limit) {
        assert o1 < o2;
        // never -1 because lengths always differ
        return FutureArrays.mismatch(b, o1, limit, b, o2, limit);
    }

    /**
     * Decompress at least {@code decompressedLen} bytes into
     * {@code dest[dOff:]}. Please note that {@code dest} must be large
     * enough to be able to hold <b>all</b> decompressed data (meaning that you
     * need to know the total decompressed length).
     * If the given bytes were compressed using a preset dictionary then the same
     * dictionary must be provided in {@code dest[dOff-dictLen:dOff]}.
     */
    public static int decompress(byte[] compressed, int decompressedLen, byte[] dest, int dOff) throws IOException {
        int cOff = 0;
        final int destEnd = dOff + decompressedLen;

        do {
            // literals
            final int token = compressed[cOff++] & 0xFF;
            int literalLen = token >>> 4;

            if (literalLen != 0) {
                if (literalLen == 0x0F) {
                    byte len;
                    while ((len = compressed[cOff++]) == (byte) 0xFF) {
                        literalLen += 0xFF;
                    }
                    literalLen += len & 0xFF;
                }
                System.arraycopy(compressed, cOff, dest, dOff, literalLen);
                cOff += literalLen;
                dOff += literalLen;
            }

            if (dOff >= destEnd) {
                break;
            }

            // matchs
            final int matchDec = (compressed[cOff++] & 0xFF) | ((compressed[cOff++] & 0xFF) << 8);
            assert matchDec > 0;

            int matchLen = token & 0x0F;
            if (matchLen == 0x0F) {
                int len;
                while ((len = compressed[cOff++]) == (byte) 0xFF) {
                    matchLen += 0xFF;
                }
                matchLen += len & 0xFF;
            }
            matchLen += MIN_MATCH;

            // copying a multiple of 8 bytes can make decompression from 5% to 10% faster
            final int fastLen = (matchLen + 7) & 0xFFFFFFF8;
            if (matchDec < matchLen || dOff + fastLen > destEnd) {
                // overlap -> naive incremental copy
                for (int ref = dOff - matchDec, end = dOff + matchLen; dOff < end; ++ref, ++dOff) {
                    dest[dOff] = dest[ref];
                }
            } else {
                // no overlap -> arraycopy
                System.arraycopy(dest, dOff - matchDec, dest, dOff, fastLen);
                dOff += matchLen;
            }
        } while (dOff < destEnd);

        return dOff;
    }

    private static int encodeLen(int l, DataOutput out) throws IOException {
        int written = 0;
        while (l >= 0xFF) {
            written++;
            out.writeByte((byte) 0xFF);
            l -= 0xFF;
        }
        written++;
        out.writeByte((byte) l);
        return written;
    }

    private static int encodeLiterals(byte[] bytes, int token, int anchor, int literalLen, DataOutput out, int cOff) throws IOException {
        out.writeByte((byte) token);
        int written = 1;

        // encode literal length
        if (literalLen >= 0x0F) {
            written += encodeLen(literalLen - 0x0F, out);
        }

        // encode literals
        out.writeBytes(bytes, anchor, literalLen);
        written += literalLen;

        return written;
    }

    private static void encodeLastLiterals(byte[] bytes, int anchor, int literalLen, DataOutput out, int cOff) throws IOException {
        final int token = Math.min(literalLen, 0x0F) << 4;
        encodeLiterals(bytes, token, anchor, literalLen, out, cOff);
    }

    private static int encodeSequence(byte[] bytes, int anchor, int matchRef, int matchOff, int matchLen, DataOutput out, int cOff)
        throws IOException {
        int cWritten = 0;

        final int literalLen = matchOff - anchor;
        assert matchLen >= 4;
        // encode token
        final int token = (Math.min(literalLen, 0x0F) << 4) | Math.min(matchLen - 4, 0x0F);
        cWritten += encodeLiterals(bytes, token, anchor, literalLen, out, cOff);

        // encode match dec
        final int matchDec = matchOff - matchRef;
        assert matchDec > 0 && matchDec < 1 << 16;
        out.writeByte((byte) matchDec);
        out.writeByte((byte) (matchDec >>> 8));
        cWritten += 2;

        // encode match len
        if (matchLen >= MIN_MATCH + 0x0F) {
            cWritten += encodeLen(matchLen - 0x0F - MIN_MATCH, out);
        }

        return cWritten;
    }

    /**
     * A record of previous occurrences of sequences of 4 bytes.
     */
    static abstract class HashTable {

        /** Reset this hash table in order to compress the given content. */
        abstract void reset(byte[] b, int off, int len);

        /** Init {@code dictLen} bytes to be used as a dictionary. */
        abstract void initDictionary(int dictLen);

        /**
         * Advance the cursor to {@off} and return an index that stored the same
         * 4 bytes as {@code b[o:o+4)}. This may only be called on strictly
         * increasing sequences of offsets. A return value of {@code -1} indicates
         * that no other index could be found. */
        abstract int get(int off);

        /**
         * Return an index that less than {@code off} and stores the same 4
         * bytes. Unlike {@link #get}, it doesn't need to be called on increasing
         * offsets. A return value of {@code -1} indicates that no other index could
         * be found. */
        abstract int previous(int off);

        // For testing
        abstract boolean assertReset();
    }

    /**
     * Simple lossy {@link EsLZ4.HashTable} that only stores the last ocurrence for
     * each hash on {@code 2^14} bytes of memory.
     */
    public static final class FastCompressionHashTable extends EsLZ4.HashTable {

        private byte[] bytes;
        private int base;
        private int lastOff;
        private int end;
        private int hashLog;
        private PackedInts.Mutable hashTable;

        /** Sole constructor */
        public FastCompressionHashTable() {}

        @Override
        void reset(byte[] bytes, int off, int len) {
            FutureObjects.checkFromIndexSize(off, len, bytes.length);
            this.bytes = bytes;
            this.base = off;
            this.end = off + len;
            final int bitsPerOffset = PackedInts.bitsRequired(len - LAST_LITERALS);
            final int bitsPerOffsetLog = 32 - Integer.numberOfLeadingZeros(bitsPerOffset - 1);
            hashLog = MEMORY_USAGE + 3 - bitsPerOffsetLog;
            if (hashTable == null || hashTable.size() < 1 << hashLog || hashTable.getBitsPerValue() < bitsPerOffset) {
                hashTable = PackedInts.getMutable(1 << hashLog, bitsPerOffset, PackedInts.DEFAULT);
            } else {
                // Avoid calling hashTable.clear(), this makes it costly to compress many short sequences otherwise.
                // Instead, get() checks that references are less than the current offset.
            }
            this.lastOff = off - 1;
        }

        @Override
        void initDictionary(int dictLen) {
            for (int i = 0; i < dictLen; ++i) {
                final int v = readInt(bytes, base + i);
                final int h = hash(v, hashLog);
                hashTable.set(h, i);
            }
            lastOff += dictLen;
        }

        @Override
        int get(int off) {
            assert off > lastOff;
            assert off < end;

            final int v = readInt(bytes, off);
            final int h = hash(v, hashLog);

            final int ref = base + (int) hashTable.get(h);
            hashTable.set(h, off - base);
            lastOff = off;

            if (ref < off && off - ref < MAX_DISTANCE && readInt(bytes, ref) == v) {
                return ref;
            } else {
                return -1;
            }
        }

        @Override
        public int previous(int off) {
            return -1;
        }

        @Override
        boolean assertReset() {
            return true;
        }

    }

    /**
     * Compress {@code bytes[off:off+len]} into {@code out} using at most 16kB of
     * memory. {@code ht} shouldn't be shared across threads but can safely be
     * reused.
     */
    public static void compress(byte[] bytes, int off, int len, byte[] out, EsLZ4.HashTable ht) throws IOException {
        compressWithDictionary(bytes, off, 0, len, out, ht);
    }

    /**
     * Compress {@code bytes[dictOff+dictLen:dictOff+dictLen+len]} into
     * {@code out} using at most 16kB of memory.
     * {@code bytes[dictOff:dictOff+dictLen]} will be used as a dictionary.
     * {@code dictLen} must not be greater than 64kB, the maximum window size.
     *
     * {@code ht} shouldn't be shared across threads but can safely be reused.
     */
    public static void compressWithDictionary(byte[] bytes, int dictOff, int dictLen, int len, byte[] out, EsLZ4.HashTable ht) throws IOException {
        FutureObjects.checkFromIndexSize(dictOff, dictLen, bytes.length);
        FutureObjects.checkFromIndexSize(dictOff + dictLen, len, bytes.length);
        if (dictLen > MAX_DISTANCE) {
            throw new IllegalArgumentException("dictLen must not be greater than 64kB, but got " + dictLen);
        }

        int cOff = 0;
        final int end = dictOff + dictLen + len;

        int off = dictOff + dictLen;
        int anchor = off;

        if (len > LAST_LITERALS + MIN_MATCH) {

            final int limit = end - LAST_LITERALS;
            final int matchLimit = limit - MIN_MATCH;
            ht.reset(bytes, dictOff, dictLen + len);
            ht.initDictionary(dictLen);

            main:
            while (off <= limit) {
                // find a match
                int ref;
                while (true) {
                    if (off >= matchLimit) {
                        break main;
                    }
                    ref = ht.get(off);
                    if (ref != -1) {
                        assert ref >= dictOff && ref < off;
                        assert readInt(bytes, ref) == readInt(bytes, off);
                        break;
                    }
                    ++off;
                }

                // compute match length
                int matchLen = MIN_MATCH + commonBytes(bytes, ref + MIN_MATCH, off + MIN_MATCH, limit);

                // try to find a better match
                for (int r = ht.previous(ref), min = Math.max(off - MAX_DISTANCE + 1, dictOff); r >= min; r = ht.previous(r)) {
                    assert readInt(bytes, r) == readInt(bytes, off);
                    int rMatchLen = MIN_MATCH + commonBytes(bytes, r + MIN_MATCH, off + MIN_MATCH, limit);
                    if (rMatchLen > matchLen) {
                        ref = r;
                        matchLen = rMatchLen;
                    }
                }

                // TODO
//                encodeSequence(bytes, anchor, ref, off, matchLen, out, cOff);
                off += matchLen;
                anchor = off;
            }
        }

        // last literals
        final int literalLen = end - anchor;
        assert literalLen >= LAST_LITERALS || literalLen == len;
        // TODO
//        encodeLastLiterals(bytes, anchor, end - anchor, out, cOff);
    }
}
