/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A @link {@link StreamOutput} that uses {@link Recycler.V<BytesRef>} to acquire pages of bytes, which
 * avoids frequent reallocation &amp; copying of the internal data. When {@link #close()} is called,
 * the bytes will be released.
 */
public class RecyclerBytesStreamOutput extends BytesStream implements Releasable {

    static final VarHandle VH_BE_INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    static final VarHandle VH_BE_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    private final ArrayList<Recycler.V<BytesRef>> pages = new ArrayList<>();
    private final Recycler<BytesRef> recycler;
    private final int pageSize;
    private int pageIndex = -1;
    private int currentCapacity = 0;
    private int currentPageOffset;

    public RecyclerBytesStreamOutput(Recycler<BytesRef> recycler) {
        this.recycler = recycler;
        try (Recycler.V<BytesRef> obtain = recycler.obtain()) {
            pageSize = obtain.v().length;
        }
        this.currentPageOffset = pageSize;
    }

    @Override
    public long position() {
        return ((long) pageSize * pageIndex) + currentPageOffset;
    }

    @Override
    public void writeByte(byte b) {
        ensureCapacity(1);
        BytesRef currentPage = pages.get(pageIndex).v();
        currentPage.bytes[currentPage.offset + currentPageOffset] = b;
        currentPageOffset++;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) {
        // nothing to copy
        if (length == 0) {
            return;
        }

        // illegal args: offset and/or length exceed array size
        if (b.length < (offset + length)) {
            throw new IllegalArgumentException("Illegal offset " + offset + "/length " + length + " for byte[] of length " + b.length);
        }

        // get enough pages for new size
        ensureCapacity(length);

        // bulk copy
        int bytesToCopy = length;
        int srcOff = offset;
        int j = 0;
        while (true) {
            BytesRef currentPage = pages.get(pageIndex + j).v();
            int toCopyThisLoop = Math.min(pageSize - currentPageOffset, bytesToCopy);
            System.arraycopy(b, srcOff, currentPage.bytes, currentPage.offset + currentPageOffset, toCopyThisLoop);
            srcOff += toCopyThisLoop;
            bytesToCopy -= toCopyThisLoop;
            if (bytesToCopy > 0) {
                currentPageOffset = 0;
            } else {
                currentPageOffset += toCopyThisLoop;
                break;
            }
            j++;
        }

        // advance
        pageIndex += j;
    }

    @Override
    public void writeInt(int i) throws IOException {
        if (4 > (pageSize - currentPageOffset)) {
            super.writeInt(i);
        } else {
            BytesRef currentPage = pages.get(pageIndex).v();
            VH_BE_INT.set(currentPage.bytes, currentPage.offset + currentPageOffset, i);
            currentPageOffset += 4;
        }
    }

    @Override
    public void writeLong(long i) throws IOException {
        if (8 > (pageSize - currentPageOffset)) {
            super.writeLong(i);
        } else {
            BytesRef currentPage = pages.get(pageIndex).v();
            VH_BE_LONG.set(currentPage.bytes, currentPage.offset + currentPageOffset, i);
            currentPageOffset += 8;
        }
    }

    @Override
    public void reset() {
        Releasables.close(pages);
        pages.clear();
        pageIndex = -1;
        currentPageOffset = pageSize;
    }

    @Override
    public void flush() {
        // nothing to do
    }

    @Override
    public void seek(long position) {
        ensureCapacityFromPosition(position);
        this.pageIndex = (int) position / pageSize;
        this.currentPageOffset = (int) position % pageSize;
    }

    public void skip(int length) {
        seek(position() + length);
    }

    @Override
    public void close() {
        try {
            Releasables.close(pages);
        } finally {
            pages.clear();
        }
    }

    /**
     * Returns the current size of the buffer.
     *
     * @return the value of the <code>count</code> field, which is the number of valid
     *         bytes in this output stream.
     * @see ByteArrayOutputStream#size()
     */
    public int size() {
        return Math.toIntExact(position());
    }

    @Override
    public BytesReference bytes() {
        int position = (int) position();
        if (position == 0) {
            return BytesArray.EMPTY;
        } else {
            final int adjustment;
            final int bytesInLastPage;
            final int remainder = position % pageSize;
            if (remainder != 0) {
                adjustment = 1;
                bytesInLastPage = remainder;
            } else {
                adjustment = 0;
                bytesInLastPage = pageSize;
            }
            final int pageCount = (position / pageSize) + adjustment;
            if (pageCount == 1) {
                BytesRef page = pages.get(0).v();
                return new BytesArray(page.bytes, page.offset, bytesInLastPage);
            } else {
                BytesReference[] references = new BytesReference[pageCount];
                for (int i = 0; i < pageCount - 1; ++i) {
                    references[i] = new BytesArray(this.pages.get(i).v());
                }
                BytesRef last = this.pages.get(pageCount - 1).v();
                references[pageCount - 1] = new BytesArray(last.bytes, last.offset, bytesInLastPage);
                return CompositeBytesReference.of(references);
            }
        }
    }

    public List<ReleasableBytesReference> retainAndGetPages() {
        int position = (int) position();
        if (position == 0) {
            return Collections.emptyList();
        } else {
            final int adjustment;
            final int bytesInLastPage;
            final int remainder = position % pageSize;
            if (remainder != 0) {
                adjustment = 1;
                bytesInLastPage = remainder;
            } else {
                adjustment = 0;
                bytesInLastPage = pageSize;
            }
            final int pageCount = (position / pageSize) + adjustment;
            if (pageCount == 1) {
                BytesRef page = pages.get(0).v();
                return Collections.singletonList(ReleasableBytesReference.wrap(new BytesArray(page.bytes, page.offset, bytesInLastPage)));
            } else {
                ReleasableBytesReference[] references = new ReleasableBytesReference[pageCount];
                for (int i = 0; i < pageCount - 1; ++i) {
                    references[i] = ReleasableBytesReference.wrap(new BytesArray(this.pages.get(i).v()));
                }
                BytesRef last = this.pages.get(pageCount - 1).v();
                references[pageCount - 1] = ReleasableBytesReference.wrap(new BytesArray(last.bytes, last.offset, bytesInLastPage));
                return Arrays.asList(references);
            }
        }
    }

    private void ensureCapacity(int bytesNeeded) {
        if (bytesNeeded > pageSize - currentPageOffset) {
            ensureCapacityFromPosition(position() + bytesNeeded);
        }
    }

    private void ensureCapacityFromPosition(long newPosition) {
        while (newPosition > currentCapacity) {
            if (newPosition > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(getClass().getSimpleName() + " cannot hold more than 2GB of data");
            }
            Recycler.V<BytesRef> newPage = recycler.obtain();
            assert pageSize == newPage.v().length;
            pages.add(newPage);
            // We are at the end of the current page, increment page index
            if (currentPageOffset == pageSize) {
                pageIndex++;
                currentPageOffset = 0;
            }
            currentCapacity += pageSize;
        }
    }
}
