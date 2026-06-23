/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.PageCacheRecycler;

public class BytesRefRecycler implements Recycler<BytesRef> {

    /**
     * A non-recycling {@link BytesRefRecycler} which simply allocates a fresh 16kiB {@code byte[]} each time. This is only really
     * appropriate for use in tests.
     */
    // TODO move to test framework?
    public static final BytesRefRecycler NON_RECYCLING_INSTANCE = new BytesRefRecycler(PageCacheRecycler.NON_RECYCLING_INSTANCE);

    /**
     * A non-recycling {@link Recycler} which allocates a fresh 8kiB {@code byte[]} on every {@link #obtain()}, bypassing
     * {@link PageCacheRecycler} (whose page size is fixed at 16kiB). This exists purely to experiment with the effect of a smaller
     * page size on the allocation/zeroing overhead seen when constructing many short-lived stream outputs; it is not intended for
     * production use.
     */
    public static final Recycler<BytesRef> NON_RECYCLING_8K_INSTANCE = new Recycler<>() {
        private static final int PAGE_SIZE = 1 << 13; // 8kiB

        @Override
        public Recycler.V<BytesRef> obtain() {
            BytesRef bytesRef = new BytesRef(new byte[PAGE_SIZE], 0, PAGE_SIZE);
            return new Recycler.V<>() {
                @Override
                public BytesRef v() {
                    return bytesRef;
                }

                @Override
                public boolean isRecycled() {
                    return false;
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public int pageSize() {
            return PAGE_SIZE;
        }
    };

    private final PageCacheRecycler recycler;

    public BytesRefRecycler(PageCacheRecycler recycler) {
        this.recycler = recycler;
    }

    @Override
    public Recycler.V<BytesRef> obtain() {
        Recycler.V<byte[]> v = recycler.bytePage(false);
        BytesRef bytesRef = new BytesRef(v.v(), 0, PageCacheRecycler.BYTE_PAGE_SIZE);
        return new Recycler.V<>() {
            @Override
            public BytesRef v() {
                return bytesRef;
            }

            @Override
            public boolean isRecycled() {
                return v.isRecycled();
            }

            @Override
            public void close() {
                v.close();
            }
        };
    }

    @Override
    public int pageSize() {
        return PageCacheRecycler.BYTE_PAGE_SIZE;
    }
}
