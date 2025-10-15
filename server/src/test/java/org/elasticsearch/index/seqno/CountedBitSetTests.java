/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.seqno;

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class CountedBitSetTests extends ESTestCase {

    public void testCompareToFixedBitset() {
        int numBits = (short) randomIntBetween(8, 4096);
        final FixedBitSet fixedBitSet = new FixedBitSet(numBits);
        final CountedBitSet countedBitSet = new CountedBitSet((short) numBits);

        for (int i = 0; i < numBits; i++) {
            if (randomBoolean()) {
                fixedBitSet.set(i);
                countedBitSet.set(i);
            }
            assertThat(countedBitSet.cardinality(), equalTo(fixedBitSet.cardinality()));
            assertThat(countedBitSet.length(), equalTo(fixedBitSet.length()));
        }

        for (int i = 0; i < numBits; i++) {
            assertThat(countedBitSet.get(i), equalTo(fixedBitSet.get(i)));
        }
    }

    public void testReleaseInternalBitSet() {
        int numBits = (short) randomIntBetween(8, 4096);
        final CountedBitSet countedBitSet = new CountedBitSet((short) numBits);
        final List<Integer> values = IntStream.range(0, numBits).boxed().toList();

        for (int i = 1; i < numBits; i++) {
            final int value = values.get(i);
            assertThat(countedBitSet.get(value), equalTo(false));
            assertThat(countedBitSet.isInternalBitsetReleased(), equalTo(false));

            countedBitSet.set(value);

            assertThat(countedBitSet.get(value), equalTo(true));
            assertThat(countedBitSet.isInternalBitsetReleased(), equalTo(false));
            assertThat(countedBitSet.length(), equalTo(numBits));
            assertThat(countedBitSet.cardinality(), equalTo(i));
        }

        // The missing piece to fill all bits.
        {
            final int value = values.get(0);
            assertThat(countedBitSet.get(value), equalTo(false));
            assertThat(countedBitSet.isInternalBitsetReleased(), equalTo(false));

            countedBitSet.set(value);

            assertThat(countedBitSet.get(value), equalTo(true));
            assertThat(countedBitSet.isInternalBitsetReleased(), equalTo(true));
            assertThat(countedBitSet.length(), equalTo(numBits));
            assertThat(countedBitSet.cardinality(), equalTo(numBits));
        }

        // Tests with released internal bitset.
        final int iterations = iterations(1000, 10000);
        for (int i = 0; i < iterations; i++) {
            final int value = randomInt(numBits - 1);
            assertThat(countedBitSet.get(value), equalTo(true));
            assertThat(countedBitSet.isInternalBitsetReleased(), equalTo(true));
            assertThat(countedBitSet.length(), equalTo(numBits));
            assertThat(countedBitSet.cardinality(), equalTo(numBits));
            if (frequently()) {
                assertThat(countedBitSet.get(value), equalTo(true));
            }
        }
    }

    public void testConsecutiveSetBits() {
        int numBits = (short) randomIntBetween(128, 256);
        final CountedBitSet countedBitSet = new CountedBitSet((short) numBits);

        // Empty bitset
        assertThat(countedBitSet.consecutiveSetBits(0), equalTo(0));
        assertThat(countedBitSet.consecutiveSetBits(randomIntBetween(0, numBits - 1)), equalTo(0));

        // Single bit
        countedBitSet.set(5);
        assertThat(countedBitSet.consecutiveSetBits(5), equalTo(1));
        assertThat(countedBitSet.consecutiveSetBits(4), equalTo(0));
        assertThat(countedBitSet.consecutiveSetBits(6), equalTo(0));

        // Range of bits
        for (int i = 10; i < 20; i++) {
            countedBitSet.set(i);
        }
        assertThat(countedBitSet.consecutiveSetBits(10), equalTo(10));
        assertThat(countedBitSet.consecutiveSetBits(15), equalTo(5));
        assertThat(countedBitSet.consecutiveSetBits(19), equalTo(1));
        assertThat(countedBitSet.consecutiveSetBits(9), equalTo(0));
        assertThat(countedBitSet.consecutiveSetBits(20), equalTo(0));

        // Set bits that span multiple words (64-bit boundaries)
        for (int i = 60; i < 130; i++) {
            countedBitSet.set(i);
        }
        assertThat(countedBitSet.consecutiveSetBits(60), equalTo(70));
        assertThat(countedBitSet.consecutiveSetBits(63), equalTo(67));
        assertThat(countedBitSet.consecutiveSetBits(64), equalTo(66));
        assertThat(countedBitSet.consecutiveSetBits(100), equalTo(30));
    }

    public void testConsecutiveSetBitsWhenAllBitsSet() {
        int numBits = (short) randomIntBetween(64, 256);
        final CountedBitSet countedBitSet = new CountedBitSet((short) numBits);

        // Set all bits
        for (int i = 0; i < numBits; i++) {
            countedBitSet.set(i);
        }

        assertTrue(countedBitSet.isInternalBitsetReleased());

        assertThat(countedBitSet.consecutiveSetBits(0), equalTo(numBits));
        assertThat(countedBitSet.consecutiveSetBits(numBits / 2), equalTo(numBits / 2));
        assertThat(countedBitSet.consecutiveSetBits(numBits - 1), equalTo(1));
    }

    public void testConsecutiveSetBitsAtWordBoundaries() {
        int numBits = 256;
        final CountedBitSet countedBitSet = new CountedBitSet((short) numBits);

        // Starting exactly at word boundary
        for (int i = 64; i < 128; i++) {
            countedBitSet.set(i);
        }
        assertThat(countedBitSet.consecutiveSetBits(64), equalTo(64));

        // Starting exactly at word boundary (index 128)
        for (int i = 128; i < 192; i++) {
            countedBitSet.set(i);
        }
        assertThat(countedBitSet.consecutiveSetBits(128), equalTo(64));

        // Consecutive bits spanning word boundaries
        for (int i = 64; i < 192; i++) {
            countedBitSet.set(i);
        }
        assertThat(countedBitSet.consecutiveSetBits(64), equalTo(128));
    }

    public void testConsecutiveSetBitsWithGaps() {
        int numBits = 200;
        final CountedBitSet countedBitSet = new CountedBitSet((short) numBits);

        // Set bits with gaps
        for (int i = 0; i < 50; i++) {
            countedBitSet.set(i);
        }
        // Gap from 50-59
        for (int i = 60; i < 100; i++) {
            countedBitSet.set(i);
        }

        assertThat(countedBitSet.consecutiveSetBits(0), equalTo(50));
        assertThat(countedBitSet.consecutiveSetBits(49), equalTo(1));
        assertThat(countedBitSet.consecutiveSetBits(50), equalTo(0));
        assertThat(countedBitSet.consecutiveSetBits(60), equalTo(40));
    }

    public void testConsecutiveSetBitsAtEndOfBitset() {
        int numBits = 100;
        final CountedBitSet countedBitSet = new CountedBitSet((short) numBits);

        // Set bits at 90 to end
        for (int i = 90; i < numBits; i++) {
            countedBitSet.set(i);
        }

        assertThat(countedBitSet.consecutiveSetBits(90), equalTo(10));
        assertThat(countedBitSet.consecutiveSetBits(95), equalTo(5));
        assertThat(countedBitSet.consecutiveSetBits(99), equalTo(1));
    }
}
