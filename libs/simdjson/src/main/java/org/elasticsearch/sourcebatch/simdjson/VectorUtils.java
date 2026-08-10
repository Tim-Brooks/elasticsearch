/*
 * @notice
 *
 * Copyright 2021-2024 The simdjson-java contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Based on a modification of https://github.com/simdjson/simdjson-java,
 * licensed under the Apache License 2.0.
 */

package org.elasticsearch.sourcebatch.simdjson;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

/**
 * Shared vector species constants for the vendored simdjson implementation.
 *
 * <p>The species is selected via the {@code es.simdjson.species} system property:
 * <ul>
 *   <li>{@code preferred} (default) — use the JVM-preferred width; must be 128/256/512-bit.
 *   <li>{@code 128} — force 128-bit (NEON on AArch64, SSE2 on x86).
 *   <li>{@code 256} — force 256-bit (AVX2 on x86).
 *   <li>{@code 512} — force 512-bit (AVX-512 on x86).
 * </ul>
 *
 * <p>Sanity-check: after construction, log {@code BYTE_SPECIES.vectorBitSize()} and confirm it
 * matches the hardware. A mismatched width means SIMD ops are not intrinsified and the benchmark
 * is measuring scalar Java, not SIMD.
 */
class VectorUtils {

    static final VectorSpecies<Integer> INT_SPECIES;
    static final VectorSpecies<Byte> BYTE_SPECIES;

    static {
        String species = System.getProperty("es.simdjson.species", "preferred");
        switch (species) {
            case "preferred" -> {
                BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;
                INT_SPECIES = IntVector.SPECIES_PREFERRED;
                assertSupportForSpecies(BYTE_SPECIES);
                assertSupportForSpecies(INT_SPECIES);
            }
            case "512" -> {
                BYTE_SPECIES = ByteVector.SPECIES_512;
                INT_SPECIES = IntVector.SPECIES_512;
            }
            case "256" -> {
                BYTE_SPECIES = ByteVector.SPECIES_256;
                INT_SPECIES = IntVector.SPECIES_256;
            }
            case "128" -> {
                BYTE_SPECIES = ByteVector.SPECIES_128;
                INT_SPECIES = IntVector.SPECIES_128;
            }
            default -> throw new IllegalArgumentException("Unsupported vector species: " + species);
        }
    }

    private static void assertSupportForSpecies(VectorSpecies<?> species) {
        VectorShape shape = species.vectorShape();
        if (shape != VectorShape.S_128_BIT && shape != VectorShape.S_256_BIT && shape != VectorShape.S_512_BIT) {
            throw new IllegalArgumentException("Unsupported vector species: " + species);
        }
    }

    static ByteVector repeat(byte[] array) {
        int n = BYTE_SPECIES.vectorByteSize() / 4;
        byte[] result = new byte[n * array.length];
        for (int dst = 0; dst < result.length; dst += array.length) {
            System.arraycopy(array, 0, result, dst, array.length);
        }
        return ByteVector.fromArray(BYTE_SPECIES, result, 0);
    }
}
