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

import java.util.Optional;

/**
 * Bootstrap class that wires a read-edge from this module to {@code jdk.incubator.vector}
 * before any vector class is loaded. Must be initialised before {@link VectorUtils} to avoid
 * the class-init ordering trap described in
 * {@code org.elasticsearch.simdvec.internal.vectorization.JdkFeatures}.
 *
 * <p>This class <em>must not</em> import any {@code jdk.incubator.vector} type — doing so would
 * cause the JVM to resolve {@code jdk.incubator.vector} during this class's own loading, which
 * would fail if the read-edge has not yet been added.
 */
public final class SimdJsonSupport {

    static final boolean VECTOR_AVAILABLE;

    static {
        Optional<Module> vec = Optional.ofNullable(SimdJsonSupport.class.getModule().getLayer())
            .orElse(ModuleLayer.boot())
            .findModule("jdk.incubator.vector");
        if (vec.isPresent()) {
            SimdJsonSupport.class.getModule().addReads(vec.get());
            VECTOR_AVAILABLE = true;
        } else {
            VECTOR_AVAILABLE = false;
        }
    }

    private SimdJsonSupport() {}

    /** Returns {@code true} if {@code jdk.incubator.vector} is available at runtime. */
    public static boolean isAvailable() {
        return VECTOR_AVAILABLE;
    }
}
