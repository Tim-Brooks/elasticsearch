/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Vendored DOM/tape JSON parser adapted from
 * <a href="https://github.com/simdjson/simdjson-java">simdjson-java</a>, with an
 * additional 128-bit NEON kernel for AWS Graviton2 / Neoverse N1 hosts.
 *
 * <p>The entry point is {@link org.elasticsearch.sourcebatch.simdjson.SimdJsonParser}.
 * See {@link org.elasticsearch.sourcebatch.simdjson.SimdJsonSupport} for runtime
 * module-graph setup (the {@code jdk.incubator.vector} read-edge must be added
 * before any vector class is loaded).
 */
module org.elasticsearch.simdjson {
    requires org.elasticsearch.xcontent;

    exports org.elasticsearch.sourcebatch.simdjson to org.elasticsearch.server;
}
