/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.util;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.xcontent.Text;

import java.util.Map;

/**
 * A cache in front of Java's string interning. This method assumes that it is only called with strings that are already part of the
 * JVM's string pool so that interning them does not grow the pool. Calling it with strings not in the interned string pool is not
 * advisable as its performance may deteriorate to slower than outright calls to {@link String#intern()}.
 */
public final class UTF8StringLiteralDeduplicator {

    private static final int MAX_SIZE = 1000;

    private final Map<String, Text> map = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    public UTF8StringLiteralDeduplicator() {}

    public Text deduplicate(String string) {
        final Text res = map.get(string);
        if (res != null) {
            return res;
        }
        final String interned = string.intern();
        if (map.size() > MAX_SIZE) {
            map.clear();
        }
        Text value = new Text(interned);
        // Realize the bytes
        value.bytes();
        map.put(interned, value);
        return value;
    }
}
