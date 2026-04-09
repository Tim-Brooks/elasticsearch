/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import java.io.IOException;

/**
 * Provides positional access to decoded numeric values in a doc values field.
 * Implementations decode blocks on demand and cache the current block.
 */
@FunctionalInterface
interface NumericValues {
    long advance(long index) throws IOException;
}
