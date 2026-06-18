/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eicf;

import org.elasticsearch.common.bytes.BytesReference;

/**
 * The serialized form of a single EICF column, held as up to four independent fields rather than a
 * single pre-concatenated blob. A column carries:
 * <ul>
 *   <li>{@code absentBitset} — LE-long bitset, bit set = absent; {@code null} when no document is
 *       absent.</li>
 *   <li>{@code typeVector} — one {@link org.elasticsearch.eirf.EirfType} byte per document;
 *       {@code null} for kinds whose per-document type is implied by {@link #kind} (LONG, DOUBLE,
 *       BOOL, STRING, BINARY).</li>
 *   <li>{@code offsets} — {@code (docCount + 1)} little-endian {@code i32} byte offsets into
 *       {@code data}; {@code null} for fixed-width kinds (LONG, DOUBLE) and BOOL.</li>
 *   <li>{@code data} — the value payload; never {@code null}, but may be empty (e.g. a BOOL
 *       column's value bitset, or an all-absent fixed column).</li>
 * </ul>
 *
 * <p>This holder performs no concatenation: an in-memory {@link EicfBatch} reads directly from
 * these fields, and they are joined into a single {@link BytesReference} only when the batch is
 * serialized via {@link EicfBatch#data()}.
 *
 * @param kind         the column kind (see {@link EicfColumnKind})
 * @param docCount     number of documents represented by this column
 * @param absentBitset the absent bitset, or {@code null} if no document is absent
 * @param typeVector   the per-document type vector, or {@code null} if implied by {@code kind}
 * @param offsets      the offset vector, or {@code null} for fixed-width / value-bitset kinds
 * @param data         the value payload (never {@code null}; may be empty)
 */
record EicfColumnData(
    byte kind,
    int docCount,
    BytesReference absentBitset,
    BytesReference typeVector,
    BytesReference offsets,
    BytesReference data
) {}
