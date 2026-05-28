/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.elasticsearch.xcontent.XContentString;

/**
 * Sink fired for every primitive leaf value during a document parse or row replay.
 *
 * <p>Invoked for {@code VALUE_STRING}, {@code VALUE_NUMBER}, and {@code VALUE_BOOLEAN} tokens
 * directly under an object. Not invoked for {@code VALUE_NULL} or for elements inside arrays or
 * empty objects encoded as {@code KEY_VALUE} leaves.
 *
 * <p>The encoder dispatches differently based on {@link #passRawText()}: sinks that want the
 * raw UTF-8 byte slice for every primitive (e.g. routing-path hashing) get
 * {@link #onTextPrimitive} for every leaf, and sinks that want typed values (e.g. tsid
 * dimensions) get {@link #onLongPrimitive} / {@link #onDoublePrimitive} /
 * {@link #onBooleanPrimitive} for numeric and boolean leaves with no wasted
 * {@code parser.optimizedText().bytes()} call.
 *
 * <p>Arrays at leaf positions are signalled via {@link #onArrayLeaf} regardless, so the caller
 * can react (e.g. throw to abandon batch encoding for the bulk).
 */
public interface LeafSink {

    LeafSink NO_OP = () -> false;

    /**
     * Returns true if this sink wants the parser's UTF-8 text bytes for every primitive leaf
     * (via {@link #onTextPrimitive}), false if it wants typed values (via
     * {@link #onLongPrimitive} / {@link #onDoublePrimitive} / {@link #onBooleanPrimitive}) for
     * numeric and boolean leaves.
     */
    boolean passRawText();

    /**
     * Called in raw-text mode ({@link #passRawText()} = {@code true}) for every primitive leaf,
     * and in typed mode for {@code STRING} leaves and unrecognized number types (BIG_DECIMAL /
     * BIG_INTEGER) that the encoder narrows to a string representation.
     *
     * @param columnIndex schema leaf index (stable across documents for a given schema)
     * @param dottedPath  cached dotted path for the column
     * @param type        the {@link EirfType} byte assigned to the value
     * @param textBytes   UTF-8 byte slice of the parser token's textual form
     */
    default void onTextPrimitive(int columnIndex, String dottedPath, byte type, XContentString.UTF8Bytes textBytes) {}

    /**
     * Called in typed mode ({@link #passRawText()} = {@code false}) for {@code INT} and
     * {@code LONG} primitives. The encoder narrows numerics that fit in the int range to
     * {@code EirfType.INT}; implementors can dispatch on {@code type} to distinguish them.
     */
    default void onLongPrimitive(int columnIndex, String dottedPath, byte type, long value) {}

    /**
     * Called in typed mode for {@code FLOAT} and {@code DOUBLE} primitives. The value passed is
     * the actual {@code double} (already reconstructed for narrowed {@code FLOAT} columns),
     * not raw bits.
     */
    default void onDoublePrimitive(int columnIndex, String dottedPath, byte type, double value) {}

    /** Called in typed mode for boolean primitives. */
    default void onBooleanPrimitive(int columnIndex, String dottedPath, boolean value) {}

    /**
     * Invoked once per array encountered as a direct leaf value under an object. The row still
     * encodes the array; this hook tells the caller that a complex value was seen at that column
     * so it can decide how to react.
     */
    default void onArrayLeaf(int columnIndex, String dottedPath) {}
}
