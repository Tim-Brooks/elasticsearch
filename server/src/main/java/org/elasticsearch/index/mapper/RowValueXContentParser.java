/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.bulk.DocBatchRowIterator;
import org.elasticsearch.action.bulk.SmallArrayReader;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.AbstractXContentParser;

import java.io.IOException;
import java.nio.CharBuffer;

/**
 * Factory methods for creating XContentParsers from row-batch data for non-scalar cases
 * (binary/xcontent, small arrays, and null values).
 *
 * <p>For scalar leaf values, {@link DocBatchRowIterator} directly implements {@link XContentParser}
 * and should be used as the parser without allocating a wrapper.
 */
public final class RowValueXContentParser {

    private RowValueXContentParser() {}

    /**
     * Create a parser for a null value. Returns VALUE_NULL on the first nextToken() call and null thereafter.
     */
    public static XContentParser forNullValue() {
        return new NullValueXContentParser();
    }

    /**
     * Create a parser that delegates to a standard XContentParser wrapping raw binary (xcontent array/nested) data,
     * using a {@link DocBatchRowIterator} positioned at the target column.
     */
    public static XContentParser forBinary(DocBatchRowIterator iterator, XContentType xContentType) throws IOException {
        byte[] bytes = iterator.rowBinaryValue();
        if (bytes == null || bytes.length == 0) {
            return forNullValue();
        }
        return xContentType.xContent().createParser(XContentParserConfiguration.EMPTY, bytes);
    }

    /**
     * Create a parser that presents a compact typed array (from a {@link SmallArrayReader}) as
     * START_ARRAY, element tokens, END_ARRAY.
     */
    public static XContentParser forSmallArray(DocBatchRowIterator iterator) {
        return new SmallArrayXContentParser(iterator.smallArrayReader());
    }

    /**
     * A minimal XContentParser that returns VALUE_NULL on the first nextToken() call and null thereafter.
     */
    private static final class NullValueXContentParser extends AbstractXContentParser {

        private Token currentToken;
        private boolean closed;

        NullValueXContentParser() {
            super(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, RestApiVersion.current());
        }

        @Override
        public Token nextToken() throws IOException {
            if (closed) return null;
            if (currentToken == null) {
                currentToken = Token.VALUE_NULL;
                return currentToken;
            }
            currentToken = null;
            return null;
        }

        @Override
        public Token currentToken() {
            return currentToken;
        }

        @Override
        public String currentName() throws IOException {
            return null;
        }

        @Override
        public void skipChildren() throws IOException {}

        @Override
        public String text() throws IOException {
            return null;
        }

        @Override
        public CharBuffer charBuffer() throws IOException {
            return null;
        }

        @Override
        public boolean hasTextCharacters() {
            return false;
        }

        @Override
        public char[] textCharacters() throws IOException {
            return new char[0];
        }

        @Override
        public int textLength() throws IOException {
            return 0;
        }

        @Override
        public int textOffset() throws IOException {
            return 0;
        }

        @Override
        public Number numberValue() throws IOException {
            return null;
        }

        @Override
        public NumberType numberType() throws IOException {
            return null;
        }

        @Override
        protected boolean doBooleanValue() throws IOException {
            return false;
        }

        @Override
        protected short doShortValue() throws IOException {
            return 0;
        }

        @Override
        protected int doIntValue() throws IOException {
            return 0;
        }

        @Override
        protected long doLongValue() throws IOException {
            return 0;
        }

        @Override
        protected float doFloatValue() throws IOException {
            return 0;
        }

        @Override
        protected double doDoubleValue() throws IOException {
            return 0;
        }

        @Override
        public Object objectText() throws IOException {
            return null;
        }

        @Override
        public Object objectBytes() throws IOException {
            return null;
        }

        @Override
        public byte[] binaryValue() throws IOException {
            return null;
        }

        @Override
        public XContentLocation getTokenLocation() {
            return new XContentLocation(0, 0);
        }

        @Override
        public XContentLocation getCurrentLocation() {
            return new XContentLocation(0, 0);
        }

        @Override
        public XContentType contentType() {
            return XContentType.JSON;
        }

        @Override
        public void allowDuplicateKeys(boolean allowDuplicateKeys) {}

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }
    }
}
