/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider.xcontent;

import com.fasterxml.jackson.core.Base64Variant;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.base.ParserMinimalBase;

import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class XContentRecursive extends ParserMinimalBase {

    private final XContentParser parser;

    public XContentRecursive(XContentParser parser) {
        this.parser = parser;
    }

    @Override
    public JsonToken nextToken() throws IOException {
        XContentParser.Token token = parser.nextToken();
        if (token == null) {
            return null;
        }
        return switch (token) {
            case START_OBJECT -> JsonToken.START_OBJECT;
            case END_OBJECT -> JsonToken.END_OBJECT;
            case START_ARRAY -> JsonToken.START_ARRAY;
            case END_ARRAY -> JsonToken.END_ARRAY;
            case FIELD_NAME -> JsonToken.FIELD_NAME;
            case VALUE_EMBEDDED_OBJECT -> JsonToken.VALUE_EMBEDDED_OBJECT;
            case VALUE_STRING -> JsonToken.VALUE_STRING;
            case XContentParser.Token.VALUE_NUMBER -> switch (parser.numberType()) {
                case INT, LONG -> JsonToken.VALUE_NUMBER_INT;
                case FLOAT, DOUBLE -> JsonToken.VALUE_NUMBER_FLOAT;
                case BIG_INTEGER, BIG_DECIMAL -> JsonToken.VALUE_STRING;
            };
            case XContentParser.Token.VALUE_BOOLEAN -> parser.booleanValue() ? JsonToken.VALUE_TRUE : JsonToken.VALUE_FALSE;
            case VALUE_NULL -> JsonToken.VALUE_NULL;
        };
    }

    @Override
    protected void _handleEOF() throws JsonParseException {

    }

    @Override
    public String getCurrentName() throws IOException {
        return parser.currentName();
    }

    @Override
    public void close() throws IOException {
        parser.close();
    }

    @Override
    public boolean isClosed() {
        return parser.isClosed();
    }

    @Override
    public JsonLocation getCurrentLocation() {
        return null;
    }

    @Override
    public JsonLocation getTokenLocation() {
        return null;
    }

    @Override
    public String getText() throws IOException {
        return parser.text();
    }

    @Override
    public char[] getTextCharacters() throws IOException {
        return new char[0];
    }

    @Override
    public boolean hasTextCharacters() {
        return false;
    }

    @Override
    public Number getNumberValue() throws IOException {
        return null;
    }

    @Override
    public NumberType getNumberType() throws IOException {
        return null;
    }

    @Override
    public int getIntValue() throws IOException {
        return 0;
    }

    @Override
    public long getLongValue() throws IOException {
        return 0;
    }

    @Override
    public BigInteger getBigIntegerValue() throws IOException {
        return null;
    }

    @Override
    public float getFloatValue() throws IOException {
        return 0;
    }

    @Override
    public double getDoubleValue() throws IOException {
        return 0;
    }

    @Override
    public BigDecimal getDecimalValue() throws IOException {
        return null;
    }

    @Override
    public int getTextLength() throws IOException {
        return parser.textLength();
    }

    @Override
    public int getTextOffset() throws IOException {
        return parser.textOffset();
    }

    @Override
    public byte[] getBinaryValue(Base64Variant b64variant) throws IOException {
        return new byte[0];
    }

    @Override
    public void overrideCurrentName(String name) {
        throw new UnsupportedOperationException("Can not currently override name during filtering read");
    }

    @Override
    public JsonStreamContext getParsingContext() {
        return null;
    }

    @Override
    public ObjectCodec getCodec() {
        return null;
    }

    @Override
    public void setCodec(ObjectCodec oc) {

    }

    @Override
    public Version version() {
        return null;
    }
}
