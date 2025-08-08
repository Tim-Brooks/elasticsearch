/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider.eson;

import com.fasterxml.jackson.core.Base64Variant;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.base.ParserMinimalBase;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class ESONFlatParser extends ParserMinimalBase {

    @Override
    public JsonToken nextToken() throws IOException {
        return null;
    }

    @Override
    protected void _handleEOF() throws JsonParseException {

    }

    @Override
    public String getCurrentName() throws IOException {
        return "";
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public JsonLocation getCurrentLocation() {
        return new JsonLocation(null, -1, -1, -1);
    }

    @Override
    public JsonLocation getTokenLocation() {
        return new JsonLocation(null, -1, -1, -1);
    }

    @Override
    public void overrideCurrentName(String name) {
        throw new UnsupportedOperationException("Can not currently override name during filtering read");
    }

    @Override
    public String getText() throws IOException {
        return "";
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
        return 0;
    }

    @Override
    public int getTextOffset() throws IOException {
        return 0;
    }

    @Override
    public byte[] getBinaryValue(Base64Variant b64variant) throws IOException {
        return new byte[0];
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

    @Override
    public JsonStreamContext getParsingContext() {
        return null;
    }
}
