/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

public class Routing implements Writeable {

    public static final byte TYPE_UTF16LE = 0;
    public static final byte TYPE_BYTES = 1;

    private final byte type;
    private final byte[] value;

    public Routing(byte type, byte[] value) {
        this.type = type;
        this.value = value;
    }

    public Routing(StreamInput in) throws IOException {
        this.type = in.readByte();
        this.value = in.readByteArray();
    }

    // TODO: Maybe cache string if valuable
    public String asString() {
        return switch (type) {
            case TYPE_UTF16LE -> {
                CharBuffer charBuffer = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).asCharBuffer();
                char[] chars = new char[value.length / 2];
                charBuffer.get(chars);
                yield new String(chars);
            }
            case TYPE_BYTES -> Strings.BASE_64_NO_PADDING_URL_ENCODER.encodeToString(value);
            default -> throw new IllegalArgumentException("Unknown routing type [" + type + "]");
        };
    }

    public static Routing fromString(String routing) {
        int strLen = routing.length();
        byte[] bytes = new byte[strLen];
        for (int i = 0; i < strLen; ++i) {
            ByteUtils.LITTLE_ENDIAN_CHAR.set(bytes, 2 * i, routing.charAt(i));
        }

        return new Routing(TYPE_UTF16LE, routing.getBytes(StandardCharsets.UTF_16LE));
    }

    public static Routing fromStringMaybeNull(@Nullable String routing) {
        return routing == null ? null : Routing.fromString(routing);
    }

    public static Routing fromBytes(byte[] routing) {
        return new Routing(TYPE_BYTES, routing);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(type);
        out.writeByteArray(value);
    }
}
