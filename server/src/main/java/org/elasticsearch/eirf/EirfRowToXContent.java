/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Converts an EIRF row to XContent output via {@link XContentBuilder}.
 */
public final class EirfRowToXContent {

    private EirfRowToXContent() {}

    /**
     * Writes a single row as a nested JSON object to the given builder.
     * The builder should not have startObject called yet - this method handles it.
     */
    public static void writeRow(EirfRowReader row, EirfSchema schema, XContentBuilder builder) throws IOException {
        int leafCount = schema.leafCount();

        int[] openStack = new int[16];
        int stackDepth = 0;

        builder.startObject();

        for (int leafIdx = 0; leafIdx < leafCount; leafIdx++) {
            if (leafIdx >= row.columnCount() || row.isNull(leafIdx)) {
                continue;
            }

            int parentNonLeaf = schema.getLeafParent(leafIdx);
            int[] targetChain = schema.getNonLeafChain(parentNonLeaf);

            int commonLen = 0;
            int minLen = Math.min(stackDepth, targetChain.length);
            for (int i = 0; i < minLen; i++) {
                if (openStack[i] != targetChain[i]) break;
                commonLen++;
            }

            for (int i = stackDepth - 1; i >= commonLen; i--) {
                builder.endObject();
            }
            stackDepth = commonLen;

            if (targetChain.length > openStack.length) {
                openStack = Arrays.copyOf(openStack, targetChain.length * 2);
            }
            for (int i = commonLen; i < targetChain.length; i++) {
                builder.field(schema.getNonLeafName(targetChain[i]));
                builder.startObject();
                openStack[stackDepth++] = targetChain[i];
            }

            String leafName = schema.getLeafName(leafIdx);
            byte type = row.getTypeByte(leafIdx);
            writeLeafValue(row, leafIdx, type, leafName, builder);
        }

        for (int i = stackDepth - 1; i >= 0; i--) {
            builder.endObject();
        }

        builder.endObject();
    }

    private static void writeLeafValue(EirfRowReader row, int leafIdx, byte type, String leafName, XContentBuilder builder)
        throws IOException {
        switch (type) {
            case EirfType.INT -> builder.field(leafName, row.getIntValue(leafIdx));
            case EirfType.FLOAT -> builder.field(leafName, row.getFloatValue(leafIdx));
            case EirfType.LONG -> builder.field(leafName, row.getLongValue(leafIdx));
            case EirfType.DOUBLE -> builder.field(leafName, row.getDoubleValue(leafIdx));
            case EirfType.SMALL_STRING, EirfType.STRING -> builder.field(leafName, row.getStringValue(leafIdx));
            case EirfType.TRUE -> builder.field(leafName, true);
            case EirfType.FALSE -> builder.field(leafName, false);
            case EirfType.SMALL_UNION_ARRAY, EirfType.UNION_ARRAY -> {
                builder.field(leafName);
                byte[] arrayData = row.getArrayValue(leafIdx);
                writeArray(arrayData, 0, arrayData.length, false, builder);
            }
            case EirfType.SMALL_FIXED_ARRAY, EirfType.FIXED_ARRAY -> {
                builder.field(leafName);
                byte[] arrayData = row.getArrayValue(leafIdx);
                writeArray(arrayData, 0, arrayData.length, true, builder);
            }
            case EirfType.SMALL_KEY_VALUE, EirfType.KEY_VALUE -> {
                builder.field(leafName);
                byte[] kvData = row.getKeyValueBytes(leafIdx);
                writeKeyValue(kvData, 0, kvData.length, builder);
            }
            case EirfType.SMALL_BINARY, EirfType.BINARY -> builder.field(leafName).value(row.getBinaryValue(leafIdx));
        }
    }

    static void writeArray(byte[] data, int offset, int length, boolean fixed, XContentBuilder builder) throws IOException {
        EirfArray reader = new EirfArray(data, offset, length, fixed);
        builder.startArray();
        while (reader.next()) {
            writeElementValue(reader, builder);
        }
        builder.endArray();
    }

    private static void writeElementValue(EirfArray reader, XContentBuilder builder) throws IOException {
        switch (reader.type()) {
            case EirfType.INT -> builder.value(reader.intValue());
            case EirfType.FLOAT -> builder.value(reader.floatValue());
            case EirfType.LONG -> builder.value(reader.longValue());
            case EirfType.DOUBLE -> builder.value(reader.doubleValue());
            case EirfType.STRING -> builder.value(reader.stringValue());
            case EirfType.TRUE -> builder.value(true);
            case EirfType.FALSE -> builder.value(false);
            case EirfType.NULL -> builder.nullValue();
            case EirfType.KEY_VALUE -> {
                int len = reader.compoundLength();
                int off = reader.compoundOffset();
                byte[] bytes = reader.compoundBytes();
                writeKeyValue(bytes, off, len, builder);
                reader.skipCompound();
            }
            case EirfType.UNION_ARRAY -> {
                int len = reader.compoundLength();
                int off = reader.compoundOffset();
                byte[] bytes = reader.compoundBytes();
                writeArray(bytes, off, len, false, builder);
                reader.skipCompound();
            }
            case EirfType.FIXED_ARRAY -> {
                int len = reader.compoundLength();
                int off = reader.compoundOffset();
                byte[] bytes = reader.compoundBytes();
                writeArray(bytes, off, len, true, builder);
                reader.skipCompound();
            }
        }
    }

    static void writeKeyValue(byte[] data, int offset, int length, XContentBuilder builder) throws IOException {
        builder.startObject();
        int end = offset + length;
        int pos = offset;
        while (pos < end) {
            int keyLen = data[pos] & 0xFF;
            pos++;
            String key = new String(data, pos, keyLen, StandardCharsets.UTF_8);
            pos += keyLen;

            byte type = data[pos];
            pos++;

            builder.field(key);
            pos = writeInlineValue(data, pos, type, builder);
        }
        builder.endObject();
    }

    private static int writeInlineValue(byte[] data, int pos, byte type, XContentBuilder builder) throws IOException {
        switch (type) {
            case EirfType.INT -> {
                builder.value(ByteUtils.readIntBE(data, pos));
                pos += 4;
            }
            case EirfType.FLOAT -> {
                builder.value(Float.intBitsToFloat(ByteUtils.readIntBE(data, pos)));
                pos += 4;
            }
            case EirfType.LONG -> {
                builder.value(readLongBE(data, pos));
                pos += 8;
            }
            case EirfType.DOUBLE -> {
                builder.value(Double.longBitsToDouble(readLongBE(data, pos)));
                pos += 8;
            }
            case EirfType.STRING -> {
                int len = ByteUtils.readIntBE(data, pos);
                pos += 4;
                builder.value(new String(data, pos, len, StandardCharsets.UTF_8));
                pos += len;
            }
            case EirfType.TRUE -> builder.value(true);
            case EirfType.FALSE -> builder.value(false);
            case EirfType.NULL -> builder.nullValue();
            case EirfType.KEY_VALUE -> {
                int len = ByteUtils.readIntBE(data, pos);
                pos += 4;
                writeKeyValue(data, pos, len, builder);
                pos += len;
            }
            case EirfType.UNION_ARRAY -> {
                int len = ByteUtils.readIntBE(data, pos);
                pos += 4;
                writeArray(data, pos, len, false, builder);
                pos += len;
            }
            case EirfType.FIXED_ARRAY -> {
                int len = ByteUtils.readIntBE(data, pos);
                pos += 4;
                writeArray(data, pos, len, true, builder);
                pos += len;
            }
        }
        return pos;
    }

    private static long readLongBE(byte[] data, int offset) {
        long hi = ByteUtils.readIntBE(data, offset) & 0xFFFFFFFFL;
        long lo = ByteUtils.readIntBE(data, offset + 4) & 0xFFFFFFFFL;
        return (hi << 32) | lo;
    }
}
