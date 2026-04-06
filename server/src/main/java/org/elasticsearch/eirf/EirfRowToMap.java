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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts an EIRF row to a nested {@code Map<String, Object>}.
 */
public final class EirfRowToMap {

    private EirfRowToMap() {}

    @SuppressWarnings("unchecked")
    public static Map<String, Object> toMap(EirfRowReader row, EirfSchema schema) throws IOException {
        int leafCount = schema.leafCount();

        Map<String, Object>[] nonLeafMaps = new Map[schema.nonLeafCount()];
        Map<String, Object> root = new LinkedHashMap<>();
        nonLeafMaps[0] = root;

        for (int leafIdx = 0; leafIdx < leafCount; leafIdx++) {
            if (leafIdx >= row.columnCount() || row.isNull(leafIdx)) {
                continue;
            }

            int parentIdx = schema.getLeafParent(leafIdx);
            Map<String, Object> parentMap = ensureMap(nonLeafMaps, parentIdx, schema);

            String leafName = schema.getLeafName(leafIdx);
            byte type = row.getTypeByte(leafIdx);
            Object value = readValue(row, leafIdx, type);
            parentMap.put(leafName, value);
        }

        return root;
    }

    private static Map<String, Object> ensureMap(Map<String, Object>[] cache, int nonLeafIdx, EirfSchema schema) {
        if (cache[nonLeafIdx] != null) {
            return cache[nonLeafIdx];
        }
        int parentIdx = schema.getNonLeafParent(nonLeafIdx);
        Map<String, Object> parentMap = ensureMap(cache, parentIdx, schema);

        String name = schema.getNonLeafName(nonLeafIdx);
        Map<String, Object> thisMap = new LinkedHashMap<>();
        parentMap.put(name, thisMap);
        cache[nonLeafIdx] = thisMap;
        return thisMap;
    }

    private static Object readValue(EirfRowReader row, int leafIdx, byte type) {
        return switch (type) {
            case EirfType.INT -> row.getIntValue(leafIdx);
            case EirfType.FLOAT -> row.getFloatValue(leafIdx);
            case EirfType.LONG -> row.getLongValue(leafIdx);
            case EirfType.DOUBLE -> row.getDoubleValue(leafIdx);
            case EirfType.STRING -> row.getStringValue(leafIdx);
            case EirfType.TRUE -> Boolean.TRUE;
            case EirfType.FALSE -> Boolean.FALSE;
            case EirfType.UNION_ARRAY -> {
                byte[] arrayData = row.getArrayValue(leafIdx);
                yield readArray(arrayData, 0, arrayData.length, false);
            }
            case EirfType.FIXED_ARRAY -> {
                byte[] arrayData = row.getArrayValue(leafIdx);
                yield readArray(arrayData, 0, arrayData.length, true);
            }
            case EirfType.KEY_VALUE -> {
                byte[] kvData = row.getKeyValueBytes(leafIdx);
                yield readKeyValue(kvData, 0, kvData.length);
            }
            case EirfType.BINARY -> row.getBinaryValue(leafIdx);
            default -> null;
        };
    }

    static List<Object> readArray(byte[] data, int offset, int length, boolean fixed) {
        EirfArray reader = new EirfArray(data, offset, length, fixed);
        List<Object> list = new ArrayList<>();
        while (reader.next()) {
            list.add(readElementValue(reader));
        }
        return list;
    }

    private static Object readElementValue(EirfArray reader) {
        return switch (reader.type()) {
            case EirfType.INT -> reader.intValue();
            case EirfType.FLOAT -> reader.floatValue();
            case EirfType.LONG -> reader.longValue();
            case EirfType.DOUBLE -> reader.doubleValue();
            case EirfType.STRING -> reader.stringValue();
            case EirfType.TRUE -> Boolean.TRUE;
            case EirfType.FALSE -> Boolean.FALSE;
            case EirfType.NULL -> null;
            case EirfType.KEY_VALUE -> {
                int len = reader.compoundLength();
                int off = reader.compoundOffset();
                byte[] bytes = reader.compoundBytes();
                reader.skipCompound();
                yield readKeyValue(bytes, off, len);
            }
            case EirfType.UNION_ARRAY -> {
                int len = reader.compoundLength();
                int off = reader.compoundOffset();
                byte[] bytes = reader.compoundBytes();
                reader.skipCompound();
                yield readArray(bytes, off, len, false);
            }
            case EirfType.FIXED_ARRAY -> {
                int len = reader.compoundLength();
                int off = reader.compoundOffset();
                byte[] bytes = reader.compoundBytes();
                reader.skipCompound();
                yield readArray(bytes, off, len, true);
            }
            default -> throw new IllegalArgumentException("Unknown array type: " + reader.type());
        };
    }

    static Map<String, Object> readKeyValue(byte[] data, int offset, int length) {
        Map<String, Object> map = new LinkedHashMap<>();
        int end = offset + length;
        int pos = offset;
        while (pos < end) {
            int keyLen = ByteUtils.readIntLE(data, pos);
            pos += 4;
            String key = new String(data, pos, keyLen, StandardCharsets.UTF_8);
            pos += keyLen;

            byte type = data[pos];
            pos++;

            Object[] result = readInlineValue(data, pos, type);
            map.put(key, result[0]);
            pos = (int) result[1];
        }
        return map;
    }

    /**
     * Reads an inline value from the byte array at the given position.
     * Returns [value, newPos] as an Object array.
     */
    private static Object[] readInlineValue(byte[] data, int pos, byte type) {
        return switch (type) {
            case EirfType.INT -> new Object[] { ByteUtils.readIntLE(data, pos), pos + 4 };
            case EirfType.FLOAT -> new Object[] { Float.intBitsToFloat(ByteUtils.readIntLE(data, pos)), pos + 4 };
            case EirfType.LONG -> new Object[] { ByteUtils.readLongLE(data, pos), pos + 8 };
            case EirfType.DOUBLE -> new Object[] { Double.longBitsToDouble(ByteUtils.readLongLE(data, pos)), pos + 8 };
            case EirfType.STRING -> {
                int len = ByteUtils.readIntLE(data, pos);
                pos += 4;
                yield new Object[] { new String(data, pos, len, StandardCharsets.UTF_8), pos + len };
            }
            case EirfType.TRUE -> new Object[] { Boolean.TRUE, pos };
            case EirfType.FALSE -> new Object[] { Boolean.FALSE, pos };
            case EirfType.NULL -> new Object[] { null, pos };
            case EirfType.KEY_VALUE -> {
                int len = ByteUtils.readIntLE(data, pos);
                pos += 4;
                yield new Object[] { readKeyValue(data, pos, len), pos + len };
            }
            case EirfType.UNION_ARRAY -> {
                int len = ByteUtils.readIntLE(data, pos);
                pos += 4;
                yield new Object[] { readArray(data, pos, len, false), pos + len };
            }
            case EirfType.FIXED_ARRAY -> {
                int len = ByteUtils.readIntLE(data, pos);
                pos += 4;
                yield new Object[] { readArray(data, pos, len, true), pos + len };
            }
            default -> new Object[] { null, pos };
        };
    }
}
