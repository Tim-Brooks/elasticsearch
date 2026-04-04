/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
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

    @SuppressWarnings("unchecked")
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

    private static Object readValue(EirfRowReader row, int leafIdx, byte type) throws IOException {
        return switch (type) {
            case EirfType.INT -> row.getIntValue(leafIdx);
            case EirfType.FLOAT -> row.getFloatValue(leafIdx);
            case EirfType.LONG -> row.getLongValue(leafIdx);
            case EirfType.DOUBLE -> row.getDoubleValue(leafIdx);
            case EirfType.SMALL_STRING, EirfType.STRING -> row.getStringValue(leafIdx);
            case EirfType.TRUE -> Boolean.TRUE;
            case EirfType.FALSE -> Boolean.FALSE;
            case EirfType.SMALL_UNION_ARRAY, EirfType.UNION_ARRAY -> readArray(row.getArrayValue(leafIdx), false);
            case EirfType.SMALL_FIXED_ARRAY, EirfType.FIXED_ARRAY -> readArray(row.getArrayValue(leafIdx), true);
            case EirfType.SMALL_XCONTENT, EirfType.XCONTENT -> readXContent(row.getXContentValue(leafIdx));
            case EirfType.SMALL_BINARY, EirfType.BINARY -> row.getBinaryValue(leafIdx);
            default -> null;
        };
    }

    private static List<Object> readArray(byte[] arrayData, boolean fixed) {
        EirfArray reader = new EirfArray(arrayData, 0, fixed);
        List<Object> list = new ArrayList<>(reader.count());
        while (reader.next()) {
            switch (reader.type()) {
                case EirfType.INT -> list.add(reader.intValue());
                case EirfType.FLOAT -> list.add(reader.floatValue());
                case EirfType.LONG -> list.add(reader.longValue());
                case EirfType.DOUBLE -> list.add(reader.doubleValue());
                case EirfType.STRING -> list.add(reader.stringValue());
                case EirfType.TRUE -> list.add(Boolean.TRUE);
                case EirfType.FALSE -> list.add(Boolean.FALSE);
                case EirfType.NULL -> list.add(null);
            }
        }
        return list;
    }

    private static Object readXContent(byte[] rawBytes) throws IOException {
        try (
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, rawBytes)
        ) {
            // Could be an array or an object
            XContentParser.Token token = parser.nextToken();
            if (token == XContentParser.Token.START_ARRAY) {
                return parser.list();
            } else if (token == XContentParser.Token.START_OBJECT) {
                return parser.map();
            } else {
                return parser.objectText();
            }
        }
    }
}
