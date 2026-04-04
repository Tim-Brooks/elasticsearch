/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Converts a {@code Map<String, Object>} to EIRF format via {@link EirfRowBuilder}.
 */
public final class EirfMapToRow {

    private EirfMapToRow() {}

    /**
     * Writes a map as a single document to the given builder.
     * Calls startDocument/endDocument on the builder.
     */
    @SuppressWarnings("unchecked")
    public static void writeToBuilder(Map<String, Object> doc, EirfRowBuilder builder) throws IOException {
        builder.startDocument();
        writeMap(doc, "", builder);
        builder.endDocument();
    }

    @SuppressWarnings("unchecked")
    private static void writeMap(Map<String, Object> map, String prefix, EirfRowBuilder builder) throws IOException {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String path = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();

            if (value instanceof Map<?, ?> nested) {
                writeMap((Map<String, Object>) nested, path, builder);
            } else if (value instanceof String s) {
                builder.setString(path, s);
            } else if (value instanceof Long l) {
                if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
                    builder.setInt(path, l.intValue());
                } else {
                    builder.setLong(path, l);
                }
            } else if (value instanceof Integer i) {
                builder.setInt(path, i);
            } else if (value instanceof Double d) {
                float f = d.floatValue();
                if ((double) f == d) {
                    builder.setFloat(path, f);
                } else {
                    builder.setDouble(path, d);
                }
            } else if (value instanceof Float f) {
                builder.setFloat(path, f);
            } else if (value instanceof Boolean b) {
                builder.setBoolean(path, b);
            } else if (value instanceof List<?> list) {
                writeList(path, list, builder);
            } else if (value == null) {
                builder.setNull(path);
            }
        }
    }

    private static void writeList(String path, List<?> list, EirfRowBuilder builder) throws IOException {
        // Serialize as XContent array
        try (XContentBuilder xb = XContentFactory.contentBuilder(XContentType.JSON)) {
            xb.startArray();
            for (Object item : list) {
                writeValue(xb, item);
            }
            xb.endArray();
            builder.setXContent(path, BytesReference.bytes(xb));
        }
    }

    @SuppressWarnings("unchecked")
    private static void writeValue(XContentBuilder xb, Object value) throws IOException {
        if (value instanceof String s) {
            xb.value(s);
        } else if (value instanceof Long l) {
            xb.value(l);
        } else if (value instanceof Integer i) {
            xb.value(i);
        } else if (value instanceof Double d) {
            xb.value(d);
        } else if (value instanceof Float f) {
            xb.value(f);
        } else if (value instanceof Boolean b) {
            xb.value(b);
        } else if (value instanceof Map<?, ?> map) {
            xb.startObject();
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) map).entrySet()) {
                xb.field(entry.getKey());
                writeValue(xb, entry.getValue());
            }
            xb.endObject();
        } else if (value instanceof List<?> list) {
            xb.startArray();
            for (Object item : list) {
                writeValue(xb, item);
            }
            xb.endArray();
        } else if (value == null) {
            xb.nullValue();
        }
    }
}
