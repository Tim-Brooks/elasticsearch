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

    @SuppressWarnings("unchecked")
    private static void writeList(String path, List<?> list, EirfRowBuilder builder) throws IOException {
        // Determine element types and check for compound elements
        boolean hasCompound = false;
        byte[] elemTypes = new byte[list.size()];
        Object[] elemData = new Object[list.size()];

        for (int i = 0; i < list.size(); i++) {
            Object item = list.get(i);
            if (item instanceof Map<?, ?> map) {
                elemTypes[i] = EirfType.KEY_VALUE;
                elemData[i] = serializeMap((Map<String, Object>) map);
                hasCompound = true;
            } else if (item instanceof List<?> nested) {
                byte[] nestedPayload = serializeList(nested);
                elemTypes[i] = nestedPayload[0]; // type marker
                elemData[i] = new byte[nestedPayload.length - 1];
                System.arraycopy(nestedPayload, 1, (byte[]) elemData[i], 0, nestedPayload.length - 1);
                hasCompound = true;
            } else if (item instanceof String s) {
                elemTypes[i] = EirfType.STRING;
                byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
                elemData[i] = new org.elasticsearch.xcontent.XContentString.UTF8Bytes(utf8, 0, utf8.length);
            } else if (item instanceof Long l) {
                if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
                    elemTypes[i] = EirfType.INT;
                    elemData[i] = l;
                } else {
                    elemTypes[i] = EirfType.LONG;
                    elemData[i] = l;
                }
            } else if (item instanceof Integer intVal) {
                elemTypes[i] = EirfType.INT;
                elemData[i] = (long) intVal;
            } else if (item instanceof Double d) {
                float f = d.floatValue();
                if ((double) f == d) {
                    elemTypes[i] = EirfType.FLOAT;
                    elemData[i] = (long) Float.floatToRawIntBits(f);
                } else {
                    elemTypes[i] = EirfType.DOUBLE;
                    elemData[i] = Double.doubleToRawLongBits(d);
                }
            } else if (item instanceof Float f) {
                elemTypes[i] = EirfType.FLOAT;
                elemData[i] = (long) Float.floatToRawIntBits(f);
            } else if (item instanceof Boolean b) {
                elemTypes[i] = b ? EirfType.TRUE : EirfType.FALSE;
            } else if (item == null) {
                elemTypes[i] = EirfType.NULL;
            }
        }

        // Decide FIXED vs UNION
        boolean useFixed = false;
        byte sharedType = 0;
        if (hasCompound == false && list.size() > 0) {
            sharedType = elemTypes[0];
            useFixed = true;
            for (int i = 1; i < list.size(); i++) {
                if (elemTypes[i] != sharedType) {
                    useFixed = false;
                    break;
                }
            }
        }

        if (useFixed) {
            byte[] packed = EirfEncoder.packFixedArray(sharedType, elemData, list.size());
            builder.setFixedArray(path, packed);
        } else {
            byte[] packed = EirfEncoder.packUnionArray(elemTypes, elemData, list.size());
            builder.setUnionArray(path, packed);
        }
    }

    /**
     * Serializes a List to array bytes. Returns type_marker(1) + payload (same convention as
     * EirfEncoder.serializeArrayPayload).
     */
    @SuppressWarnings("unchecked")
    static byte[] serializeList(List<?> list) {
        boolean hasCompound = false;
        byte[] elemTypes = new byte[list.size()];
        Object[] elemData = new Object[list.size()];

        for (int i = 0; i < list.size(); i++) {
            Object item = list.get(i);
            if (item instanceof Map<?, ?> map) {
                elemTypes[i] = EirfType.KEY_VALUE;
                elemData[i] = serializeMap((Map<String, Object>) map);
                hasCompound = true;
            } else if (item instanceof List<?> nested) {
                byte[] nestedPayload = serializeList(nested);
                elemTypes[i] = nestedPayload[0];
                elemData[i] = new byte[nestedPayload.length - 1];
                System.arraycopy(nestedPayload, 1, (byte[]) elemData[i], 0, nestedPayload.length - 1);
                hasCompound = true;
            } else if (item instanceof String s) {
                elemTypes[i] = EirfType.STRING;
                byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
                elemData[i] = new org.elasticsearch.xcontent.XContentString.UTF8Bytes(utf8, 0, utf8.length);
            } else if (item instanceof Long l) {
                if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
                    elemTypes[i] = EirfType.INT;
                    elemData[i] = l;
                } else {
                    elemTypes[i] = EirfType.LONG;
                    elemData[i] = l;
                }
            } else if (item instanceof Integer intVal) {
                elemTypes[i] = EirfType.INT;
                elemData[i] = (long) intVal;
            } else if (item instanceof Double d) {
                float f = d.floatValue();
                if ((double) f == d) {
                    elemTypes[i] = EirfType.FLOAT;
                    elemData[i] = (long) Float.floatToRawIntBits(f);
                } else {
                    elemTypes[i] = EirfType.DOUBLE;
                    elemData[i] = Double.doubleToRawLongBits(d);
                }
            } else if (item instanceof Float f) {
                elemTypes[i] = EirfType.FLOAT;
                elemData[i] = (long) Float.floatToRawIntBits(f);
            } else if (item instanceof Boolean b) {
                elemTypes[i] = b ? EirfType.TRUE : EirfType.FALSE;
            } else if (item == null) {
                elemTypes[i] = EirfType.NULL;
            }
        }

        boolean useFixed = false;
        byte sharedType = 0;
        if (hasCompound == false && list.size() > 0) {
            sharedType = elemTypes[0];
            useFixed = true;
            for (int i = 1; i < list.size(); i++) {
                if (elemTypes[i] != sharedType) {
                    useFixed = false;
                    break;
                }
            }
        }

        byte[] packed;
        byte arrayType;
        if (useFixed) {
            packed = EirfEncoder.packFixedArray(sharedType, elemData, list.size());
            arrayType = EirfType.FIXED_ARRAY;
        } else {
            packed = EirfEncoder.packUnionArray(elemTypes, elemData, list.size());
            arrayType = EirfType.UNION_ARRAY;
        }

        byte[] result = new byte[1 + packed.length];
        result[0] = arrayType;
        System.arraycopy(packed, 0, result, 1, packed.length);
        return result;
    }

    /**
     * Serializes a Map to KEY_VALUE payload bytes (entries only, no outer length prefix).
     */
    @SuppressWarnings("unchecked")
    static byte[] serializeMap(Map<String, Object> map) {
        EirfEncoder.GrowableBuffer gb = new EirfEncoder.GrowableBuffer(64);

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            Object value = entry.getValue();

            gb.ensure(gb.pos + 4 + keyBytes.length + 1 + 12);

            // key_length(i32 LE) + key_bytes
            ByteUtils.writeIntLE(keyBytes.length, gb.buf, gb.pos);
            gb.pos += 4;
            System.arraycopy(keyBytes, 0, gb.buf, gb.pos, keyBytes.length);
            gb.pos += keyBytes.length;

            // type(1) + value_data
            writeMapValue(gb, value);
        }

        return gb.toArray();
    }

    @SuppressWarnings("unchecked")
    private static void writeMapValue(EirfEncoder.GrowableBuffer gb, Object value) {
        if (value instanceof String s) {
            byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
            gb.ensure(gb.pos + 1 + 4 + utf8.length);
            gb.buf[gb.pos++] = EirfType.STRING;
            ByteUtils.writeIntLE(utf8.length, gb.buf, gb.pos);
            gb.pos += 4;
            System.arraycopy(utf8, 0, gb.buf, gb.pos, utf8.length);
            gb.pos += utf8.length;
        } else if (value instanceof Long l) {
            if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
                gb.ensure(gb.pos + 5);
                gb.buf[gb.pos++] = EirfType.INT;
                ByteUtils.writeIntLE(l.intValue(), gb.buf, gb.pos);
                gb.pos += 4;
            } else {
                gb.ensure(gb.pos + 9);
                gb.buf[gb.pos++] = EirfType.LONG;
                ByteUtils.writeLongLE(l, gb.buf, gb.pos);
                gb.pos += 8;
            }
        } else if (value instanceof Integer i) {
            gb.ensure(gb.pos + 5);
            gb.buf[gb.pos++] = EirfType.INT;
            ByteUtils.writeIntLE(i, gb.buf, gb.pos);
            gb.pos += 4;
        } else if (value instanceof Double d) {
            float f = d.floatValue();
            if ((double) f == d) {
                gb.ensure(gb.pos + 5);
                gb.buf[gb.pos++] = EirfType.FLOAT;
                ByteUtils.writeIntLE(Float.floatToRawIntBits(f), gb.buf, gb.pos);
                gb.pos += 4;
            } else {
                gb.ensure(gb.pos + 9);
                gb.buf[gb.pos++] = EirfType.DOUBLE;
                ByteUtils.writeLongLE(Double.doubleToRawLongBits(d), gb.buf, gb.pos);
                gb.pos += 8;
            }
        } else if (value instanceof Float f) {
            gb.ensure(gb.pos + 5);
            gb.buf[gb.pos++] = EirfType.FLOAT;
            ByteUtils.writeIntLE(Float.floatToRawIntBits(f), gb.buf, gb.pos);
            gb.pos += 4;
        } else if (value instanceof Boolean b) {
            gb.ensure(gb.pos + 1);
            gb.buf[gb.pos++] = b ? EirfType.TRUE : EirfType.FALSE;
        } else if (value instanceof Map<?, ?> nested) {
            byte[] kvBytes = serializeMap((Map<String, Object>) nested);
            gb.ensure(gb.pos + 1 + 4 + kvBytes.length);
            gb.buf[gb.pos++] = EirfType.KEY_VALUE;
            ByteUtils.writeIntLE(kvBytes.length, gb.buf, gb.pos);
            gb.pos += 4;
            System.arraycopy(kvBytes, 0, gb.buf, gb.pos, kvBytes.length);
            gb.pos += kvBytes.length;
        } else if (value instanceof List<?> list) {
            byte[] arrayPayload = serializeList(list);
            byte arrayType = arrayPayload[0];
            int payloadLen = arrayPayload.length - 1;
            gb.ensure(gb.pos + 1 + 4 + payloadLen);
            gb.buf[gb.pos++] = arrayType;
            ByteUtils.writeIntLE(payloadLen, gb.buf, gb.pos);
            gb.pos += 4;
            System.arraycopy(arrayPayload, 1, gb.buf, gb.pos, payloadLen);
            gb.pos += payloadLen;
        } else if (value == null) {
            gb.ensure(gb.pos + 1);
            gb.buf[gb.pos++] = EirfType.NULL;
        }
    }
}
