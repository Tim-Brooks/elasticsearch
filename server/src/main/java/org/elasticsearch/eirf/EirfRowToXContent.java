/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
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
                writeArray(row.getArrayValue(leafIdx), false, builder);
            }
            case EirfType.SMALL_FIXED_ARRAY, EirfType.FIXED_ARRAY -> {
                builder.field(leafName);
                writeArray(row.getArrayValue(leafIdx), true, builder);
            }
            case EirfType.SMALL_XCONTENT, EirfType.XCONTENT -> {
                byte[] rawBytes = row.getXContentValue(leafIdx);
                builder.field(leafName);
                writeRawXContent(rawBytes, builder);
            }
            case EirfType.SMALL_BINARY, EirfType.BINARY -> builder.field(leafName).value(row.getBinaryValue(leafIdx));
        }
    }

    private static void writeArray(byte[] arrayData, boolean fixed, XContentBuilder builder) throws IOException {
        EirfArray reader = new EirfArray(arrayData, 0, fixed);
        builder.startArray();
        while (reader.next()) {
            switch (reader.type()) {
                case EirfType.INT -> builder.value(reader.intValue());
                case EirfType.FLOAT -> builder.value(reader.floatValue());
                case EirfType.LONG -> builder.value(reader.longValue());
                case EirfType.DOUBLE -> builder.value(reader.doubleValue());
                case EirfType.STRING -> builder.value(reader.stringValue());
                case EirfType.TRUE -> builder.value(true);
                case EirfType.FALSE -> builder.value(false);
                case EirfType.NULL -> builder.nullValue();
            }
        }
        builder.endArray();
    }

    private static void writeRawXContent(byte[] rawBytes, XContentBuilder builder) throws IOException {
        try (
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, rawBytes)
        ) {
            builder.copyCurrentStructure(parser);
        }
    }
}
