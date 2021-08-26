/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.compress;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;

public class EsLZ4Tests extends ESTestCase {

    public void testCompress() {
        final String uncompressed = randomRealisticUnicodeOfCodepointLength(64 * 1024);
        byte[] uncompressedBytes = uncompressed.getBytes(StandardCharsets.UTF_8);
        // TODO
        byte[] compressed = new byte[uncompressedBytes.length * 2];
        EsLZ4.compress(uncompressedBytes, 0, uncompressedBytes.length, compressed, new EsLZ4.FastCompressionHashTable());

        LZ4FastDecompressor decompressor = LZ4Factory.safeInstance().fastDecompressor();
        byte[] output = new byte[uncompressedBytes.length];
        decompressor.decompress(compressed, output);

        assertArrayEquals(uncompressedBytes, output);
    }
}
