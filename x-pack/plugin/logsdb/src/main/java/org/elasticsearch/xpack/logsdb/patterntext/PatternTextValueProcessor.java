/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.util.ByteUtils;

import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class PatternTextValueProcessor {
    private static final Pattern DELIMITER = Pattern.compile("[\\s\\[\\]]");
    public static final int MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE = 8 * 1024;

    public record Parts(
        String template,
        String templateId,
        List<String> args,
        List<Arg.Info> argsInfo,
        boolean useBinaryDocValuesForRawText
    ) {
        Parts(String template, List<String> args, List<Arg.Info> argsInfo) {
            this(template, PatternTextValueProcessor.templateId(template), args, argsInfo, false);
        }

        static Parts lengthExceeded(String template) {
            return new Parts(template, PatternTextValueProcessor.templateId(template), null, null, true);
        }
    }

    public static int originalSize(String template, String[] args) {
        int size = template.length();
        for (var arg : args) {
            size += arg.length();
        }
        return size;
    }

    static String templateId(String template) {
        byte[] bytes = template.getBytes(StandardCharsets.UTF_8);
        MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
        MurmurHash3.hash128(bytes, 0, bytes.length, 0, hash);
        byte[] hashBytes = new byte[8];
        ByteUtils.writeLongLE(hash.h1, hashBytes, 0);
        return Strings.BASE_64_NO_PADDING_URL_ENCODER.encodeToString(hashBytes);
    }

    static Parts split(String text) {
        if (text.length() > MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE) {
            return splitInternal(CharBuffer.wrap(text).subSequence(0, MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE), true);
        } else {
            return splitInternal(text, false);
        }
    }

    static Parts splitInternal(CharSequence text, boolean exceedsMaxLength) {
        StringBuilder template = new StringBuilder(text.length());
        List<String> args = new ArrayList<>();
        List<Arg.Info> argsInfo = new ArrayList<>();
        String[] tokens = DELIMITER.split(text);
        int textIndex = 0;
        for (String token : tokens) {
            if (token.isEmpty()) {
                // add the previous delimiter
                if (textIndex < text.length() - 1) {
                    template.append(text.charAt(textIndex++));
                }
            } else {
                if (Arg.isArg(token)) {
                    args.add(token);
                    argsInfo.add(new Arg.Info(Arg.Type.GENERIC, template.length()));
                } else {
                    template.append(token);
                }
                textIndex += token.length();
                if (textIndex < text.length()) {
                    template.append(text.charAt(textIndex++));
                }
            }
        }
        while (textIndex < text.length()) {
            template.append(text.charAt(textIndex++));
        }

        return exceedsMaxLength ? Parts.lengthExceeded(text.toString()) : new Parts(template.toString(), args, argsInfo);
    }

    // For testing
    public static String merge(Parts parts) {
        return merge(parts.template, parts.args.toArray(String[]::new), parts.argsInfo);
    }

    static String merge(String template, String[] args, List<Arg.Info> argsInfo) {
        StringBuilder builder = new StringBuilder(originalSize(template, args));
        int numArgs = args.length;

        int nextToWrite = 0;
        for (int i = 0; i < numArgs; i++) {
            String arg = args[i];
            var argInfo = argsInfo.get(i);

            builder.append(template, nextToWrite, argInfo.offsetInTemplate());
            builder.append(arg);
            nextToWrite = argInfo.offsetInTemplate();
        }

        if (nextToWrite < template.length()) {
            builder.append(template, nextToWrite, template.length());
        }
        return builder.toString();
    }

    // ---------------------------------------------------------------------------------------------------------
    // Byte-level split for the columnar batch indexing fast path.
    //
    // The source values are already UTF-8 in the columnar (EICF) format, so the columnar path splits and
    // encodes directly on those bytes instead of materializing a Java String and running the regex {@link
    // #splitInternal} above (which allocates a String[], a String per token, a StringBuilder, and two lists
    // per document). {@link #splitUtf8} produces byte-identical results to {@link #splitInternal}: the
    // {@code splitInternal} loop is equivalent to "remove every arg run, keep every delimiter and non-arg
    // run in place", since each delimiter in the text is appended to the template exactly once and arg runs
    // are dropped. The only subtleties it must preserve are encoded as comments below.
    // ---------------------------------------------------------------------------------------------------------

    /** The eight ASCII delimiters of the regex {@code [\s\[\]]} ({@code \s} is ASCII-only here). */
    private static boolean isDelimiterByte(byte b) {
        return b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '\f' || b == 0x0B || b == '[' || b == ']';
    }

    /** RFC 4648 URL-safe Base64 alphabet, matching {@link Strings#BASE_64_NO_PADDING_URL_ENCODER} and {@code Arg.ENCODER}. */
    private static final byte[] B64URL = new byte[64];
    static {
        final String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        for (int i = 0; i < 64; i++) {
            B64URL[i] = (byte) alphabet.charAt(i);
        }
    }

    /**
     * Reusable per-batch scratch for {@link #splitUtf8}. A single instance is allocated once per
     * {@code mapColumnBatch} call and reused across every document in the batch, so the common case
     * allocates nothing beyond the growing backing buffers.
     */
    static final class Utf8SplitScratch {
        /** Assembled template bytes (delimiters + non-arg runs). */
        byte[] template = new byte[256];
        int templateLen;
        /**
         * Running UTF-16 length of {@link #template}. {@link Arg.Info#offsetInTemplate()} is a char index
         * (the decode/{@link #merge} path indexes into the template String by char), so arg offsets must be
         * recorded in chars, not bytes.
         */
        int templateCharLen;
        /** Args reference the source byte array directly (offset/length), so no per-arg copy is made here. */
        int[] argOff = new int[8];
        int[] argLen = new int[8];
        int[] argCharOffset = new int[8];
        int argCount;

        private final byte[] hashBytes = new byte[8];
        private final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
        private byte[] argsInfoRaw = new byte[16];
        /** Shared output buffer for the two base64 encodings; each result is consumed before the next overwrites it. */
        byte[] b64 = new byte[16];

        private void reset() {
            templateLen = 0;
            templateCharLen = 0;
            argCount = 0;
        }

        private void appendTemplate(byte[] src, int off, int len, int chars) {
            if (templateLen + len > template.length) {
                template = ArrayUtil.grow(template, templateLen + len);
            }
            System.arraycopy(src, off, template, templateLen, len);
            templateLen += len;
            templateCharLen += chars;
        }

        private void appendTemplateByte(byte b) {
            if (templateLen == template.length) {
                template = ArrayUtil.grow(template, templateLen + 1);
            }
            template[templateLen++] = b;
            templateCharLen++; // delimiters are ASCII -> one char
        }

        private void recordArg(int off, int len) {
            if (argCount == argOff.length) {
                argOff = ArrayUtil.grow(argOff, argCount + 1);
                argLen = ArrayUtil.grow(argLen, argCount + 1);
                argCharOffset = ArrayUtil.grow(argCharOffset, argCount + 1);
            }
            argOff[argCount] = off;
            argLen[argCount] = len;
            argCharOffset[argCount] = templateCharLen;
            argCount++;
        }

        /** Encodes the template id (murmur128 low half, little-endian, base64-url-no-pad) into {@link #b64}; returns its length. */
        int encodeTemplateId() {
            MurmurHash3.hash128(template, 0, templateLen, 0, hash);
            ByteUtils.writeLongLE(hash.h1, hashBytes, 0);
            ensureB64(hashBytes.length);
            return base64UrlNoPad(hashBytes, 0, hashBytes.length, b64);
        }

        /** Encodes the args-info column value ({@link Arg#encodeInfoRaw} then base64-url-no-pad) into {@link #b64}; returns its length. */
        int encodeArgsInfo() {
            final int maxRaw = Arg.encodeInfoRawMaxSize(argCount);
            if (argsInfoRaw.length < maxRaw) {
                argsInfoRaw = new byte[maxRaw];
            }
            final int rawLen = Arg.encodeInfoRaw(argCount, argCharOffset, argsInfoRaw);
            ensureB64(rawLen);
            return base64UrlNoPad(argsInfoRaw, 0, rawLen, b64);
        }

        private void ensureB64(int rawLen) {
            final int needed = ((rawLen + 2) / 3) * 4;
            if (b64.length < needed) {
                b64 = new byte[needed];
            }
        }
    }

    /**
     * Splits {@code src[off, off+len)} (well-formed UTF-8) into a template plus argument runs, writing the
     * results into {@code out}. Byte-identical in result to {@link #splitInternal}: a maximal run of
     * non-delimiter bytes is an "arg" when it contains any digit (matching {@code Arg.isArg}'s
     * {@link Character#isDigit} semantics, including non-ASCII digits) and is dropped from the template;
     * every delimiter byte and every non-arg run is appended to the template in place.
     *
     * <p>Only valid for complete values (callers route values longer than
     * {@link #MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE} bytes to the String path), so multi-byte sequences are
     * always complete and bounds checks during decoding are unnecessary.
     */
    static void splitUtf8(byte[] src, int off, int len, Utf8SplitScratch out) {
        out.reset();
        final int end = off + len;
        int i = off;
        while (i < end) {
            final byte b = src[i];
            if (isDelimiterByte(b)) {
                out.appendTemplateByte(b);
                i++;
                continue;
            }
            // Maximal non-delimiter run (a token). Continuation bytes (>= 0x80) never match a delimiter, so a
            // plain byte-by-byte advance finds the run end correctly even across multi-byte sequences.
            final int tokenStart = i;
            boolean isArg = false;
            int tokenChars = 0;
            int j = i;
            while (j < end) {
                final byte tb = src[j];
                if (isDelimiterByte(tb)) {
                    break;
                }
                if ((tb & 0xC0) != 0x80) {
                    tokenChars++; // ASCII byte or multi-byte lead: starts one char
                }
                if (tb >= '0' && tb <= '9') {
                    isArg = true;
                } else if ((tb & 0xC0) == 0xC0) { // multi-byte lead
                    if ((tb & 0xFF) >= 0xF0) {
                        // Supplementary code point: two UTF-16 units. Arg.isArg inspects UTF-16 chars with
                        // Character.isDigit(char), and surrogates are never digits, so a 4-byte sequence is
                        // never treated as a digit by the row path — match that and only count the extra unit.
                        tokenChars++;
                    } else if (isArg == false && Character.isDigit(codePointAt(src, j))) {
                        // BMP code point (2- or 3-byte): the decoded code point equals the single UTF-16 char
                        // the row path sees, so Character.isDigit agrees.
                        isArg = true;
                    }
                }
                j++;
            }
            final int tokenLen = j - tokenStart;
            if (isArg) {
                out.recordArg(tokenStart, tokenLen);
            } else {
                out.appendTemplate(src, tokenStart, tokenLen, tokenChars);
            }
            i = j;
        }
    }

    /** Decodes the (complete, well-formed) UTF-8 code point whose lead byte is at {@code i}. */
    private static int codePointAt(byte[] src, int i) {
        final int b0 = src[i] & 0xFF;
        if (b0 < 0x80) {
            return b0;
        }
        if (b0 < 0xE0) {
            return ((b0 & 0x1F) << 6) | (src[i + 1] & 0x3F);
        }
        if (b0 < 0xF0) {
            return ((b0 & 0x0F) << 12) | ((src[i + 1] & 0x3F) << 6) | (src[i + 2] & 0x3F);
        }
        return ((b0 & 0x07) << 18) | ((src[i + 1] & 0x3F) << 12) | ((src[i + 2] & 0x3F) << 6) | (src[i + 3] & 0x3F);
    }

    /** Base64-url-no-pad encodes {@code src[off, off+len)} into {@code dst} (must be large enough); returns bytes written. */
    static int base64UrlNoPad(byte[] src, int off, int len, byte[] dst) {
        int di = 0;
        int i = off;
        int remaining = len;
        while (remaining >= 3) {
            final int b0 = src[i++] & 0xFF;
            final int b1 = src[i++] & 0xFF;
            final int b2 = src[i++] & 0xFF;
            dst[di++] = B64URL[b0 >>> 2];
            dst[di++] = B64URL[((b0 & 0x3) << 4) | (b1 >>> 4)];
            dst[di++] = B64URL[((b1 & 0xF) << 2) | (b2 >>> 6)];
            dst[di++] = B64URL[b2 & 0x3F];
            remaining -= 3;
        }
        if (remaining == 1) {
            final int b0 = src[i] & 0xFF;
            dst[di++] = B64URL[b0 >>> 2];
            dst[di++] = B64URL[(b0 & 0x3) << 4];
        } else if (remaining == 2) {
            final int b0 = src[i++] & 0xFF;
            final int b1 = src[i] & 0xFF;
            dst[di++] = B64URL[b0 >>> 2];
            dst[di++] = B64URL[((b0 & 0x3) << 4) | (b1 >>> 4)];
            dst[di++] = B64URL[(b1 & 0xF) << 2];
        }
        return di;
    }
}
