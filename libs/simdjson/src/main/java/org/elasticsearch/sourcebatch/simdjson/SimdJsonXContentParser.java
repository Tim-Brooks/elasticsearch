/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch.simdjson;

import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.AbstractXContentParser;

import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.elasticsearch.sourcebatch.simdjson.Tape.DOUBLE;
import static org.elasticsearch.sourcebatch.simdjson.Tape.END_ARRAY;
import static org.elasticsearch.sourcebatch.simdjson.Tape.END_OBJECT;
import static org.elasticsearch.sourcebatch.simdjson.Tape.FALSE_VALUE;
import static org.elasticsearch.sourcebatch.simdjson.Tape.INT64;
import static org.elasticsearch.sourcebatch.simdjson.Tape.NULL_VALUE;
import static org.elasticsearch.sourcebatch.simdjson.Tape.START_ARRAY;
import static org.elasticsearch.sourcebatch.simdjson.Tape.START_OBJECT;
import static org.elasticsearch.sourcebatch.simdjson.Tape.STRING;
import static org.elasticsearch.sourcebatch.simdjson.Tape.TRUE_VALUE;

/**
 * A streaming {@link org.elasticsearch.xcontent.XContentParser} that walks the finished simdjson tape as a
 * linear cursor, bypassing the allocation-heavy {@link JsonValue} DOM.
 *
 * <p><strong>Lifecycle:</strong> one instance should be owned per thread and reused across documents:
 * <pre>{@code
 *   SimdJsonXContentParser parser = new SimdJsonXContentParser(maxDocBytes, 64);
 *   for (byte[] doc : docs) {
 *       parser.reset(doc, doc.length);
 *       parser.nextToken(); // START_OBJECT
 *       // ... walk tokens ...
 *   }
 * }</pre>
 *
 * <p><strong>Zero-copy strings:</strong> {@link #optimizedText()} returns a
 * {@link XContentString.UTF8Bytes} slice directly into the internal string buffer — no copy,
 * no allocation for repeated field names (the name cache is keyed by byte content and survives
 * {@code reset()} calls). The slice is valid only until the next {@link #reset}: the string
 * buffer is overwritten on each parse.
 *
 * <p><strong>O(1) skipChildren:</strong> {@link #skipChildren()} uses the tape's precomputed
 * matching-brace index to jump in O(1) regardless of the skipped subtree size.
 *
 * <p><strong>Not thread-safe.</strong>
 *
 * <p><strong>Benchmark scope only.</strong> Token locations, deprecation handling, and number
 * source text are stubs. {@code text()} on a {@code VALUE_NUMBER} formats back from the parsed
 * value ({@code Long.toString} / {@code Double.toString}), so formatting like {@code "1.50"} or
 * {@code "1e3"} is not round-tripped.
 */
public final class SimdJsonXContentParser extends AbstractXContentParser {

    // ------------------------------------------------------------------
    // Name cache — open-addressed, byte[] content keyed, survives reset()
    // ------------------------------------------------------------------

    private static final int CACHE_CAPACITY = 2048; // must be power of two
    private static final int CACHE_MAX_COUNT = CACHE_CAPACITY * 3 / 4; // 75% load factor

    private final String[] cacheNames = new String[CACHE_CAPACITY];
    private final byte[][] cacheKeys = new byte[CACHE_CAPACITY][];
    private int cacheCount;

    // ------------------------------------------------------------------
    // Parser state
    // ------------------------------------------------------------------

    private final SimdJsonParser simdParser;

    /** The tape produced by the most recent {@link #reset} call. */
    private Tape tape;

    /**
     * The string buffer produced by the most recent {@link #reset} call.
     *
     * <p>Layout per string: {@code [4-byte big-endian length][unescaped UTF-8 bytes]}.
     * The SIMD copy loop overshoots by up to one vector width (16–64 bytes) past the logical
     * end; never read beyond {@code offset + Integer.BYTES + len}.
     */
    private byte[] stringBuffer;

    /** Tape index of the next slot to consume. Starts at 1 (the root value). */
    private int idx;

    /** Tape index of the terminating ROOT slot (exclusive end). */
    private int endIdx;

    /** Current nesting depth; 0 = document root. */
    private int depth;

    /** Whether the next token in the current object scope is a field name. */
    private boolean expectName;

    /**
     * Whether we are inside an object ({@code true}) or array ({@code false}) at each depth.
     * Index 0 is unused (document level).
     */
    private final boolean[] inObject;

    /**
     * Tape slot of the field name saved when entering each depth level.
     * {@code nameIdxStack[d]} holds the nameIdx active when we pushed to {@code d+1}.
     */
    private final int[] nameIdxStack;

    /** Tape slot of the most recent field name, or -1 at the document root. */
    private int nameIdx = -1;

    /**
     * The tape slot whose string value {@link #currentName()} should return for the current token.
     * Set explicitly by {@link #nextToken()} for every token:
     * <ul>
     *   <li>FIELD_NAME and object-scope scalars/containers carry the relevant field-name slot.</li>
     *   <li>Array-scope scalars and any container whose immediate parent is an array carry -1 so
     *       that {@code currentName()} returns {@code null}, matching Jackson's contract.</li>
     * </ul>
     */
    private int currentNameSlot = -1;

    /** Tape slot of the current scalar value (STRING, INT64, DOUBLE, TRUE, FALSE, NULL). */
    private int valueIdx;

    private Token currentToken;
    private boolean closed;

    /**
     * Constructs a reusable parser for documents up to {@code capacity} bytes and nesting
     * up to {@code maxDepth} levels.
     *
     * @throws IllegalStateException if {@code jdk.incubator.vector} is not available at runtime
     */
    public SimdJsonXContentParser(int capacity, int maxDepth) {
        super(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS);
        this.simdParser = new SimdJsonParser(capacity, maxDepth);
        this.inObject = new boolean[maxDepth + 1];
        this.nameIdxStack = new int[maxDepth + 1];
        Arrays.fill(nameIdxStack, -1);
        // tape and stringBuffer are null until the first reset()
    }

    /**
     * Runs the two-stage simdjson parse on {@code source[0..len)} and resets the cursor to the
     * root. Call this before consuming tokens for each new document. The parser is reusable —
     * repeated calls do not allocate.
     *
     * <p><strong>Lifetime warning:</strong> any {@link XContentString.UTF8Bytes} slice returned by
     * {@link #optimizedText()} is invalidated by the next {@code reset()} call. Consumers that
     * retain slices (e.g. via {@code EscfRowBuffer}) must drain their
     * scratch state before calling {@code reset()} again.
     *
     * @param source UTF-8 JSON bytes
     * @param len    number of valid bytes in {@code source}
     * @throws JsonParsingException if the input is not valid JSON or UTF-8
     */
    public void reset(byte[] source, int len) {
        simdParser.parseToTape(source, len);
        this.tape = simdParser.tape();
        this.stringBuffer = simdParser.stringBuffer();
        this.idx = 1; // slot 0 is the ROOT header
        this.endIdx = tape.getMatchingBraceIndex(0); // slot of the terminating ROOT
        this.depth = 0;
        this.expectName = false;
        this.nameIdx = -1;
        this.currentNameSlot = -1;
        this.currentToken = null;
        this.closed = false;
    }

    // ------------------------------------------------------------------
    // Token navigation
    // ------------------------------------------------------------------

    @Override
    public Token nextToken() {
        if (closed || tape == null) {
            return currentToken = null;
        }
        if (idx >= endIdx) {
            return currentToken = null;
        }

        char t = tape.getType(idx);

        // Disambiguate key STRING vs value STRING using the expectName flag.
        if (t == STRING && expectName) {
            nameIdx = idx++;
            expectName = false;
            currentNameSlot = nameIdx;
            return currentToken = Token.FIELD_NAME;
        }

        switch (t) {
            case START_OBJECT -> {
                // Capture the parent scope before push() increments depth.
                // Objects whose immediate parent is an array have no enclosing field name.
                boolean parentIsObject = inObject[depth];
                push(true);
                idx++;
                expectName = true;
                currentNameSlot = parentIsObject ? nameIdx : -1;
                return currentToken = Token.START_OBJECT;
            }
            case START_ARRAY -> {
                // Same rule as START_OBJECT: arrays whose parent is an array have no name.
                boolean parentIsObject = inObject[depth];
                push(false);
                idx++;
                currentNameSlot = parentIsObject ? nameIdx : -1;
                return currentToken = Token.START_ARRAY;
            }
            case END_OBJECT -> {
                pop();
                idx++;
                expectName = inObject[depth];
                // After pop(), depth is the parent depth. If the parent is an array the
                // closed container was an array element and has no enclosing field name.
                currentNameSlot = inObject[depth] ? nameIdx : -1;
                return currentToken = Token.END_OBJECT;
            }
            case END_ARRAY -> {
                pop();
                idx++;
                expectName = inObject[depth];
                currentNameSlot = inObject[depth] ? nameIdx : -1;
                return currentToken = Token.END_ARRAY;
            }
            case STRING, TRUE_VALUE, FALSE_VALUE, NULL_VALUE -> {
                valueIdx = idx++;
                expectName = inObject[depth];
                // Array elements have no field name.
                currentNameSlot = inObject[depth] ? nameIdx : -1;
                return currentToken = (t == STRING ? Token.VALUE_STRING : t == NULL_VALUE ? Token.VALUE_NULL : Token.VALUE_BOOLEAN);
            }
            case INT64, DOUBLE -> {
                valueIdx = idx;
                idx += 2;
                expectName = inObject[depth];
                currentNameSlot = inObject[depth] ? nameIdx : -1;
                return currentToken = Token.VALUE_NUMBER;
            }
            default -> {
                // ROOT sentinel or unexpected — done
                return currentToken = null;
            }
        }
    }

    /**
     * Skips the current container (object or array) in O(1) using the tape's precomputed
     * matching-brace index. After return the current token is the matching END_OBJECT or
     * END_ARRAY and {@link #currentName()} returns the enclosing field name.
     */
    @Override
    public void skipChildren() throws IOException {
        if (currentToken != Token.START_OBJECT && currentToken != Token.START_ARRAY) {
            return; // no-op per XContentParser contract
        }
        // idx was incremented when we processed START, so startIdx = idx - 1
        int startIdx = idx - 1;
        // getMatchingBraceIndex gives one PAST the matching END slot
        int endSlot = tape.getMatchingBraceIndex(startIdx) - 1;
        pop(); // undo the push done in nextToken()
        idx = endSlot + 1;
        expectName = inObject[depth];
        currentNameSlot = inObject[depth] ? nameIdx : -1;
        currentToken = tape.getType(endSlot) == END_OBJECT ? Token.END_OBJECT : Token.END_ARRAY;
    }

    @Override
    public Token currentToken() {
        return currentToken;
    }

    @Override
    public String currentName() {
        if (currentNameSlot < 0) {
            return null;
        }
        int off = (int) tape.getValue(currentNameSlot);
        int len = IntegerUtils.toInt(stringBuffer, off);
        return lookupName(off + Integer.BYTES, len);
    }

    // ------------------------------------------------------------------
    // String / text access
    // ------------------------------------------------------------------

    @Override
    public String text() {
        return switch (currentToken) {
            case VALUE_STRING -> {
                int off = (int) tape.getValue(valueIdx);
                int len = IntegerUtils.toInt(stringBuffer, off);
                yield new String(stringBuffer, off + Integer.BYTES, len, StandardCharsets.UTF_8);
            }
            case VALUE_NUMBER -> tape.getType(valueIdx) == INT64
                ? Long.toString(tape.getInt64Value(valueIdx))
                : Double.toString(tape.getDouble(valueIdx));
            case VALUE_BOOLEAN -> Boolean.toString(tape.getType(valueIdx) == TRUE_VALUE);
            case VALUE_NULL -> "null";
            default -> throw new IllegalStateException("text() called on non-value token: " + currentToken);
        };
    }

    /**
     * Returns a zero-copy view of the current string value directly into the internal string
     * buffer. The returned slice is valid until the next {@link #reset(byte[], int)} call.
     *
     * <p>Only valid when {@link #currentToken()} is {@code VALUE_STRING}. For other tokens,
     * falls back to a wrapped {@link Text#Text(String)} allocation.
     */
    @Override
    public XContentString optimizedText() {
        if (currentToken == Token.VALUE_STRING) {
            int off = (int) tape.getValue(valueIdx);
            int len = IntegerUtils.toInt(stringBuffer, off);
            // 1-arg Text constructor: stringLength() is computed lazily only if needed
            // (e.g. numeric-string coercion), avoiding an unconditional UTF-16 re-scan.
            return new Text(new XContentString.UTF8Bytes(stringBuffer, off + Integer.BYTES, len));
        }
        return new Text(text());
    }

    @Override
    public CharBuffer charBuffer() {
        return CharBuffer.wrap(text());
    }

    @Override
    public Object objectText() {
        return switch (currentToken) {
            case VALUE_STRING -> text();
            case VALUE_NUMBER -> numberValue();
            case VALUE_BOOLEAN -> doBooleanValue();
            case VALUE_NULL -> null;
            default -> text();
        };
    }

    @Override
    public Object objectBytes() {
        return switch (currentToken) {
            case VALUE_STRING -> charBuffer();
            case VALUE_NUMBER -> numberValue();
            case VALUE_BOOLEAN -> doBooleanValue();
            case VALUE_NULL -> null;
            default -> charBuffer();
        };
    }

    @Override
    public boolean hasTextCharacters() {
        return false;
    }

    @Override
    public char[] textCharacters() throws IOException {
        return text().toCharArray();
    }

    @Override
    public int textLength() throws IOException {
        return text().length();
    }

    @Override
    public int textOffset() throws IOException {
        return 0;
    }

    // ------------------------------------------------------------------
    // Numbers
    // ------------------------------------------------------------------

    @Override
    public Number numberValue() {
        return tape.getType(valueIdx) == INT64 ? tape.getInt64Value(valueIdx) : tape.getDouble(valueIdx);
    }

    @Override
    public NumberType numberType() {
        return tape.getType(valueIdx) == INT64 ? NumberType.LONG : NumberType.DOUBLE;
    }

    @Override
    protected boolean doBooleanValue() {
        return tape.getType(valueIdx) == TRUE_VALUE;
    }

    @Override
    protected long doLongValue() {
        if (tape.getType(valueIdx) == INT64) {
            return tape.getInt64Value(valueIdx);
        }
        return (long) tape.getDouble(valueIdx);
    }

    @Override
    protected double doDoubleValue() {
        if (tape.getType(valueIdx) == DOUBLE) {
            return tape.getDouble(valueIdx);
        }
        return tape.getInt64Value(valueIdx);
    }

    @Override
    protected int doIntValue() {
        return (int) doLongValue();
    }

    @Override
    protected short doShortValue() {
        return (short) doLongValue();
    }

    @Override
    protected float doFloatValue() {
        return (float) doDoubleValue();
    }

    // ------------------------------------------------------------------
    // Stubs — not needed for benchmark / batch encoding
    // ------------------------------------------------------------------

    @Override
    public byte[] binaryValue() {
        // JSON never yields VALUE_EMBEDDED_OBJECT, so this is unreachable in practice.
        throw new UnsupportedOperationException("binaryValue() is not supported by SimdJsonXContentParser");
    }

    @Override
    public XContentLocation getTokenLocation() {
        return XContentLocation.UNKNOWN; // tape stores no source byte offsets
    }

    @Override
    public XContentLocation getCurrentLocation() {
        return XContentLocation.UNKNOWN;
    }

    @Override
    public XContentType contentType() {
        return XContentType.JSON;
    }

    @Override
    public void allowDuplicateKeys(boolean allowDuplicateKeys) {
        // no-op: simdjson has no duplicate-key detection. Callers (e.g. EscfEncoder) pass
        // allowDuplicateKeys=true, so the absence of detection is correct.
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
    }

    // ------------------------------------------------------------------
    // Depth stack helpers
    // ------------------------------------------------------------------

    private void push(boolean isObjectScope) {
        nameIdxStack[depth] = nameIdx;
        depth++;
        inObject[depth] = isObjectScope;
    }

    private void pop() {
        depth--;
        nameIdx = nameIdxStack[depth];
    }

    // ------------------------------------------------------------------
    // Field-name cache
    // ------------------------------------------------------------------

    /**
     * Returns the canonical {@link String} for the field name at {@code stringBuffer[off, off+len)},
     * consulting an open-addressed cache that survives across {@link #reset} calls. Cache hits
     * return zero allocation; misses decode UTF-8 and store the result. Once the cache reaches
     * its capacity limit, decodes fall through without caching so unbounded distinct-key input
     * cannot exhaust heap.
     */
    private String lookupName(int off, int len) {
        int h = fnvHash(stringBuffer, off, len) & (CACHE_CAPACITY - 1);
        for (int i = h;; i = (i + 1) & (CACHE_CAPACITY - 1)) {
            byte[] key = cacheKeys[i];
            if (key == null) {
                // cache miss — decode and insert if there is room
                String s = new String(stringBuffer, off, len, StandardCharsets.UTF_8);
                if (cacheCount < CACHE_MAX_COUNT) {
                    byte[] copy = Arrays.copyOfRange(stringBuffer, off, off + len);
                    cacheKeys[i] = copy;
                    cacheNames[i] = s;
                    cacheCount++;
                }
                return s;
            }
            if (Arrays.equals(key, 0, key.length, stringBuffer, off, off + len)) {
                return cacheNames[i]; // cache hit — zero allocation
            }
            // collision — linear probe
        }
    }

    private static int fnvHash(byte[] b, int off, int len) {
        int h = 0x811c9dc5;
        for (int i = off, end = off + len; i < end; i++) {
            h ^= b[i] & 0xFF;
            h *= 0x01000193;
        }
        return h;
    }
}
