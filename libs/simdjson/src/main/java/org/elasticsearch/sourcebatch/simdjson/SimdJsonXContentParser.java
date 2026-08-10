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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
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
    // Name cache — open-addressed, quad-keyed, survives reset()
    //
    // For names up to MAX_INLINE_BYTES (16 bytes), the key bytes are
    // packed into up to MAX_INLINE_QUADS (4) int slots stored inline in
    // cacheQuads[], compared as ints — no Arrays.equals call, no byte[]
    // indirection. Longer names keep a byte[] copy in cacheKeys[] and
    // use the old Arrays.equals path.
    //
    // The stored hash (cacheHashes[]) acts as a cheap prefilter: only
    // when the 32-bit hash matches do we compare lengths and then quads.
    // Empty slots have cacheHashes[i] == 0 AND cacheNames[i] == null;
    // a key that hashes to 0 is treated as having hash 1 (one-off in
    // hashName()) to keep the slot-empty sentinel unambiguous.
    //
    // Hash: native-order 64-bit VarHandle reads, 4-bytes at a time, with
    // individually loaded tail bytes (never reads past off+len, so the
    // SIMD string-buffer overshoot cannot contaminate the key).
    // ------------------------------------------------------------------

    private static final int CACHE_CAPACITY = 2048; // must be power of two
    private static final int CACHE_MAX_COUNT = CACHE_CAPACITY * 3 / 4; // 75% load factor
    /** Field names longer than this fall back to a byte[] copy + Arrays.equals. */
    private static final int MAX_INLINE_BYTES = 16;
    /** Number of int quads needed per cache slot for inline keys. */
    private static final int MAX_INLINE_QUADS = MAX_INLINE_BYTES / Integer.BYTES; // 4

    /** VarHandle for reading 4 bytes from a byte[] as a native-order int. */
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());

    private final String[] cacheNames = new String[CACHE_CAPACITY];
    /** Stored hash per slot; 0 means empty. Keys that hash to 0 are stored as 1. */
    private final int[] cacheHashes = new int[CACHE_CAPACITY];
    /** Stored byte length per slot. */
    private final int[] cacheLens = new int[CACHE_CAPACITY];
    /**
     * Inline int quads: {@code cacheQuads[i * MAX_INLINE_QUADS + q]} holds the q-th quad of
     * the name at slot i. Only populated when {@code cacheLens[i] <= MAX_INLINE_BYTES}.
     */
    private final int[] cacheQuads = new int[CACHE_CAPACITY * MAX_INLINE_QUADS];
    /** Byte-array copies for names longer than {@link #MAX_INLINE_BYTES}. Null otherwise. */
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
     *
     * <p>For names up to {@value #MAX_INLINE_BYTES} bytes, key comparison is done via up to
     * {@value #MAX_INLINE_QUADS} int-equality checks (no {@code Arrays.equals} call). Longer names
     * compare against a stored {@code byte[]} copy.
     *
     * <p>No byte is read beyond {@code stringBuffer[off + len - 1]}: the SIMD string-buffer copy
     * loop overshoots the logical string end by up to one vector width, so those bytes are
     * in-bounds but may contain stale data. Tail bytes (remainder after the last full int) are
     * loaded individually rather than as a masked aligned int.
     */
    private String lookupName(int off, int len) {
        int h = hashName(stringBuffer, off, len);
        int slot = h & (CACHE_CAPACITY - 1);
        for (int i = slot;; i = (i + 1) & (CACHE_CAPACITY - 1)) {
            int sh = cacheHashes[i];
            if (sh == 0) {
                // empty slot — cache miss
                String s = new String(stringBuffer, off, len, StandardCharsets.UTF_8);
                if (cacheCount < CACHE_MAX_COUNT) {
                    cacheHashes[i] = h;
                    cacheLens[i] = len;
                    cacheNames[i] = s;
                    if (len <= MAX_INLINE_BYTES) {
                        storeInlineQuads(i, stringBuffer, off, len);
                    } else {
                        cacheKeys[i] = Arrays.copyOfRange(stringBuffer, off, off + len);
                    }
                    cacheCount++;
                }
                return s;
            }
            if (sh == h && cacheLens[i] == len && keysMatch(i, stringBuffer, off, len)) {
                return cacheNames[i]; // cache hit — zero allocation
            }
            // hash or length mismatch — linear probe
        }
    }

    /**
     * Compares the key at cache slot {@code i} against {@code buf[off, off+len)}.
     * For short keys (≤ {@value #MAX_INLINE_BYTES}), compares stored int quads;
     * for long keys, delegates to {@code Arrays.equals}.
     */
    private boolean keysMatch(int i, byte[] buf, int off, int len) {
        if (len <= MAX_INLINE_BYTES) {
            int base = i * MAX_INLINE_QUADS;
            int fullQuads = len >>> 2;         // number of complete 4-byte groups
            int tail = len & 3;                // remaining bytes after the last full group
            // Compare complete 4-byte groups as ints
            for (int q = 0; q < fullQuads; q++) {
                if (cacheQuads[base + q] != (int) INT_HANDLE.get(buf, off + q * Integer.BYTES)) {
                    return false;
                }
            }
            // Compare tail bytes individually (never reads past off+len)
            int tailOff = off + fullQuads * Integer.BYTES;
            int storedTail = cacheQuads[base + fullQuads]; // pre-assembled tail int from insert
            return switch (tail) {
                case 0 -> true;
                case 1 -> (storedTail & 0xFF) == (buf[tailOff] & 0xFF);
                case 2 -> (storedTail & 0xFFFF) == ((buf[tailOff] & 0xFF) | ((buf[tailOff + 1] & 0xFF) << 8));
                case 3 -> storedTail == ((buf[tailOff] & 0xFF) | ((buf[tailOff + 1] & 0xFF) << 8) | ((buf[tailOff + 2] & 0xFF) << 16));
                default -> throw new AssertionError("impossible tail: " + tail);
            };
        }
        byte[] key = cacheKeys[i];
        return Arrays.equals(key, 0, key.length, buf, off, off + len);
    }

    /**
     * Packs the name bytes at {@code buf[off, off+len)} into the inline quad slots for cache
     * slot {@code i}. Called only when {@code len <= MAX_INLINE_BYTES}. Tail bytes (the partial
     * last group, if any) are stored in a single int without reading past {@code off+len}.
     */
    private void storeInlineQuads(int i, byte[] buf, int off, int len) {
        int base = i * MAX_INLINE_QUADS;
        int fullQuads = len >>> 2;
        int tail = len & 3;
        for (int q = 0; q < fullQuads; q++) {
            cacheQuads[base + q] = (int) INT_HANDLE.get(buf, off + q * Integer.BYTES);
        }
        if (tail > 0) {
            int tailOff = off + fullQuads * Integer.BYTES;
            int t = buf[tailOff] & 0xFF;
            if (tail >= 2) t |= (buf[tailOff + 1] & 0xFF) << 8;
            if (tail == 3) t |= (buf[tailOff + 2] & 0xFF) << 16;
            cacheQuads[base + fullQuads] = t;
        }
    }

    /**
     * Computes a 32-bit hash of {@code buf[off, off+len)} suitable for indexing the name cache.
     * Reads 4 bytes at a time via a native-order {@link VarHandle}; never reads beyond
     * {@code off + len}. Returns 1 instead of 0 so that the value 0 can serve as the
     * empty-slot sentinel in {@link #cacheHashes}.
     */
    private static int hashName(byte[] buf, int off, int len) {
        // Murmur3-style: 32-bit multiply-xor with a final avalanche.
        int h = 0x9747b28c ^ len;
        int pos = off;
        int rem = len;
        while (rem >= 4) {
            int k = (int) INT_HANDLE.get(buf, pos);
            k *= 0xcc9e2d51;
            k = Integer.rotateLeft(k, 15);
            k *= 0x1b873593;
            h ^= k;
            h = Integer.rotateLeft(h, 13);
            h = h * 5 + 0xe6546b64;
            pos += 4;
            rem -= 4;
        }
        // Tail: 1–3 bytes, loaded individually (never past off+len)
        if (rem > 0) {
            int k = buf[pos] & 0xFF;
            if (rem >= 2) k |= (buf[pos + 1] & 0xFF) << 8;
            if (rem == 3) k |= (buf[pos + 2] & 0xFF) << 16;
            k *= 0xcc9e2d51;
            k = Integer.rotateLeft(k, 15);
            k *= 0x1b873593;
            h ^= k;
        }
        // Avalanche
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        // Map 0 to 1 so 0 stays the empty-slot sentinel
        return h == 0 ? 1 : h;
    }
}
