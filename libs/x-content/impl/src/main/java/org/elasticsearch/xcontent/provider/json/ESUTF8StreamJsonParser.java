/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider.json;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.json.UTF8StreamJsonParser;
import com.fasterxml.jackson.core.sym.ByteQuadsCanonicalizer;

import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.provider.OptimizedTextCapable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class ESUTF8StreamJsonParser extends UTF8StreamJsonParser implements OptimizedTextCapable {
    protected int stringEnd = -1;
    protected int stringLength;

    private final List<Integer> backslashes = new ArrayList<>();

    public ESUTF8StreamJsonParser(
        IOContext ctxt,
        int features,
        InputStream in,
        ObjectCodec codec,
        ByteQuadsCanonicalizer sym,
        byte[] inputBuffer,
        int start,
        int end,
        int bytesPreProcessed,
        boolean bufferRecyclable
    ) {
        super(ctxt, features, in, codec, sym, inputBuffer, start, end, bytesPreProcessed, bufferRecyclable);
    }

    /**
     * Method that will try to get underlying UTF-8 encoded bytes of the current string token.
     * This is only a best-effort attempt; if there is some reason the bytes cannot be retrieved, this method will return null.
     */
    @Override
    public Text getValueAsText() throws IOException {
        if (_currToken == JsonToken.VALUE_STRING && _tokenIncomplete) {
            if (stringEnd > 0) {
                final int len = stringEnd - 1 - _inputPtr;
                return new Text(new XContentString.UTF8Bytes(_inputBuffer, _inputPtr, len), stringLength);
            }
            return _finishAndReturnText();
        }
        return null;
    }

    protected Text _finishAndReturnText() throws IOException {
        int ptr = _inputPtr;
        if (ptr >= _inputEnd) {
            _loadMoreGuaranteed();
            ptr = _inputPtr;
        }

        final int startPtr = ptr;
        final byte[] inputBuffer = _inputBuffer;
        final int max = _inputEnd;
        int codePointCount = 0;

        // Fast path: scan for quote or backslash first, counting code points as we go
        while (ptr < max) {
            byte b = inputBuffer[ptr];
            if (b == INT_QUOTE) {
                // Found end quote - string has no escapes
                int byteLength = ptr - startPtr;
                stringLength = codePointCount;
                stringEnd = ptr + 1;
                return new Text(new XContentString.UTF8Bytes(inputBuffer, startPtr, byteLength), codePointCount);
            }
            if (b == INT_BACKSLASH) {
                // Found escape - switch to escape handling
                break;
            }
            // For bytes < 128 (ASCII), we can skip the codes table lookup
            if (b >= 0) {
                codePointCount++;
                ptr++;
            } else {
                // Non-ASCII handling...
                int c = b & 0xFF;
                int codeType = INPUT_CODES_UTF8[c];
                if (codeType == 0) {
                    codePointCount++;
                    ptr++;
                } else if (codeType >= 2 && codeType <= 4) {
                    if (ptr + codeType > max) {
                        return null;
                    }
                    codePointCount++;
                    ptr += codeType;
                } else {
                    return null;
                }
            }
        }

        // Escape handling path - continue counting code points during the scan
        if (ptr >= max) {
            return null;
        }

        int[] escapePositions = new int[16]; // Small initial size

        int escapeCount = 0;
        int scanPtr = ptr;

        // Scan to find escapes and end quote, continuing to count code points
        while (scanPtr < max) {
            byte b = inputBuffer[scanPtr];
            if (b == INT_QUOTE) {
                break; // Found end
            }

            if (b == INT_BACKSLASH) {
                // Grow array if needed
                if (escapeCount >= escapePositions.length) {
                    int[] newArray = new int[escapePositions.length * 2];
                    System.arraycopy(escapePositions, 0, newArray, 0, escapeCount);
                    escapePositions = newArray;
                }
                escapePositions[escapeCount++] = scanPtr;

                scanPtr++;
                if (scanPtr >= max) {
                    return null;
                }
                b = inputBuffer[scanPtr];
                if (b == '"' || b == '/' || b == '\\') {
                    codePointCount++; // The escaped character counts as 1 code point
                    scanPtr++;
                } else {
                    return null; // Unsupported escape
                }
            } else if (b >= 0) {
                codePointCount++;
                scanPtr++;
            } else {
                // Non-ASCII
                int c = b & 0xFF;
                int codeType = INPUT_CODES_UTF8[c];
                if (codeType == 0) {
                    codePointCount++;
                    scanPtr++;
                } else if (codeType >= 2 && codeType <= 4) {
                    if (scanPtr + codeType > max) {
                        return null;
                    }
                    codePointCount++;
                    scanPtr += codeType;
                } else {
                    return null;
                }
            }
        }

        if (scanPtr >= max) {
            return null; // Didn't find closing quote
        }

        stringEnd = scanPtr + 1;

        // Calculate exact byte size: total bytes minus number of backslashes
        int exactByteSize = (scanPtr - startPtr) - escapeCount;

        // Allocate exact size buffer
        byte[] resultBuffer = new byte[exactByteSize];
        int writePos = 0;

        // Copy everything before the first backslash
        int beforeEscapeLength = ptr - startPtr;
        System.arraycopy(inputBuffer, startPtr, resultBuffer, 0, beforeEscapeLength);
        writePos = beforeEscapeLength;

        // Second pass: process escapes (we already have the correct code point count)
        while (ptr < scanPtr) {
            byte b = inputBuffer[ptr];

            if (b == INT_BACKSLASH) {
                ptr++; // Skip backslash
                b = inputBuffer[ptr]; // Get escaped character
                resultBuffer[writePos++] = b;
                ptr++;
            } else if (b >= 0) {
                // ASCII
                resultBuffer[writePos++] = b;
                ptr++;
            } else {
                // Non-ASCII - copy multi-byte sequence
                int c = b & 0xFF;
                int codeType = INPUT_CODES_UTF8[c];
                if (codeType == 0) {
                    resultBuffer[writePos++] = b;
                    ptr++;
                } else if (codeType >= 2 && codeType <= 4) {
                    System.arraycopy(inputBuffer, ptr, resultBuffer, writePos, codeType);
                    writePos += codeType;
                    ptr += codeType;
                }
            }
        }

        stringLength = codePointCount;
        return new Text(new XContentString.UTF8Bytes(resultBuffer), codePointCount);
    }

    public boolean writeUTF8TextToStream(OutputStream out) throws IOException {
        if (_currToken == JsonToken.VALUE_STRING && _tokenIncomplete) {
            if (stringEnd > 0) {
                final int len = stringEnd - 1 - _inputPtr;
                XContentString.UTF8Bytes utf8Bytes = new XContentString.UTF8Bytes(_inputBuffer, _inputPtr, len);
                out.write(utf8Bytes.bytes(), utf8Bytes.offset(), utf8Bytes.length());
                return true;
            }
            return _finishAndWriteToStream(out);
        } else {
            return false;
        }
    }

    protected boolean _finishAndWriteToStream(OutputStream out) throws IOException {
        int ptr = _inputPtr;
        if (ptr >= _inputEnd) {
            _loadMoreGuaranteed();
            ptr = _inputPtr;
        }

        final int startPtr = ptr;
        final byte[] inputBuffer = _inputBuffer;
        final int max = _inputEnd;

        // Fast path: scan for quote or backslash first
        while (ptr < max) {
            byte b = inputBuffer[ptr];
            if (b == INT_QUOTE) {
                // Found end quote - string has no escapes
                // Write the entire string directly to output
                out.write(inputBuffer, startPtr, ptr - startPtr);
                stringEnd = ptr + 1;
                return true;
            }
            if (b == INT_BACKSLASH) {
                // Found escape - switch to escape handling
                break;
            }
            // For bytes < 128 (ASCII), we can skip the codes table lookup
            if (b >= 0) {
                ptr++;
            } else {
                // Non-ASCII handling...
                int c = b & 0xFF;
                int codeType = INPUT_CODES_UTF8[c];
                if (codeType == 0) {
                    ptr++;
                } else if (codeType >= 2 && codeType <= 4) {
                    if (ptr + codeType > max) {
                        return false;
                    }
                    ptr += codeType;
                } else {
                    return false;
                }
            }
        }

        // Escape handling path - optimized for streaming
        if (ptr >= max) {
            return false;
        }

        // Write everything before the first backslash
        if (ptr > startPtr) {
            out.write(inputBuffer, startPtr, ptr - startPtr);
        }

        // Process escaped content
        while (ptr < max) {
            byte b = inputBuffer[ptr];
            if (b == INT_QUOTE) {
                // End of string
                stringEnd = ptr + 1;
                return true;
            }

            if (b == INT_BACKSLASH) {
                ptr++;
                if (ptr >= max) {
                    return false;
                }
                b = inputBuffer[ptr];
                // Only handle simple escapes
                if (b == '"' || b == '/' || b == '\\') {
                    out.write(b);
                    ptr++;
                } else {
                    // Unsupported escape
                    return false;
                }
            } else if (b >= 0) {
                // ASCII - write single byte
                out.write(b);
                ptr++;
            } else {
                // Non-ASCII
                int c = b & 0xFF;
                int codeType = INPUT_CODES_UTF8[c];
                if (codeType == 0) {
                    out.write(b);
                    ptr++;
                } else if (codeType >= 2 && codeType <= 4) {
                    if (ptr + codeType > max) {
                        return false;
                    }
                    // Write multi-byte sequence directly
                    out.write(inputBuffer, ptr, codeType);
                    ptr += codeType;
                } else {
                    return false;
                }
            }
        }

        return false; // Didn't find closing quote
    }

    @Override
    public JsonToken nextToken() throws IOException {
        if (_currToken == JsonToken.VALUE_STRING && _tokenIncomplete && stringEnd > 0) {
            _inputPtr = stringEnd;
            _tokenIncomplete = false;
        }
        stringEnd = -1;
        return super.nextToken();
    }

    @Override
    public boolean nextFieldName(SerializableString str) throws IOException {
        if (_currToken == JsonToken.VALUE_STRING && _tokenIncomplete && stringEnd > 0) {
            _inputPtr = stringEnd;
            _tokenIncomplete = false;
        }
        stringEnd = -1;
        return super.nextFieldName(str);
    }

    @Override
    public String nextFieldName() throws IOException {
        if (_currToken == JsonToken.VALUE_STRING && _tokenIncomplete && stringEnd > 0) {
            _inputPtr = stringEnd;
            _tokenIncomplete = false;
        }
        stringEnd = -1;
        return super.nextFieldName();
    }
}
