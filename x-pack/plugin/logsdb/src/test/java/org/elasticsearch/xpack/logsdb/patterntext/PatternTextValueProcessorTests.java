/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class PatternTextValueProcessorTests extends ESTestCase {

    public void testEmpty() throws IOException {
        String text = "";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals(text, parts.template());
        assertTrue(parts.args().isEmpty());
        assertEquals(text, PatternTextValueProcessor.merge(parts));
    }

    public void testWhitespace() throws IOException {
        String text = " ";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals(text, parts.template());
        assertTrue(parts.args().isEmpty());
        assertEquals(text, PatternTextValueProcessor.merge(parts));
    }

    public void testWithoutTimestamp() throws IOException {
        String text = " some text with arg1 and 2arg2 and 333 ";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals(" some text with  and  and  ", parts.template());
        assertThat(parts.args(), Matchers.contains("arg1", "2arg2", "333"));
        assertThat(parts.argsInfo(), equalTo(info(16, 21, 26)));
        assertEquals(text, PatternTextValueProcessor.merge(parts));
    }

    public void testWithTimestamp() throws IOException {
        String text = " 2021-04-13T13:51:38.000Z some text with arg1 and arg2 and arg3";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals("  some text with  and  and ", parts.template());
        assertThat(parts.args(), Matchers.contains("2021-04-13T13:51:38.000Z", "arg1", "arg2", "arg3"));
        assertThat(parts.argsInfo(), equalTo(info(1, 17, 22, 27)));
        assertEquals(text, PatternTextValueProcessor.merge(parts));
    }

    public void testWithDateSpaceTime() throws IOException {
        String text = " 2021-04-13 13:51:38 some text with arg1 and arg2 and arg3";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals("   some text with  and  and ", parts.template());
        assertThat(parts.argsInfo(), equalTo(info(1, 2, 18, 23, 28)));
        assertThat(parts.args(), Matchers.contains("2021-04-13", "13:51:38", "arg1", "arg2", "arg3"));
        assertEquals(text, PatternTextValueProcessor.merge(parts));
    }

    public void testMalformedDate() throws IOException {
        String text = "2020/09/06 10:11:38 Using namespace: kubernetes-dashboard' | HTTP status: 400, message: [1:395]";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals("  Using namespace: kubernetes-dashboard' | HTTP status:  message: []", parts.template());
        assertThat(parts.argsInfo(), equalTo(info(0, 1, 56, 67)));
        assertThat(parts.args(), Matchers.contains("2020/09/06", "10:11:38", "400,", "1:395"));
        assertEquals(text, PatternTextValueProcessor.merge(parts));
    }

    public void testUUID() throws IOException {
        String text = "[2020-08-18T00:58:56.751+00:00][15][2354][action_controller][INFO]: [18be2355-6306-4a00-9db9-f0696aa1a225] "
            + "some text with arg1 and arg2";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals("[][][][action_controller][INFO]: [] some text with  and ", parts.template());
        assertThat(parts.argsInfo(), equalTo(info(1, 3, 5, 34, 51, 56)));
        assertThat(
            parts.args(),
            Matchers.contains("2020-08-18T00:58:56.751+00:00", "15", "2354", "18be2355-6306-4a00-9db9-f0696aa1a225", "arg1", "arg2")
        );
        assertEquals(text, PatternTextValueProcessor.merge(parts));
    }

    public void testIP() throws IOException {
        String text = "[2020-08-18T00:58:56.751+00:00][15][2354][action_controller][INFO]: from 94.168.152.150 and arg1";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals("[][][][action_controller][INFO]: from  and ", parts.template());
        assertThat(parts.argsInfo(), equalTo(info(1, 3, 5, 38, 43)));
        assertThat(parts.args(), Matchers.contains("2020-08-18T00:58:56.751+00:00", "15", "2354", "94.168.152.150", "arg1"));
        assertEquals(text, PatternTextValueProcessor.merge(parts));
    }

    public void testSecondDate() throws IOException {
        String text = "[2020-08-18T00:58:56.751+00:00][15][2354][action_controller][INFO]: at 2020-08-18 00:58:56 +0000 and arg1";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals("[][][][action_controller][INFO]: at    and ", parts.template());
        assertThat(parts.argsInfo(), equalTo(info(1, 3, 5, 36, 37, 38, 43)));
        assertThat(
            parts.args(),
            Matchers.contains("2020-08-18T00:58:56.751+00:00", "15", "2354", "2020-08-18", "00:58:56", "+0000", "arg1")
        );
        assertEquals(text, PatternTextValueProcessor.merge(parts));
    }

    public void testWithTimestampStartBrackets() throws IOException {
        String text = "[2020-08-18T00:58:56] Found 123 errors for service [cheddar1]";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals("[] Found  errors for service []", parts.template());
        assertThat(parts.argsInfo(), equalTo(info(1, 9, 30)));
        assertThat(parts.args(), Matchers.contains("2020-08-18T00:58:56", "123", "cheddar1"));
        assertEquals(text, PatternTextValueProcessor.merge(parts));
    }

    public void testTemplateIdIsExpectedShape() throws IOException {
        String text = "[2020-08-18T00:58:56] Found 123 errors for service [cheddar1]";
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);
        assertEquals("1l_PtCLQ5xY", parts.templateId());
    }

    public void testTemplateIdHasVeryFewCollisions() throws IOException {
        Set<String> templates = new HashSet<>();
        Set<String> ids = new HashSet<>();

        for (int i = 0; i < 1000; i++) {
            var template = randomTemplate();
            var parts = new PatternTextValueProcessor.Parts(template, List.of(), List.of());
            templates.add(template);
            ids.add(parts.templateId());
        }
        // This can technically fail due to hash collision, but it should happen quite rarely.
        assertEquals(templates.size(), ids.size());
    }

    private static String randomTemplate() {
        StringBuilder sb = new StringBuilder();
        int numTokens = randomIntBetween(1, 20);
        for (int i = 0; i < numTokens; i++) {
            var token = randomBoolean() ? randomAlphaOfLength(between(1, 10)) : "";
            sb.append(token);
            sb.append(randomDelimiter());
        }
        return sb.toString();
    }

    private static String randomDelimiter() {
        return randomFrom(List.of(" ", "\n", "\t", "[", "]"));
    }

    private static List<Arg.Info> info(int... offsets) throws IOException {
        return Arrays.stream(offsets).mapToObj(o -> new Arg.Info(Arg.Type.GENERIC, o)).toList();
    }

    // ----- Byte-level splitUtf8 (columnar batch path) must be byte-identical to the String split path -----

    public void testSplitUtf8MatchesStringPathFixedCases() throws IOException {
        // The same inputs exercised by the String-path tests above, plus delimiter/Unicode edge cases.
        for (String text : List.of(
            "",
            " ",
            "\t\n\f\r[]",
            "no-delimiters-no-digits",
            " some text with arg1 and 2arg2 and 333 ",
            " 2021-04-13T13:51:38.000Z some text with arg1 and arg2 and arg3",
            "[2020-08-18T00:58:56] Found 123 errors for service [cheddar1]",
            "[a][b][c] plain words only",
            "   leading and trailing spaces   ",
            "consecutive[[]]brackets",
            // Non-ASCII, no digits: stays in the template, contributes multi-byte chars to arg offsets.
            "café münchen 数据 value7 end",
            // BMP non-ASCII decimal digits (Character.isDigit(char) true) make their token an arg: Arabic-Indic, Devanagari.
            "token٣ and ३abc plain",
            // Supplementary (4-byte) code points. Arg.isArg inspects UTF-16 chars, and surrogates are never digits,
            // so even a "mathematical bold digit" (𝟎, U+1D7CE) is NOT treated as a digit — these stay in the template.
            "emoji😀 here 𝟎 math tail",
            "😀😀 word value42"
        )) {
            assertSplitEquivalent(text);
        }
    }

    public void testSplitUtf8MatchesStringPathRandom() throws IOException {
        for (int iter = 0; iter < 2000; iter++) {
            assertSplitEquivalent(randomLogLine());
        }
    }

    private static String randomLogLine() {
        StringBuilder sb = new StringBuilder();
        int numTokens = randomIntBetween(0, 12);
        for (int i = 0; i < numTokens; i++) {
            if (randomBoolean()) {
                sb.append(randomLogDelimiter());
            }
            sb.append(randomToken());
        }
        // Random trailing delimiters (exercise the trailing-empty-token handling of the String split path).
        int trailing = randomIntBetween(0, 3);
        for (int i = 0; i < trailing; i++) {
            sb.append(randomLogDelimiter());
        }
        return sb.toString();
    }

    private static String randomToken() {
        return switch (randomIntBetween(0, 6)) {
            case 0 -> randomAlphaOfLength(between(1, 8));
            case 1 -> Integer.toString(randomInt());
            case 2 -> "2021-04-13T13:51:38.000Z";
            case 3 -> "18be2355-6306-4a00-9db9-f0696aa1a225";
            case 4 -> "caféññten"; // multi-byte, no digit
            case 5 -> "token٣३"; // BMP multi-byte Unicode digits -> arg
            case 6 -> "𝟎😀"; // supplementary code points -> NOT an arg (surrogates are never digits)
            default -> throw new AssertionError();
        };
    }

    private static String randomLogDelimiter() {
        return randomFrom(List.of(" ", "\n", "\t", "\r", "\f", "", "[", "]"));
    }

    /** Asserts the byte-level {@code splitUtf8} produces template/templateId/argsInfo/args byte-identical to {@code split}. */
    private static void assertSplitEquivalent(String text) throws IOException {
        final byte[] src = text.getBytes(StandardCharsets.UTF_8);
        final PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(text);

        final PatternTextValueProcessor.Utf8SplitScratch scratch = new PatternTextValueProcessor.Utf8SplitScratch();
        PatternTextValueProcessor.splitUtf8(src, 0, src.length, scratch);

        assertArrayEquals(
            "template bytes for [" + text + "]",
            parts.template().getBytes(StandardCharsets.UTF_8),
            Arrays.copyOf(scratch.template, scratch.templateLen)
        );

        final int idLen = scratch.encodeTemplateId();
        assertEquals("templateId for [" + text + "]", parts.templateId(), new String(scratch.b64, 0, idLen, StandardCharsets.US_ASCII));

        final int infoLen = scratch.encodeArgsInfo();
        assertEquals(
            "argsInfo for [" + text + "]",
            Arg.encodeInfo(parts.argsInfo()),
            new String(scratch.b64, 0, infoLen, StandardCharsets.US_ASCII)
        );

        assertEquals("arg count for [" + text + "]", parts.args().size(), scratch.argCount);
        final StringBuilder joined = new StringBuilder();
        for (int a = 0; a < scratch.argCount; a++) {
            if (a > 0) {
                joined.append(' ');
            }
            joined.append(new String(src, scratch.argOff[a], scratch.argLen[a], StandardCharsets.UTF_8));
        }
        final String expectedArgs = parts.args().isEmpty() ? "" : Arg.encodeRemainingArgs(parts);
        assertEquals("joined args for [" + text + "]", expectedArgs, joined.toString());
    }
}
