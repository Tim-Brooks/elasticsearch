/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.doc

import groovy.transform.PackageScope
import org.elasticsearch.gradle.internal.doc.SnippetsTask.Snippet
import org.gradle.api.InvalidUserDataException
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.model.ObjectFactory

import javax.inject.Inject;
import java.nio.file.Files
import java.nio.file.Path

/**
 * Generates REST tests for each snippet marked // TEST.
 */
class RestTestsFromSnippetsTask extends SnippetsTask {
    /**
     * These languages aren't supported by the syntax highlighter so we
     * shouldn't use them.
     */
    private static final List BAD_LANGUAGES = ['json', 'javascript']

    /**
     * Test setups defined in the build instead of the docs so they can be
     * shared between many doc files.
     */
    @Input
    Map<String, String> setups = new HashMap()

    /**
     * Test teardowns defined in the build instead of the docs so they can be
     * shared between many doc files.
     */
    @Input
    Map<String, String> teardowns = new HashMap()

    /**
     * A list of files that contain snippets that *probably* should be
     * converted to `// CONSOLE` but have yet to be converted. If a file is in
     * this list and doesn't contain unconverted snippets this task will fail.
     * If there are unconverted snippets not in this list then this task will
     * fail. All files are paths relative to the docs dir.
     */
    @Input
    List<String> expectedUnconvertedCandidates = []

    /**
     * Root directory of the tests being generated. To make rest tests happy
     * we generate them in a testRoot which is contained in this directory.
     */
    private DirectoryProperty testRoot

    @Internal
    Set<String> names = new HashSet<>()

    @Inject
    RestTestsFromSnippetsTask(ObjectFactory objectFactory) {
        testRoot = objectFactory.directoryProperty()
        TestBuilder builder = new TestBuilder()
        perSnippet builder.&handleSnippet
        doLast builder.&checkUnconverted
        doLast builder.&finishLastTest
    }

    /**
     * Root directory containing all the files generated by this task. It is
     * contained within testRoot.
     */
    File outputRoot() {
        return new File(testRoot.get().asFile, '/rest-api-spec/test')
    }

    @OutputDirectory
    DirectoryProperty getTestRoot() {
        return testRoot
    }
/**
     * Is this snippet a candidate for conversion to `// CONSOLE`?
     */
    static isConsoleCandidate(Snippet snippet) {
        /* Snippets that are responses or already marked as `// CONSOLE` or
         * `// NOTCONSOLE` are not candidates. */
        if (snippet.console != null || snippet.testResponse) {
            return false
        }
        /* js snippets almost always should be marked with `// CONSOLE`. js
         * snippets that shouldn't be marked `// CONSOLE`, like examples for
         * js client, should always be marked with `// NOTCONSOLE`.
         *
         * `sh` snippets that contain `curl` almost always should be marked
         * with `// CONSOLE`. In the exceptionally rare cases where they are
         * not communicating with Elasticsearch, like the examples in the ec2
         * and gce discovery plugins, the snippets should be marked
         * `// NOTCONSOLE`. */
        return snippet.language == 'js' || snippet.curl
    }

    /**
     * Certain requests should not have the shard failure check because the
     * format of the response is incompatible i.e. it is not a JSON object.
     */
    static shouldAddShardFailureCheck(String path) {
        return path.startsWith('_cat') == false && path.startsWith('_ml/datafeeds/') == false
    }

    /**
     * Converts Kibana's block quoted strings into standard JSON. These
     * {@code """} delimited strings can be embedded in CONSOLE and can
     * contain newlines and {@code "} without the normal JSON escaping.
     * This has to add it.
     */
    @PackageScope
    static String replaceBlockQuote(String body) {
        int start = body.indexOf('"""');
        if (start < 0) {
            return body
        }
        /*
         * 1.3 is a fairly wild guess of the extra space needed to hold
         * the escaped string.
         */
        StringBuilder result = new StringBuilder((int) (body.length() * 1.3));
        int startOfNormal = 0;
        while (start >= 0) {
            int end = body.indexOf('"""', start + 3);
            if (end < 0) {
                throw new InvalidUserDataException(
                    "Invalid block quote starting at $start in:\n$body")
            }
            result.append(body.substring(startOfNormal, start));
            result.append('"');
            result.append(body.substring(start + 3, end)
                .replace('"', '\\"')
                .replace("\n", "\\n"));
            result.append('"');
            startOfNormal = end + 3;
            start = body.indexOf('"""', startOfNormal);
        }
        result.append(body.substring(startOfNormal));
        return result.toString();
    }

    private class TestBuilder {
        private static final String SYNTAX = {
            String method = /(?<method>GET|PUT|POST|HEAD|OPTIONS|DELETE)/
            String pathAndQuery = /(?<pathAndQuery>[^\n]+)/
            String badBody = /GET|PUT|POST|HEAD|OPTIONS|DELETE|startyaml|#/
            String body = /(?<body>(?:\n(?!$badBody)[^\n]+)+)/
            String rawRequest = /(?:$method\s+$pathAndQuery$body?)/
            String yamlRequest = /(?:startyaml(?s)(?<yaml>.+?)(?-s)endyaml)/
            String nonComment = /(?:$rawRequest|$yamlRequest)/
            String comment = /(?<comment>#.+)/
            /(?:$comment|$nonComment)\n+/
        }()

        /**
         * The file in which we saw the last snippet that made a test.
         */
        Path lastDocsPath

        /**
         * The file we're building.
         */
        PrintWriter current

        /**
         * Files containing all snippets that *probably* should be converted
         * to `// CONSOLE` but have yet to be converted. All files are paths
         * relative to the docs dir.
         */
        Set<String> unconvertedCandidates = new HashSet<>()

        /**
         * The last non-TESTRESPONSE snippet.
         */
        Snippet previousTest

        /**
         * Called each time a snippet is encountered. Tracks the snippets and
         * calls buildTest to actually build the test.
         */
        void handleSnippet(Snippet snippet) {
            if (RestTestsFromSnippetsTask.isConsoleCandidate(snippet)) {
                unconvertedCandidates.add(snippet.path.toString()
                    .replace('\\', '/'))
            }
            if (BAD_LANGUAGES.contains(snippet.language)) {
                throw new InvalidUserDataException(
                        "$snippet: Use `js` instead of `${snippet.language}`.")
            }
            if (snippet.testSetup) {
                testSetup(snippet)
                previousTest = snippet
                return
            }
            if (snippet.testTearDown) {
                testTearDown(snippet)
                previousTest = snippet
                return
            }
            if (snippet.testResponse || snippet.language == 'console-result') {
                response(snippet)
                return
            }
            if ((snippet.language == 'js') && (snippet.console)) {
                throw new InvalidUserDataException(
                        "$snippet: Use `[source,console]` instead of `// CONSOLE`.")
            }
            if (snippet.test || snippet.language == 'console') {
                test(snippet)
                previousTest = snippet
                return
            }
            // Must be an unmarked snippet....
        }

        private void test(Snippet test) {
            setupCurrent(test)

            if (test.continued) {
                /* Catch some difficult to debug errors with // TEST[continued]
                 * and throw a helpful error message. */
                if (previousTest == null || previousTest.path != test.path) {
                    throw new InvalidUserDataException("// TEST[continued] " +
                        "cannot be on first snippet in a file: $test")
                }
                if (previousTest != null && previousTest.testSetup) {
                    throw new InvalidUserDataException("// TEST[continued] " +
                        "cannot immediately follow // TESTSETUP: $test")
                }
                if (previousTest != null && previousTest.testTearDown) {
                    throw new InvalidUserDataException("// TEST[continued] " +
                        "cannot immediately follow // TEARDOWN: $test")
                }
            } else {
                current.println('---')
                current.println("\"line_$test.start\":")
                /* The Elasticsearch test runner doesn't support quite a few
                 * constructs unless we output this skip. We don't know if
                 * we're going to use these constructs, but we might so we
                 * output the skip just in case. */
                current.println("  - skip:")
                current.println("      features: ")
                current.println("        - default_shards")
                current.println("        - stash_in_key")
                current.println("        - stash_in_path")
                current.println("        - stash_path_replace")
                current.println("        - warnings")
                if (test.testEnv != null) {
                    switch (test.testEnv) {
                    case 'basic':
                    case 'gold':
                    case 'platinum':
                    case 'enterprise':
                        current.println("        - xpack")
                        break;
                    default:
                        throw new InvalidUserDataException('Unsupported testEnv: '
                                + test.testEnv)
                    }
                }
            }
            if (test.skip) {
                if (test.continued) {
                    throw new InvalidUserDataException("Continued snippets "
                        + "can't be skipped")
                }
                current.println("        - always_skip")
                current.println("      reason: $test.skip")
            }
            if (test.setup != null) {
                setup(test)
            }

            body(test, false)

            if (test.teardown != null) {
                teardown(test)
            }
        }

        private void setup(final Snippet snippet) {
            // insert a setup defined outside of the docs
            for (final String name : snippet.setup.split(',')) {
                final String setup = setups[name]
                if (setup == null) {
                    throw new InvalidUserDataException(
                        "Couldn't find named setup $name for $snippet"
                    )
                }
                current.println("# Named setup ${name}")
                current.println(setup)
            }
        }

        private void teardown(final Snippet snippet) {
            // insert a teardown defined outside of the docs
            for (final String name : snippet.teardown.split(',')) {
                final String teardown = teardowns[name]
                if (teardown == null) {
                    throw new InvalidUserDataException(
                        "Couldn't find named teardown $name for $snippet"
                    )
                }
                current.println("# Named teardown ${name}")
                current.println(teardown)
            }
        }

        private void response(Snippet response) {
            if (null == response.skip) {
                current.println("  - match: ")
                current.println("      \$body: ")
                replaceBlockQuote(response.contents).eachLine {
                    current.println("        $it")
                }
            }
        }

        void emitDo(String method, String pathAndQuery, String body,
                String catchPart, List warnings, boolean inSetup, boolean skipShardFailures) {
            def (String path, String query) = pathAndQuery.tokenize('?')
            if (path == null) {
                path = '' // Catch requests to the root...
            } else {
                path = path.replace('<', '%3C').replace('>', '%3E')
            }
            current.println("  - do:")
            if (catchPart != null) {
                current.println("      catch: $catchPart")
            }
            if (false == warnings.isEmpty()) {
                current.println("      warnings:")
                for (String warning in warnings) {
                    // Escape " because we're going to quote the warning
                    String escaped = warning.replaceAll('"', '\\\\"')
                    /* Quote the warning in case it starts with [ which makes
                     * it look too much like an array. */
                    current.println("         - \"$escaped\"")
                }
            }
            current.println("      raw:")
            current.println("        method: $method")
            current.println("        path: \"$path\"")
            if (query != null) {
                for (String param: query.tokenize('&')) {
                    def (String name, String value) = param.tokenize('=')
                    if (value == null) {
                        value = ''
                    }
                    current.println("        $name: \"$value\"")
                }
            }
            if (body != null) {
                // Throw out the leading newline we get from parsing the body
                body = body.substring(1)
                // Replace """ quoted strings with valid json ones
                body = replaceBlockQuote(body)
                current.println("        body: |")
                body.eachLine { current.println("          $it") }
            }
            /* Catch any shard failures. These only cause a non-200 response if
             * no shard succeeds. But we need to fail the tests on all of these
             * because they mean invalid syntax or broken queries or something
             * else that we don't want to teach people to do. The REST test
             * framework doesn't allow us to have assertions in the setup
             * section so we have to skip it there. We also omit the assertion
             * from APIs that don't return a JSON object
             */
            if (false == inSetup && skipShardFailures == false && shouldAddShardFailureCheck(path)) {
                current.println("  - is_false: _shards.failures")
            }
        }

        private void testSetup(Snippet snippet) {
            if (lastDocsPath == snippet.path) {
                throw new InvalidUserDataException("$snippet: wasn't first. TESTSETUP can only be used in the first snippet of a document.")
            }
            setupCurrent(snippet)
            current.println('---')
            current.println("setup:")
            if (snippet.setup != null) {
                setup(snippet)
            }
            body(snippet, true)
        }

        private void testTearDown(Snippet snippet) {
            if (previousTest.testSetup == false && lastDocsPath == snippet.path) {
                throw new InvalidUserDataException("$snippet must follow test setup or be first")
            }
            setupCurrent(snippet)
            current.println('---')
            current.println('teardown:')
            body(snippet, true)
        }

        private void body(Snippet snippet, boolean inSetup) {
            parse("$snippet", snippet.contents, SYNTAX) { matcher, last ->
                if (matcher.group("comment") != null) {
                    // Comment
                    return
                }
                String yamlRequest = matcher.group("yaml");
                if (yamlRequest != null) {
                    current.println(yamlRequest)
                    return
                }
                String method = matcher.group("method")
                String pathAndQuery = matcher.group("pathAndQuery")
                String body = matcher.group("body")
                String catchPart = last ? snippet.catchPart : null
                if (pathAndQuery.startsWith('/')) {
                    // Leading '/'s break the generated paths
                    pathAndQuery = pathAndQuery.substring(1)
                }
                emitDo(method, pathAndQuery, body, catchPart, snippet.warnings,
                    inSetup, snippet.skipShardsFailures)
            }
        }

        private PrintWriter setupCurrent(Snippet test) {
            if (lastDocsPath == test.path) {
                return
            }
            finishLastTest()
            lastDocsPath = test.path

            // Make the destination file:
            // Shift the path into the destination directory tree
            Path dest = outputRoot().toPath().resolve(test.path)
            // Replace the extension
            String fileName = dest.getName(dest.nameCount - 1)
            dest = dest.parent.resolve(fileName.replace('.asciidoc', '.yml'))

            // Now setup the writer
            Files.createDirectories(dest.parent)
            current = dest.newPrintWriter('UTF-8')
        }

        void finishLastTest() {
            if (current != null) {
                current.close()
                current = null
            }
        }

        void checkUnconverted() {
            List<String> listedButNotFound = []
            for (String listed : expectedUnconvertedCandidates) {
                if (false == unconvertedCandidates.remove(listed)) {
                    listedButNotFound.add(listed)
                }
            }
            String message = ""
            if (false == listedButNotFound.isEmpty()) {
                Collections.sort(listedButNotFound)
                listedButNotFound = listedButNotFound.collect {'    ' + it}
                message += "Expected unconverted snippets but none found in:\n"
                message += listedButNotFound.join("\n")
            }
            if (false == unconvertedCandidates.isEmpty()) {
                List<String> foundButNotListed =
                    new ArrayList<>(unconvertedCandidates)
                Collections.sort(foundButNotListed)
                foundButNotListed = foundButNotListed.collect {'    ' + it}
                if (false == "".equals(message)) {
                    message += "\n"
                }
                message += "Unexpected unconverted snippets:\n"
                message += foundButNotListed.join("\n")
            }
            if (false == "".equals(message)) {
                throw new InvalidUserDataException(message);
            }
        }
    }
}
