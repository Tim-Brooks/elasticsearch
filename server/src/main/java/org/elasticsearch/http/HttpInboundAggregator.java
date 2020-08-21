/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class HttpInboundAggregator {

    private ReleasableBytesReference firstContent;
    private ArrayList<ReleasableBytesReference> contentAggregation;
    private HttpRequestHeader currentHeader;
    private long contentLengthHeaderValue = -1;
    private boolean isClosed = false;

    public HttpRequest fragmentReceived(Object fragment) {
        if (fragment instanceof HttpRequestHeader) {
            headerReceived((HttpRequestHeader) fragment);
            return null;
        } else if (fragment instanceof ReleasableBytesReference) {
            contentReceived((ReleasableBytesReference) fragment);
            return null;
        } else if (fragment instanceof Exception) {

            return null;
        } else if (fragment instanceof EndContent) {
            return finishAggregation();
        } else {
            throw new AssertionError();
        }
    }

    public void headerReceived(HttpRequestHeader header) {
        ensureOpen();
        if (isAggregating()) {
            // Shortcircuit, this is non-recoverable. Maybe close.
            throw new IllegalStateException("Received new http header, but already aggregating request");
        }
        assert firstContent == null && contentAggregation == null;
        currentHeader = header;
        this.contentLengthHeaderValue = getContentLength(header);
    }

    private static long getContentLength(HttpRequestHeader header) {
        String stringValue = header.headers.get(DefaultRestChannel.CONTENT_LENGTH).get(0);
        if (stringValue != null) {
            return Long.parseLong(stringValue);
        } else {
            return -1L;
        }
    }

    public void contentReceived(ReleasableBytesReference content) {
        ensureOpen();
        assert isAggregating();
        if (isShortCircuited() == false) {
            if (isFirstContent()) {
                firstContent = content.retain();
            } else {
                if (contentAggregation == null) {
                    contentAggregation = new ArrayList<>(4);
                    assert firstContent != null;
                    contentAggregation.add(firstContent);
                    firstContent = null;
                }
                contentAggregation.add(content.retain());
            }
        }
    }

    public HttpRequest finishAggregation() {
        return null;
    }

    public boolean isAggregating() {
        return currentHeader != null;
    }

    private boolean isShortCircuited() {
        return false;
    }

    private boolean isFirstContent() {
        return firstContent == null && contentAggregation == null;
    }

    private void ensureOpen() {
        if (isClosed) {
            throw new IllegalStateException("Aggregator is already closed");
        }
    }

    private static class HttpRequestHeader {

        private final HttpRequest.HttpVersion protocolVersion;
        private final RestRequest.Method method;
        private final String uri;
        private final Map<String, List<String>> headers;
        private final Supplier<List<String>> strictCookies;

        private HttpRequestHeader(HttpRequest.HttpVersion protocolVersion, RestRequest.Method method, String uri,
                                  Map<String, List<String>> headers, Supplier<List<String>> strictCookies) {
            this.protocolVersion = protocolVersion;
            this.method = method;
            this.uri = uri;
            this.headers = headers;
            this.strictCookies = strictCookies;
        }
    }

    private static class AggregatedRequest implements HttpRequest {

        private final HttpRequestHeader header;

        private AggregatedRequest(HttpRequestHeader header) {
            this.header = header;
        }

        @Override
        public RestRequest.Method method() {
            return header.method;
        }

        @Override
        public String uri() {
            return header.uri;
        }

        @Override
        public BytesReference content() {
            return null;
        }

        @Override
        public Map<String, List<String>> getHeaders() {
            return header.headers;
        }

        @Override
        public List<String> strictCookies() {
            return header.strictCookies.get();
        }

        @Override
        public HttpVersion protocolVersion() {
            return header.protocolVersion;
        }

        @Override
        public HttpRequest removeHeader(String header) {
            // TODO: Implement
            return null;
        }

        @Override
        public HttpResponse createResponse(RestStatus status, BytesReference content) {
            return null;
        }

        @Override
        public Exception getInboundException() {
            return null;
        }

        @Override
        public void release() {

        }

        @Override
        public HttpRequest releaseAndCopy() {
            return null;
        }
    }

    private static class EndContent {

    }
}
