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

import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.rest.RestRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class HttpInboundAggregator {

    private ReleasableBytesReference firstContent;
    private ArrayList<ReleasableBytesReference> contentAggregation;
    private HttpRequestHeader currentHeader;

    public void forwardFragment(Object fragment) {
        if (fragment instanceof HttpRequestHeader) {

        } else if (fragment instanceof ReleasableBytesReference) {

        } else if (fragment instanceof Exception) {

        } else if (fragment instanceof EndContent) {

        } else {
            throw new AssertionError();
        }
    }

    public HttpRequest finishAggregation() {
        return null;
    }

    private static class HttpRequestHeader {

        private final HttpRequest.HttpVersion httpVersion;
        private final RestRequest.Method method;
        private final String uri;
        private final Map<String, List<String>> headers;
        private final Supplier<List<String>> strictCookies;

        private HttpRequestHeader(HttpRequest.HttpVersion httpVersion, RestRequest.Method method, String uri,
                                  Map<String, List<String>> headers, Supplier<List<String>> strictCookies) {
            this.httpVersion = httpVersion;
            this.method = method;
            this.uri = uri;
            this.headers = headers;
            this.strictCookies = strictCookies;
        }
    }

    private static class EndContent {

    }
}
