/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import static org.elasticsearch.common.Strings.collectionToDelimitedString;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_HEADERS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_METHODS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_MAX_AGE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class CorsHandlerTests extends ESTestCase {

    public void testCorsConfigWithBadRegex() {
        final Settings settings = Settings.builder()
            .put(SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "/[*/")
            .put(SETTING_CORS_ALLOW_CREDENTIALS.getKey(), true)
            .build();
        SettingsException e = expectThrows(SettingsException.class, () -> CorsHandler.fromSettings(settings));
        assertThat(e.getMessage(), containsString("Bad regex in [http.cors.allow-origin]: [/[*/]"));
        assertThat(e.getCause(), instanceOf(PatternSyntaxException.class));
    }

    public void testCorsConfig() {
        final Set<String> methods = new HashSet<>(Arrays.asList("get", "options", "post"));
        final Set<String> headers = new HashSet<>(Arrays.asList("Content-Type", "Content-Length"));
        final String prefix = randomBoolean() ? " " : ""; // sometimes have a leading whitespace between comma delimited elements
        final Settings settings = Settings.builder()
            .put(SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "*")
            .put(SETTING_CORS_ALLOW_METHODS.getKey(), collectionToDelimitedString(methods, ",", prefix, ""))
            .put(SETTING_CORS_ALLOW_HEADERS.getKey(), collectionToDelimitedString(headers, ",", prefix, ""))
            .put(SETTING_CORS_ALLOW_CREDENTIALS.getKey(), true)
            .build();
        final CorsHandler.Config corsConfig = CorsHandler.fromSettings(settings);
        assertTrue(corsConfig.isAnyOriginSupported());
        assertEquals(headers, corsConfig.allowedRequestHeaders());
        assertEquals(methods.stream().map(s -> s.toUpperCase(Locale.ENGLISH)).collect(Collectors.toSet()),
            corsConfig.allowedRequestMethods().stream().map(RestRequest.Method::name).collect(Collectors.toSet()));
    }

    public void testCorsConfigWithDefaults() {
        final Set<String> methods = Strings.commaDelimitedListToSet(SETTING_CORS_ALLOW_METHODS.getDefault(Settings.EMPTY));
        final Set<String> headers = Strings.commaDelimitedListToSet(SETTING_CORS_ALLOW_HEADERS.getDefault(Settings.EMPTY));
        final long maxAge = SETTING_CORS_MAX_AGE.getDefault(Settings.EMPTY);
        final Settings settings = Settings.builder().put(SETTING_CORS_ENABLED.getKey(), true).build();
        final CorsHandler.Config corsConfig = CorsHandler.fromSettings(settings);
        assertFalse(corsConfig.isAnyOriginSupported());
        assertEquals(Collections.emptySet(), corsConfig.origins().get());
        assertEquals(headers, corsConfig.allowedRequestHeaders());
        assertEquals(methods, corsConfig.allowedRequestMethods().stream().map(RestRequest.Method::name).collect(Collectors.toSet()));
        assertEquals(maxAge, corsConfig.maxAge());
        assertFalse(corsConfig.isCredentialsAllowed());
    }

    public void testAnyOriginPreflight() {
        final Set<String> methods = new HashSet<>(Arrays.asList("get", "options", "post"));
        final Set<String> headers = new HashSet<>(Arrays.asList("Content-Type", "Content-Length"));
        final String prefix = randomBoolean() ? " " : ""; // sometimes have a leading whitespace between comma delimited elements
        final boolean allowCredentials = randomBoolean();
        final Settings settings = Settings.builder()
            .put(SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "*")
            .put(SETTING_CORS_ALLOW_METHODS.getKey(), Strings.collectionToDelimitedString(methods, ",", prefix, ""))
            .put(SETTING_CORS_ALLOW_HEADERS.getKey(), Strings.collectionToDelimitedString(headers, ",", prefix, ""))
            .put(SETTING_CORS_ALLOW_CREDENTIALS.getKey(), allowCredentials)
            .build();
        final CorsHandler corsHandler = CorsHandler.handlerFromSettings(settings);

        TestRequest request = new TestRequest("elastic.co", RestRequest.Method.OPTIONS);
        TestResponse httpResponse = (TestResponse) corsHandler.handleRequest(request);
        assertNotNull(httpResponse);
        assertEquals(RestStatus.OK, httpResponse.getRestStatus());
        assertEquals(methods.stream().map(s -> s.toUpperCase(Locale.ENGLISH)).collect(Collectors.toSet()),
            new HashSet<>(httpResponse.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_METHODS)));
        assertEquals(headers, new HashSet<>(httpResponse.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_HEADERS)));
        assertEquals("1728000", httpResponse.getAllHeaders(CorsHandler.ACCESS_CONTROL_MAX_AGE).get(0));
        if (allowCredentials) {
            // Origin is set to vary when any origin and credentials are allowed
            assertEquals(CorsHandler.ORIGIN, httpResponse.getAllHeaders(CorsHandler.VARY).get(0));
            assertEquals("elastic.co", httpResponse.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN).get(0));
            assertEquals("true", httpResponse.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_CREDENTIALS).get(0));
        } else {
            assertEquals("*", httpResponse.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN).get(0));
            assertNull(httpResponse.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_CREDENTIALS));
        }
    }

    public void testSpecificOriginPreflight() {
        final Set<String> methods = new HashSet<>(Arrays.asList("get", "options", "post"));
        final Set<String> headers = new HashSet<>(Arrays.asList("Content-Type", "Content-Length"));
        final String prefix = randomBoolean() ? " " : ""; // sometimes have a leading whitespace between comma delimited elements
        final boolean allowCredentials = randomBoolean();
        Settings.Builder builder = Settings.builder()
            .put(SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "elastic.co")
            .put(SETTING_CORS_ALLOW_METHODS.getKey(), Strings.collectionToDelimitedString(methods, ",", prefix, ""))
            .put(SETTING_CORS_ALLOW_HEADERS.getKey(), Strings.collectionToDelimitedString(headers, ",", prefix, ""))
            .put(SETTING_CORS_ALLOW_CREDENTIALS.getKey(), allowCredentials);
        if (randomBoolean()) {
            builder.put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "elastic.co");
        } else {
            builder.put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "/elast.+/");
        }
        final CorsHandler corsHandler = CorsHandler.handlerFromSettings(builder.build());

        TestRequest request = new TestRequest("elastic.co", RestRequest.Method.OPTIONS);
        TestResponse httpResponse = (TestResponse) corsHandler.handleRequest(request);
        assertNotNull(httpResponse);
        assertEquals(RestStatus.OK, httpResponse.getRestStatus());
        assertEquals(methods.stream().map(s -> s.toUpperCase(Locale.ENGLISH)).collect(Collectors.toSet()),
            new HashSet<>(httpResponse.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_METHODS)));
        assertEquals(headers, new HashSet<>(httpResponse.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_HEADERS)));
        assertEquals("1728000", httpResponse.getAllHeaders(CorsHandler.ACCESS_CONTROL_MAX_AGE).get(0));
        assertEquals(CorsHandler.ORIGIN, httpResponse.getAllHeaders(CorsHandler.VARY).get(0));
        assertEquals("elastic.co", httpResponse.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN).get(0));
        if (allowCredentials) {
            assertEquals("true", httpResponse.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_CREDENTIALS).get(0));
        } else {
            assertNull(httpResponse.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_CREDENTIALS));
        }

        TestRequest request2 = new TestRequest("invalid_elastic.co", RestRequest.Method.OPTIONS);
        TestResponse httpResponse2 = (TestResponse) corsHandler.handleRequest(request2);
        assertNotNull(httpResponse2);
        assertEquals(RestStatus.OK, httpResponse2.getRestStatus());
        // invalid_elastic.co is not allowed so these headers are null
        assertNull(httpResponse2.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN));
        assertNull(httpResponse2.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_METHODS));
        assertNull(httpResponse2.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_HEADERS));
        assertNull(httpResponse2.getAllHeaders(CorsHandler.ACCESS_CONTROL_MAX_AGE));
        assertNull(httpResponse2.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_CREDENTIALS));
    }

    public void testCorsPreflightDisabled() {
        final Settings settings = Settings.builder().put(SETTING_CORS_ENABLED.getKey(), false).build();
        final CorsHandler corsHandler = CorsHandler.handlerFromSettings(settings);
        assertNull(corsHandler.handleRequest(new TestRequest("elastic.co", RestRequest.Method.OPTIONS)));
    }

    public void testShortCircuit() {
        Settings settings = Settings.builder()
            .put(SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "elastic.co")
            .build();

        final CorsHandler corsHandler = CorsHandler.handlerFromSettings(settings);
        TestResponse httpResponse = (TestResponse) corsHandler.handleRequest(new TestRequest("invalid_elastic.co", RestRequest.Method.GET));
        assertEquals(RestStatus.FORBIDDEN, httpResponse.getRestStatus());
    }

    public void testSetCorsHeadersWithSpecificOrigin() {
        boolean allowCredentials = randomBoolean();
        Settings.Builder builder = Settings.builder().put(SETTING_CORS_ENABLED.getKey(), true);
        builder.put(SETTING_CORS_ALLOW_CREDENTIALS.getKey(), allowCredentials);
        if (randomBoolean()) {
            builder.put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "elastic.co");
        } else {
            builder.put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "/elast.+/");
        }
        final CorsHandler corsHandler = CorsHandler.handlerFromSettings(builder.build());

        TestRequest request = new TestRequest("elastic.co", RestRequest.Method.GET);
        TestResponse response = (TestResponse) request.createResponse(RestStatus.OK, BytesArray.EMPTY);
        corsHandler.setCorsResponseHeaders(request, response);
        assertEquals("elastic.co", response.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN).get(0));
        if (allowCredentials) {
            assertEquals("true", response.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_CREDENTIALS).get(0));
        } else {
            assertNull(response.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_CREDENTIALS));
        }
    }

    public void testSetCorsHeadersWithAnyOrigin() {
        boolean allowCredentials = randomBoolean();
        Settings.Builder builder = Settings.builder()
            .put(SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "*")
            .put(SETTING_CORS_ALLOW_CREDENTIALS.getKey(), allowCredentials);

        final CorsHandler corsHandler = CorsHandler.handlerFromSettings(builder.build());

        TestRequest request = new TestRequest("elastic.co", RestRequest.Method.GET);
        TestResponse response = request.createResponse(RestStatus.OK, BytesArray.EMPTY);
        corsHandler.setCorsResponseHeaders(request, response);
        if (allowCredentials) {
            assertEquals("elastic.co", response.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN).get(0));
            assertEquals(CorsHandler.ORIGIN, response.getAllHeaders(CorsHandler.VARY).get(0));
            assertEquals("true", response.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_CREDENTIALS).get(0));
        } else {
            assertEquals("*", response.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN).get(0));
            assertNull(response.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_CREDENTIALS));
        }
    }

    public void testSetCorsHeadersWithNonCorsRequest() {
        Settings.Builder builder = Settings.builder()
            .put(SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "*");

        final CorsHandler corsHandler = CorsHandler.handlerFromSettings(builder.build());

        TestRequest request = new TestRequest("elastic.co", RestRequest.Method.GET, false);
        TestResponse response = request.createResponse(RestStatus.OK, BytesArray.EMPTY);
        corsHandler.setCorsResponseHeaders(request, response);
        assertNull(response.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN));
    }

    public void testCorsSetHeadersDisabled() {
        final Settings settings = Settings.builder().put(SETTING_CORS_ENABLED.getKey(), false).build();
        final CorsHandler corsHandler = CorsHandler.handlerFromSettings(settings);
        TestRequest request = new TestRequest("elastic.co", RestRequest.Method.OPTIONS);
        TestResponse response = request.createResponse(RestStatus.OK, BytesArray.EMPTY);
        corsHandler.setCorsResponseHeaders(request, response);
        assertNull(response.getAllHeaders(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN));
    }

    private static class TestRequest implements HttpRequest {

        private final AtomicReference<RestStatus> responseStatus = new AtomicReference<>();

        private final RestRequest.Method method;
        private final Map<String, List<String>> headers = new HashMap<>();

        private TestRequest(String origin, RestRequest.Method method) {
            this(origin, method, true);
        }

        private TestRequest(String origin, RestRequest.Method method, boolean isCors) {
            this.method = method;
            if (isCors) {
                headers.put(CorsHandler.ORIGIN, Collections.singletonList(origin));
                if (method == RestRequest.Method.OPTIONS) {
                    headers.put(CorsHandler.ACCESS_CONTROL_REQUEST_METHOD, Collections.singletonList("POST"));
                }
            }
        }

        @Override
        public RestRequest.Method method() {
            return method;
        }

        @Override
        public String uri() {
            return null;
        }

        @Override
        public BytesReference content() {
            return BytesArray.EMPTY;
        }

        @Override
        public Map<String, List<String>> getHeaders() {
            return headers;
        }

        @Override
        public List<String> strictCookies() {
            return Collections.emptyList();
        }

        @Override
        public HttpVersion protocolVersion() {
            return HttpVersion.HTTP_1_1;
        }

        @Override
        public HttpRequest removeHeader(String header) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TestResponse createResponse(RestStatus status, BytesReference content) {
            return new TestResponse(status, content);
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
            return this;
        }
    }

    private static class TestResponse implements HttpResponse {


        private final RestStatus status;
        private final BytesReference content;
        private final HashMap<String, List<String>> headers = new HashMap<>();

        public TestResponse(RestStatus status, BytesReference content) {
            this.status = status;
            this.content = content;
        }

        @Override
        public void addHeader(String name, String value) {
            List<String> values = headers.computeIfAbsent(name, (n) -> new ArrayList<>());
            values.add(value);
        }

        @Override
        public boolean containsHeader(String name) {
            return headers.containsKey(name);
        }

        public RestStatus getRestStatus() {
            return status;
        }

        public List<String> getAllHeaders(String name) {
            return headers.get(name);
        }
    }
}
