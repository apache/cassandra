/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils;

import java.net.ProtocolException;
import java.net.SocketTimeoutException;
import java.util.Map;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.cassandra.utils.HttpService.HttpConfig;
import org.apache.cassandra.utils.HttpService.HttpResponse;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.Options.ChunkedEncodingPolicy.ALWAYS;
import static com.github.tomakehurst.wiremock.core.Options.ChunkedEncodingPolicy.NEVER;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class HttpServiceTest
{
    private static final int CONNECT_TIMEOUT_MS = 5000;
    private static final int READ_TIMEOUT_MS = 5000;

    @Rule
    // WireMock chunks responses by default, which HttpService reports as an unknown-length body (null). Force
    // Content-Length so the body-bearing tests exercise the normal path; the chunked case opts back in below.
    public final WireMockRule service = new WireMockRule(wireMockConfig().bindAddress("127.0.0.1")
                                                                         .dynamicPort()
                                                                         .useChunkedTransferEncoding(NEVER));

    // a second server that always chunks, so a response with no Content-Length can be exercised
    @Rule
    public final WireMockRule chunkedService = new WireMockRule(wireMockConfig().bindAddress("127.0.0.1")
                                                                                .dynamicPort()
                                                                                .useChunkedTransferEncoding(ALWAYS));

    private String baseUrl;
    private String chunkedBaseUrl;

    @Before
    public void setup()
    {
        baseUrl = "http://127.0.0.1:" + service.port();
        chunkedBaseUrl = "http://127.0.0.1:" + chunkedService.port();
    }

    @Test
    public void testGet() throws Exception
    {
        service.stubFor(get(urlEqualTo("/get")).willReturn(textResponse(200, "test-response")));

        HttpResponse response = HttpService.execute(baseUrl + "/get", "GET", defaultConfig());

        assertEquals(200, response.getStatusCode());
        assertEquals("OK", response.getStatusMessage());
        assertEquals("test-response", response.getBody());

        service.verify(getRequestedFor(urlEqualTo("/get")));
    }

    @Test
    public void testPut() throws Exception
    {
        service.stubFor(put(urlEqualTo("/put")).willReturn(textResponse(200, "test-token")));

        HttpResponse response = HttpService.execute(baseUrl + "/put", "PUT", defaultConfig());

        assertEquals(200, response.getStatusCode());
        assertEquals("test-token", response.getBody());

        service.verify(putRequestedFor(urlEqualTo("/put")));
    }

    @Test
    public void testHeaders() throws Exception
    {
        // the stub only matches when the header is present, so a missing header would 404 rather than 200
        service.stubFor(get(urlEqualTo("/headers"))
                        .withHeader("X-Test-Header", equalTo("test-value"))
                        .willReturn(textResponse(200, "success")));

        HttpConfig config = new HttpConfig(CONNECT_TIMEOUT_MS,
                                           READ_TIMEOUT_MS,
                                           Map.of("X-Test-Header", "test-value"));

        HttpResponse response = HttpService.execute(baseUrl + "/headers", "GET", config);

        assertEquals(200, response.getStatusCode());
        assertEquals("success", response.getBody());
    }

    @Test
    public void testRawResponseStatus() throws Exception
    {
        service.stubFor(get(urlEqualTo("/status")).willReturn(textResponse(201, "created")));

        HttpResponse response = HttpService.execute(baseUrl + "/status", "GET", defaultConfig());

        assertEquals(201, response.getStatusCode());
        assertEquals("Created", response.getStatusMessage());
        assertEquals("created", response.getBody());
    }

    @Test
    public void testReadTimeout()
    {
        service.stubFor(get(urlEqualTo("/timeout"))
                        .willReturn(textResponse(200, "delayed-response")
                                    .withFixedDelay(1000)));

        HttpConfig config = new HttpConfig(CONNECT_TIMEOUT_MS, 100, Map.of());

        assertThatThrownBy(() -> HttpService.execute(baseUrl + "/timeout", "GET", config))
        .isInstanceOf(SocketTimeoutException.class);
    }

    @Test
    public void testNullHeaders() throws Exception
    {
        service.stubFor(get(urlEqualTo("/null-headers"))
                        .willReturn(textResponse(200, "success")));

        HttpConfig config = new HttpConfig(CONNECT_TIMEOUT_MS, READ_TIMEOUT_MS, null);

        HttpResponse response = HttpService.execute(baseUrl + "/null-headers", "GET", config);

        assertEquals(200, response.getStatusCode());
        assertEquals("success", response.getBody());
    }

    @Test
    public void testUnknownContentLengthReturnsNullBody() throws Exception
    {
        chunkedService.stubFor(get(urlEqualTo("/chunked"))
                               .willReturn(textResponse(200, "chunked-response")));

        HttpResponse response = HttpService.execute(chunkedBaseUrl + "/chunked", "GET", defaultConfig());

        assertEquals(200, response.getStatusCode());
        assertNull(response.getBody());
    }

    @Test
    public void testInvalidMethodThrowsIOException()
    {
        // rejected by HttpURLConnection before any request is made, so no stub is needed
        assertThatThrownBy(() -> HttpService.execute(baseUrl + "/invalid", "INVALID METHOD", defaultConfig()))
        .isInstanceOf(ProtocolException.class);
    }

    @Test
    public void testErrorResponseBodyIsReturned() throws Exception
    {
        service.stubFor(get(urlEqualTo("/unauthorized")).willReturn(textResponse(401, "Unauthorized")));

        HttpResponse response = HttpService.execute(baseUrl + "/unauthorized", "GET", defaultConfig());

        assertEquals(401, response.getStatusCode());
        assertEquals("Unauthorized", response.getStatusMessage());
        assertEquals("Unauthorized", response.getBody());
    }

    private HttpConfig defaultConfig()
    {
        return new HttpConfig(CONNECT_TIMEOUT_MS, READ_TIMEOUT_MS, Map.of());
    }

    private static com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder textResponse(int status, String body)
    {
        return aResponse().withStatus(status)
                          .withHeader("Content-Type", "text/plain; charset=UTF-8")
                          .withBody(body);
    }
}
