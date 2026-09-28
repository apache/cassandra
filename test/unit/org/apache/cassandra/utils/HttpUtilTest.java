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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class HttpUtilTest
{
    private static final int CONNECT_TIMEOUT_MS = 5000;
    private static final int READ_TIMEOUT_MS = 5000;

    private HttpServer server;
    private String baseUrl;

    @Before
    public void setup() throws IOException
    {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.start();

        baseUrl = "http://localhost:" + server.getAddress().getPort();
    }

    @After
    public void teardown()
    {
        if (server != null)
            server.stop(0);
    }

    @Test
    public void testGet() throws Exception
    {
        server.createContext("/get", exchange ->
        {
            assertEquals("GET", exchange.getRequestMethod());
            sendResponse(exchange, 200, "test-response");
        });

        HttpUtil.HttpResponse response =
        HttpUtil.executeGet(baseUrl + "/get", defaultConfig());

        assertEquals(200, response.getStatusCode());
        assertEquals("OK", response.getStatusMessage());
        assertEquals("test-response", response.getBody());
    }

    @Test
    public void testPut() throws Exception
    {
        server.createContext("/put", exchange ->
        {
            assertEquals("PUT", exchange.getRequestMethod());
            sendResponse(exchange, 200, "test-token");
        });

        HttpUtil.HttpResponse response =
        HttpUtil.execute(baseUrl + "/put", "PUT", defaultConfig());

        assertEquals(200, response.getStatusCode());
        assertEquals("test-token", response.getBody());
    }

    @Test
    public void testHeaders() throws Exception
    {
        server.createContext("/headers", exchange ->
        {
            assertEquals("GET", exchange.getRequestMethod());
            assertEquals("test-value",
                         exchange.getRequestHeaders().getFirst("X-Test-Header"));

            sendResponse(exchange, 200, "success");
        });

        HttpUtil.HttpConfig config =
        new HttpUtil.HttpConfig(CONNECT_TIMEOUT_MS,
                                READ_TIMEOUT_MS,
                                Map.of("X-Test-Header", "test-value"));

        HttpUtil.HttpResponse response =
        HttpUtil.executeGet(baseUrl + "/headers", config);

        assertEquals(200, response.getStatusCode());
        assertEquals("success", response.getBody());
    }

    @Test
    public void testRawResponseStatus() throws Exception
    {
        server.createContext("/status", exchange ->
        {
            sendResponse(exchange, 201, "created");
        });

        HttpUtil.HttpResponse response =
        HttpUtil.executeGet(baseUrl + "/status", defaultConfig());

        assertEquals(201, response.getStatusCode());
        assertEquals("Created", response.getStatusMessage());
        assertEquals("created", response.getBody());
    }

    @Test
    public void testReadTimeout() throws Exception
    {
        server.createContext("/timeout", exchange ->
        {
            try
            {
                Thread.sleep(1000);
                sendResponse(exchange, 200, "delayed-response");
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
            catch (IOException ignored)
            {
                // The client may close the connection after the read timeout.
            }
        });

        HttpUtil.HttpConfig config =
        new HttpUtil.HttpConfig(CONNECT_TIMEOUT_MS,
                                100,
                                Map.of());

        try
        {
            HttpUtil.executeGet(baseUrl + "/timeout", config);
            fail("Expected SocketTimeoutException");
        }
        catch (SocketTimeoutException expected)
        {
            // Expected
        }
    }

    @Test
    public void testNullHeaders() throws Exception
    {
        server.createContext("/null-headers", exchange ->
        {
            sendResponse(exchange, 200, "success");
        });

        HttpUtil.HttpConfig config =
        new HttpUtil.HttpConfig(CONNECT_TIMEOUT_MS,
                                READ_TIMEOUT_MS,
                                null);

        HttpUtil.HttpResponse response =
        HttpUtil.executeGet(baseUrl + "/null-headers", config);

        assertEquals(200, response.getStatusCode());
        assertEquals("success", response.getBody());
    }

    @Test
    public void testUnknownContentLengthReturnsNullBody() throws Exception
    {
        server.createContext("/chunked", exchange ->
        {
            byte[] response = "chunked-response".getBytes(StandardCharsets.UTF_8);

            // A response length of 0 enables chunked transfer encoding.
            exchange.sendResponseHeaders(200, 0);

            try (OutputStream output = exchange.getResponseBody())
            {
                output.write(response);
            }
        });

        HttpUtil.HttpResponse response =
        HttpUtil.executeGet(baseUrl + "/chunked", defaultConfig());

        assertEquals(200, response.getStatusCode());
        assertNull(response.getBody());
    }

    @Test
    public void testInvalidMethodThrowsIOException()
    {
        try
        {
            HttpUtil.execute(baseUrl + "/invalid",
                            "INVALID METHOD",
                            defaultConfig());

            fail("Expected IOException");
        }
        catch (IOException expected)
        {
            // Expected
        }
    }

    @Test
    public void testErrorResponseBodyIsReturned() throws Exception
    {
        server.createContext("/unauthorized", exchange ->
        {
            sendResponse(exchange, 401, "Unauthorized");
        });

        HttpUtil.HttpResponse response =
        HttpUtil.executeGet(baseUrl + "/unauthorized", defaultConfig());

        assertEquals(401, response.getStatusCode());
        assertEquals("Unauthorized", response.getStatusMessage());
        assertEquals("Unauthorized", response.getBody());
    }

    private HttpUtil.HttpConfig defaultConfig()
    {
        return new HttpUtil.HttpConfig(CONNECT_TIMEOUT_MS,
                                       READ_TIMEOUT_MS,
                                       Map.of());
    }

    private static void sendResponse(HttpExchange exchange,
                                     int statusCode,
                                     String body) throws IOException
    {
        byte[] response = body.getBytes(StandardCharsets.UTF_8);

        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");

        exchange.sendResponseHeaders(statusCode, response.length);

        try (OutputStream output = exchange.getResponseBody())
        {
            output.write(response);
        }
    }
}