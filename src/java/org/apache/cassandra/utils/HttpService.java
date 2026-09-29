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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A generic service for making HTTP requests.
 * This class provides reusable HTTP transport logic for components that need to retrieve
 * information from external HTTP services during startup.
 *
 * <p>This utility is intended for startup-time operations where synchronous, blocking HTTP
 * requests are appropriate. Examples include retrieving cloud metadata or configuration
 * information required before Cassandra begins serving requests.
 *
 * <p><b>Design principles:</b>
 * <ul>
 *   <li><b>Synchronous requests only</b> - Requests block until the operation completes or times out</li>
 *   <li><b>Raw HTTP responses</b> - The execute methods return HTTP responses without validating the status code</li>
 *   <li><b>Configurable timeouts</b> - Connection and read timeouts are supplied by the caller</li>
 *   <li><b>Header support</b> - Request headers can be supplied by the caller</li>
 *   <li><b>Diagnostic logging</b> - Request metadata is logged without logging header values or response contents</li>
 * </ul>
 */
public abstract class HttpService
{
    private static final Logger logger = LoggerFactory.getLogger(HttpService.class);

    public final String serviceUrl;
    public final int requestTimeoutMs;

    public HttpService(String serviceUrl, int requestTimeoutMs)
    {
        this.serviceUrl = serviceUrl;
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public String apiCall(String query) throws IOException
    {
        return apiCall(serviceUrl, query, "GET", ImmutableMap.of(), 200);
    }

    public String apiCall(String query, Map<String, String> extraHeaders) throws IOException
    {
        return apiCall(serviceUrl, query, "GET", extraHeaders, 200);
    }

    public String apiCall(String url,
                          String query,
                          String method,
                          Map<String, String> extraHeaders,
                          int expectedResponseCode) throws IOException
    {
        HttpConfig config = new HttpConfig(requestTimeoutMs,
                                           0,
                                           extraHeaders);

        HttpResponse response = HttpService.execute(url + query, method, config);

        if (response.getStatusCode() != expectedResponseCode)
            throw new HttpException(response.getStatusCode(), response.getStatusMessage());

        return response.getBody();
    }

    /**
     * Configuration for an HTTP request.
     */
    public static class HttpConfig
    {
        private final int connectTimeoutMs;
        private final int readTimeoutMs;
        private final Map<String, String> headers;

        public HttpConfig(int connectTimeoutMs, int readTimeoutMs, Map<String, String> headers)
        {
            this.connectTimeoutMs = connectTimeoutMs;
            this.readTimeoutMs = readTimeoutMs;
            this.headers = headers != null ? headers : Map.of();
        }

        public int getConnectTimeoutMs()
        {
            return connectTimeoutMs;
        }

        public int getReadTimeoutMs()
        {
            return readTimeoutMs;
        }

        public Map<String, String> getHeaders()
        {
            return headers;
        }
    }

    /**
     * Raw HTTP response containing the status code, status message, and response body.
     * Interpretation of the response is the caller's responsibility.
     */
    public static class HttpResponse
    {
        private final int statusCode;
        private final String statusMessage;
        private final String body;

        public HttpResponse(int statusCode, String statusMessage, String body)
        {
            this.statusCode = statusCode;
            this.statusMessage = statusMessage;
            this.body = body;
        }

        public int getStatusCode()
        {
            return statusCode;
        }

        public String getStatusMessage()
        {
            return statusMessage;
        }

        public String getBody()
        {
            return body;
        }
    }

    /**
     * Execute a synchronous HTTP GET request.
     *
     * @param url    the URL to request
     * @param config the HTTP configuration containing timeouts and headers
     * @return the raw HTTP response
     * @throws IOException if the HTTP request or response read fails
     */
    public static HttpResponse executeGet(String url, HttpConfig config) throws IOException
    {
        logger.trace("Executing GET request via executeGet convenience method");
        return execute(url, "GET", config);
    }

    /**
     * Execute a synchronous HTTP request using the specified method.
     *
     * <p>This method performs the HTTP transport operation and returns the response without
     * interpreting the status code or parsing the response body.
     *
     * @param url    the URL to request
     * @param method the HTTP method
     * @param config the HTTP configuration containing timeouts and headers
     * @return the raw HTTP response
     * @throws IOException if the HTTP request or response read fails
     */
    public static HttpResponse execute(String url, String method, HttpConfig config) throws IOException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Executing HTTP {} request to URL: {} with connection timeout: {}ms, read timeout: {}ms",
                         method,
                         url,
                         config.getConnectTimeoutMs(),
                         config.getReadTimeoutMs());
        }

        if (logger.isTraceEnabled() && !config.getHeaders().isEmpty())
        {
            logger.trace("Request header names: {}", config.getHeaders().keySet());
        }

        HttpURLConnection conn = null;
        long startTime = System.currentTimeMillis();

        try
        {
            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod(method);
            conn.setConnectTimeout(config.getConnectTimeoutMs());
            conn.setReadTimeout(config.getReadTimeoutMs());

            for (Map.Entry<String, String> header : config.getHeaders().entrySet())
            {
                conn.setRequestProperty(header.getKey(), header.getValue());

                if (logger.isTraceEnabled())
                {
                    logger.trace("Setting request header: {}", header.getKey());
                }
            }

            logger.trace("Opening connection to {}", url);

            int statusCode = conn.getResponseCode();
            String statusMessage = conn.getResponseMessage();
            long responseTime = System.currentTimeMillis() - startTime;

            if (logger.isDebugEnabled())
            {
                logger.debug("HTTP {} request to {} completed in {}ms with status: {} {}",
                             method, url, responseTime, statusCode, statusMessage);
            }

            String responseBody = readResponseBody(conn, statusCode);

            if (logger.isTraceEnabled())
            {
                int bodyLength = responseBody != null ? responseBody.length() : 0;
                logger.trace("Response body length: {} bytes", bodyLength);
            }

            return new HttpResponse(statusCode, statusMessage, responseBody);
        }
        catch (IOException e)
        {
            long failureTime = System.currentTimeMillis() - startTime;
            logger.error("HTTP {} request to {} failed after {}ms: {}",
                         method, url, failureTime, e.getMessage(), e);
            throw e;
        }
        finally
        {
            if (conn != null)
            {
                logger.trace("Disconnecting HTTP connection to {}", url);
                conn.disconnect();
            }
        }
    }

    /**
     * Read the HTTP response body using content-length based handling.
     *
     * @param conn the HTTP connection
     * @param statusCode the HTTP response status code
     * @return the response body as a UTF-8 string, or null if the content length is -1
     * @throws IOException if reading the response fails
     */
    private static String readResponseBody(HttpURLConnection conn, int statusCode) throws IOException
    {
        int contentLength = conn.getContentLength();

        if (logger.isTraceEnabled())
        {
            logger.trace("Reading response body with content length: {}", contentLength);
        }

        if (contentLength == -1)
        {
            logger.trace("Content length is -1, returning null response body");
            return null;
        }

        if (statusCode >= 400)
        {
            InputStream errorStream = conn.getErrorStream();

            if (errorStream == null)
                return null;

            byte[] buffer = new byte[contentLength];
            int bytesRead = 0;

            try (InputStream inputStream = errorStream)
            {
                while (bytesRead < contentLength)
                {
                    int read = inputStream.read(buffer,
                                                bytesRead,
                                                contentLength - bytesRead);
                    if (read == -1)
                        break;

                    bytesRead += read;
                }
            }

            return bytesRead == 0
                   ? null
                   : new String(buffer, 0, bytesRead, UTF_8);
        }
        byte[] buffer = new byte[contentLength];

        try (DataInputStream dataInputStream = new DataInputStream((InputStream) conn.getContent()))
        {
            dataInputStream.readFully(buffer);
            logger.trace("Successfully read {} bytes from response", contentLength);
        }

        return new String(buffer, UTF_8);
    }

    public static final class HttpException extends IOException
    {
        public final int responseCode;
        public final String responseMessage;

        public HttpException(int responseCode, String responseMessage)
        {
            super("HTTP response code: " + responseCode + " (" + responseMessage + ')');
            this.responseCode = responseCode;
            this.responseMessage = responseMessage;
        }
    }
}