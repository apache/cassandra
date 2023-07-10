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

package org.apache.cassandra.locator;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.exceptions.ConfigurationException;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

abstract class AbstractCloudMetadataServiceConnector
{
    static final String METADATA_URL_PROPERTY = "metadata_url";
    static final String METADATA_REQUEST_TIMEOUT_PROPERTY = "metadata_request_timeout";
    static final String DEFAULT_METADATA_REQUEST_TIMEOUT = "30s";

    protected final String metadataServiceUrl;
    protected final int requestTimeoutMs;

    private final SnitchProperties properties;

    public AbstractCloudMetadataServiceConnector(SnitchProperties snitchProperties)
    {
        this.properties = snitchProperties;
        String parsedMetadataServiceUrl = properties.get(METADATA_URL_PROPERTY, null);

        try
        {
            URL url = new URL(parsedMetadataServiceUrl);
            url.toURI();

            this.metadataServiceUrl = parsedMetadataServiceUrl;
        }
        catch (MalformedURLException | IllegalArgumentException | URISyntaxException ex)
        {
            throw new ConfigurationException(format("Snitch metadata service URL '%s' is invalid. Please review snitch properties " +
                                                    "defined in the configured '%s' configuration file.",
                                                    parsedMetadataServiceUrl,
                                                    CassandraRelevantProperties.CASSANDRA_RACKDC_PROPERTIES.getKey()),
                                             ex);
        }

        String metadataRequestTimeout = properties.get(METADATA_REQUEST_TIMEOUT_PROPERTY, DEFAULT_METADATA_REQUEST_TIMEOUT);

        try
        {
            this.requestTimeoutMs = new DurationSpec.IntMillisecondsBound(metadataRequestTimeout).toMilliseconds();
        }
        catch (IllegalArgumentException ex)
        {
            throw new ConfigurationException(format("%s as value of %s is invalid duration! " + ex.getMessage(),
                                                    metadataRequestTimeout,
                                                    METADATA_REQUEST_TIMEOUT_PROPERTY));
        }
    }

    public SnitchProperties getProperties()
    {
        return properties;
    }

    public final String apiCall(String query) throws IOException
    {
        return apiCall(metadataServiceUrl, query, "GET", ImmutableMap.of(), 200);
    }

    public final String apiCall(String query, Map<String, String> extraHeaders) throws IOException
    {
        return apiCall(metadataServiceUrl, query, "GET", extraHeaders, 200);
    }

    public String apiCall(String url,
                          String query,
                          String method,
                          Map<String, String> extraHeaders,
                          int expectedResponseCode) throws IOException
    {
        HttpURLConnection conn = null;
        try
        {
            // Populate the region and zone by introspection, fail if 404 on metadata
            conn = (HttpURLConnection) new URL(url + query).openConnection();
            extraHeaders.forEach(conn::setRequestProperty);
            conn.setRequestMethod(method);
            conn.setConnectTimeout(requestTimeoutMs);
            if (conn.getResponseCode() != expectedResponseCode)
                throw new HttpException(conn.getResponseCode(), conn.getResponseMessage());

            // Read the information. I wish I could say (String) conn.getContent() here...
            int cl = conn.getContentLength();

            if (cl == -1)
                return null;

            byte[] b = new byte[cl];
            try (DataInputStream d = new DataInputStream((InputStream) conn.getContent()))
            {
                d.readFully(b);
            }
            return new String(b, UTF_8);
        }
        finally
        {
            if (conn != null)
                conn.disconnect();
        }
    }

    @Override
    public String toString()
    {
        return format("%s{%s=%s,%s=%s}", getClass().getName(),
                      METADATA_URL_PROPERTY, metadataServiceUrl,
                      METADATA_REQUEST_TIMEOUT_PROPERTY, requestTimeoutMs);
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

    public static class DefaultCloudMetadataServiceConnector extends AbstractCloudMetadataServiceConnector
    {
        public DefaultCloudMetadataServiceConnector(SnitchProperties properties)
        {
            super(properties);
        }
    }
}
