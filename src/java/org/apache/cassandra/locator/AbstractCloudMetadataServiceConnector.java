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

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.HttpService;

import static java.lang.String.format;

public abstract class AbstractCloudMetadataServiceConnector extends HttpService
{
    public static final String METADATA_URL_PROPERTY = "metadata_url";
    public static final String METADATA_REQUEST_TIMEOUT_PROPERTY = "metadata_request_timeout";
    public static final String DEFAULT_METADATA_REQUEST_TIMEOUT = "30s";
    private final SnitchProperties properties;

    public AbstractCloudMetadataServiceConnector(SnitchProperties snitchProperties)
    {
        super(parseServiceUrl(snitchProperties),
              parseRequestTimeout(snitchProperties));

        this.properties = snitchProperties;
    }

    public SnitchProperties getProperties()
    {
        return properties;
    }

    @Override
    public String toString()
    {
        return format("%s{%s=%s,%s=%s}", getClass().getName(),
                      METADATA_URL_PROPERTY, serviceUrl,
                      METADATA_REQUEST_TIMEOUT_PROPERTY, requestTimeoutMs);
    }

    public static class DefaultCloudMetadataServiceConnector extends AbstractCloudMetadataServiceConnector
    {
        public DefaultCloudMetadataServiceConnector(SnitchProperties properties)
        {
            super(properties);
        }
    }

    private static String parseServiceUrl(SnitchProperties snitchProperties)
    {
        String parsedMetadataServiceUrl = snitchProperties.get(METADATA_URL_PROPERTY, null);

        try
        {
            URL url = new URL(parsedMetadataServiceUrl);
            url.toURI();

            return parsedMetadataServiceUrl;
        }
        catch (MalformedURLException | IllegalArgumentException | URISyntaxException ex)
        {
            throw new ConfigurationException(format("Snitch metadata service URL '%s' is invalid. Please review snitch properties " +
                                                    "defined in the configured '%s' configuration file.",
                                                    parsedMetadataServiceUrl,
                                                    CassandraRelevantProperties.CASSANDRA_RACKDC_PROPERTIES.getKey()),
                                             ex);
        }
    }

    private static int parseRequestTimeout(SnitchProperties snitchProperties)
    {
        String metadataRequestTimeout = snitchProperties.get(METADATA_REQUEST_TIMEOUT_PROPERTY, DEFAULT_METADATA_REQUEST_TIMEOUT);

        try
        {
            return new DurationSpec.IntMillisecondsBound(metadataRequestTimeout).toMilliseconds();
        }
        catch (IllegalArgumentException ex)
        {
            throw new ConfigurationException(format("%s as value of %s is invalid duration! " + ex.getMessage(),
                                                    metadataRequestTimeout,
                                                    METADATA_REQUEST_TIMEOUT_PROPERTY));
        }
    }
}
