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

import static java.lang.String.format;

abstract class AbstractCloudMetadataServiceConnector extends HttpServiceConnector
{
    static final String METADATA_URL_PROPERTY = "metadata_url";
    static final String METADATA_REQUEST_TIMEOUT_PROPERTY = "metadata_request_timeout";
    static final String DEFAULT_METADATA_REQUEST_TIMEOUT = "30s";

    private final SnitchProperties properties;

    AbstractCloudMetadataServiceConnector(SnitchProperties snitchProperties)
    {
        super(HttpServiceConnector.resolveMetadataUrl(snitchProperties.getProperties(), METADATA_URL_PROPERTY),
              HttpServiceConnector.resolveRequestTimeoutMs(snitchProperties.getProperties(),
                                                           METADATA_REQUEST_TIMEOUT_PROPERTY,
                                                           DEFAULT_METADATA_REQUEST_TIMEOUT));
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
                      METADATA_URL_PROPERTY, metadataServiceUrl,
                      METADATA_REQUEST_TIMEOUT_PROPERTY, requestTimeoutMs);
    }

    public static class DefaultCloudMetadataServiceConnector extends AbstractCloudMetadataServiceConnector
    {
        public DefaultCloudMetadataServiceConnector(SnitchProperties properties)
        {
            super(properties);
        }
    }
}
