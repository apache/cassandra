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

import java.io.IOException;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.locator.AbstractCloudMetadataServiceConnector.DefaultCloudMetadataServiceConnector;

import static org.apache.cassandra.locator.AbstractCloudMetadataServiceConnector.METADATA_URL_PROPERTY;

public class GoogleCloudLocationProvider extends CloudMetadataLocationProvider
{
    static final String DEFAULT_METADATA_SERVICE_URL = "http://metadata.google.internal";
    static final String ZONE_NAME_QUERY_URL = "/computeMetadata/v1/instance/zone";
    static final ImmutableMap<String, String> HEADERS = ImmutableMap.of("Metadata-Flavor", "Google");

    /**
     * Used via reflection by DatabaseDescriptor::createInitialLocationProvider
     */
    public GoogleCloudLocationProvider() throws IOException
    {
        this(new SnitchProperties());
    }

    public GoogleCloudLocationProvider(SnitchProperties properties) throws IOException
    {
        this(new DefaultCloudMetadataServiceConnector(properties.putIfAbsent(METADATA_URL_PROPERTY,
                                                                             DEFAULT_METADATA_SERVICE_URL)));
    }

    public GoogleCloudLocationProvider(AbstractCloudMetadataServiceConnector connector) throws IOException
    {
        super(connector, c -> SnitchUtils.parseLocation(c.apiCall(ZONE_NAME_QUERY_URL, HEADERS),
                                                        c.getProperties().getDcSuffix()));
    }
}
