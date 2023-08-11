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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.cassandra.locator.AbstractCloudMetadataServiceConnector.DefaultCloudMetadataServiceConnector;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;
import static org.apache.cassandra.locator.AbstractCloudMetadataServiceConnector.METADATA_URL_PROPERTY;

/**
 * AzureSnitch will resolve datacenter and rack by calling {@code /metadata/instance/compute} endpoint returning
 * the response in JSON format for API version {@code 2021-12-13}. The version of API is configurable via property
 * {@code azure_api_version} in cassandra-rackdc.properties.
 * <p>
 * A datacenter is resolved from {@code location} field and a rack is resolved by looking into {@code zone} field first.
 * When zone is not set, or it is empty string, it will look into {@code platformFaultDomain} field. Such resolved
 * value is prepended by {@code rack-} string.
 */
public class AzureSnitch extends AbstractCloudMetadataServiceSnitch
{
    static final String DEFAULT_METADATA_SERVICE_URL = "http://169.254.169.254";
    static final String METADATA_QUERY_TEMPLATE = "/metadata/instance/compute?api-version=%s&format=json";
    static final String METADATA_HEADER = "Metadata";
    static final String API_VERSION_PROPERTY_KEY = "azure_api_version";
    static final String DEFAULT_API_VERSION = "2021-12-13";

    public AzureSnitch() throws IOException
    {
        this(new SnitchProperties());
    }

    public AzureSnitch(SnitchProperties properties) throws IOException
    {
        this(new DefaultCloudMetadataServiceConnector(properties.putIfAbsent(METADATA_URL_PROPERTY,
                                                                             DEFAULT_METADATA_SERVICE_URL)));
    }

    public AzureSnitch(AbstractCloudMetadataServiceConnector connector) throws IOException
    {
        super(connector, resolveDcAndRack(connector));
    }

    private static Pair<String, String> resolveDcAndRack(AbstractCloudMetadataServiceConnector connector) throws IOException
    {
        String apiVersion = connector.getProperties().get(API_VERSION_PROPERTY_KEY, DEFAULT_API_VERSION);
        String response = connector.apiCall(format(METADATA_QUERY_TEMPLATE, apiVersion), ImmutableMap.of(METADATA_HEADER, "true"));
        JsonNode jsonNode = JsonUtils.JSON_OBJECT_MAPPER.readTree(response);

        JsonNode location = jsonNode.get("location");
        JsonNode zone = jsonNode.get("zone");
        JsonNode platformFaultDomain = jsonNode.get("platformFaultDomain");

        String datacenter;
        String rack;

        if (location == null || location.isNull() || location.asText().isEmpty())
            datacenter = DEFAULT_DC;
        else
            datacenter = location.asText();

        if (zone == null || zone.isNull() || zone.asText().isEmpty())
        {
            if (platformFaultDomain == null || platformFaultDomain.isNull() || platformFaultDomain.asText().isEmpty())
            {
                rack = DEFAULT_RACK;
            }
            else
            {
                rack = platformFaultDomain.asText();
            }
        }
        else
        {
            rack = zone.asText();
        }

        return Pair.create(datacenter + connector.getProperties().getDcSuffix(), "rack-" + rack);
    }
}
