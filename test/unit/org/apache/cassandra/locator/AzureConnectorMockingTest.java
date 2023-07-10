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

import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.cassandra.locator.AbstractCloudMetadataServiceConnector.DefaultCloudMetadataServiceConnector;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.cassandra.locator.AbstractCloudMetadataServiceConnector.METADATA_URL_PROPERTY;
import static org.apache.cassandra.locator.AzureSnitch.API_VERSION_PROPERTY_KEY;
import static org.apache.cassandra.locator.AzureSnitch.METADATA_HEADER;
import static org.apache.cassandra.locator.AzureSnitch.METADATA_QUERY_TEMPLATE;
import static org.junit.Assert.assertEquals;

public class AzureConnectorMockingTest
{
    @Rule
    public final WireMockRule service = new WireMockRule(wireMockConfig().bindAddress("127.0.0.1").port(8080));

    @Test
    public void testConnector() throws Throwable
    {
        String RESPONSE = "{\"location\": \"PolandCentral\", \"zone\": \"1\", \"platformFaultDomain\": \"5\"}";

        service.stubFor(get(urlEqualTo(format(METADATA_QUERY_TEMPLATE, "2021-12-13")))
                        .withHeader(METADATA_HEADER, equalTo("true"))
                        .willReturn(aResponse().withBody(RESPONSE)
                                               .withStatus(200)
                                               .withHeader("Content-Type", "application/json; charset=utf-8")
                                               .withHeader("Content-Length", String.valueOf(RESPONSE.getBytes(UTF_8).length))));

        Properties p = new Properties();
        p.setProperty(METADATA_URL_PROPERTY, "http://127.0.0.1:8080");

        SnitchProperties snitchProperties = new SnitchProperties(p);
        AzureSnitch azureSnitch = new AzureSnitch(new DefaultCloudMetadataServiceConnector(snitchProperties));

        assertEquals("rack-1", azureSnitch.getLocalRack());
        assertEquals("PolandCentral", azureSnitch.getLocalDatacenter());
    }

    @Test
    public void testMissingZone() throws Throwable
    {
        String RESPONSE = "{\"location\": \"PolandCentral\", \"zone\": \"\", \"platformFaultDomain\": \"5\"}";

        service.stubFor(get(urlEqualTo(format(METADATA_QUERY_TEMPLATE, "2021-12-14")))
                        .withHeader(METADATA_HEADER, equalTo("true"))
                        .willReturn(aResponse().withBody(RESPONSE)
                                               .withStatus(200)
                                               .withHeader("Content-Type", "application/json; charset=utf-8")
                                               .withHeader("Content-Length", String.valueOf(RESPONSE.getBytes(UTF_8).length))));

        Properties p = new Properties();
        p.setProperty(METADATA_URL_PROPERTY, "http://127.0.0.1:8080");
        p.setProperty(API_VERSION_PROPERTY_KEY, "2021-12-14");

        SnitchProperties snitchProperties = new SnitchProperties(p);
        AzureSnitch azureSnitch = new AzureSnitch(new DefaultCloudMetadataServiceConnector(snitchProperties));

        assertEquals("rack-5", azureSnitch.getLocalRack());
        assertEquals("PolandCentral", azureSnitch.getLocalDatacenter());
    }
}
