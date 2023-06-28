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

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.cassandra.locator.Ec2MetadataServiceConnector.V2Connector;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.cassandra.locator.Ec2MetadataServiceConnector.EC2MetadataType.v2;
import static org.apache.cassandra.locator.Ec2MetadataServiceConnector.EC2_METADATA_TYPE_PROPERTY;
import static org.apache.cassandra.locator.Ec2MetadataServiceConnector.V1Connector.EC2_METADATA_URL_PROPERTY;
import static org.apache.cassandra.locator.Ec2MetadataServiceConnector.V2Connector.AWS_EC2_METADATA_TOKEN_HEADER;
import static org.apache.cassandra.locator.Ec2MetadataServiceConnector.V2Connector.AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER;
import static org.apache.cassandra.locator.Ec2MetadataServiceConnector.V2Connector.AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER_PROPERTY;
import static org.apache.cassandra.locator.Ec2MetadataServiceConnector.V2Connector.TOKEN_QUERY;
import static org.apache.cassandra.locator.Ec2Snitch.ZONE_NAME_QUERY;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class Ec2V2ConnectorMockingTest
{
    private static final String token = "thisismytoken";
    private static final String az = "us-east-1a";

    @Rule
    public WireMockRule v2Service = new WireMockRule(wireMockConfig().bindAddress("127.0.0.1").port(8080));

    @Test
    public void testV2Connector() throws Throwable
    {
        v2Service.stubFor(tokenRequest(100, 200, token));
        v2Service.stubFor(azRequest(az, 200, token));

        assertEquals(az, getConnector(100).apiCall(ZONE_NAME_QUERY));
    }

    @Test
    public void testV2ConnectorWhenUnauthorized()
    {
        String unauthorizedBody = "<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>\n" +
                                  "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"\n" +
                                  "\t\"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">\n" +
                                  "<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\" lang=\"en\">\n" +
                                  " <head>\n" +
                                  "  <title>401 - Unauthorized</title>\n" +
                                  " </head>\n" +
                                  " <body>\n" +
                                  "  <h1>401 - Unauthorized</h1>\n" +
                                  " </body>\n" +
                                  "</html>\n";

        v2Service.stubFor(tokenRequest(100, 200, token));

        v2Service.stubFor(get(urlEqualTo(ZONE_NAME_QUERY)).withHeader(AWS_EC2_METADATA_TOKEN_HEADER, equalTo(token))
                                                          .willReturn(aResponse().withStatus(401)
                                                                                 .withStatusMessage("Unauthorized")
                                                                                 .withHeader("Content-Type", "text/html")
                                                                                 .withHeader("Content-Length", String.valueOf(unauthorizedBody.getBytes(UTF_8).length))));

        V2Connector.HTTP_REQUEST_RETRIES = 0;
        Ec2MetadataServiceConnector v2Connector = getConnector(100);

        assertThatExceptionOfType(AbstractCloudMetadataServiceConnector.HttpException.class)
        .isThrownBy(() -> v2Connector.apiCall(ZONE_NAME_QUERY))
        .matches(ex -> ex.responseCode == 401 && "Unauthorized".equals(ex.responseMessage),
                 "exception should have response code 401 with response message 'Unauthorized'");
    }

    @Test
    public void testCachedToken() throws Throwable
    {
        v2Service.stubFor(tokenRequest(30, 200, token));
        v2Service.stubFor(azRequest(az, 200, token));

        V2Connector spiedConnector = (V2Connector) spy(getConnector(30));

        spiedConnector.apiCall(ZONE_NAME_QUERY);
        verify(spiedConnector, times(1)).getToken();

        // lets wait 10 seconds and make a call again
        // which will use cached token and will not call token endpoint
        // for the second time
        Thread.sleep(10000);

        // as token is not expired yet, another call to getToken will not be done
        spiedConnector.apiCall(ZONE_NAME_QUERY);

        // here we still have just 1 call to getToken method because we used a cached token
        verify(spiedConnector, times(1)).getToken();
    }

    @Test
    public void testExpiredTokenInteraction() throws Throwable
    {
        v2Service.stubFor(tokenRequest(30, 200, token));
        v2Service.stubFor(azRequest(az, 200, token));

        V2Connector spiedConnector = (V2Connector) spy(getConnector(30));

        spiedConnector.apiCall(ZONE_NAME_QUERY);
        verify(spiedConnector, times(1)).getToken();

        // lets expire the token
        Thread.sleep(35000);

        // as token is expired, another call to getToken will be done
        spiedConnector.apiCall(ZONE_NAME_QUERY);
        verify(spiedConnector, times(2)).getToken();
    }

    private Ec2MetadataServiceConnector getConnector(int tokenTTL)
    {
        Properties p = new Properties();
        p.setProperty(EC2_METADATA_TYPE_PROPERTY, v2.toString());
        p.setProperty(EC2_METADATA_URL_PROPERTY, "http://127.0.0.1:8080");
        p.setProperty(AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER_PROPERTY, String.valueOf(tokenTTL));

        return Ec2MetadataServiceConnector.create(new SnitchProperties(p));
    }

    private MappingBuilder tokenRequest(int ttl, int status, String tokenToReturn)
    {
        return put(urlEqualTo(TOKEN_QUERY)).withHeader(AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER, equalTo(String.valueOf(ttl)))
                                           .willReturn(aResponse().withBody(tokenToReturn)
                                                                  .withStatus(status)
                                                                  .withHeader("Content-Type", "text/plain")
                                                                  .withHeader("Content-Length", String.valueOf(tokenToReturn.getBytes(UTF_8).length)));
    }

    private MappingBuilder azRequest(String az, int status, String token)
    {
        return get(urlEqualTo(ZONE_NAME_QUERY)).withHeader(AWS_EC2_METADATA_TOKEN_HEADER, equalTo(token))
                                               .willReturn(aResponse().withBody(az)
                                                                      .withStatus(status)
                                                                      .withHeader("Content-Type", "text/plain")
                                                                      .withHeader("Content-Length", String.valueOf(az.getBytes(UTF_8).length)));
    }
}
