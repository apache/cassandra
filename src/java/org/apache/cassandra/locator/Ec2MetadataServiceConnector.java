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
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static org.apache.cassandra.locator.Ec2MetadataServiceConnector.EC2MetadataType.v2;

abstract class Ec2MetadataServiceConnector extends AbstractCloudMetadataServiceConnector
{
    static final String EC2_METADATA_TYPE_PROPERTY = "ec2_metadata_type";
    static final String EC2_METADATA_URL_PROPERTY = "ec2_metadata_url";
    static final String DEFAULT_EC2_METADATA_URL = "http://169.254.169.254";

    Ec2MetadataServiceConnector(SnitchProperties properties)
    {
        super(properties.putIfAbsent(METADATA_URL_PROPERTY, properties.get(EC2_METADATA_URL_PROPERTY, DEFAULT_EC2_METADATA_URL)));
    }

    enum EC2MetadataType
    {
        v1(props -> Ec2MetadataServiceConnector.V1Connector.create(props)),
        v2(props -> Ec2MetadataServiceConnector.V2Connector.create(props));

        private final Function<SnitchProperties, Ec2MetadataServiceConnector> provider;

        EC2MetadataType(Function<SnitchProperties, Ec2MetadataServiceConnector> provider)
        {
            this.provider = provider;
        }

        Ec2MetadataServiceConnector create(SnitchProperties properties)
        {
            return provider.apply(properties);
        }
    }

    static Ec2MetadataServiceConnector create(SnitchProperties props)
    {
        try
        {
            return EC2MetadataType.valueOf(props.get(EC2_METADATA_TYPE_PROPERTY, v2.name())).create(props);
        }
        catch (IllegalArgumentException ex)
        {
            throw new ConfigurationException(format("%s must be one of %s", EC2_METADATA_TYPE_PROPERTY,
                                                    stream(EC2MetadataType.values()).map(Enum::name).collect(joining(", "))));
        }
    }

    static class V1Connector extends Ec2MetadataServiceConnector
    {
        static V1Connector create(SnitchProperties props)
        {
            return new V1Connector(props);
        }

        V1Connector(SnitchProperties props)
        {
            super(props);
        }

        @Override
        public String toString()
        {
            return String.format("%s{%s=%s}", V1Connector.class.getName(), METADATA_URL_PROPERTY, metadataServiceUrl);
        }
    }

    static class V2Connector extends Ec2MetadataServiceConnector
    {
        static final int MAX_TOKEN_TIME_IN_SECONDS = 21600;
        static final int MIN_TOKEN_TIME_IN_SECONDS = 30;

        static final String AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER_PROPERTY = "ec2_metadata_token_ttl_seconds";
        static final String AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER = "X-aws-ec2-metadata-token-ttl-seconds";
        static final String AWS_EC2_METADATA_TOKEN_HEADER = "X-aws-ec2-metadata-token";
        static final String TOKEN_QUERY = "/latest/api/token";

        @VisibleForTesting
        static int HTTP_REQUEST_RETRIES = 1;

        private Pair<String, Long> token;
        @VisibleForTesting
        final Duration tokenTTL;

        static V2Connector create(SnitchProperties props)
        {
            String tokenTTLString = props.get(AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER_PROPERTY,
                                              Integer.toString(MAX_TOKEN_TIME_IN_SECONDS));

            Duration tokenTTL;
            try
            {
                tokenTTL = Duration.ofSeconds(Integer.parseInt(tokenTTLString));

                if (tokenTTL.getSeconds() < MIN_TOKEN_TIME_IN_SECONDS || tokenTTL.getSeconds() > MAX_TOKEN_TIME_IN_SECONDS)
                {
                    throw new ConfigurationException(format("property %s was set to %s seconds which is not in allowed range of [%s..%s]",
                                                            AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER_PROPERTY,
                                                            tokenTTL.getSeconds(),
                                                            MIN_TOKEN_TIME_IN_SECONDS,
                                                            MAX_TOKEN_TIME_IN_SECONDS));
                }
            }
            catch (NumberFormatException ex)
            {
                throw new ConfigurationException(format("Unable to parse integer from property %s, value to parse: %s",
                                                        AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER_PROPERTY, tokenTTLString));
            }

            return new V2Connector(props, tokenTTL);
        }

        V2Connector(SnitchProperties properties, Duration tokenTTL)
        {
            super(properties);
            this.tokenTTL = tokenTTL;
        }

        @Override
        public String apiCall(String url, String query, String method, Map<String, String> extraHeaders, int expectedResponseCode) throws IOException
        {
            Map<String, String> headers = new HashMap<>(extraHeaders);
            for (int retry = 0; retry <= HTTP_REQUEST_RETRIES; retry++)
            {
                String resolvedToken;
                if (token != null && token.right > Clock.Global.currentTimeMillis())
                    resolvedToken = token.left;
                else
                    resolvedToken = getToken();

                try
                {
                    headers.put(AWS_EC2_METADATA_TOKEN_HEADER, resolvedToken);
                    return super.apiCall(url, query, method, headers, expectedResponseCode);
                }
                catch (HttpException ex)
                {
                    if (retry == HTTP_REQUEST_RETRIES)
                        throw ex;

                    if (ex.responseCode == 401) // invalidate token if it is 401
                        this.token = null;
                }
            }

            throw new AssertionError();
        }

        @Override
        public String toString()
        {
            return String.format("%s{%s=%s,%s=%s}",
                                 V2Connector.class.getName(),
                                 METADATA_URL_PROPERTY, metadataServiceUrl,
                                 AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER_PROPERTY,
                                 tokenTTL.getSeconds());
        }

        /**
         * Get a session token to enable requests to the meta data service.
         * <a href="https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html">IMDSv2</a>
         */
        @VisibleForTesting
        public synchronized String getToken()
        {
            try
            {
                token = Pair.create(super.apiCall(metadataServiceUrl,
                                                  TOKEN_QUERY,
                                                  "PUT",
                                                  ImmutableMap.of(AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER, String.valueOf(tokenTTL.getSeconds())),
                                                  200),
                                    Clock.Global.currentTimeMillis() + tokenTTL.toMillis() - TimeUnit.SECONDS.toMillis(5));
                return token.left;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
