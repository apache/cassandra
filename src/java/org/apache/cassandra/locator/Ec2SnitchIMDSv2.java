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
import java.io.FilterInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the Ec2Snitch which uses Instance Meta Data Service v2 (IMDSv2) which requires you
 * to get a session token first before calling the metaData service
 */
public class Ec2SnitchIMDSv2 extends Ec2Snitch
{
    protected static final Logger logger = LoggerFactory.getLogger(Ec2SnitchIMDSv2.class);
    private static final long REFRESH_TOKEN_TIME = 21600;
    private static final String AWS_EC2_METADATA_HEADER_TTL = "X-aws-ec2-metadata-token-ttl-seconds";
    private static final String TOKEN_TTL_SECONDS = String.valueOf(REFRESH_TOKEN_TIME);
    private static final String AWS_EC2_METADATA_HEADER = "X-aws-ec2-metadata-token";
    private static final String TOKEN_ENDPOINT = "http://169.254.169.254/latest/api/token";

    private String myToken;
    private Long myLastTokenTime;


    public Ec2SnitchIMDSv2() throws IOException, ConfigurationException
    {
        super();
    }

    @Override
    String awsApiCall(final String url) throws IOException, ConfigurationException
    {
        // Populate the region and zone by introspection, fail if 404 on metadata
        if (myToken == null || myLastTokenTime == null
            || System.currentTimeMillis() - myLastTokenTime > (REFRESH_TOKEN_TIME - 100))
        {
            getAndSetNewToken();
        }
        final HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestProperty(AWS_EC2_METADATA_HEADER, myToken);
        return getContent(conn);
    }

    /**
     * Get a session token to enable requests to the meta data service.
     * https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html
     *
     * @throws IOException
     */
    private void getAndSetNewToken() throws IOException
    {
        final URL url = new URL(TOKEN_ENDPOINT);
        final HttpURLConnection http = (HttpURLConnection) url.openConnection();
        http.setRequestProperty(AWS_EC2_METADATA_HEADER_TTL, TOKEN_TTL_SECONDS);
        http.setRequestMethod("PUT");

        myToken = getContent(http);
        myLastTokenTime = System.currentTimeMillis();
    }

    private String getContent(final HttpURLConnection conn) throws IOException
    {
        DataInputStream d = null;
        try
        {
            if (conn.getResponseCode() != 200)
            {
                throw new ConfigurationException(
                "Ec2SnitchIMDSv2 was unable to execute the API call. Not an ec2 node?");
            }
            // Read the information. I wish I could say (String) conn.getContent() here...
            final int cl = conn.getContentLength();
            final byte[] b = new byte[cl];
            d = new DataInputStream((FilterInputStream) conn.getContent());
            d.readFully(b);
            return new String(b, StandardCharsets.UTF_8);
        }
        finally
        {
            FileUtils.close(d);
            conn.disconnect();
        }
    }
}
