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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;


public class UrlSeedProvider implements SeedProvider
{
    private static final Logger logger = LoggerFactory.getLogger(UrlSeedProvider.class);
    @VisibleForTesting
    static final String URL_KEY ="seeds_url";
    private static final String READ_TIMEOUT_KEY ="read_timeout";
    private static final String CONNECT_TIMEOUT_KEY ="connect_timeout";

    public UrlSeedProvider(Map<String, String> args)
    {
    }

    /*
retry logic - I'm not sure we need this (or if it's beyond the scope of this ticket), or we can do it in a follow up ticket. wdyt, rustyrazorblade?
TLS support - If we do need this, we can do this in a follow up ticket
in #getUrlContent, please close the resources in a finally block.
might want to log a warning if there are no seeds returned from the call
     */

    private List<InetAddressAndPort> getUrlContent(Map<String,String> params)
    {

        try
        {
            URL seedUrl = new URL(params.get(URL_KEY));
            URLConnection connection = seedUrl.openConnection();

            connection.setReadTimeout(new DurationSpec.IntMillisecondsBound(params.getOrDefault(READ_TIMEOUT_KEY, "0ms")).toMilliseconds());
            connection.setConnectTimeout(new DurationSpec.IntMillisecondsBound(params.getOrDefault(CONNECT_TIMEOUT_KEY, "0ms")).toMilliseconds());

            List<InetAddressAndPort> seeds = new ArrayList<>();
            try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream())))
            {
                String host;
                while ((host = in.readLine()) != null)
                {
                    try
                    {
                        seeds.add(InetAddressAndPort.getByNameOverrideDefaults(host, null));
                    }
                    catch (UnknownHostException e)
                    {
                        logger.warn("Seed provider couldn't lookup host {}", host);
                    }
                }
            }
            return seeds;
        }
        catch (MalformedURLException e)
        {
            logger.warn("Seed url {} is malformed", params.get(URL_KEY));
        }
        catch (IOException e)
        {
            logger.warn("Unable to connect to the seed url {}", params.get(URL_KEY));
        }

        return Collections.emptyList();
    }

    public List<InetAddressAndPort> getSeeds()
    {

        Config conf;
        try
        {
            conf = DatabaseDescriptor.loadConfig();
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }

        assert conf.seed_provider != null : "conf.seed_provider is null!";

        assert conf.seed_provider.parameters != null : "conf.seed_provider.parameters is null!";

        return Collections.unmodifiableList(getUrlContent(conf.seed_provider.parameters));

    }
}