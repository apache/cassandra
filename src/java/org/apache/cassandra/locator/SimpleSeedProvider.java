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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleSeedProvider implements SeedProvider
{
    private static final Logger logger = LoggerFactory.getLogger(SimpleSeedProvider.class);
    @VisibleForTesting
    public static final String SEEDS_KEY = "seeds";
    @VisibleForTesting
    public static final String RESOLVE_MULTIPLE_IP_ADDRESSES_PER_DNS_RECORD_KEY = "resolve_multiple_ip_addresses_per_dns_record";
    private static final int SEED_COUNT_WARN_THRESHOLD = CassandraRelevantProperties.SEED_COUNT_WARN_THRESHOLD.getInt(20);

    private final String[] defaultSeeds;

    public SimpleSeedProvider(Map<String, String> args)
    {
        defaultSeeds = new String[] { FBUtilities.getLocalAddressAndPort().getHostAddress(true) };
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

        boolean resolveMultipleIps;
        String[] hosts;

        Map<String, String> parameters = conf.seed_provider.parameters;
        if (parameters == null)
        {
            resolveMultipleIps = false;
            hosts = defaultSeeds;
        }
        else
        {
            hosts = parameters.getOrDefault(SEEDS_KEY, defaultSeeds[0]).split(",", -1);
            resolveMultipleIps = Boolean.parseBoolean(parameters.getOrDefault(RESOLVE_MULTIPLE_IP_ADDRESSES_PER_DNS_RECORD_KEY, Boolean.FALSE.toString()));
        }

        List<InetAddressAndPort> seeds = new ArrayList<>(hosts.length);

        for (String host : hosts)
        {
            try
            {
                if (!host.trim().isEmpty())
                {
                    if (resolveMultipleIps)
                    {
                        List<InetAddressAndPort> resolvedSeeds = InetAddressAndPort.getAllByName(host.trim());
                        seeds.addAll(resolvedSeeds);
                        logger.debug("{} resolves to {}", host, resolvedSeeds);
                    }
                    else
                    {
                        InetAddressAndPort addressAndPort = InetAddressAndPort.getByName(host.trim());
                        seeds.add(addressAndPort);
                        logger.debug("Only resolving one IP per DNS record - {} resolves to {}", host, addressAndPort);
                    }
                }
            }
            catch (UnknownHostException ex)
            {
                // not fatal... DD will bark if there end up being zero seeds.
                logger.warn("Seed provider couldn't lookup host {}", host);
            }
        }

        if (seeds.size() > SEED_COUNT_WARN_THRESHOLD)
            logger.warn("Seed provider returned more than {} seeds. " +
                        "A large seed list may impact effectiveness of the third gossip round.",
                        SEED_COUNT_WARN_THRESHOLD);

        return Collections.unmodifiableList(seeds);
    }
}
