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

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleSeedProvider implements SeedProvider
{
    private static final Logger logger = LoggerFactory.getLogger(SimpleSeedProvider.class);

    public SimpleSeedProvider(Map<String, String> args) {}

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
        String[] hosts = conf.seed_provider.parameters.get("seeds").split(",", -1);
        List<InetAddressAndPort> seeds = new ArrayList<>(hosts.length);
        for (String host : hosts)
        {
            try
            {
                if(!host.trim().isEmpty()) {
                    seeds.add(InetAddressAndPort.getByName(host.trim()));
                }
            }
            catch (UnknownHostException ex)
            {
                // not fatal... DD will bark if there end up being zero seeds.
                logger.warn("Seed provider couldn't lookup host {}", host);
            }
        }
        return Collections.unmodifiableList(seeds);
    }
}
