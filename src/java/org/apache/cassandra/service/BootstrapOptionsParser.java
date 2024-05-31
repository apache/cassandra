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

package org.apache.cassandra.service;


import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.BootstrapSourceFilter;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;

/**
 * Constructs {@link BootstrapSourceFilter} from system properties and others.
 */
public class BootstrapOptionsParser
{
    /**
     * The system property to specify the sources in specific dc and rack.
     * <p>
     * Example:
     * <dl>
     *     <dt>{@code -Dcassandra.bootstrap.include_dcs="dc1"}</li>
     *     <dd>Include only sources in dc1.</dd>
     *     <dt>{@code -Dcassandra.bootstrap.include_dcs="dc1:rack1"}</li>
     *     <dd>Include only sources in rack1 of dc1.</dd>
     *     <dt>{@code -Dcassandra.bootstrap.include_dcs="dc1:rack1,dc1:rack2"}</li>
     *     <dd>Include only sources in rack1 and rack2 of dc1.</dd>
     * </dl>
     */
    public static final String BOOTSTRAP_INCLUDE_DCS = Config.PROPERTY_PREFIX + "bootstrap.include_dcs";

    /**
     * The system property to specify the sources to exclude in specific dc and rack.
     */
    public static final String BOOTSTRAP_EXCLUDE_DCS = Config.PROPERTY_PREFIX + "bootstrap.exclude_dcs";

    /**
     * The system property to specify the sources using their IP addresses.
     * <p>
     * Example:
     * <dl>
     *     <dt>{@code -Dcassandra.bootstrap.include_sources="10.0.0.1,10.0.0.2"}</li>
     *     <dd>Include only 10.0.0.1 and 10.0.0.2 as sources regardless of dc and rack</dd>
     * </dl>
     * Can be used with {@code cassandra.bootstrap.include_dcs} to further restrict the sources.
     */
    public static final String BOOTSTRAP_INCLUDE_SOURCES = Config.PROPERTY_PREFIX + "bootstrap.include_sources";

    public static BootstrapSourceFilter parse(TokenMetadata tmd, IEndpointSnitch snitch)
    {
        BootstrapSourceFilter.Builder builder = BootstrapSourceFilter.builder(tmd, snitch);

        String includeDcRackOption = System.getProperty(BOOTSTRAP_INCLUDE_DCS);
        if (includeDcRackOption != null)
        {
            String[] dcRacks = includeDcRackOption.split(",");
            for (String dcRack : dcRacks)
            {
                String[] parts = dcRack.split(":");
                if (parts.length == 1)
                    builder.includeDc(parts[0].trim());
                else if (parts.length == 2)
                    builder.includeDcRack(parts[0].trim(), parts[1].trim());
                else
                    throw new IllegalArgumentException("Invalid dc:rack option: " + includeDcRackOption);
            }
        }

        String excludeDcRackOption = System.getProperty(BOOTSTRAP_EXCLUDE_DCS);
        if (excludeDcRackOption != null)
        {
            String[] dcRacks = excludeDcRackOption.split(",");
            for (String dcRack : dcRacks)
            {
                String[] parts = dcRack.split(":");
                if (parts.length == 1)
                    builder.excludeDc(parts[0].trim());
                else if (parts.length == 2)
                    builder.excludeDcRack(parts[0].trim(), parts[1].trim());
                else
                    throw new IllegalArgumentException("Invalid dc:rack option: " + excludeDcRackOption);
            }
        }

        String includeSourcesOption = System.getProperty(BOOTSTRAP_INCLUDE_SOURCES);
        if (includeSourcesOption != null)
        {
            String[] sources = includeSourcesOption.split(",");
            for (String source : sources)
            {
                try
                {
                    InetAddressAndPort sourceAddress = InetAddressAndPort.getByName(source.trim());
                    builder.include(sourceAddress);
                }
                catch (Exception e)
                {
                    throw new IllegalArgumentException("Invalid source address in option: " + includeSourcesOption, e);
                }
            }
        }

        return builder.build();
    }
}
