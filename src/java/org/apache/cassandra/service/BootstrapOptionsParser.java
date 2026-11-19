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


import org.apache.cassandra.dht.BootstrapSourceFilter;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.config.CassandraRelevantProperties.*;

/**
 * Constructs {@link BootstrapSourceFilter} from system properties and others.
 */
public class BootstrapOptionsParser
{

    public static BootstrapSourceFilter parse(ClusterMetadata metadata, IEndpointSnitch snitch)
    {
        BootstrapSourceFilter.Builder builder = BootstrapSourceFilter.builder(metadata, snitch);

        String includeDcRackOption = BOOTSTRAP_INCLUDE_DCS.getString();
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
        String excludeDcRackOption = BOOTSTRAP_EXCLUDE_DCS.getString();
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

        String includeSourcesOption = BOOTSTRAP_INCLUDE_SOURCES.getString();
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
