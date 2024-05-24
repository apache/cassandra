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

package org.apache.cassandra.dht;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.TokenMetadata;

/**
 * Defines options for the bootstrap process to restrict the sources from which to stream data.
 */
public class BootstrapSourceFilter implements RangeStreamer.ISourceFilter
{
    private final List<RangeStreamer.ISourceFilter> filters = new ArrayList<>();

    private BootstrapSourceFilter(Collection<RangeStreamer.ISourceFilter> filters)
    {
        this.filters.addAll(filters);
    }

    /**
     * Tests all subfilters and returns true if all of them return true.
     * If subfilters are empty, always return true.
     *
     * @param endpoint endpoint to test
     * @return true if all subfilters return true, false otherwise
     */
    @Override
    public boolean shouldInclude(InetAddress endpoint)
    {
        if (filters.isEmpty())
            return true;

        for (RangeStreamer.ISourceFilter filter : filters)
        {
            if (!filter.shouldInclude(endpoint))
                return false;
        }

        return true;
    }

    /**
     * @return the builder for {@link BootstrapSourceFilter}.
     */
    public static Builder builder(TokenMetadata tmd, IEndpointSnitch snitch)
    {
        return new Builder(tmd.cloneOnlyTokenMap().getAllMembers(), snitch);
    }

    /**
     * Builder class for {@link BootstrapSourceFilter}.
     */
    public static class Builder
    {
        /*
         * Special value to include all racks in a datacenter.
         * A whitespace is used to avoid conflict with the actual rack name.
         * It is assumed to be safe because the rack name cannot contain just a whitespace,
         * since the implementations of IEndpointSnitch are expected to trim the rack name.
         */
        private static final String ALL_RACKS = " ";

        private final Set<InetAddress> sources = new HashSet<>();
        private final SetMultimap<String, String> includedDcRacks = HashMultimap.create();
        private final SetMultimap<String, String> excludedDcRacks = HashMultimap.create();
        private final Collection<InetAddress> allMembers;
        private final IEndpointSnitch snitch;

        private Builder(Collection<InetAddress> allMembers, IEndpointSnitch snitch)
        {
            this.allMembers = allMembers;
            this.snitch = snitch;
        }

        /**
         * Add source to include in the bootstrap process.
         *
         * @param source inet address of the source to include
         * @return this builder
         * @throws IllegalArgumentException if the source is not part of the token metadata
         */
        public Builder include(InetAddress source)
        {
            if (!allMembers.contains(source))
                throw new IllegalArgumentException(String.format("The source %s is not part of the cluster", source));
            this.sources.add(source);
            return this;
        }

        /**
         * Add srouces from specified datacenters to include in the bootstrap process.
         *
         * @param datacenter the name of the datacenter to include sources from
         * @return this builder
         * @throws IllegalArgumentException if the dc is not part of the token metadata
         */
        public Builder includeDc(String datacenter)
        {
            return includeDcRack(datacenter, null);
        }

        /**
         * Add srouces from specified datacenters and racks to include in the bootstrap process.
         * If the same datacenter name or rack name is specified multiple times using this method, the last one is used.
         *
         * @param datacenter the name of the datacenter
         * @param rack the name of the rack in datacenter to include sources from. If null, include all racks in the datacenter.
         * @return this builder
         * @throws IllegalArgumentException if the dc is not part of the token metadata
         */
        public Builder includeDcRack(String datacenter, String rack)
        {
            validateDcRack(datacenter, rack);
            if (rack == null)
                includedDcRacks.put(datacenter, ALL_RACKS);
            else
                includedDcRacks.put(datacenter, rack);
            return this;
        }

        public Builder excludeDc(String datacenter)
        {
            return excludeDcRack(datacenter, null);
        }

        /**
         * Exclude srouces from specified datacenters and racks to include in the bootstrap process.
         *
         * @param datacenter the name of the datacenter
         * @param rack the name of the rack in datacenter to include sources from. If null, include all racks in the datacenter.
         * @return this builder
         * @throws IllegalArgumentException if the dc is not part of the token metadata
         */
        public Builder excludeDcRack(String datacenter, String rack)
        {
            validateDcRack(datacenter, rack);
            if (rack == null)
                excludedDcRacks.put(datacenter, ALL_RACKS);
            else
                excludedDcRacks.put(datacenter, rack);
            return this;
        }

        public BootstrapSourceFilter build()
        {
            // Validate conflicting case
            // - the same datacenter/rack pair is both included and excluded
            for (Map.Entry<String, String> dcRack : includedDcRacks.entries())
            {
                if (excludedDcRacks.containsEntry(dcRack.getKey(), dcRack.getValue()))
                    throw new IllegalArgumentException(String.format("%s%s is included and excluded",
                                                                     dcRack.getKey(),
                                                                     ALL_RACKS.equals(dcRack.getValue()) ? "" : ':' + dcRack.getValue()));
            }

            // Build final filter
            List<RangeStreamer.ISourceFilter> filters = new ArrayList<>();
            if (!sources.isEmpty())
                filters.add(sources::contains);

            if (!includedDcRacks.isEmpty())
                filters.add(endpoint -> {
                    String dc = snitch.getDatacenter(endpoint);
                    String rack = snitch.getRack(endpoint);
                    return includedDcRacks.containsEntry(dc, ALL_RACKS) || includedDcRacks.containsEntry(dc, rack);
                });

            if (!excludedDcRacks.isEmpty())
                filters.add(endpoint -> {
                    String dc = snitch.getDatacenter(endpoint);
                    String rack = snitch.getRack(endpoint);
                    return !(excludedDcRacks.containsEntry(dc, ALL_RACKS) || excludedDcRacks.containsEntry(dc, rack));
                });
            return new BootstrapSourceFilter(filters);
        }

        /**
         * Validate that given datacenter and rack pair exists in the cluster.
         *
         * @param datacenter name of the datacenter
         * @param rack name of the rack
         */
        private void validateDcRack(String datacenter, String rack)
        {
            if (datacenter == null)
                throw new IllegalArgumentException("Datacenter name cannot be null");

            boolean found = false;
            for (InetAddress node : allMembers) {
                if (datacenter.equals(snitch.getDatacenter(node))) {
                    if (rack != null) {
                        if (rack.equals(snitch.getRack(node))) {
                            found = true;
                            break;
                        }
                    } else {
                        found = true;
                        break;
                    }
                }
            }

            if (!found) {
                String message = rack == null
                                 ? String.format("%s is not part of the cluster", datacenter)
                                 : String.format("%s:%s is not part of the cluster", datacenter, rack);
                throw new IllegalArgumentException(message);
            }
        }
    }
}