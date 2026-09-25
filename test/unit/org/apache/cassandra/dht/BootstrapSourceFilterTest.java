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

import java.net.UnknownHostException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.BootstrapOptionsParser;

import static org.apache.cassandra.config.CassandraRelevantProperties.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BootstrapSourceFilterTest
{
    private static final IEndpointSnitch snitch = new AbstractNetworkTopologySnitch()
    {
        @Override
        public String getRack(InetAddressAndPort endpoint)
        {
            return "Rack-" + endpoint.addressBytes[2];
        }

        @Override
        public String getDatacenter(InetAddressAndPort endpoint)
        {
            return "DC-" + endpoint.addressBytes[1];
        }
    };

    @BeforeClass
    public static void setUpSnitch()
    {
        ServerTestUtils.prepareServerNoRegister();

        // add 2 DCs, with 3 racks each, and 2 nodes per rack (6 nodes per DC)
        for (int dc = 1; dc <= 2; dc++)
        {
            for (int rack = 1; rack <= 3; rack++)
            {
                for (int node = 1; node <= 2; node++)
                {
                    InetAddressAndPort addr = addr(dc, rack, node);
                    Token t = DatabaseDescriptor.getPartitioner().getRandomToken();
                    ClusterMetadataTestHelper.addEndpoint(addr, t, snitch.getDatacenter(addr), snitch.getRack(addr));
                }
            }
        }
        DatabaseDescriptor.setEndpointSnitch(snitch);
        ServerTestUtils.markCMS();
    }

    /**
     * Topology that contains 3 DCs, with 2 racks each, and 3 nodes per rack (6 nodes per DC).
     */
    private final ClusterMetadata clusterMetadata;
    private final AbstractReplicationStrategy replicationStrategy;

    public BootstrapSourceFilterTest()
    {
        this.clusterMetadata = ClusterMetadata.current();
        replicationStrategy = new SimpleStrategy("ks", ImmutableMap.of(SimpleStrategy.REPLICATION_FACTOR, "1"));
    }

    @Test
    public void testEmpty()
    {
        BootstrapSourceFilter filter = BootstrapSourceFilter.builder(clusterMetadata, snitch).build();

        Set<InetAddressAndPort> restrictedSources = StreamSupport.stream(replicationStrategy.getRangeAddresses(clusterMetadata)
                                                                                            .flattenValues().spliterator(), false)
                                                                 .filter(filter::apply)
                                                                 .map(Replica::endpoint)
                                                                 .collect(Collectors.toSet());

        // All 12 nodes should be included
        assertEquals(12, restrictedSources.size());
    }

    @Test
    public void testIncludeSources()
    {
        BootstrapSourceFilter filter = BootstrapSourceFilter.builder(clusterMetadata, snitch)
                                                            .include(addr(1, 1, 1))
                                                            .include(addr(1, 1, 2))
                                                            .build();

        Set<InetAddressAndPort> restrictedSources = StreamSupport.stream(replicationStrategy.getRangeAddresses(clusterMetadata)
                                                                                            .flattenValues().spliterator(), false)
                                                                 .filter(filter::apply)
                                                                 .map(Replica::endpoint)
                                                                 .collect(Collectors.toSet());

        assertEquals(2, restrictedSources.size());
        assertTrue(restrictedSources.contains(addr(1, 1, 1)));
        assertTrue(restrictedSources.contains(addr(1, 1, 2)));
    }

    @Test
    public void testIncludeDc()
    {
        BootstrapSourceFilter filter = BootstrapSourceFilter.builder(clusterMetadata, snitch)
                                                            .includeDc("DC-1")
                                                            .build();

        Set<InetAddressAndPort> restrictedSources = StreamSupport.stream(replicationStrategy.getRangeAddresses(clusterMetadata)
                                                                                            .flattenValues().spliterator(), false)
                                                                 .filter(filter::apply)
                                                                 .map(Replica::endpoint)
                                                                 .collect(Collectors.toSet());

        // 6 nodes in DC-1 should be included
        assertEquals(6, restrictedSources.size());
        assertTrue(restrictedSources.contains(addr(1, 1, 1)));
        assertTrue(restrictedSources.contains(addr(1, 1, 2)));
        assertTrue(restrictedSources.contains(addr(1, 2, 1)));
        assertTrue(restrictedSources.contains(addr(1, 2, 2)));
        assertTrue(restrictedSources.contains(addr(1, 3, 1)));
        assertTrue(restrictedSources.contains(addr(1, 3, 2)));
    }

    @Test
    public void testIncludeDcTwice()
    {
        // When including both the entire DC and the specific rack in DC, the entire DC should be included
        BootstrapSourceFilter filter = BootstrapSourceFilter.builder(clusterMetadata, snitch)
                                                            .includeDcRack("DC-1", "Rack-1")
                                                            .includeDc("DC-1")
                                                            .build();

        Set<InetAddressAndPort> restrictedSources = StreamSupport.stream(replicationStrategy.getRangeAddresses(clusterMetadata)
                                                                                            .flattenValues().spliterator(), false)
                                                                 .filter(filter::apply)
                                                                 .map(Replica::endpoint)
                                                                 .collect(Collectors.toSet());

        // All 6 nodes in DC-1 should be included
        assertEquals(6, restrictedSources.size());
        assertTrue(restrictedSources.contains(addr(1, 1, 1)));
        assertTrue(restrictedSources.contains(addr(1, 1, 2)));
        assertTrue(restrictedSources.contains(addr(1, 2, 1)));
        assertTrue(restrictedSources.contains(addr(1, 2, 2)));
        assertTrue(restrictedSources.contains(addr(1, 3, 1)));
        assertTrue(restrictedSources.contains(addr(1, 3, 2)));
    }

    @Test
    public void testIncludeDcRack()
    {
        BootstrapSourceFilter filter = BootstrapSourceFilter.builder(clusterMetadata, snitch)
                                                            .includeDcRack("DC-1", "Rack-2")
                                                            .includeDcRack("DC-2", "Rack-1")
                                                            .build();

        Set<InetAddressAndPort> restrictedSources = StreamSupport.stream(replicationStrategy.getRangeAddresses(clusterMetadata)
                                                                                            .flattenValues().spliterator(), false)
                                                                 .filter(filter::apply)
                                                                 .map(Replica::endpoint)
                                                                 .collect(Collectors.toSet());

        // 4 nodes from DC-1/Rack-2 and DC-2/Rack-1 should be included
        assertEquals(4, restrictedSources.size());
        assertTrue(restrictedSources.contains(addr(1, 2, 1)));
        assertTrue(restrictedSources.contains(addr(1, 2, 2)));
        assertTrue(restrictedSources.contains(addr(2, 1, 1)));
        assertTrue(restrictedSources.contains(addr(2, 1, 2)));
    }

    @Test
    public void testExcludeDc()
    {
        BootstrapSourceFilter filter = BootstrapSourceFilter.builder(clusterMetadata, snitch)
                                                            .excludeDc("DC-1")
                                                            .build();

        Set<InetAddressAndPort> restrictedSources = StreamSupport.stream(replicationStrategy.getRangeAddresses(clusterMetadata)
                                                                                            .flattenValues().spliterator(), false)
                                                                 .filter(filter::apply)
                                                                 .map(Replica::endpoint)
                                                                 .collect(Collectors.toSet());

        // 6 nodes in DC-1 should be included
        assertEquals(6, restrictedSources.size());
        assertTrue(restrictedSources.contains(addr(2, 1, 1)));
        assertTrue(restrictedSources.contains(addr(2, 1, 2)));
        assertTrue(restrictedSources.contains(addr(2, 2, 1)));
        assertTrue(restrictedSources.contains(addr(2, 2, 2)));
        assertTrue(restrictedSources.contains(addr(2, 3, 1)));
        assertTrue(restrictedSources.contains(addr(2, 3, 2)));
    }

    @Test
    public void testExcludeDcRack()
    {
        BootstrapSourceFilter filter = BootstrapSourceFilter.builder(clusterMetadata, snitch)
                                                            .excludeDcRack("DC-1", "Rack-2")
                                                            .excludeDc("DC-2")
                                                            .build();

        Set<InetAddressAndPort> restrictedSources = StreamSupport.stream(replicationStrategy.getRangeAddresses(clusterMetadata)
                                                                                            .flattenValues().spliterator(), false)
                                                                 .filter(filter::apply)
                                                                 .map(Replica::endpoint)
                                                                 .collect(Collectors.toSet());

        // 2 nodes in DC-1/Rack-1 and 2 nodes in DC-1/Rack-3 should be included
        assertEquals(4, restrictedSources.size());
        assertTrue(restrictedSources.contains(addr(1, 1, 1)));
        assertTrue(restrictedSources.contains(addr(1, 1, 2)));
        assertTrue(restrictedSources.contains(addr(1, 3, 1)));
        assertTrue(restrictedSources.contains(addr(1, 3, 2)));
    }

    @Test
    public void testIncludeAndExcludeDc()
    {
        BootstrapSourceFilter filter = BootstrapSourceFilter.builder(clusterMetadata, snitch)
                                                            .includeDc("DC-1")
                                                            .excludeDcRack("DC-1", "Rack-2")
                                                            .build();

        Set<InetAddressAndPort> restrictedSources = StreamSupport.stream(replicationStrategy.getRangeAddresses(clusterMetadata)
                                                                                            .flattenValues().spliterator(), false)
                                                                 .filter(filter::apply)
                                                                 .map(Replica::endpoint)
                                                                 .collect(Collectors.toSet());

        // 2 nodes in DC-1/Rack-1 and 2 nodes in DC-1/Rack-3 should be included
        assertEquals(4, restrictedSources.size());
        assertTrue(restrictedSources.contains(addr(1, 1, 1)));
        assertTrue(restrictedSources.contains(addr(1, 1, 2)));
        assertTrue(restrictedSources.contains(addr(1, 3, 1)));
        assertTrue(restrictedSources.contains(addr(1, 3, 2)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIncludeNullSource()
    {
        BootstrapSourceFilter.builder(clusterMetadata, snitch)
                             .include(null)
                             .build();
    }


    @Test(expected = IllegalArgumentException.class)
    public void testIncludeNullDc()
    {
        BootstrapSourceFilter.builder(clusterMetadata, snitch)
                             .includeDc(null)
                             .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIncludeNonexistingDc()
    {
        BootstrapSourceFilter.builder(clusterMetadata, snitch)
                             .includeDc("Illegal-DC")
                             .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIncludeNonexistingRack()
    {
        BootstrapSourceFilter.builder(clusterMetadata, snitch)
                             .includeDcRack("DC-1", "Illegal-Rack")
                             .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSameDcIsIncludedAndExcluded()
    {
        BootstrapSourceFilter.builder(clusterMetadata, snitch)
                             .includeDc("DC-1")
                             .excludeDc("DC-1")
                             .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSameDcRackIsIncludedAndExcluded()
    {
        BootstrapSourceFilter.builder(clusterMetadata, snitch)
                             .includeDcRack("DC-1", "Rack-1")
                             .excludeDcRack("DC-1", "Rack-1")
                             .build();
    }

    @Test
    public void testParseIncludeDcs()
    {
        BOOTSTRAP_INCLUDE_DCS.setString("DC-1:Rack-1,DC-2");
        try
        {
            BootstrapSourceFilter filter = BootstrapOptionsParser.parse(clusterMetadata, snitch);

            Set<InetAddressAndPort> restrictedSources = StreamSupport.stream(replicationStrategy.getRangeAddresses(clusterMetadata)
                                                                                                .flattenValues().spliterator(), false)
                                                                     .filter(filter::apply)
                                                                     .map(Replica::endpoint)
                                                                     .collect(Collectors.toSet());

            // 2 nodes in DC-1/Rack-1 and 6 nodes in DC-2 should be included
            assertEquals(8, restrictedSources.size());
            assertTrue(restrictedSources.contains(addr(1, 1, 1)));
            assertTrue(restrictedSources.contains(addr(1, 1, 2)));
            assertTrue(restrictedSources.contains(addr(2, 1, 1)));
            assertTrue(restrictedSources.contains(addr(2, 1, 2)));
            assertTrue(restrictedSources.contains(addr(2, 2, 1)));
            assertTrue(restrictedSources.contains(addr(2, 2, 2)));
            assertTrue(restrictedSources.contains(addr(2, 3, 1)));
            assertTrue(restrictedSources.contains(addr(2, 3, 2)));
        }
        finally
        {
            BOOTSTRAP_INCLUDE_DCS.clearValue();
        }
    }

    @Test
    public void testParseExcludeDcs()
    {
        BOOTSTRAP_EXCLUDE_DCS.setString("DC-1:Rack-2,DC-2");
        try
        {
            BootstrapSourceFilter filter = BootstrapOptionsParser.parse(clusterMetadata, snitch);

            Set<InetAddressAndPort> restrictedSources = StreamSupport.stream(replicationStrategy.getRangeAddresses(clusterMetadata)
                                                                                                .flattenValues().spliterator(), false)
                                                                     .filter(filter::apply)
                                                                     .map(Replica::endpoint)
                                                                     .collect(Collectors.toSet());

            // 2 nodes in DC-1/Rack-1 and 2 nodes in DC-1/Rack-3 should be included
            assertEquals(4, restrictedSources.size());
            assertTrue(restrictedSources.contains(addr(1, 1, 1)));
            assertTrue(restrictedSources.contains(addr(1, 1, 2)));
            assertTrue(restrictedSources.contains(addr(1, 3, 1)));
            assertTrue(restrictedSources.contains(addr(1, 3, 2)));
        }
        finally
        {
            BOOTSTRAP_EXCLUDE_DCS.clearValue();
        }
    }

    @Test
    public void testParseIncludeSources()
    {
        BOOTSTRAP_INCLUDE_SOURCES.setString("1.1.1.1, 1.1.1.2");
        try
        {
            BootstrapSourceFilter filter = BootstrapOptionsParser.parse(clusterMetadata, snitch);

            Set<InetAddressAndPort> restrictedSources = StreamSupport.stream(replicationStrategy.getRangeAddresses(clusterMetadata)
                                                                                                .flattenValues().spliterator(), false)
                                                                     .filter(filter::apply)
                                                                     .map(Replica::endpoint)
                                                                     .collect(Collectors.toSet());

            assertEquals(2, restrictedSources.size());
            assertTrue(restrictedSources.contains(addr(1, 1, 1)));
            assertTrue(restrictedSources.contains(addr(1, 1, 2)));
        }
        finally
        {
            BOOTSTRAP_INCLUDE_SOURCES.clearValue();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseIncludeInvalidSources()
    {
        BOOTSTRAP_INCLUDE_SOURCES.setString("1.1.1.1,invalid1,invalid2");
        try
        {
            BootstrapOptionsParser.parse(clusterMetadata, snitch);
        }
        finally
        {
            BOOTSTRAP_INCLUDE_SOURCES.clearValue();
        }
    }

    private static InetAddressAndPort addr(int dc, int rack, int node)
    {
        byte[] addr = new byte[]{ 1, (byte) dc, (byte) rack, (byte) node };
        try
        {
            return InetAddressAndPort.getByAddress(addr);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }
}
