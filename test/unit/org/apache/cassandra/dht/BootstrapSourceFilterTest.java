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
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.BootstrapOptionsParser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BootstrapSourceFilterTest
{
    private static final IEndpointSnitch snitch = new AbstractNetworkTopologySnitch()
    {
        @Override
        public String getRack(InetAddress endpoint)
        {
            return "Rack-" + endpoint.getAddress()[2];
        }

        @Override
        public String getDatacenter(InetAddress endpoint)
        {
            return "DC-" + endpoint.getAddress()[1];
        }
    };

    @BeforeClass
    public static void setUpSnitch()
    {
        DatabaseDescriptor.setEndpointSnitch(snitch);
    }

    /**
     * Topology that contains 3 DCs, with 2 racks each, and 3 nodes per rack (6 nodes per DC).
     */
    private final TokenMetadata tmd;

    public BootstrapSourceFilterTest()
    {
        this.tmd = new TokenMetadata();
        IPartitioner p = new Murmur3Partitioner();

        // add 2 DCs, with 3 racks each, and 2 nodes per rack (6 nodes per DC)
        for (int dc = 1; dc <= 2; dc++)
        {
            for (int rack = 1; rack <= 3; rack++)
            {
                for (int node = 1; node <= 2; node++)
                {
                    InetAddress addr = addr(dc, rack, node);
                    tmd.updateNormalTokens(Collections.singleton(p.getRandomToken()), addr);
                    tmd.updateHostId(UUID.randomUUID(), addr);
                }
            }
        }
    }

    @Test
    public void testEmpty()
    {
        BootstrapSourceFilter filter = BootstrapSourceFilter.builder(tmd, snitch).build();
        Set<InetAddress> restrictedSources = tmd.getAllEndpoints().stream()
                                                .filter(filter::shouldInclude)
                                                .collect(Collectors.toSet());
        // All 12 nodes should be included
        assertEquals(12, restrictedSources.size());
    }

    @Test
    public void testIncludeSources()
    {
        BootstrapSourceFilter filter = BootstrapSourceFilter.builder(tmd, snitch)
                                                             .include(addr(1, 1, 1))
                                                             .include(addr(1, 1, 2))
                                                             .build();
        Set<InetAddress> restrictedSources = tmd.getAllEndpoints().stream()
                                                .filter(filter::shouldInclude)
                                                .collect(Collectors.toSet());
        assertEquals(2, restrictedSources.size());
        assertTrue(restrictedSources.contains(addr(1, 1, 1)));
        assertTrue(restrictedSources.contains(addr(1, 1, 2)));
    }

    @Test
    public void testIncludeDc()
    {
        BootstrapSourceFilter filter = BootstrapSourceFilter.builder(tmd, snitch)
                                                             .includeDc("DC-1")
                                                             .build();
        Set<InetAddress> restrictedSources = tmd.getAllEndpoints().stream()
                                                .filter(filter::shouldInclude)
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
        BootstrapSourceFilter filter = BootstrapSourceFilter.builder(tmd, snitch)
                                                            .includeDcRack("DC-1", "Rack-1")
                                                            .includeDc("DC-1")
                                                            .build();
        Set<InetAddress> restrictedSources = tmd.getAllEndpoints().stream()
                                                .filter(filter::shouldInclude)
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
        BootstrapSourceFilter filter = BootstrapSourceFilter.builder(tmd, snitch)
                                                             .includeDcRack("DC-1", "Rack-2")
                                                             .includeDcRack("DC-2", "Rack-1")
                                                             .build();
        Set<InetAddress> restrictedSources = tmd.getAllEndpoints().stream()
                                                .filter(filter::shouldInclude)
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
        BootstrapSourceFilter filter = BootstrapSourceFilter.builder(tmd, snitch)
                                                            .excludeDc("DC-1")
                                                            .build();
        Set<InetAddress> restrictedSources = tmd.getAllEndpoints().stream()
                                                .filter(filter::shouldInclude)
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
        BootstrapSourceFilter filter = BootstrapSourceFilter.builder(tmd, snitch)
                                                            .excludeDcRack("DC-1", "Rack-2")
                                                            .excludeDc("DC-2")
                                                            .build();
        Set<InetAddress> restrictedSources = tmd.getAllEndpoints().stream()
                                                .filter(filter::shouldInclude)
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
        BootstrapSourceFilter filter = BootstrapSourceFilter.builder(tmd, snitch)
                                                            .includeDc("DC-1")
                                                            .excludeDcRack("DC-1", "Rack-2")
                                                            .build();
        Set<InetAddress> restrictedSources = tmd.getAllEndpoints().stream()
                                                .filter(filter::shouldInclude)
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
        BootstrapSourceFilter.builder(tmd, snitch)
                             .include(null)
                             .build();
    }


    @Test(expected = IllegalArgumentException.class)
    public void testIncludeNullDc()
    {
        BootstrapSourceFilter.builder(tmd, snitch)
                             .includeDc(null)
                             .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIncludeNonexistingDc()
    {
        BootstrapSourceFilter.builder(tmd, snitch)
                             .includeDc("Illegal-DC")
                             .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIncludeNonexistingRack()
    {
        BootstrapSourceFilter.builder(tmd, snitch)
                             .includeDcRack("DC-1", "Illegal-Rack")
                             .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSameDcIsIncludedAndExcluded()
    {
        BootstrapSourceFilter.builder(tmd, snitch)
                             .includeDc("DC-1")
                             .excludeDc("DC-1")
                             .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSameDcRackIsIncludedAndExcluded()
    {
        BootstrapSourceFilter.builder(tmd, snitch)
                             .includeDcRack("DC-1", "Rack-1")
                             .excludeDcRack("DC-1", "Rack-1")
                             .build();
    }

    @Test
    public void testParseIncludeDcs() {
        System.setProperty(BootstrapOptionsParser.BOOTSTRAP_INCLUDE_DCS, "DC-1:Rack-1,DC-2");
        try
        {
            BootstrapSourceFilter filter = BootstrapOptionsParser.parse(tmd, snitch);
            Set<InetAddress> restrictedSources = tmd.getAllEndpoints().stream()
                                                    .filter(filter::shouldInclude)
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
            System.clearProperty(BootstrapOptionsParser.BOOTSTRAP_INCLUDE_DCS);
        }
    }

    @Test
    public void testParseExcludeDcs()
    {
        System.setProperty(BootstrapOptionsParser.BOOTSTRAP_EXCLUDE_DCS, "DC-1:Rack-2,DC-2");
        try
        {
            BootstrapSourceFilter filter = BootstrapOptionsParser.parse(tmd, snitch);
            Set<InetAddress> restrictedSources = tmd.getAllEndpoints().stream()
                                                    .filter(filter::shouldInclude)
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
            System.clearProperty(BootstrapOptionsParser.BOOTSTRAP_EXCLUDE_DCS);
        }
    }
    @Test
    public void testParseIncludeSources() {
        System.setProperty(BootstrapOptionsParser.BOOTSTRAP_INCLUDE_SOURCES, "1.1.1.1, 1.1.1.2");
        try
        {
            BootstrapSourceFilter filter = BootstrapOptionsParser.parse(tmd, snitch);
            Set<InetAddress> restrictedSources = tmd.getAllEndpoints().stream()
                                                    .filter(filter::shouldInclude)
                                                    .collect(Collectors.toSet());
            assertEquals(2, restrictedSources.size());
            assertTrue(restrictedSources.contains(addr(1, 1, 1)));
            assertTrue(restrictedSources.contains(addr(1, 1, 2)));
        }
        finally
        {
            System.clearProperty(BootstrapOptionsParser.BOOTSTRAP_INCLUDE_SOURCES);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseIncludeInvalidSources() {
        System.setProperty(BootstrapOptionsParser.BOOTSTRAP_INCLUDE_SOURCES, "1.1.1.1,invalid1,invalid2");
        try
        {
            BootstrapOptionsParser.parse(tmd, snitch);
        }
        finally
        {
            System.clearProperty(BootstrapOptionsParser.BOOTSTRAP_INCLUDE_SOURCES);
        }
    }
    private static InetAddress addr(int dc, int rack, int node)
    {
        byte[] addr = new byte[]{ 1, (byte) dc, (byte) rack, (byte) node };
        try
        {
            return InetAddress.getByAddress(addr);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }
}
