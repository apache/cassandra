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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;

public class RangeFetchMapCalculatorTest
{
    @BeforeClass
    public static void setupUpSnitch()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setEndpointSnitch(new AbstractNetworkTopologySnitch()
        {
            //Odd IPs are in DC1 and Even are in DC2. Endpoints upto .14 will have unique racks and
            // then will be same for a set of three.
            @Override
            public String getRack(InetAddress endpoint)
            {
                return "RAC1";
            }

            @Override
            public String getDatacenter(InetAddress endpoint)
            {
                if (getIPLastPart(endpoint) <= 50)
                    return DatabaseDescriptor.getLocalDataCenter();
                else if (getIPLastPart(endpoint) % 2 == 0)
                    return DatabaseDescriptor.getLocalDataCenter();
                else
                    return DatabaseDescriptor.getLocalDataCenter() + "Remote";
            }

            private int getIPLastPart(InetAddress endpoint)
            {
                String str = endpoint.toString();
                int index = str.lastIndexOf(".");
                return Integer.parseInt(str.substring(index + 1).trim());
            }
        });
    }

    @Test
    public void testWithSingleSource() throws Exception
    {
        Multimap<Range<Token>, InetAddress> rangesWithSources = HashMultimap.create();
        addRangeAndSources(rangesWithSources, 1, 10, "127.0.0.1");
        addRangeAndSources(rangesWithSources, 11, 20, "127.0.0.2");
        addRangeAndSources(rangesWithSources, 21, 30, "127.0.0.3");
        addRangeAndSources(rangesWithSources, 31, 40, "127.0.0.4");
        addRangeAndSources(rangesWithSources, 41, 50, "127.0.0.5");

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources, new ArrayList<RangeStreamer.ISourceFilter>(), "Test");
        Multimap<InetAddress, Range<Token>> map = calculator.getRangeFetchMap();
        validateRange(rangesWithSources, map);

        Assert.assertEquals(4, map.asMap().keySet().size());
    }

    @Test
    public void testWithNonOverlappingSource() throws Exception
    {
        Multimap<Range<Token>, InetAddress> rangesWithSources = HashMultimap.create();
        addRangeAndSources(rangesWithSources, 1, 10, "127.0.0.1", "127.0.0.2");
        addRangeAndSources(rangesWithSources, 11, 20, "127.0.0.3", "127.0.0.4");
        addRangeAndSources(rangesWithSources, 21, 30, "127.0.0.5", "127.0.0.6");
        addRangeAndSources(rangesWithSources, 31, 40, "127.0.0.7", "127.0.0.8");
        addRangeAndSources(rangesWithSources, 41, 50, "127.0.0.9", "127.0.0.10");

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources, new ArrayList<RangeStreamer.ISourceFilter>(), "Test");
        Multimap<InetAddress, Range<Token>> map = calculator.getRangeFetchMap();
        validateRange(rangesWithSources, map);

        Assert.assertEquals(5, map.asMap().keySet().size());
    }

    @Test
    public void testWithRFThreeReplacement() throws Exception
    {
        Multimap<Range<Token>, InetAddress> rangesWithSources = HashMultimap.create();
        addRangeAndSources(rangesWithSources, 1, 10, "127.0.0.1", "127.0.0.2");
        addRangeAndSources(rangesWithSources, 11, 20, "127.0.0.2", "127.0.0.3");
        addRangeAndSources(rangesWithSources, 21, 30, "127.0.0.3", "127.0.0.4");

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources, new ArrayList<RangeStreamer.ISourceFilter>(), "Test");
        Multimap<InetAddress, Range<Token>> map = calculator.getRangeFetchMap();
        validateRange(rangesWithSources, map);

        //We should validate that it streamed from 3 unique sources
        Assert.assertEquals(3, map.asMap().keySet().size());
    }

    @Test
    public void testForMultipleRoundsComputation() throws Exception
    {
        Multimap<Range<Token>, InetAddress> rangesWithSources = HashMultimap.create();
        addRangeAndSources(rangesWithSources, 1, 10, "127.0.0.3");
        addRangeAndSources(rangesWithSources, 11, 20, "127.0.0.3");
        addRangeAndSources(rangesWithSources, 21, 30, "127.0.0.3");
        addRangeAndSources(rangesWithSources, 31, 40, "127.0.0.3");
        addRangeAndSources(rangesWithSources, 41, 50, "127.0.0.3", "127.0.0.2");

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources, new ArrayList<RangeStreamer.ISourceFilter>(), "Test");
        Multimap<InetAddress, Range<Token>> map = calculator.getRangeFetchMap();
        validateRange(rangesWithSources, map);

        //We should validate that it streamed from 2 unique sources
        Assert.assertEquals(2, map.asMap().keySet().size());

        assertArrays(Arrays.asList(generateRange(1, 10), generateRange(11, 20), generateRange(21, 30), generateRange(31, 40)),
                map.asMap().get(InetAddress.getByName("127.0.0.3")));
        assertArrays(Arrays.asList(generateRange(41, 50)), map.asMap().get(InetAddress.getByName("127.0.0.2")));
    }

    @Test
    public void testForMultipleRoundsComputationWithLocalHost() throws Exception
    {
        Multimap<Range<Token>, InetAddress> rangesWithSources = HashMultimap.create();
        addRangeAndSources(rangesWithSources, 1, 10, "127.0.0.1");
        addRangeAndSources(rangesWithSources, 11, 20, "127.0.0.1");
        addRangeAndSources(rangesWithSources, 21, 30, "127.0.0.1");
        addRangeAndSources(rangesWithSources, 31, 40, "127.0.0.1");
        addRangeAndSources(rangesWithSources, 41, 50, "127.0.0.1", "127.0.0.2");

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources, new ArrayList<RangeStreamer.ISourceFilter>(), "Test");
        Multimap<InetAddress, Range<Token>> map = calculator.getRangeFetchMap();
        validateRange(rangesWithSources, map);

        //We should validate that it streamed from only non local host and only one range
        Assert.assertEquals(1, map.asMap().keySet().size());

        assertArrays(Arrays.asList(generateRange(41, 50)), map.asMap().get(InetAddress.getByName("127.0.0.2")));
    }

    @Test
    public void testForEmptyGraph() throws Exception
    {
        Multimap<Range<Token>, InetAddress> rangesWithSources = HashMultimap.create();
        addRangeAndSources(rangesWithSources, 1, 10, "127.0.0.1");
        addRangeAndSources(rangesWithSources, 11, 20, "127.0.0.1");
        addRangeAndSources(rangesWithSources, 21, 30, "127.0.0.1");
        addRangeAndSources(rangesWithSources, 31, 40, "127.0.0.1");
        addRangeAndSources(rangesWithSources, 41, 50, "127.0.0.1");

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources, new ArrayList<RangeStreamer.ISourceFilter>(), "Test");
        Multimap<InetAddress, Range<Token>> map = calculator.getRangeFetchMap();
        //All ranges map to local host so we will not stream anything.
        Assert.assertTrue(map.isEmpty());
    }

    @Test
    public void testWithNoSourceWithLocal() throws Exception
    {
        Multimap<Range<Token>, InetAddress> rangesWithSources = HashMultimap.create();
        addRangeAndSources(rangesWithSources, 1, 10, "127.0.0.1", "127.0.0.5");
        addRangeAndSources(rangesWithSources, 11, 20, "127.0.0.2");
        addRangeAndSources(rangesWithSources, 21, 30, "127.0.0.3");

        //Return false for all except 127.0.0.5
        final RangeStreamer.ISourceFilter filter = new RangeStreamer.ISourceFilter()
        {
            public boolean shouldInclude(InetAddress endpoint)
            {
                try
                {
                    if (endpoint.equals(InetAddress.getByName("127.0.0.5")))
                        return false;
                    else
                        return true;
                }
                catch (UnknownHostException e)
                {
                    return true;
                }
            }
        };

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources, Arrays.asList(filter), "Test");
        Multimap<InetAddress, Range<Token>> map = calculator.getRangeFetchMap();

        validateRange(rangesWithSources, map);

        //We should validate that it streamed from only non local host and only one range
        Assert.assertEquals(2, map.asMap().keySet().size());

        assertArrays(Arrays.asList(generateRange(11, 20)), map.asMap().get(InetAddress.getByName("127.0.0.2")));
        assertArrays(Arrays.asList(generateRange(21, 30)), map.asMap().get(InetAddress.getByName("127.0.0.3")));
    }

    @Test (expected = IllegalStateException.class)
    public void testWithNoLiveSource() throws Exception
    {
        Multimap<Range<Token>, InetAddress> rangesWithSources = HashMultimap.create();
        addRangeAndSources(rangesWithSources, 1, 10,  "127.0.0.5");
        addRangeAndSources(rangesWithSources, 11, 20, "127.0.0.2");
        addRangeAndSources(rangesWithSources, 21, 30, "127.0.0.3");

        final RangeStreamer.ISourceFilter allDeadFilter = new RangeStreamer.ISourceFilter()
        {
            public boolean shouldInclude(InetAddress endpoint)
            {
                return false;
            }
        };

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources, Arrays.asList(allDeadFilter), "Test");
        calculator.getRangeFetchMap();
    }

    @Test
    public void testForLocalDC() throws Exception
    {
        Multimap<Range<Token>, InetAddress> rangesWithSources = HashMultimap.create();
        addRangeAndSources(rangesWithSources, 1, 10, "127.0.0.1", "127.0.0.3", "127.0.0.53");
        addRangeAndSources(rangesWithSources, 11, 20, "127.0.0.1", "127.0.0.3", "127.0.0.57");
        addRangeAndSources(rangesWithSources, 21, 30, "127.0.0.2", "127.0.0.59", "127.0.0.61");

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources, new ArrayList<>(), "Test");
        Multimap<InetAddress, Range<Token>> map = calculator.getRangeFetchMap();
        validateRange(rangesWithSources, map);
        Assert.assertEquals(2, map.asMap().size());

        //Should have streamed from local DC endpoints
        assertArrays(Arrays.asList(generateRange(21, 30)), map.asMap().get(InetAddress.getByName("127.0.0.2")));
        assertArrays(Arrays.asList(generateRange(1, 10), generateRange(11, 20)), map.asMap().get(InetAddress.getByName("127.0.0.3")));
    }

    @Test
    public void testForRemoteDC() throws Exception
    {
        Multimap<Range<Token>, InetAddress> rangesWithSources = HashMultimap.create();
        addRangeAndSources(rangesWithSources, 1, 10, "127.0.0.3", "127.0.0.51");
        addRangeAndSources(rangesWithSources, 11, 20, "127.0.0.3", "127.0.0.55");
        addRangeAndSources(rangesWithSources, 21, 30, "127.0.0.2", "127.0.0.59");

        //Reject only 127.0.0.3 and accept everyone else
        final RangeStreamer.ISourceFilter localHostFilter = new RangeStreamer.ISourceFilter()
        {
            public boolean shouldInclude(InetAddress endpoint)
            {
                try
                {
                    if (endpoint.equals(InetAddress.getByName("127.0.0.3")))
                        return false;
                    else
                        return true;
                }
                catch (UnknownHostException e)
                {
                    return true;
                }
            }
        };

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources, Arrays.asList(localHostFilter), "Test");
        Multimap<InetAddress, Range<Token>> map = calculator.getRangeFetchMap();
        validateRange(rangesWithSources, map);
        Assert.assertEquals(3, map.asMap().size());

        //Should have streamed from remote DC endpoint
        assertArrays(Arrays.asList(generateRange(1, 10)), map.asMap().get(InetAddress.getByName("127.0.0.51")));
        assertArrays(Arrays.asList(generateRange(11, 20)), map.asMap().get(InetAddress.getByName("127.0.0.55")));
        assertArrays(Arrays.asList(generateRange(21, 30)), map.asMap().get(InetAddress.getByName("127.0.0.2")));
    }

    private void assertArrays(Collection<Range<Token>> expected, Collection<Range<Token>> result)
    {
        Assert.assertEquals(expected.size(), result.size());
        Assert.assertTrue(result.containsAll(expected));
    }

    private void validateRange(Multimap<Range<Token>, InetAddress> rangesWithSources, Multimap<InetAddress, Range<Token>> result)
    {
        for (Map.Entry<InetAddress, Range<Token>> entry : result.entries())
        {
            Assert.assertTrue(rangesWithSources.get(entry.getValue()).contains(entry.getKey()));
        }
    }

    private void addRangeAndSources(Multimap<Range<Token>, InetAddress> rangesWithSources, int left, int right, String... hosts) throws UnknownHostException
    {
        for (InetAddress endpoint : makeAddrs(hosts))
        {
            rangesWithSources.put(generateRange(left, right), endpoint);
        }
    }

    private Collection<InetAddress> makeAddrs(String... hosts) throws UnknownHostException
    {
        ArrayList<InetAddress> addrs = new ArrayList<InetAddress>(hosts.length);
        for (String host : hosts)
            addrs.add(InetAddress.getByName(host));
        return addrs;
    }

    private Range<Token> generateRange(int left, int right)
    {
        return new Range<Token>(new RandomPartitioner.BigIntegerToken(String.valueOf(left)), new RandomPartitioner.BigIntegerToken(String.valueOf(right)));
    }
}
