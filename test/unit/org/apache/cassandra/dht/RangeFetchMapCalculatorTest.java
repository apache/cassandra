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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.Multimap;
import org.apache.cassandra.locator.EndpointsByRange;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RangeFetchMapCalculatorTest
{
    @BeforeClass
    public static void setupUpSnitch()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(RandomPartitioner.instance);
        DatabaseDescriptor.setEndpointSnitch(new AbstractNetworkTopologySnitch()
        {
            //Odd IPs are in DC1 and Even are in DC2. Endpoints upto .14 will have unique racks and
            // then will be same for a set of three.
            @Override
            public String getRack(InetAddressAndPort endpoint)
            {
                return "RAC1";
            }

            @Override
            public String getDatacenter(InetAddressAndPort endpoint)
            {
                if (getIPLastPart(endpoint) <= 50)
                    return DatabaseDescriptor.getLocalDataCenter();
                else if (getIPLastPart(endpoint) % 2 == 0)
                    return DatabaseDescriptor.getLocalDataCenter();
                else
                    return DatabaseDescriptor.getLocalDataCenter() + "Remote";
            }

            private int getIPLastPart(InetAddressAndPort endpoint)
            {
                String str = endpoint.getAddress().toString();
                int index = str.lastIndexOf(".");
                return Integer.parseInt(str.substring(index + 1).trim());
            }
        });
    }

    @Test
    public void testWithSingleSource() throws Exception
    {
        EndpointsByRange.Builder rangesWithSources = new EndpointsByRange.Builder();
        addNonTrivialRangeAndSources(rangesWithSources, 1, 10, "127.0.0.1");
        addNonTrivialRangeAndSources(rangesWithSources, 11, 20, "127.0.0.2");
        addNonTrivialRangeAndSources(rangesWithSources, 21, 30, "127.0.0.3");
        addNonTrivialRangeAndSources(rangesWithSources, 31, 40, "127.0.0.4");
        addNonTrivialRangeAndSources(rangesWithSources, 41, 50, "127.0.0.5");

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources.build(), Collections.emptyList(), "Test");
        Multimap<InetAddressAndPort, Range<Token>> map = calculator.getRangeFetchMap();
        validateRange(rangesWithSources, map);

        Assert.assertEquals(4, map.asMap().keySet().size());
    }

    @Test
    public void testWithNonOverlappingSource() throws Exception
    {
        EndpointsByRange.Builder rangesWithSources = new EndpointsByRange.Builder();
        addNonTrivialRangeAndSources(rangesWithSources, 1, 10, "127.0.0.1", "127.0.0.2");
        addNonTrivialRangeAndSources(rangesWithSources, 11, 20, "127.0.0.3", "127.0.0.4");
        addNonTrivialRangeAndSources(rangesWithSources, 21, 30, "127.0.0.5", "127.0.0.6");
        addNonTrivialRangeAndSources(rangesWithSources, 31, 40, "127.0.0.7", "127.0.0.8");
        addNonTrivialRangeAndSources(rangesWithSources, 41, 50, "127.0.0.9", "127.0.0.10");

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources.build(), Collections.emptyList(), "Test");
        Multimap<InetAddressAndPort, Range<Token>> map = calculator.getRangeFetchMap();
        validateRange(rangesWithSources, map);

        Assert.assertEquals(5, map.asMap().keySet().size());
    }

    @Test
    public void testWithRFThreeReplacement() throws Exception
    {
        EndpointsByRange.Builder rangesWithSources = new EndpointsByRange.Builder();
        addNonTrivialRangeAndSources(rangesWithSources, 1, 10, "127.0.0.1", "127.0.0.2");
        addNonTrivialRangeAndSources(rangesWithSources, 11, 20, "127.0.0.2", "127.0.0.3");
        addNonTrivialRangeAndSources(rangesWithSources, 21, 30, "127.0.0.3", "127.0.0.4");

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources.build(), Collections.emptyList(), "Test");
        Multimap<InetAddressAndPort, Range<Token>> map = calculator.getRangeFetchMap();
        validateRange(rangesWithSources, map);

        //We should validate that it streamed from 3 unique sources
        Assert.assertEquals(3, map.asMap().keySet().size());
    }

    @Test
    public void testForMultipleRoundsComputation() throws Exception
    {
        EndpointsByRange.Builder rangesWithSources = new EndpointsByRange.Builder();
        addNonTrivialRangeAndSources(rangesWithSources, 1, 10, "127.0.0.3");
        addNonTrivialRangeAndSources(rangesWithSources, 11, 20, "127.0.0.3");
        addNonTrivialRangeAndSources(rangesWithSources, 21, 30, "127.0.0.3");
        addNonTrivialRangeAndSources(rangesWithSources, 31, 40, "127.0.0.3");
        addNonTrivialRangeAndSources(rangesWithSources, 41, 50, "127.0.0.3", "127.0.0.2");

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources.build(), Collections.emptyList(), "Test");
        Multimap<InetAddressAndPort, Range<Token>> map = calculator.getRangeFetchMap();
        validateRange(rangesWithSources, map);

        //We should validate that it streamed from 2 unique sources
        Assert.assertEquals(2, map.asMap().keySet().size());

        assertArrays(Arrays.asList(generateNonTrivialRange(1, 10), generateNonTrivialRange(11, 20), generateNonTrivialRange(21, 30), generateNonTrivialRange(31, 40)),
                map.asMap().get(InetAddressAndPort.getByName("127.0.0.3")));
        assertArrays(Arrays.asList(generateNonTrivialRange(41, 50)), map.asMap().get(InetAddressAndPort.getByName("127.0.0.2")));
    }

    @Test
    public void testForMultipleRoundsComputationWithLocalHost() throws Exception
    {
        EndpointsByRange.Builder rangesWithSources = new EndpointsByRange.Builder();
        addNonTrivialRangeAndSources(rangesWithSources, 1, 10, "127.0.0.1");
        addNonTrivialRangeAndSources(rangesWithSources, 11, 20, "127.0.0.1");
        addNonTrivialRangeAndSources(rangesWithSources, 21, 30, "127.0.0.1");
        addNonTrivialRangeAndSources(rangesWithSources, 31, 40, "127.0.0.1");
        addNonTrivialRangeAndSources(rangesWithSources, 41, 50, "127.0.0.1", "127.0.0.2");

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources.build(), Collections.emptyList(), "Test");
        Multimap<InetAddressAndPort, Range<Token>> map = calculator.getRangeFetchMap();
        validateRange(rangesWithSources, map);

        //We should validate that it streamed from only non local host and only one range
        Assert.assertEquals(1, map.asMap().keySet().size());

        assertArrays(Arrays.asList(generateNonTrivialRange(41, 50)), map.asMap().get(InetAddressAndPort.getByName("127.0.0.2")));
    }

    @Test
    public void testForEmptyGraph() throws Exception
    {
        EndpointsByRange.Builder rangesWithSources = new EndpointsByRange.Builder();
        addNonTrivialRangeAndSources(rangesWithSources, 1, 10, "127.0.0.1");
        addNonTrivialRangeAndSources(rangesWithSources, 11, 20, "127.0.0.1");
        addNonTrivialRangeAndSources(rangesWithSources, 21, 30, "127.0.0.1");
        addNonTrivialRangeAndSources(rangesWithSources, 31, 40, "127.0.0.1");
        addNonTrivialRangeAndSources(rangesWithSources, 41, 50, "127.0.0.1");

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources.build(), Collections.emptyList(), "Test");
        Multimap<InetAddressAndPort, Range<Token>> map = calculator.getRangeFetchMap();
        //All ranges map to local host so we will not stream anything.
        assertTrue(map.isEmpty());
    }

    @Test
    public void testWithNoSourceWithLocal() throws Exception
    {
        EndpointsByRange.Builder rangesWithSources = new EndpointsByRange.Builder();
        addNonTrivialRangeAndSources(rangesWithSources, 1, 10, "127.0.0.1", "127.0.0.5");
        addNonTrivialRangeAndSources(rangesWithSources, 11, 20, "127.0.0.2");
        addNonTrivialRangeAndSources(rangesWithSources, 21, 30, "127.0.0.3");

        //Return false for all except 127.0.0.5
        final RangeStreamer.SourceFilter filter = new RangeStreamer.SourceFilter()
        {
            public boolean apply(Replica replica)
            {
                try
                {
                    if (replica.endpoint().equals(InetAddressAndPort.getByName("127.0.0.5")))
                        return false;
                    else
                        return true;
                }
                catch (UnknownHostException e)
                {
                    return true;
                }
            }

            public String message(Replica replica)
            {
                return "Doesn't match 127.0.0.5";
            }
        };

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources.build(), Arrays.asList(filter), "Test");
        Multimap<InetAddressAndPort, Range<Token>> map = calculator.getRangeFetchMap();

        validateRange(rangesWithSources, map);

        //We should validate that it streamed from only non local host and only one range
        Assert.assertEquals(2, map.asMap().keySet().size());

        assertArrays(Arrays.asList(generateNonTrivialRange(11, 20)), map.asMap().get(InetAddressAndPort.getByName("127.0.0.2")));
        assertArrays(Arrays.asList(generateNonTrivialRange(21, 30)), map.asMap().get(InetAddressAndPort.getByName("127.0.0.3")));
    }

    @Test (expected = IllegalStateException.class)
    public void testWithNoLiveSource() throws Exception
    {
        EndpointsByRange.Builder rangesWithSources = new EndpointsByRange.Builder();
        addNonTrivialRangeAndSources(rangesWithSources, 1, 10, "127.0.0.5");
        addNonTrivialRangeAndSources(rangesWithSources, 11, 20, "127.0.0.2");
        addNonTrivialRangeAndSources(rangesWithSources, 21, 30, "127.0.0.3");

        final RangeStreamer.SourceFilter allDeadFilter = new RangeStreamer.SourceFilter()
        {
            public boolean apply(Replica replica)
            {
                return false;
            }

            public String message(Replica replica)
            {
                return "All dead";
            }
        };

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources.build(), Arrays.asList(allDeadFilter), "Test");
        calculator.getRangeFetchMap();
    }

    @Test
    public void testForLocalDC() throws Exception
    {
        EndpointsByRange.Builder rangesWithSources = new EndpointsByRange.Builder();
        addNonTrivialRangeAndSources(rangesWithSources, 1, 10, "127.0.0.1", "127.0.0.3", "127.0.0.53");
        addNonTrivialRangeAndSources(rangesWithSources, 11, 20, "127.0.0.1", "127.0.0.3", "127.0.0.57");
        addNonTrivialRangeAndSources(rangesWithSources, 21, 30, "127.0.0.2", "127.0.0.59", "127.0.0.61");

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources.build(), new ArrayList<>(), "Test");
        Multimap<InetAddressAndPort, Range<Token>> map = calculator.getRangeFetchMap();
        validateRange(rangesWithSources, map);
        Assert.assertEquals(2, map.asMap().size());

        //Should have streamed from local DC endpoints
        assertArrays(Arrays.asList(generateNonTrivialRange(21, 30)), map.asMap().get(InetAddressAndPort.getByName("127.0.0.2")));
        assertArrays(Arrays.asList(generateNonTrivialRange(1, 10), generateNonTrivialRange(11, 20)), map.asMap().get(InetAddressAndPort.getByName("127.0.0.3")));
    }

    @Test
    public void testForRemoteDC() throws Exception
    {
        EndpointsByRange.Builder rangesWithSources = new EndpointsByRange.Builder();
        addNonTrivialRangeAndSources(rangesWithSources, 1, 10, "127.0.0.3", "127.0.0.51");
        addNonTrivialRangeAndSources(rangesWithSources, 11, 20, "127.0.0.3", "127.0.0.55");
        addNonTrivialRangeAndSources(rangesWithSources, 21, 30, "127.0.0.2", "127.0.0.59");

        //Reject only 127.0.0.3 and accept everyone else
        final RangeStreamer.SourceFilter localHostFilter = new RangeStreamer.SourceFilter()
        {
            public boolean apply(Replica replica)
            {
                try
                {
                    if (replica.endpoint().equals(InetAddressAndPort.getByName("127.0.0.3")))
                        return false;
                    else
                        return true;
                }
                catch (UnknownHostException e)
                {
                    return true;
                }
            }

            public String message(Replica replica)
            {
                return "Not 127.0.0.3";
            }
        };

        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources.build(), Arrays.asList(localHostFilter), "Test");
        Multimap<InetAddressAndPort, Range<Token>> map = calculator.getRangeFetchMap();
        validateRange(rangesWithSources, map);
        Assert.assertEquals(3, map.asMap().size());

        //Should have streamed from remote DC endpoint
        assertArrays(Arrays.asList(generateNonTrivialRange(1, 10)), map.asMap().get(InetAddressAndPort.getByName("127.0.0.51")));
        assertArrays(Arrays.asList(generateNonTrivialRange(11, 20)), map.asMap().get(InetAddressAndPort.getByName("127.0.0.55")));
        assertArrays(Arrays.asList(generateNonTrivialRange(21, 30)), map.asMap().get(InetAddressAndPort.getByName("127.0.0.2")));
    }

    @Test
    public void testTrivialRanges() throws UnknownHostException
    {
        EndpointsByRange.Builder rangesWithSources = new EndpointsByRange.Builder();
        // add non-trivial ranges
        addNonTrivialRangeAndSources(rangesWithSources, 1, 10, "127.0.0.3", "127.0.0.51");
        addNonTrivialRangeAndSources(rangesWithSources, 11, 20, "127.0.0.3", "127.0.0.55");
        addNonTrivialRangeAndSources(rangesWithSources, 21, 30, "127.0.0.2", "127.0.0.59");
        // and a trivial one:
        addTrivialRangeAndSources(rangesWithSources, 1, 10, "127.0.0.3", "127.0.0.51");
        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources.build(), Collections.emptyList(), "Test");
        Multimap<InetAddressAndPort, Range<Token>> optMap = calculator.getRangeFetchMapForNonTrivialRanges();
        Multimap<InetAddressAndPort, Range<Token>> trivialMap = calculator.getRangeFetchMapForTrivialRanges(optMap);
        assertTrue(trivialMap.get(InetAddressAndPort.getByName("127.0.0.3")).contains(generateTrivialRange(1,10)) ^
                   trivialMap.get(InetAddressAndPort.getByName("127.0.0.51")).contains(generateTrivialRange(1,10)));
        assertFalse(optMap.containsKey(generateTrivialRange(1, 10)));
    }

    @Test(expected = IllegalStateException.class)
    public void testNotEnoughEndpointsForTrivialRange() throws UnknownHostException
    {
        EndpointsByRange.Builder rangesWithSources = new EndpointsByRange.Builder();
        // add non-trivial ranges
        addNonTrivialRangeAndSources(rangesWithSources, 1, 10, "127.0.0.3", "127.0.0.51");
        addNonTrivialRangeAndSources(rangesWithSources, 11, 20, "127.0.0.3", "127.0.0.55");
        addNonTrivialRangeAndSources(rangesWithSources, 21, 30, "127.0.0.2", "127.0.0.59");
        // and a trivial one:
        addTrivialRangeAndSources(rangesWithSources, 1, 10, "127.0.0.3");

        RangeStreamer.SourceFilter filter = new RangeStreamer.SourceFilter()
        {
            public boolean apply(Replica replica)
            {
                try
                {
                    if (replica.endpoint().equals(InetAddressAndPort.getByName("127.0.0.3")))
                        return false;
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
                return true;
            }

            public String message(Replica replica)
            {
                return "Not 127.0.0.3";
            }
        };
        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources.build(), Collections.singleton(filter), "Test");
        Multimap<InetAddressAndPort, Range<Token>> optMap = calculator.getRangeFetchMapForNonTrivialRanges();
        Multimap<InetAddressAndPort, Range<Token>> trivialMap = calculator.getRangeFetchMapForTrivialRanges(optMap);

    }

   @Test
    public void testTrivalRangeLocalHostStreaming() throws UnknownHostException
    {
        // trivial ranges ranges should not try to stream from localhost
        EndpointsByRange.Builder rangesWithSources = new EndpointsByRange.Builder();
        addTrivialRangeAndSources(rangesWithSources, 21, 30, "127.0.0.2", "127.0.0.1");
        addTrivialRangeAndSources(rangesWithSources, 31, 40, "127.0.0.1", "127.0.0.2");
        EndpointsByRange ebr = rangesWithSources.build();
        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(ebr, Collections.emptyList(), "Test");
        RangeStreamer.validateRangeFetchMap(ebr, calculator.getRangeFetchMap(), "Test");
    }


    private void assertArrays(Collection<Range<Token>> expected, Collection<Range<Token>> result)
    {
        Assert.assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));
    }

    private void validateRange(EndpointsByRange.Builder rangesWithSources, Multimap<InetAddressAndPort, Range<Token>> result)
    {
        for (Map.Entry<InetAddressAndPort, Range<Token>> entry : result.entries())
        {
            assertTrue(rangesWithSources.get(entry.getValue()).endpoints().contains(entry.getKey()));
        }
    }

    private void addNonTrivialRangeAndSources(EndpointsByRange.Builder rangesWithSources, int left, int right, String... hosts) throws UnknownHostException
    {
        for (InetAddressAndPort endpoint : makeAddrs(hosts))
        {
            Range<Token> range = generateNonTrivialRange(left, right);
            rangesWithSources.put(range, Replica.fullReplica(endpoint, range));
        }
    }

    private void addTrivialRangeAndSources(EndpointsByRange.Builder rangesWithSources, int left, int right, String... hosts) throws UnknownHostException
    {
        for (InetAddressAndPort endpoint : makeAddrs(hosts))
        {
            Range<Token> range = generateTrivialRange(left, right);
            rangesWithSources.put(range, Replica.fullReplica(endpoint, range));
        }
    }

    private Collection<InetAddressAndPort> makeAddrs(String... hosts) throws UnknownHostException
    {
        ArrayList<InetAddressAndPort> addrs = new ArrayList<>(hosts.length);
        for (String host : hosts)
            addrs.add(InetAddressAndPort.getByName(host));
        return addrs;
    }

    private Range<Token> generateNonTrivialRange(int left, int right)
    {
        // * 1000 to make sure we dont filter away any trivial ranges:
        return new Range<>(new RandomPartitioner.BigIntegerToken(String.valueOf(left * 10000)), new RandomPartitioner.BigIntegerToken(String.valueOf(right * 10000)));
    }

    private Range<Token> generateTrivialRange(int left, int right)
    {
        Range<Token> r = new Range<>(new RandomPartitioner.BigIntegerToken(String.valueOf(left)), new RandomPartitioner.BigIntegerToken(String.valueOf(right)));
        assertTrue(RangeFetchMapCalculator.isTrivial(r));
        return r;
    }
}
