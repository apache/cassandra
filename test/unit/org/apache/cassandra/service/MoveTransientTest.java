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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Iterables;

import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RangeStreamer;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.locator.Replica.fullReplica;
import static org.apache.cassandra.locator.Replica.transientReplica;
import static org.apache.cassandra.service.StorageServiceTest.assertMultimapEqualsIgnoreOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This is also fairly effectively testing source retrieval for bootstrap as well since RangeStreamer
 * is used to calculate the endpoints to fetch from and check they are alive for both RangeRelocator (move) and
 * bootstrap (RangeRelocator).
 */
public class MoveTransientTest
{
    private static final Logger logger = LoggerFactory.getLogger(MoveTransientTest.class);

    static InetAddressAndPort address01;
    static InetAddressAndPort address02;
    static InetAddressAndPort address03;
    static InetAddressAndPort address04;
    static InetAddressAndPort address05;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        address01 = InetAddressAndPort.getByName("127.0.0.1");
        address02 = InetAddressAndPort.getByName("127.0.0.2");
        address03 = InetAddressAndPort.getByName("127.0.0.3");
        address04 = InetAddressAndPort.getByName("127.0.0.4");
        address05 = InetAddressAndPort.getByName("127.0.0.5");
    }

    private final List<InetAddressAndPort> downNodes = new ArrayList<>();

    final RangeStreamer.SourceFilter alivePredicate = new RangeStreamer.SourceFilter()
    {
        public boolean apply(Replica replica)
        {
            return !downNodes.contains(replica.endpoint());
        }

        public String message(Replica replica)
        {
            return "Down nodes: " + downNodes;
        }
    };

    final RangeStreamer.SourceFilter sourceFilterDownNodesPredicate = new RangeStreamer.SourceFilter()
    {
        public boolean apply(Replica replica)
        {
            return !sourceFilterDownNodes.contains(replica.endpoint());
        }

        public String message(Replica replica)
        {
            return "Source filter down nodes: " + sourceFilterDownNodes;
        }
    };

    private final List<InetAddressAndPort> sourceFilterDownNodes = new ArrayList<>();

    private final Collection<RangeStreamer.SourceFilter> sourceFilters = Arrays.asList(alivePredicate,
                                                                                       sourceFilterDownNodesPredicate,
                                                                                       new RangeStreamer.ExcludeLocalNodeFilter()
    );

    @After
    public void clearDownNode()
    {
        downNodes.clear();
        sourceFilterDownNodes.clear();
    }

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    final Token oneToken = new RandomPartitioner.BigIntegerToken("1");
    final Token twoToken = new RandomPartitioner.BigIntegerToken("2");
    final Token threeToken = new RandomPartitioner.BigIntegerToken("3");
    final Token fourToken = new RandomPartitioner.BigIntegerToken("4");
    final Token sixToken = new RandomPartitioner.BigIntegerToken("6");
    final Token sevenToken = new RandomPartitioner.BigIntegerToken("7");
    final Token nineToken = new RandomPartitioner.BigIntegerToken("9");
    final Token elevenToken = new RandomPartitioner.BigIntegerToken("11");
    final Token fourteenToken = new RandomPartitioner.BigIntegerToken("14");

    final Range<Token> range_1_2 = new Range(oneToken, threeToken);
    final Range<Token> range_3_6 = new Range(threeToken, sixToken);
    final Range<Token> range_6_9 = new Range(sixToken, nineToken);
    final Range<Token> range_9_11 = new Range(nineToken, elevenToken);
    final Range<Token> range_11_1 = new Range(elevenToken, oneToken);


    final RangesAtEndpoint current = RangesAtEndpoint.of(new Replica(address01, range_1_2, true),
                                                         new Replica(address01, range_11_1, true),
                                                         new Replica(address01, range_9_11, false));

    public Token token(String s)
    {
        return new RandomPartitioner.BigIntegerToken(s);
    }

    public Range<Token> range(String start, String end)
    {
        return new Range<>(token(start), token(end));
    }

    /**
     * Ring with start A 1-3 B 3-6 C 6-9 D 9-1
     * A's token moves from 3 to 4.
     * <p>
     * Result is A gains some range
     *
     * @throws Exception
     */
    @Test
    public void testCalculateStreamAndFetchRangesMoveForward() throws Exception
    {
        calculateStreamAndFetchRangesMoveForward();
    }

    private Pair<RangesAtEndpoint, RangesAtEndpoint> calculateStreamAndFetchRangesMoveForward() throws Exception
    {
        Range<Token> aPrimeRange = new Range<>(oneToken, fourToken);

        RangesAtEndpoint updated = RangesAtEndpoint.of(
                new Replica(address01, aPrimeRange, true),
                new Replica(address01, range_11_1, true),
                new Replica(address01, range_9_11, false)
        );

        Pair<RangesAtEndpoint, RangesAtEndpoint> result = RangeRelocator.calculateStreamAndFetchRanges(current, updated);
        assertContentsIgnoreOrder(result.left);
        assertContentsIgnoreOrder(result.right, fullReplica(address01, threeToken, fourToken));
        return result;
    }

    /**
     * Ring with start A 1-3 B 3-6 C 6-9 D 9-11 E 11-1
     * A's token moves from 3 to 14
     * <p>
     * Result is A loses range and it must be streamed
     *
     * @throws Exception
     */
    @Test
    public void testCalculateStreamAndFetchRangesMoveBackwardBetween() throws Exception
    {
        calculateStreamAndFetchRangesMoveBackwardBetween();
    }

    public Pair<RangesAtEndpoint, RangesAtEndpoint> calculateStreamAndFetchRangesMoveBackwardBetween() throws Exception
    {
        Range<Token> aPrimeRange = new Range<>(elevenToken, fourteenToken);

        RangesAtEndpoint updated = RangesAtEndpoint.of(
            new Replica(address01, aPrimeRange, true),
            new Replica(address01, range_9_11, true),
            new Replica(address01, range_6_9, false)
        );


        Pair<RangesAtEndpoint, RangesAtEndpoint> result = RangeRelocator.calculateStreamAndFetchRanges(current, updated);
        assertContentsIgnoreOrder(result.left, fullReplica(address01, oneToken, threeToken), fullReplica(address01, fourteenToken, oneToken));
        assertContentsIgnoreOrder(result.right, transientReplica(address01, sixToken, nineToken), fullReplica(address01, nineToken, elevenToken));
        return result;
    }

    /**
     * Ring with start A 1-3 B 3-6 C 6-9 D 9-11 E 11-1
     * A's token moves from 3 to 2
     *
     * Result is A loses range and it must be streamed
     * @throws Exception
     */
    @Test
    public void testCalculateStreamAndFetchRangesMoveBackward() throws Exception
    {
        calculateStreamAndFetchRangesMoveBackward();
    }

    private Pair<RangesAtEndpoint, RangesAtEndpoint> calculateStreamAndFetchRangesMoveBackward() throws Exception
    {
        Range<Token> aPrimeRange = new Range<>(oneToken, twoToken);

        RangesAtEndpoint updated = RangesAtEndpoint.of(
            new Replica(address01, aPrimeRange, true),
            new Replica(address01, range_11_1, true),
            new Replica(address01, range_9_11, false)
        );

        Pair<RangesAtEndpoint, RangesAtEndpoint> result = RangeRelocator.calculateStreamAndFetchRanges(current, updated);

        //Moving backwards has no impact on any replica. We already fully replicate counter clockwise
        //The transient replica does transiently replicate slightly more, but that is addressed by cleanup
        assertContentsIgnoreOrder(result.left, fullReplica(address01, twoToken, threeToken));
        assertContentsIgnoreOrder(result.right);

        return result;
    }

    /**
     * Ring with start A 1-3 B 3-6 C 6-9 D 9-11 E 11-1
     * A's moves from 3 to 7
     *
     * @throws Exception
     */
    private Pair<RangesAtEndpoint, RangesAtEndpoint> calculateStreamAndFetchRangesMoveForwardBetween() throws Exception
    {
        Range<Token> aPrimeRange = new Range<>(sixToken, sevenToken);
        Range<Token> bPrimeRange = new Range<>(oneToken, sixToken);

        RangesAtEndpoint updated = RangesAtEndpoint.of(
            new Replica(address01, aPrimeRange, true),
            new Replica(address01, bPrimeRange, true),
            new Replica(address01, range_11_1, false)
        );

        Pair<RangesAtEndpoint, RangesAtEndpoint> result = RangeRelocator.calculateStreamAndFetchRanges(current, updated);

        assertContentsIgnoreOrder(result.left, fullReplica(address01, elevenToken, oneToken), transientReplica(address01, nineToken, elevenToken));
        assertContentsIgnoreOrder(result.right, fullReplica(address01, threeToken, sixToken), fullReplica(address01, sixToken, sevenToken));
        return result;
    }

    /**
     * Ring with start A 1-3 B 3-6 C 6-9 D 9-11 E 11-1
     * A's token moves from 3 to 7
     *
     * @throws Exception
     */
    @Test
    public void testCalculateStreamAndFetchRangesMoveForwardBetween() throws Exception
    {
        calculateStreamAndFetchRangesMoveForwardBetween();
    }

    @Test
    public void testResubtract()
    {
        Token oneToken = new RandomPartitioner.BigIntegerToken("0001");
        Token tenToken = new RandomPartitioner.BigIntegerToken("0010");
        Token fiveToken = new RandomPartitioner.BigIntegerToken("0005");

        Range<Token> range_1_10 = new Range<>(oneToken, tenToken);
        Range<Token> range_1_5 = new Range<>(oneToken, tenToken);
        Range<Token> range_5_10 = new Range<>(fiveToken, tenToken);

        RangesAtEndpoint singleRange = RangesAtEndpoint.of(
        new Replica(address01, range_1_10, true)
        );

        RangesAtEndpoint splitRanges = RangesAtEndpoint.of(
        new Replica(address01, range_1_5, true),
        new Replica(address01, range_5_10, true)
        );

        // forward
        Pair<RangesAtEndpoint, RangesAtEndpoint> calculated = RangeRelocator.calculateStreamAndFetchRanges(singleRange, splitRanges);
        assertTrue(calculated.left.toString(), calculated.left.isEmpty());
        assertTrue(calculated.right.toString(), calculated.right.isEmpty());

        // backward
        calculated = RangeRelocator.calculateStreamAndFetchRanges(splitRanges, singleRange);
        assertTrue(calculated.left.toString(), calculated.left.isEmpty());
        assertTrue(calculated.right.toString(), calculated.right.isEmpty());
    }

    /**
     * Construct the ring state for calculateStreamAndFetchRangesMoveBackwardBetween
     * Where are A moves from 3 to 14
     * @return
     */
    private Pair<TokenMetadata, TokenMetadata> constructTMDsMoveBackwardBetween()
    {
        TokenMetadata tmd = new TokenMetadata();
        tmd.updateNormalToken(range_1_2.right, address01);
        tmd.updateNormalToken(range_3_6.right, address02);
        tmd.updateNormalToken(range_6_9.right, address03);
        tmd.updateNormalToken(range_9_11.right, address04);
        tmd.updateNormalToken(range_11_1.right, address05);
        tmd.addMovingEndpoint(fourteenToken, address01);
        TokenMetadata updated = tmd.cloneAfterAllSettled();

        return Pair.create(tmd, updated);
    }


    /**
     * Construct the ring state for calculateStreamAndFetchRangesMoveForwardBetween
     * Where are A moves from 3 to 7
     * @return
     */
    private Pair<TokenMetadata, TokenMetadata> constructTMDsMoveForwardBetween()
    {
        TokenMetadata tmd = new TokenMetadata();
        tmd.updateNormalToken(range_1_2.right, address01);
        tmd.updateNormalToken(range_3_6.right, address02);
        tmd.updateNormalToken(range_6_9.right, address03);
        tmd.updateNormalToken(range_9_11.right, address04);
        tmd.updateNormalToken(range_11_1.right, address05);
        tmd.addMovingEndpoint(sevenToken, address01);
        TokenMetadata updated = tmd.cloneAfterAllSettled();

        return Pair.create(tmd, updated);
    }

    private Pair<TokenMetadata, TokenMetadata> constructTMDsMoveBackward()
    {
        TokenMetadata tmd = new TokenMetadata();
        tmd.updateNormalToken(range_1_2.right, address01);
        tmd.updateNormalToken(range_3_6.right, address02);
        tmd.updateNormalToken(range_6_9.right, address03);
        tmd.updateNormalToken(range_9_11.right, address04);
        tmd.updateNormalToken(range_11_1.right, address05);
        tmd.addMovingEndpoint(twoToken, address01);
        TokenMetadata updated = tmd.cloneAfterAllSettled();

        return Pair.create(tmd, updated);
    }

    private Pair<TokenMetadata, TokenMetadata> constructTMDsMoveForward()
    {
        TokenMetadata tmd = new TokenMetadata();
        tmd.updateNormalToken(range_1_2.right, address01);
        tmd.updateNormalToken(range_3_6.right, address02);
        tmd.updateNormalToken(range_6_9.right, address03);
        tmd.updateNormalToken(range_9_11.right, address04);
        tmd.updateNormalToken(range_11_1.right, address05);
        tmd.addMovingEndpoint(fourToken, address01);
        TokenMetadata updated = tmd.cloneAfterAllSettled();

        return Pair.create(tmd, updated);
    }


    @Test
    public void testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpoints() throws Exception
    {
        EndpointsByReplica.Builder expectedResult = new EndpointsByReplica.Builder();

        InetAddressAndPort cOrB = (downNodes.contains(address03) || sourceFilterDownNodes.contains(address03)) ? address02 : address03;

        //Need to pull the full replica and the transient replica that is losing the range
        expectedResult.put(fullReplica(address01, sixToken, sevenToken), fullReplica(address04, sixToken, nineToken));
        expectedResult.put(fullReplica(address01, sixToken, sevenToken), transientReplica(address05, sixToken, nineToken));

        //Same need both here as well
        expectedResult.put(fullReplica(address01, threeToken, sixToken), fullReplica(cOrB, threeToken, sixToken));
        expectedResult.put(fullReplica(address01, threeToken, sixToken), transientReplica(address04, threeToken, sixToken));

        invokeCalculateRangesToFetchWithPreferredEndpoints(calculateStreamAndFetchRangesMoveForwardBetween().right,
                                                           constructTMDsMoveForwardBetween(),
                                                           expectedResult.build());
    }

    @Test
    public void testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpointsDownNodes() throws Exception
    {
        for (InetAddressAndPort downNode : new InetAddressAndPort[] { address04, address05 })
        {
            downNodes.clear();
            downNodes.add(downNode);
            boolean threw = false;
            try
            {
                testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpoints();
            }
            catch (IllegalStateException ise)
            {
                ise.printStackTrace();
                assertTrue(downNode.toString(),
                           ise.getMessage().contains("Down nodes: [" + downNode + "]"));
                threw = true;
            }
            assertTrue("Didn't throw for " + downNode, threw);
        }

        //Shouldn't throw because another full replica is available
        downNodes.clear();
        downNodes.add(address03);
        testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpoints();
    }

    @Test
    public void testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpointsDownNodesSourceFilter() throws Exception
    {
        for (InetAddressAndPort downNode : new InetAddressAndPort[] { address04, address05 })
        {
            sourceFilterDownNodes.clear();
            sourceFilterDownNodes.add(downNode);
            boolean threw = false;
            try
            {
                testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpoints();
            }
            catch (IllegalStateException ise)
            {
                ise.printStackTrace();
                assertTrue(downNode.toString(),
                           ise.getMessage().startsWith("Necessary replicas for strict consistency were removed by source filters:")
                           && ise.getMessage().contains(downNode.toString()));
                threw = true;
            }
            assertTrue("Didn't throw for " + downNode, threw);
        }

        //Shouldn't throw because another full replica is available
        sourceFilterDownNodes.clear();
        sourceFilterDownNodes.add(address03);
        testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpoints();
    }

    @Test
    public void testMoveBackwardBetweenCalculateRangesToFetchWithPreferredEndpoints() throws Exception
    {
        EndpointsByReplica.Builder expectedResult = new EndpointsByReplica.Builder();

        //Need to pull the full replica and the transient replica that is losing the range
        expectedResult.put(fullReplica(address01, nineToken, elevenToken), fullReplica(address05, nineToken, elevenToken));
        expectedResult.put(transientReplica(address01, sixToken, nineToken), transientReplica(address05, sixToken, nineToken));

        invokeCalculateRangesToFetchWithPreferredEndpoints(calculateStreamAndFetchRangesMoveBackwardBetween().right,
                                                           constructTMDsMoveBackwardBetween(),
                                                           expectedResult.build());

    }

    @Test(expected = IllegalStateException.class)
    public void testMoveBackwardBetweenCalculateRangesToFetchWithPreferredEndpointsDownNodes() throws Exception
    {
        //Any replica can be the full replica so this will always fail on the transient range
        downNodes.add(address05);
        testMoveBackwardBetweenCalculateRangesToFetchWithPreferredEndpoints();
    }

    @Test(expected = IllegalStateException.class)
    public void testMoveBackwardBetweenCalculateRangesToFetchWithPreferredEndpointsDownNodesSourceFilter() throws Exception
    {
        //Any replica can be the full replica so this will always fail on the transient range
        sourceFilterDownNodes.add(address05);
        testMoveBackwardBetweenCalculateRangesToFetchWithPreferredEndpoints();
    }


    //There is no down node version of this test because nothing needs to be fetched
    @Test
    public void testMoveBackwardCalculateRangesToFetchWithPreferredEndpoints() throws Exception
    {
        //Moving backwards should fetch nothing and fetch ranges is emptys so this doesn't test a ton
        EndpointsByReplica.Builder expectedResult = new EndpointsByReplica.Builder();

        invokeCalculateRangesToFetchWithPreferredEndpoints(calculateStreamAndFetchRangesMoveBackward().right,
                                                           constructTMDsMoveBackward(),
                                                           expectedResult.build());

    }

    @Test
    public void testMoveForwardCalculateRangesToFetchWithPreferredEndpoints() throws Exception
    {
        EndpointsByReplica.Builder expectedResult = new EndpointsByReplica.Builder();

        InetAddressAndPort cOrBAddress = (downNodes.contains(address03) || sourceFilterDownNodes.contains(address03)) ? address02 : address03;

        //Need to pull the full replica and the transient replica that is losing the range
        expectedResult.put(fullReplica(address01, threeToken, fourToken), fullReplica(cOrBAddress, threeToken, sixToken));
        expectedResult.put(fullReplica(address01, threeToken, fourToken), transientReplica(address04, threeToken, sixToken));

        invokeCalculateRangesToFetchWithPreferredEndpoints(calculateStreamAndFetchRangesMoveForward().right,
                                                           constructTMDsMoveForward(),
                                                           expectedResult.build());

    }

    @Test
    public void testMoveForwardCalculateRangesToFetchWithPreferredEndpointsDownNodes() throws Exception
    {
        downNodes.add(address04);
        boolean threw = false;
        try
        {
            testMoveForwardCalculateRangesToFetchWithPreferredEndpoints();
        }
        catch (IllegalStateException ise)
        {
            ise.printStackTrace();
            assertTrue(address04.toString(),
                       ise.getMessage().contains("Down nodes: [" + address04 + "]"));
            threw = true;
        }
        assertTrue("Didn't throw for " + address04, threw);

        //Shouldn't throw because another full replica is available
        downNodes.clear();
        downNodes.add(address03);
        testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpoints();
    }

    @Test
    public void testMoveForwardCalculateRangesToFetchWithPreferredEndpointsDownNodesSourceFilter() throws Exception
    {
        sourceFilterDownNodes.add(address04);
        boolean threw = false;
        try
        {
            testMoveForwardCalculateRangesToFetchWithPreferredEndpoints();
        }
        catch (IllegalStateException ise)
        {
            ise.printStackTrace();
            assertTrue(address04.toString(),
                       ise.getMessage().startsWith("Necessary replicas for strict consistency were removed by source filters:")
                       && ise.getMessage().contains(address04.toString()));
            threw = true;
        }
        assertTrue("Didn't throw for " + address04, threw);

        //Shouldn't throw because another full replica is available
        sourceFilterDownNodes.clear();
        sourceFilterDownNodes.add(address03);
        testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpoints();
    }

    private void invokeCalculateRangesToFetchWithPreferredEndpoints(RangesAtEndpoint toFetch,
                                                                    Pair<TokenMetadata, TokenMetadata> tmds,
                                                                    EndpointsByReplica expectedResult)
    {
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);

        EndpointsByReplica result = RangeStreamer.calculateRangesToFetchWithPreferredEndpoints((address, replicas) -> replicas.sorted((a, b) -> b.endpoint().compareTo(a.endpoint())),
                                                                                               simpleStrategy(tmds.left),
                                                                                               toFetch,
                                                                                               true,
                                                                                               tmds.left,
                                                                                               tmds.right,
                                                                                               "TestKeyspace",
                                                                                               sourceFilters);
        logger.info("Ranges to fetch with preferred endpoints");
        logger.info(result.toString());
        assertMultimapEqualsIgnoreOrder(expectedResult, result);
    }

    private AbstractReplicationStrategy simpleStrategy(TokenMetadata tmd)
    {
        IEndpointSnitch snitch = new AbstractEndpointSnitch()
        {
            public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
            {
                return 0;
            }

            public String getRack(InetAddressAndPort endpoint)
            {
                return "R1";
            }

            public String getDatacenter(InetAddressAndPort endpoint)
            {
                return "DC1";
            }
        };

        return new SimpleStrategy("MoveTransientTest",
                                  tmd,
                                  snitch,
                                  com.google.common.collect.ImmutableMap.of("replication_factor", "3/1"));
    }

    @Test
    public void testMoveForwardBetweenCalculateRangesToStreamWithPreferredEndpoints() throws Exception
    {
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
        RangesByEndpoint.Builder expectedResult = new RangesByEndpoint.Builder();

        //Need to pull the full replica and the transient replica that is losing the range
        expectedResult.put(address02, transientReplica(address02, nineToken, elevenToken));
        expectedResult.put(address02, fullReplica(address02, elevenToken, oneToken));

        invokeCalculateRangesToStreamWithPreferredEndpoints(calculateStreamAndFetchRangesMoveForwardBetween().left,
                                                            constructTMDsMoveForwardBetween(),
                                                            expectedResult.build());
    }

    @Test
    public void testMoveBackwardBetweenCalculateRangesToStreamWithPreferredEndpoints() throws Exception
    {
        RangesByEndpoint.Builder expectedResult = new RangesByEndpoint.Builder();

        expectedResult.put(address02, fullReplica(address02, fourteenToken, oneToken));

        expectedResult.put(address04, transientReplica(address04, oneToken, threeToken));

        expectedResult.put(address03, fullReplica(address03, oneToken, threeToken));
        expectedResult.put(address03, transientReplica(address03, fourteenToken, oneToken));

        invokeCalculateRangesToStreamWithPreferredEndpoints(calculateStreamAndFetchRangesMoveBackwardBetween().left,
                                                            constructTMDsMoveBackwardBetween(),
                                                            expectedResult.build());
    }

    @Test
    public void testMoveBackwardCalculateRangesToStreamWithPreferredEndpoints() throws Exception
    {
        RangesByEndpoint.Builder expectedResult = new RangesByEndpoint.Builder();
        expectedResult.put(address03, fullReplica(address03, twoToken, threeToken));
        expectedResult.put(address04, transientReplica(address04, twoToken, threeToken));

        invokeCalculateRangesToStreamWithPreferredEndpoints(calculateStreamAndFetchRangesMoveBackward().left,
                                                            constructTMDsMoveBackward(),
                                                            expectedResult.build());
    }

    @Test
    public void testMoveForwardCalculateRangesToStreamWithPreferredEndpoints() throws Exception
    {
        //Nothing to stream moving forward because we are acquiring more range not losing range
        RangesByEndpoint.Builder expectedResult = new RangesByEndpoint.Builder();

        invokeCalculateRangesToStreamWithPreferredEndpoints(calculateStreamAndFetchRangesMoveForward().left,
                                                            constructTMDsMoveForward(),
                                                            expectedResult.build());
    }

    private void invokeCalculateRangesToStreamWithPreferredEndpoints(RangesAtEndpoint toStream,
                                                                     Pair<TokenMetadata, TokenMetadata> tmds,
                                                                     RangesByEndpoint expectedResult)
    {
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
        RangeRelocator relocator = new RangeRelocator();
        RangesByEndpoint result = relocator.calculateRangesToStreamWithEndpoints(toStream,
                                                                                 simpleStrategy(tmds.left),
                                                                                 tmds.left,
                                                                                 tmds.right);
        logger.info("Ranges to stream by endpoint");
        logger.info(result.toString());
        assertMultimapEqualsIgnoreOrder(expectedResult, result);
    }

    private static void assertContentsIgnoreOrder(RangesAtEndpoint ranges, Replica ... replicas)
    {
        assertEquals(ranges.size(), replicas.length);
        for (Replica replica : replicas)
        {
            if (!ranges.contains(replica))
                assertTrue(Iterables.elementsEqual(RangesAtEndpoint.of(replicas), ranges));
        }
    }

}
