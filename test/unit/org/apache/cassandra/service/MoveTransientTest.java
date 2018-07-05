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
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Predicate;
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

    static InetAddressAndPort aAddress;
    static InetAddressAndPort bAddress;
    static InetAddressAndPort cAddress;
    static InetAddressAndPort dAddress;
    static InetAddressAndPort eAddress;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        aAddress = InetAddressAndPort.getByName("127.0.0.1");
        bAddress = InetAddressAndPort.getByName("127.0.0.2");
        cAddress = InetAddressAndPort.getByName("127.0.0.3");
        dAddress = InetAddressAndPort.getByName("127.0.0.4");
        eAddress = InetAddressAndPort.getByName("127.0.0.5");
    }

    private final List<InetAddressAndPort> downNodes = new ArrayList();
    Predicate<Replica> alivePredicate = replica -> !downNodes.contains(replica.endpoint());

    private final List<InetAddressAndPort> sourceFilterDownNodes = new ArrayList<>();
    private final Collection<Predicate<Replica>> sourceFilters = Collections.singleton(replica -> !sourceFilterDownNodes.contains(replica.endpoint()));

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

    Token oneToken = new RandomPartitioner.BigIntegerToken("1");
    Token twoToken = new RandomPartitioner.BigIntegerToken("2");
    Token threeToken = new RandomPartitioner.BigIntegerToken("3");
    Token fourToken = new RandomPartitioner.BigIntegerToken("4");
    Token sixToken = new RandomPartitioner.BigIntegerToken("6");
    Token sevenToken = new RandomPartitioner.BigIntegerToken("7");
    Token nineToken = new RandomPartitioner.BigIntegerToken("9");
    Token elevenToken = new RandomPartitioner.BigIntegerToken("11");
    Token fourteenToken = new RandomPartitioner.BigIntegerToken("14");

    Range<Token> aRange = new Range(oneToken, threeToken);
    Range<Token> bRange = new Range(threeToken, sixToken);
    Range<Token> cRange = new Range(sixToken, nineToken);
    Range<Token> dRange = new Range(nineToken, elevenToken);
    Range<Token> eRange = new Range(elevenToken, oneToken);


    RangesAtEndpoint current = RangesAtEndpoint.of(new Replica(aAddress, aRange, true),
                                       new Replica(aAddress, eRange, true),
                                       new Replica(aAddress, dRange, false));


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
                new Replica(aAddress, aPrimeRange, true),
                new Replica(aAddress, eRange, true),
                new Replica(aAddress, dRange, false)
        );

        Pair<RangesAtEndpoint, RangesAtEndpoint> result = StorageService.calculateStreamAndFetchRanges(current, updated);
        assertContentsIgnoreOrder(result.left);
        assertContentsIgnoreOrder(result.right, fullReplica(aAddress, threeToken, fourToken));
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
            new Replica(aAddress, aPrimeRange, true),
            new Replica(aAddress, dRange, true),
            new Replica(aAddress, cRange, false)
        );


        Pair<RangesAtEndpoint, RangesAtEndpoint> result = StorageService.calculateStreamAndFetchRanges(current, updated);
        assertContentsIgnoreOrder(result.left, fullReplica(aAddress, oneToken, threeToken), fullReplica(aAddress, fourteenToken, oneToken));
        assertContentsIgnoreOrder(result.right, transientReplica(aAddress, sixToken, nineToken), fullReplica(aAddress, nineToken, elevenToken));
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
            new Replica(aAddress, aPrimeRange, true),
            new Replica(aAddress, eRange, true),
            new Replica(aAddress, dRange, false)
        );

        Pair<RangesAtEndpoint, RangesAtEndpoint> result = StorageService.calculateStreamAndFetchRanges(current, updated);

        //Moving backwards has no impact on any replica. We already fully replicate counter clockwise
        //The transient replica does transiently replicate slightly more, but that is addressed by cleanup
        assertContentsIgnoreOrder(result.left, fullReplica(aAddress, twoToken, threeToken));
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
            new Replica(aAddress, aPrimeRange, true),
            new Replica(aAddress, bPrimeRange, true),
            new Replica(aAddress, eRange, false)
        );

        Pair<RangesAtEndpoint, RangesAtEndpoint> result = StorageService.calculateStreamAndFetchRanges(current, updated);

        assertContentsIgnoreOrder(result.left, fullReplica(aAddress, elevenToken, oneToken), transientReplica(aAddress, nineToken, elevenToken));
        assertContentsIgnoreOrder(result.right, fullReplica(aAddress, threeToken, sixToken), fullReplica(aAddress, sixToken, sevenToken));
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

    /**
     * Construct the ring state for calculateStreamAndFetchRangesMoveBackwardBetween
     * Where are A moves from 3 to 14
     * @return
     */
    private Pair<TokenMetadata, TokenMetadata> constructTMDsMoveBackwardBetween()
    {
        TokenMetadata tmd = new TokenMetadata();
        tmd.updateNormalToken(aRange.right, aAddress);
        tmd.updateNormalToken(bRange.right, bAddress);
        tmd.updateNormalToken(cRange.right, cAddress);
        tmd.updateNormalToken(dRange.right, dAddress);
        tmd.updateNormalToken(eRange.right, eAddress);
        tmd.addMovingEndpoint(fourteenToken, aAddress);
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
        tmd.updateNormalToken(aRange.right, aAddress);
        tmd.updateNormalToken(bRange.right, bAddress);
        tmd.updateNormalToken(cRange.right, cAddress);
        tmd.updateNormalToken(dRange.right, dAddress);
        tmd.updateNormalToken(eRange.right, eAddress);
        tmd.addMovingEndpoint(sevenToken, aAddress);
        TokenMetadata updated = tmd.cloneAfterAllSettled();

        return Pair.create(tmd, updated);
    }

    private Pair<TokenMetadata, TokenMetadata> constructTMDsMoveBackward()
    {
        TokenMetadata tmd = new TokenMetadata();
        tmd.updateNormalToken(aRange.right, aAddress);
        tmd.updateNormalToken(bRange.right, bAddress);
        tmd.updateNormalToken(cRange.right, cAddress);
        tmd.updateNormalToken(dRange.right, dAddress);
        tmd.updateNormalToken(eRange.right, eAddress);
        tmd.addMovingEndpoint(twoToken, aAddress);
        TokenMetadata updated = tmd.cloneAfterAllSettled();

        return Pair.create(tmd, updated);
    }

    private Pair<TokenMetadata, TokenMetadata> constructTMDsMoveForward()
    {
        TokenMetadata tmd = new TokenMetadata();
        tmd.updateNormalToken(aRange.right, aAddress);
        tmd.updateNormalToken(bRange.right, bAddress);
        tmd.updateNormalToken(cRange.right, cAddress);
        tmd.updateNormalToken(dRange.right, dAddress);
        tmd.updateNormalToken(eRange.right, eAddress);
        tmd.addMovingEndpoint(fourToken, aAddress);
        TokenMetadata updated = tmd.cloneAfterAllSettled();

        return Pair.create(tmd, updated);
    }


    @Test
    public void testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpoints() throws Exception
    {
        EndpointsByReplica.Mutable expectedResult = new EndpointsByReplica.Mutable();

        InetAddressAndPort cOrB = (downNodes.contains(cAddress) || sourceFilterDownNodes.contains(cAddress)) ? bAddress : cAddress;

        //Need to pull the full replica and the transient replica that is losing the range
        expectedResult.put(fullReplica(aAddress, sixToken, sevenToken),  fullReplica(dAddress, sixToken, nineToken));
        expectedResult.put(fullReplica(aAddress, sixToken, sevenToken), transientReplica(eAddress, sixToken, nineToken));

        //Same need both here as well
        expectedResult.put(fullReplica(aAddress, threeToken, sixToken), fullReplica(cOrB, threeToken, sixToken));
        expectedResult.put(fullReplica(aAddress, threeToken, sixToken), transientReplica(dAddress, threeToken, sixToken));

        invokeCalculateRangesToFetchWithPreferredEndpoints(calculateStreamAndFetchRangesMoveForwardBetween().right,
                                                           constructTMDsMoveForwardBetween(),
                                                           expectedResult.asImmutableView());
    }

    @Test
    public void testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpointsDownNodes() throws Exception
    {
        for (InetAddressAndPort downNode : new InetAddressAndPort[] {dAddress, eAddress})
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
                           ise.getMessage().startsWith("A node required to move the data consistently is down:")
                                    && ise.getMessage().contains(downNode.toString()));
                threw = true;
            }
            assertTrue("Didn't throw for " + downNode, threw);
        }

        //Shouldn't throw because another full replica is available
        downNodes.clear();
        downNodes.add(cAddress);
        testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpoints();
    }

    @Test
    public void testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpointsDownNodesSourceFilter() throws Exception
    {
        for (InetAddressAndPort downNode : new InetAddressAndPort[] {dAddress, eAddress})
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
        sourceFilterDownNodes.add(cAddress);
        testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpoints();
    }

    @Test
    public void testMoveBackwardBetweenCalculateRangesToFetchWithPreferredEndpoints() throws Exception
    {
        EndpointsByReplica.Mutable expectedResult = new EndpointsByReplica.Mutable();

        //Need to pull the full replica and the transient replica that is losing the range
        expectedResult.put(fullReplica(aAddress, nineToken, elevenToken), fullReplica(eAddress, nineToken, elevenToken));
        expectedResult.put(transientReplica(aAddress, sixToken, nineToken), transientReplica(eAddress, sixToken, nineToken));

        invokeCalculateRangesToFetchWithPreferredEndpoints(calculateStreamAndFetchRangesMoveBackwardBetween().right,
                                                           constructTMDsMoveBackwardBetween(),
                                                           expectedResult.asImmutableView());

    }

    @Test(expected = IllegalStateException.class)
    public void testMoveBackwardBetweenCalculateRangesToFetchWithPreferredEndpointsDownNodes() throws Exception
    {
        //Any replica can be the full replica so this will always fail on the transient range
        downNodes.add(eAddress);
        testMoveBackwardBetweenCalculateRangesToFetchWithPreferredEndpoints();
    }

    @Test(expected = IllegalStateException.class)
    public void testMoveBackwardBetweenCalculateRangesToFetchWithPreferredEndpointsDownNodesSourceFilter() throws Exception
    {
        //Any replica can be the full replica so this will always fail on the transient range
        sourceFilterDownNodes.add(eAddress);
        testMoveBackwardBetweenCalculateRangesToFetchWithPreferredEndpoints();
    }


    //There is no down node version of this test because nothing needs to be fetched
    @Test
    public void testMoveBackwardCalculateRangesToFetchWithPreferredEndpoints() throws Exception
    {
        //Moving backwards should fetch nothing and fetch ranges is emptys so this doesn't test a ton
        EndpointsByReplica.Mutable expectedResult = new EndpointsByReplica.Mutable();

        invokeCalculateRangesToFetchWithPreferredEndpoints(calculateStreamAndFetchRangesMoveBackward().right,
                                                           constructTMDsMoveBackward(),
                                                           expectedResult.asImmutableView());

    }

    @Test
    public void testMoveForwardCalculateRangesToFetchWithPreferredEndpoints() throws Exception
    {
        EndpointsByReplica.Mutable expectedResult = new EndpointsByReplica.Mutable();

        InetAddressAndPort cOrBAddress = (downNodes.contains(cAddress) || sourceFilterDownNodes.contains(cAddress)) ? bAddress : cAddress;

        //Need to pull the full replica and the transient replica that is losing the range
        expectedResult.put(fullReplica(aAddress, threeToken, fourToken), fullReplica(cOrBAddress, threeToken, sixToken));
        expectedResult.put(fullReplica(aAddress, threeToken, fourToken), transientReplica(dAddress, threeToken, sixToken));

        invokeCalculateRangesToFetchWithPreferredEndpoints(calculateStreamAndFetchRangesMoveForward().right,
                                                           constructTMDsMoveForward(),
                                                           expectedResult.asImmutableView());

    }

    @Test
    public void testMoveForwardCalculateRangesToFetchWithPreferredEndpointsDownNodes() throws Exception
    {
        downNodes.add(dAddress);
        boolean threw = false;
        try
        {
            testMoveForwardCalculateRangesToFetchWithPreferredEndpoints();
        }
        catch (IllegalStateException ise)
        {
            ise.printStackTrace();
            assertTrue(dAddress.toString(),
                       ise.getMessage().startsWith("A node required to move the data consistently is down:")
                       && ise.getMessage().contains(dAddress.toString()));
            threw = true;
        }
        assertTrue("Didn't throw for " + dAddress, threw);

        //Shouldn't throw because another full replica is available
        downNodes.clear();
        downNodes.add(cAddress);
        testMoveForwardBetweenCalculateRangesToFetchWithPreferredEndpoints();
    }

    @Test
    public void testMoveForwardCalculateRangesToFetchWithPreferredEndpointsDownNodesSourceFilter() throws Exception
    {
        sourceFilterDownNodes.add(dAddress);
        boolean threw = false;
        try
        {
            testMoveForwardCalculateRangesToFetchWithPreferredEndpoints();
        }
        catch (IllegalStateException ise)
        {
            ise.printStackTrace();
            assertTrue(dAddress.toString(),
                       ise.getMessage().startsWith("Necessary replicas for strict consistency were removed by source filters:")
                       && ise.getMessage().contains(dAddress.toString()));
            threw = true;
        }
        assertTrue("Didn't throw for " + dAddress, threw);

        //Shouldn't throw because another full replica is available
        sourceFilterDownNodes.clear();
        sourceFilterDownNodes.add(cAddress);
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
                                                                                                                   alivePredicate,
                                                                                                                   "OldNetworkTopologyStrategyTest",
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
        RangesByEndpoint.Mutable expectedResult = new RangesByEndpoint.Mutable();

        //Need to pull the full replica and the transient replica that is losing the range
        expectedResult.put(bAddress, transientReplica(bAddress, nineToken, elevenToken));
        expectedResult.put(bAddress, fullReplica(bAddress, elevenToken, oneToken));

        invokeCalculateRangesToStreamWithPreferredEndpoints(calculateStreamAndFetchRangesMoveForwardBetween().left,
                                                            constructTMDsMoveForwardBetween(),
                                                            expectedResult.asImmutableView());
    }

    @Test
    public void testMoveBackwardBetweenCalculateRangesToStreamWithPreferredEndpoints() throws Exception
    {
        RangesByEndpoint.Mutable expectedResult = new RangesByEndpoint.Mutable();

        expectedResult.put(bAddress, fullReplica(bAddress, fourteenToken, oneToken));

        expectedResult.put(dAddress, transientReplica(dAddress, oneToken, threeToken));

        expectedResult.put(cAddress, fullReplica(cAddress, oneToken, threeToken));
        expectedResult.put(cAddress, transientReplica(cAddress, fourteenToken, oneToken));

        invokeCalculateRangesToStreamWithPreferredEndpoints(calculateStreamAndFetchRangesMoveBackwardBetween().left,
                                                            constructTMDsMoveBackwardBetween(),
                                                            expectedResult.asImmutableView());
    }

    @Test
    public void testMoveBackwardCalculateRangesToStreamWithPreferredEndpoints() throws Exception
    {
        RangesByEndpoint.Mutable expectedResult = new RangesByEndpoint.Mutable();
        expectedResult.put(cAddress, fullReplica(cAddress, twoToken, threeToken));
        expectedResult.put(dAddress, transientReplica(dAddress, twoToken, threeToken));

        invokeCalculateRangesToStreamWithPreferredEndpoints(calculateStreamAndFetchRangesMoveBackward().left,
                                                            constructTMDsMoveBackward(),
                                                            expectedResult.asImmutableView());
    }

    @Test
    public void testMoveForwardCalculateRangesToStreamWithPreferredEndpoints() throws Exception
    {
        //Nothing to stream moving forward because we are acquiring more range not losing range
        RangesByEndpoint.Mutable expectedResult = new RangesByEndpoint.Mutable();

        invokeCalculateRangesToStreamWithPreferredEndpoints(calculateStreamAndFetchRangesMoveForward().left,
                                                            constructTMDsMoveForward(),
                                                            expectedResult.asImmutableView());
    }

    private void invokeCalculateRangesToStreamWithPreferredEndpoints(RangesAtEndpoint toStream,
                                                                     Pair<TokenMetadata, TokenMetadata> tmds,
                                                                     RangesByEndpoint expectedResult)
    {
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
        StorageService.RangeRelocator relocator = new StorageService.RangeRelocator();
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
            if (!ranges.contains(replica))
                assertEquals(RangesAtEndpoint.of(replicas), ranges);
    }

}
