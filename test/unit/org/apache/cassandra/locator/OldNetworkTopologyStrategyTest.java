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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.RandomPartitioner.BigIntegerToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;

public class OldNetworkTopologyStrategyTest
{
    private List<Token> keyTokens;
    private TokenMetadata tmd;
    private Map<String, ArrayList<InetAddress>> expectedResults;

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void init()
    {
        keyTokens = new ArrayList<Token>();
        tmd = new TokenMetadata();
        expectedResults = new HashMap<String, ArrayList<InetAddress>>();
    }

    /**
     * 4 same rack endpoints
     *
     * @throws java.net.UnknownHostException
     */
    @Test
    public void testBigIntegerEndpointsA() throws UnknownHostException
    {
        RackInferringSnitch endpointSnitch = new RackInferringSnitch();

        AbstractReplicationStrategy strategy = new OldNetworkTopologyStrategy("Keyspace1", tmd, endpointSnitch, optsWithRF(1));
        addEndpoint("0", "5", "254.0.0.1");
        addEndpoint("10", "15", "254.0.0.2");
        addEndpoint("20", "25", "254.0.0.3");
        addEndpoint("30", "35", "254.0.0.4");

        expectedResults.put("5", buildResult("254.0.0.2", "254.0.0.3", "254.0.0.4"));
        expectedResults.put("15", buildResult("254.0.0.3", "254.0.0.4", "254.0.0.1"));
        expectedResults.put("25", buildResult("254.0.0.4", "254.0.0.1", "254.0.0.2"));
        expectedResults.put("35", buildResult("254.0.0.1", "254.0.0.2", "254.0.0.3"));

        testGetEndpoints(strategy, keyTokens.toArray(new Token[0]));
    }

    /**
     * 3 same rack endpoints
     * 1 external datacenter
     *
     * @throws java.net.UnknownHostException
     */
    @Test
    public void testBigIntegerEndpointsB() throws UnknownHostException
    {
        RackInferringSnitch endpointSnitch = new RackInferringSnitch();

        AbstractReplicationStrategy strategy = new OldNetworkTopologyStrategy("Keyspace1", tmd, endpointSnitch, optsWithRF(1));
        addEndpoint("0", "5", "254.0.0.1");
        addEndpoint("10", "15", "254.0.0.2");
        addEndpoint("20", "25", "254.1.0.3");
        addEndpoint("30", "35", "254.0.0.4");

        expectedResults.put("5", buildResult("254.0.0.2", "254.1.0.3", "254.0.0.4"));
        expectedResults.put("15", buildResult("254.1.0.3", "254.0.0.4", "254.0.0.1"));
        expectedResults.put("25", buildResult("254.0.0.4", "254.1.0.3", "254.0.0.1"));
        expectedResults.put("35", buildResult("254.0.0.1", "254.1.0.3", "254.0.0.2"));

        testGetEndpoints(strategy, keyTokens.toArray(new Token[0]));
    }

    /**
     * 2 same rack endpoints
     * 1 same datacenter, different rack endpoints
     * 1 external datacenter
     *
     * @throws java.net.UnknownHostException
     */
    @Test
    public void testBigIntegerEndpointsC() throws UnknownHostException
    {
        RackInferringSnitch endpointSnitch = new RackInferringSnitch();

        AbstractReplicationStrategy strategy = new OldNetworkTopologyStrategy("Keyspace1", tmd, endpointSnitch, optsWithRF(1));
        addEndpoint("0", "5", "254.0.0.1");
        addEndpoint("10", "15", "254.0.0.2");
        addEndpoint("20", "25", "254.0.1.3");
        addEndpoint("30", "35", "254.1.0.4");

        expectedResults.put("5", buildResult("254.0.0.2", "254.0.1.3", "254.1.0.4"));
        expectedResults.put("15", buildResult("254.0.1.3", "254.1.0.4", "254.0.0.1"));
        expectedResults.put("25", buildResult("254.1.0.4", "254.0.0.1", "254.0.0.2"));
        expectedResults.put("35", buildResult("254.0.0.1", "254.0.1.3", "254.1.0.4"));

        testGetEndpoints(strategy, keyTokens.toArray(new Token[0]));
    }

    private ArrayList<InetAddress> buildResult(String... addresses) throws UnknownHostException
    {
        ArrayList<InetAddress> result = new ArrayList<InetAddress>();
        for (String address : addresses)
        {
            result.add(InetAddress.getByName(address));
        }
        return result;
    }

    private void addEndpoint(String endpointTokenID, String keyTokenID, String endpointAddress) throws UnknownHostException
    {
        BigIntegerToken endpointToken = new BigIntegerToken(endpointTokenID);

        BigIntegerToken keyToken = new BigIntegerToken(keyTokenID);
        keyTokens.add(keyToken);

        InetAddress ep = InetAddress.getByName(endpointAddress);
        tmd.updateNormalToken(endpointToken, ep);
    }

    private void testGetEndpoints(AbstractReplicationStrategy strategy, Token[] keyTokens)
    {
        for (Token keyToken : keyTokens)
        {
            List<InetAddress> endpoints = strategy.getNaturalEndpoints(keyToken);
            for (int j = 0; j < endpoints.size(); j++)
            {
                ArrayList<InetAddress> hostsExpected = expectedResults.get(keyToken.toString());
                assertEquals(endpoints.get(j), hostsExpected.get(j));
            }
        }
    }

    /**
     * test basic methods to move a node. For sure, it's not the best place, but it's easy to test
     *
     * @throws java.net.UnknownHostException
     */
    @Test
    public void testMoveLeft() throws UnknownHostException
    {
        // Moves to the left : nothing to fetch, last part to stream

        int movingNodeIdx = 1;
        BigIntegerToken newToken = new BigIntegerToken("21267647932558653966460912964485513216");
        BigIntegerToken[] tokens = initTokens();
        BigIntegerToken[] tokensAfterMove = initTokensAfterMove(tokens, movingNodeIdx, newToken);
        Pair<Set<Range<Token>>, Set<Range<Token>>> ranges = calculateStreamAndFetchRanges(tokens, tokensAfterMove, movingNodeIdx);

        assertEquals(ranges.left.iterator().next().left, tokensAfterMove[movingNodeIdx]);
        assertEquals(ranges.left.iterator().next().right, tokens[movingNodeIdx]);
        assertEquals("No data should be fetched", ranges.right.size(), 0);

    }

    @Test
    public void testMoveRight() throws UnknownHostException
    {
        // Moves to the right : last part to fetch, nothing to stream

        int movingNodeIdx = 1;
        BigIntegerToken newToken = new BigIntegerToken("35267647932558653966460912964485513216");
        BigIntegerToken[] tokens = initTokens();
        BigIntegerToken[] tokensAfterMove = initTokensAfterMove(tokens, movingNodeIdx, newToken);
        Pair<Set<Range<Token>>, Set<Range<Token>>> ranges = calculateStreamAndFetchRanges(tokens, tokensAfterMove, movingNodeIdx);

        assertEquals("No data should be streamed", ranges.left.size(), 0);
        assertEquals(ranges.right.iterator().next().left, tokens[movingNodeIdx]);
        assertEquals(ranges.right.iterator().next().right, tokensAfterMove[movingNodeIdx]);

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMoveMiddleOfRing() throws UnknownHostException
    {
        // moves to another position in the middle of the ring : should stream all its data, and fetch all its new data

        int movingNodeIdx = 1;
        int movingNodeIdxAfterMove = 4;
        BigIntegerToken newToken = new BigIntegerToken("90070591730234615865843651857942052864");
        BigIntegerToken[] tokens = initTokens();
        BigIntegerToken[] tokensAfterMove = initTokensAfterMove(tokens, movingNodeIdx, newToken);
        Pair<Set<Range<Token>>, Set<Range<Token>>> ranges = calculateStreamAndFetchRanges(tokens, tokensAfterMove, movingNodeIdx);

        // sort the results, so they can be compared
        Range<Token>[] toStream = ranges.left.toArray(new Range[0]);
        Range<Token>[] toFetch = ranges.right.toArray(new Range[0]);
        Arrays.sort(toStream);
        Arrays.sort(toFetch);

        // build expected ranges
        Range<Token>[] toStreamExpected = new Range[2];
        toStreamExpected[0] = new Range<Token>(getToken(movingNodeIdx - 2, tokens), getToken(movingNodeIdx - 1, tokens));
        toStreamExpected[1] = new Range<Token>(getToken(movingNodeIdx - 1, tokens), getToken(movingNodeIdx, tokens));
        Arrays.sort(toStreamExpected);
        Range<Token>[] toFetchExpected = new Range[2];
        toFetchExpected[0] = new Range<Token>(getToken(movingNodeIdxAfterMove - 1, tokens), getToken(movingNodeIdxAfterMove, tokens));
        toFetchExpected[1] = new Range<Token>(getToken(movingNodeIdxAfterMove, tokensAfterMove), getToken(movingNodeIdx, tokensAfterMove));
        Arrays.sort(toFetchExpected);

        assertEquals(Arrays.equals(toStream, toStreamExpected), true);
        assertEquals(Arrays.equals(toFetch, toFetchExpected), true);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMoveAfterNextNeighbors() throws UnknownHostException
    {
        // moves after its next neighbor in the ring

        int movingNodeIdx = 1;
        int movingNodeIdxAfterMove = 2;
        BigIntegerToken newToken = new BigIntegerToken("52535295865117307932921825928971026432");
        BigIntegerToken[] tokens = initTokens();
        BigIntegerToken[] tokensAfterMove = initTokensAfterMove(tokens, movingNodeIdx, newToken);
        Pair<Set<Range<Token>>, Set<Range<Token>>> ranges = calculateStreamAndFetchRanges(tokens, tokensAfterMove, movingNodeIdx);


        // sort the results, so they can be compared
        Range<Token>[] toStream = ranges.left.toArray(new Range[0]);
        Range<Token>[] toFetch = ranges.right.toArray(new Range[0]);
        Arrays.sort(toStream);
        Arrays.sort(toFetch);

        // build expected ranges
        Range<Token>[] toStreamExpected = new Range[1];
        toStreamExpected[0] = new Range<Token>(getToken(movingNodeIdx - 2, tokens), getToken(movingNodeIdx - 1, tokens));
        Arrays.sort(toStreamExpected);
        Range<Token>[] toFetchExpected = new Range[2];
        toFetchExpected[0] = new Range<Token>(getToken(movingNodeIdxAfterMove - 1, tokens), getToken(movingNodeIdxAfterMove, tokens));
        toFetchExpected[1] = new Range<Token>(getToken(movingNodeIdxAfterMove, tokensAfterMove), getToken(movingNodeIdx, tokensAfterMove));
        Arrays.sort(toFetchExpected);

        assertEquals(Arrays.equals(toStream, toStreamExpected), true);
        assertEquals(Arrays.equals(toFetch, toFetchExpected), true);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMoveBeforePreviousNeighbor() throws UnknownHostException
    {
        // moves before its previous neighbor in the ring

        int movingNodeIdx = 1;
        int movingNodeIdxAfterMove = 7;
        BigIntegerToken newToken = new BigIntegerToken("158873535527910577765226390751398592512");
        BigIntegerToken[] tokens = initTokens();
        BigIntegerToken[] tokensAfterMove = initTokensAfterMove(tokens, movingNodeIdx, newToken);
        Pair<Set<Range<Token>>, Set<Range<Token>>> ranges = calculateStreamAndFetchRanges(tokens, tokensAfterMove, movingNodeIdx);

        Range<Token>[] toStream = ranges.left.toArray(new Range[0]);
        Range<Token>[] toFetch = ranges.right.toArray(new Range[0]);
        Arrays.sort(toStream);
        Arrays.sort(toFetch);

        Range<Token>[] toStreamExpected = new Range[2];
        toStreamExpected[0] = new Range<Token>(getToken(movingNodeIdx, tokensAfterMove), getToken(movingNodeIdx - 1, tokensAfterMove));
        toStreamExpected[1] = new Range<Token>(getToken(movingNodeIdx - 1, tokens), getToken(movingNodeIdx, tokens));
        Arrays.sort(toStreamExpected);
        Range<Token>[] toFetchExpected = new Range[1];
        toFetchExpected[0] = new Range<Token>(getToken(movingNodeIdxAfterMove - 1, tokens), getToken(movingNodeIdxAfterMove, tokens));
        Arrays.sort(toFetchExpected);

        System.out.println("toStream : " + Arrays.toString(toStream));
        System.out.println("toFetch : " + Arrays.toString(toFetch));
        System.out.println("toStreamExpected : " + Arrays.toString(toStreamExpected));
        System.out.println("toFetchExpected : " + Arrays.toString(toFetchExpected));

        assertEquals(Arrays.equals(toStream, toStreamExpected), true);
        assertEquals(Arrays.equals(toFetch, toFetchExpected), true);
    }

    private BigIntegerToken[] initTokensAfterMove(BigIntegerToken[] tokens,
            int movingNodeIdx, BigIntegerToken newToken)
    {
        BigIntegerToken[] tokensAfterMove = tokens.clone();
        tokensAfterMove[movingNodeIdx] = newToken;
        return tokensAfterMove;
    }

    private BigIntegerToken[] initTokens()
    {
        BigIntegerToken[] tokens = new BigIntegerToken[] {
                new BigIntegerToken("0"), // just to be able to test
                new BigIntegerToken("34028236692093846346337460743176821145"),
                new BigIntegerToken("42535295865117307932921825928971026432"),
                new BigIntegerToken("63802943797675961899382738893456539648"),
                new BigIntegerToken("85070591730234615865843651857942052864"),
                new BigIntegerToken("106338239662793269832304564822427566080"),
                new BigIntegerToken("127605887595351923798765477786913079296"),
                new BigIntegerToken("148873535527910577765226390751398592512")
        };
        return tokens;
    }

    private TokenMetadata initTokenMetadata(BigIntegerToken[] tokens)
            throws UnknownHostException
    {
        TokenMetadata tokenMetadataCurrent = new TokenMetadata();

        int lastIPPart = 1;
        for (BigIntegerToken token : tokens)
            tokenMetadataCurrent.updateNormalToken(token, InetAddress.getByName("254.0.0." + Integer.toString(lastIPPart++)));

        return tokenMetadataCurrent;
    }

    private BigIntegerToken getToken(int idx, BigIntegerToken[] tokens)
    {
        if (idx >= tokens.length)
            idx = idx % tokens.length;
        while (idx < 0)
            idx += tokens.length;

        return tokens[idx];

    }

    private Pair<Set<Range<Token>>, Set<Range<Token>>> calculateStreamAndFetchRanges(BigIntegerToken[] tokens, BigIntegerToken[] tokensAfterMove, int movingNodeIdx) throws UnknownHostException
    {
        RackInferringSnitch endpointSnitch = new RackInferringSnitch();

        InetAddress movingNode = InetAddress.getByName("254.0.0." + Integer.toString(movingNodeIdx + 1));


        TokenMetadata tokenMetadataCurrent = initTokenMetadata(tokens);
        TokenMetadata tokenMetadataAfterMove = initTokenMetadata(tokensAfterMove);
        AbstractReplicationStrategy strategy = new OldNetworkTopologyStrategy("Keyspace1", tokenMetadataCurrent, endpointSnitch, optsWithRF(2));

        Collection<Range<Token>> currentRanges = strategy.getAddressRanges().get(movingNode);
        Collection<Range<Token>> updatedRanges = strategy.getPendingAddressRanges(tokenMetadataAfterMove, tokensAfterMove[movingNodeIdx], movingNode);

        Pair<Set<Range<Token>>, Set<Range<Token>>> ranges = StorageService.instance.calculateStreamAndFetchRanges(currentRanges, updatedRanges);

        return ranges;
    }

    private static Map<String, String> optsWithRF(int rf)
    {
        return Collections.singletonMap("replication_factor", Integer.toString(rf));
    }
}
