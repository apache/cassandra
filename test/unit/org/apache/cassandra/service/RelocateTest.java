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

import static org.junit.Assert.*;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.dht.BigIntegerToken;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class RelocateTest
{
    private static final int TOKENS_PER_NODE = 256;
    private static final int TOKEN_STEP = 10;
    private static final IPartitioner<?> partitioner = new RandomPartitioner();
    private static IPartitioner<?> oldPartitioner;
    private static VersionedValue.VersionedValueFactory vvFactory;

    private StorageService ss = StorageService.instance;
    private TokenMetadata tmd = StorageService.instance.getTokenMetadata();

    @Before
    public void init()
    {
        tmd.clearUnsafe();
    }

    @BeforeClass
    public static void setUp() throws Exception
    {
        oldPartitioner = StorageService.instance.setPartitionerUnsafe(partitioner);
        SchemaLoader.loadSchema();
        vvFactory = new VersionedValue.VersionedValueFactory(partitioner);
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        StorageService.instance.setPartitionerUnsafe(oldPartitioner);
        SchemaLoader.stopGossiper();
    }

    /** Setup a virtual node ring */
    private static Map<Token<?>, InetAddress> createInitialRing(int size) throws UnknownHostException
    {
        Map<Token<?>, InetAddress> tokenMap = new HashMap<Token<?>, InetAddress>();
        int currentToken = TOKEN_STEP;

        for(int i = 0; i < size; i++)
        {
            InetAddress endpoint = InetAddress.getByName("127.0.0." + String.valueOf(i + 1));
            Gossiper.instance.initializeNodeUnsafe(endpoint, UUID.randomUUID(), 1);
            List<Token> tokens = new ArrayList<Token>();

            for (int j = 0; j < TOKENS_PER_NODE; j++)
            {
                Token token = new BigIntegerToken(String.valueOf(currentToken));
                tokenMap.put(token, endpoint);
                tokens.add(token);
                currentToken += TOKEN_STEP;
            }

            Gossiper.instance.injectApplicationState(endpoint, ApplicationState.TOKENS, vvFactory.tokens(tokens));
            StorageService.instance.onChange(endpoint, ApplicationState.STATUS, vvFactory.normal(tokens));
        }

        return tokenMap;
    }

    // Copy-pasta from MoveTest.java
    private AbstractReplicationStrategy getStrategy(String table, TokenMetadata tmd) throws ConfigurationException
    {
        KSMetaData ksmd = Schema.instance.getKSMetaData(table);
        return AbstractReplicationStrategy.createReplicationStrategy(
                table,
                ksmd.strategyClass,
                tmd,
                new SimpleSnitch(),
                ksmd.strategyOptions);
    }

    /** Ensure proper write endpoints during relocation */
    @Test
    public void testWriteEndpointsDuringRelocate() throws Exception
    {
        Map<Token<?>, InetAddress> tokenMap = createInitialRing(5);
        Map<Token, List<InetAddress>> expectedEndpoints = new HashMap<Token, List<InetAddress>>();


        for (Token<?> token : tokenMap.keySet())
        {
            BigIntegerToken keyToken = new BigIntegerToken(((BigInteger)token.token).add(new BigInteger("5")));
            List<InetAddress> endpoints = new ArrayList<InetAddress>();
            Iterator<Token> tokenIter = TokenMetadata.ringIterator(tmd.sortedTokens(), keyToken, false);
            while (tokenIter.hasNext())
            {
                InetAddress ep = tmd.getEndpoint(tokenIter.next());
                if (!endpoints.contains(ep))
                    endpoints.add(ep);
            }
            expectedEndpoints.put(keyToken, endpoints);
        }

        // Relocate the first token from the first endpoint, to the second endpoint.
        Token relocateToken = new BigIntegerToken(String.valueOf(TOKEN_STEP));
        ss.onChange(
                InetAddress.getByName("127.0.0.2"),
                ApplicationState.STATUS,
                vvFactory.relocating(Collections.singleton(relocateToken)));
        assertTrue(tmd.isRelocating(relocateToken));

        AbstractReplicationStrategy strategy;
        for (String table : Schema.instance.getNonSystemTables())
        {
            strategy = getStrategy(table, tmd);
            for (Token token : tokenMap.keySet())
            {
                BigIntegerToken keyToken = new BigIntegerToken(((BigInteger)token.token).add(new BigInteger("5")));

                HashSet<InetAddress> actual = new HashSet<InetAddress>(tmd.getWriteEndpoints(keyToken, table, strategy.calculateNaturalEndpoints(keyToken, tmd.cloneOnlyTokenMap())));
                HashSet<InetAddress> expected = new HashSet<InetAddress>();

                for (int i = 0; i < actual.size(); i++)
                    expected.add(expectedEndpoints.get(keyToken).get(i));

                assertEquals("mismatched endpoint sets", expected, actual);
            }
        }
    }

    /** Use STATUS changes to trigger membership update and validate results. */
    @Test
    public void testRelocationSuccess() throws UnknownHostException
    {
        createInitialRing(5);

        // Node handling the relocation (dst), and the token being relocated (src).
        InetAddress relocator = InetAddress.getByName("127.0.0.3");
        Token relocatee = new BigIntegerToken(String.valueOf(TOKEN_STEP));

        // Send RELOCATING and ensure token status
        ss.onChange(relocator, ApplicationState.STATUS, vvFactory.relocating(Collections.singleton(relocatee)));
        assertTrue(tmd.isRelocating(relocatee));

        // Create a list of the endpoint's existing tokens, and add the relocatee to it.
        List<Token> tokens = new ArrayList<Token>(tmd.getTokens(relocator));
        SystemTable.updateTokens(tokens);
        tokens.add(relocatee);

        // Send a normal status, then ensure all is copesetic.
        Gossiper.instance.injectApplicationState(relocator, ApplicationState.TOKENS, vvFactory.tokens(tokens));
        ss.onChange(relocator, ApplicationState.STATUS, vvFactory.normal(tokens));

        // Relocating entries are removed after RING_DELAY
        try
        {
            Thread.sleep(StorageService.RING_DELAY + 10);
        }
        catch (InterruptedException e)
        {
            System.err.println("ACHTUNG! Interrupted; testRelocationSuccess() will almost certainly fail!");
        }

        assertTrue(!tmd.isRelocating(relocatee));
        assertEquals(tmd.getEndpoint(relocatee), relocator);
    }
}
