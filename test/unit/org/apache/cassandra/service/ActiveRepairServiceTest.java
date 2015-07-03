/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.*;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

public class ActiveRepairServiceTest
{
    public static final String KEYSPACE5 = "Keyspace5";
    public static final String CF_STANDRAD1 = "Standard1";
    public static final String CF_COUNTER = "Counter1";

    public String cfname;
    public ColumnFamilyStore store;
    public InetAddress LOCAL, REMOTE;

    private boolean initialized;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE5,
                                    KeyspaceParams.simple(2),
                                    SchemaLoader.standardCFMD(KEYSPACE5, CF_COUNTER),
                                    SchemaLoader.standardCFMD(KEYSPACE5, CF_STANDRAD1));
    }

    @Before
    public void prepare() throws Exception
    {
        if (!initialized)
        {
            SchemaLoader.startGossiper();
            initialized = true;

            LOCAL = FBUtilities.getBroadcastAddress();
            // generate a fake endpoint for which we can spoof receiving/sending trees
            REMOTE = InetAddress.getByName("127.0.0.2");
        }

        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.clearUnsafe();
        StorageService.instance.setTokens(Collections.singleton(tmd.partitioner.getRandomToken()));
        tmd.updateNormalToken(tmd.partitioner.getMinimumToken(), REMOTE);
        assert tmd.isMember(REMOTE);
    }

    @Test
    public void testGetNeighborsPlusOne() throws Throwable
    {
        // generate rf+1 nodes, and ensure that all nodes are returned
        Set<InetAddress> expected = addTokens(1 + Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        expected.remove(FBUtilities.getBroadcastAddress());
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(KEYSPACE5);
        Set<InetAddress> neighbors = new HashSet<>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(ActiveRepairService.getNeighbors(KEYSPACE5, range, null, null));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsTimesTwo() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf*2 nodes, and ensure that only neighbors specified by the ARS are returned
        addTokens(2 * Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        AbstractReplicationStrategy ars = Keyspace.open(KEYSPACE5).getReplicationStrategy();
        Set<InetAddress> expected = new HashSet<>();
        for (Range<Token> replicaRange : ars.getAddressRanges().get(FBUtilities.getBroadcastAddress()))
        {
            expected.addAll(ars.getRangeAddresses(tmd.cloneOnlyTokenMap()).get(replicaRange));
        }
        expected.remove(FBUtilities.getBroadcastAddress());
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(KEYSPACE5);
        Set<InetAddress> neighbors = new HashSet<>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(ActiveRepairService.getNeighbors(KEYSPACE5, range, null, null));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsPlusOneInLocalDC() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf+1 nodes, and ensure that all nodes are returned
        Set<InetAddress> expected = addTokens(1 + Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        expected.remove(FBUtilities.getBroadcastAddress());
        // remove remote endpoints
        TokenMetadata.Topology topology = tmd.cloneOnlyTokenMap().getTopology();
        HashSet<InetAddress> localEndpoints = Sets.newHashSet(topology.getDatacenterEndpoints().get(DatabaseDescriptor.getLocalDataCenter()));
        expected = Sets.intersection(expected, localEndpoints);

        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(KEYSPACE5);
        Set<InetAddress> neighbors = new HashSet<>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(ActiveRepairService.getNeighbors(KEYSPACE5, range, Arrays.asList(DatabaseDescriptor.getLocalDataCenter()), null));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsTimesTwoInLocalDC() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf*2 nodes, and ensure that only neighbors specified by the ARS are returned
        addTokens(2 * Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        AbstractReplicationStrategy ars = Keyspace.open(KEYSPACE5).getReplicationStrategy();
        Set<InetAddress> expected = new HashSet<>();
        for (Range<Token> replicaRange : ars.getAddressRanges().get(FBUtilities.getBroadcastAddress()))
        {
            expected.addAll(ars.getRangeAddresses(tmd.cloneOnlyTokenMap()).get(replicaRange));
        }
        expected.remove(FBUtilities.getBroadcastAddress());
        // remove remote endpoints
        TokenMetadata.Topology topology = tmd.cloneOnlyTokenMap().getTopology();
        HashSet<InetAddress> localEndpoints = Sets.newHashSet(topology.getDatacenterEndpoints().get(DatabaseDescriptor.getLocalDataCenter()));
        expected = Sets.intersection(expected, localEndpoints);

        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(KEYSPACE5);
        Set<InetAddress> neighbors = new HashSet<>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(ActiveRepairService.getNeighbors(KEYSPACE5, range, Arrays.asList(DatabaseDescriptor.getLocalDataCenter()), null));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsTimesTwoInSpecifiedHosts() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf*2 nodes, and ensure that only neighbors specified by the hosts are returned
        addTokens(2 * Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        AbstractReplicationStrategy ars = Keyspace.open(KEYSPACE5).getReplicationStrategy();
        List<InetAddress> expected = new ArrayList<>();
        for (Range<Token> replicaRange : ars.getAddressRanges().get(FBUtilities.getBroadcastAddress()))
        {
            expected.addAll(ars.getRangeAddresses(tmd.cloneOnlyTokenMap()).get(replicaRange));
        }

        expected.remove(FBUtilities.getBroadcastAddress());
        Collection<String> hosts = Arrays.asList(FBUtilities.getBroadcastAddress().getCanonicalHostName(),expected.get(0).getCanonicalHostName());

       assertEquals(expected.get(0), ActiveRepairService.getNeighbors(KEYSPACE5,
               StorageService.instance.getLocalRanges(KEYSPACE5).iterator().next(),
               null, hosts).iterator().next());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNeighborsSpecifiedHostsWithNoLocalHost() throws Throwable
    {
        addTokens(2 * Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        //Dont give local endpoint
        Collection<String> hosts = Arrays.asList("127.0.0.3");
        ActiveRepairService.getNeighbors(KEYSPACE5, StorageService.instance.getLocalRanges(KEYSPACE5).iterator().next(), null, hosts);
    }

    Set<InetAddress> addTokens(int max) throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        Set<InetAddress> endpoints = new HashSet<>();
        for (int i = 1; i <= max; i++)
        {
            InetAddress endpoint = InetAddress.getByName("127.0.0." + i);
            tmd.updateNormalToken(tmd.partitioner.getRandomToken(), endpoint);
            endpoints.add(endpoint);
        }
        return endpoints;
    }
}
