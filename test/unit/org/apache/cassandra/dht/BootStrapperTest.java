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
package org.apache.cassandra.dht;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Set;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.OperationType;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.*;

public class BootStrapperTest extends SchemaLoader
{
    @Test
    public void testTokenRoundtrip() throws Exception
    {
        StorageService.instance.initServer(0);
        // fetch a bootstrap token from the local node
        assert BootStrapper.getBootstrapTokenFrom(FBUtilities.getBroadcastAddress()) != null;
    }

    @Test
    public void testMulitipleAutomaticBootstraps() throws IOException
    {
        StorageService ss = StorageService.instance;
        generateFakeEndpoints(5);
        InetAddress[] addrs = new InetAddress[]
        {
            InetAddress.getByName("127.0.0.2"),
            InetAddress.getByName("127.0.0.3"),
            InetAddress.getByName("127.0.0.4"),
            InetAddress.getByName("127.0.0.5"),
        };
        InetAddress[] bootstrapAddrs = new InetAddress[]
        {
            InetAddress.getByName("127.0.0.12"),
            InetAddress.getByName("127.0.0.13"),
            InetAddress.getByName("127.0.0.14"),
            InetAddress.getByName("127.0.0.15"),
        };
        UUID[] bootstrapHostIds = new UUID[]
        {
            UUID.randomUUID(),
            UUID.randomUUID(),
            UUID.randomUUID(),
            UUID.randomUUID(),
        };
        Map<InetAddress, Double> load = new HashMap<InetAddress, Double>();
        for (int i = 0; i < addrs.length; i++)
        {
            Gossiper.instance.initializeNodeUnsafe(addrs[i], 1);
            load.put(addrs[i], (double)i+2);
            // also make bootstrapping nodes present in gossip
            Gossiper.instance.initializeNodeUnsafe(bootstrapAddrs[i], 1);
        }

        // give every node a bootstrap source.
        for (int i = 3; i >=0; i--)
        {
            InetAddress bootstrapSource = BootStrapper.getBootstrapSource(ss.getTokenMetadata(), load);
            assert bootstrapSource != null;
            assert bootstrapSource.equals(addrs[i]) : String.format("expected %s but got %s for %d", addrs[i], bootstrapSource, i);
            assert !ss.getTokenMetadata().getBootstrapTokens().containsValue(bootstrapSource);

            Range<Token> range = ss.getPrimaryRangeForEndpoint(bootstrapSource);
            Token token = StorageService.getPartitioner().midpoint(range.left, range.right);
            assert range.contains(token);
            ss.onChange(bootstrapAddrs[i],
                        ApplicationState.STATUS,
                        StorageService.instance.valueFactory.bootstrapping(Collections.<Token>singleton(token), bootstrapHostIds[i]));
        }

        // any further attempt to bootsrtap should fail since every node in the cluster is splitting.
        try
        {
            BootStrapper.getBootstrapSource(ss.getTokenMetadata(), load);
            throw new AssertionError("This bootstrap should have failed.");
        }
        catch (RuntimeException ex)
        {
            // success!
        }

        // indicate that one of the nodes is done. see if the node it was bootstrapping from is still available.
        Range<Token> range = ss.getPrimaryRangeForEndpoint(addrs[2]);
        Token token = StorageService.getPartitioner().midpoint(range.left, range.right);
        ss.onChange(bootstrapAddrs[2],
                    ApplicationState.STATUS,
                    StorageService.instance.valueFactory.normal(Collections.singleton(token), bootstrapHostIds[2]));
        load.put(bootstrapAddrs[2], 0d);
        InetAddress addr = BootStrapper.getBootstrapSource(ss.getTokenMetadata(), load);
        assert addr != null && addr.equals(addrs[2]);
    }

    @Test
    public void testGuessToken() throws IOException
    {
        StorageService ss = StorageService.instance;

        generateFakeEndpoints(5);

        InetAddress two = InetAddress.getByName("127.0.0.2");
        InetAddress three = InetAddress.getByName("127.0.0.3");
        InetAddress four = InetAddress.getByName("127.0.0.4");
        InetAddress five = InetAddress.getByName("127.0.0.5");

        Map<InetAddress, Double> load = new HashMap<InetAddress, Double>();
        load.put(two, 2.0);
        load.put(three, 3.0);
        load.put(four, 4.0);
        load.put(five, 5.0);

        TokenMetadata tmd = ss.getTokenMetadata();
        InetAddress source = BootStrapper.getBootstrapSource(tmd, load);
        assert five.equals(source) : five + " != " + source;

        InetAddress myEndpoint = InetAddress.getByName("127.0.0.1");
        Range<Token> range5 = ss.getPrimaryRangeForEndpoint(five);
        Token fakeToken = StorageService.getPartitioner().midpoint(range5.left, range5.right);
        assert range5.contains(fakeToken);
        ss.onChange(myEndpoint,
                    ApplicationState.STATUS,
                    StorageService.instance.valueFactory.bootstrapping(Collections.<Token>singleton(fakeToken), UUID.randomUUID()));
        tmd = ss.getTokenMetadata();

        InetAddress source4 = BootStrapper.getBootstrapSource(tmd, load);
        assert four.equals(source4) : four + " != " + source4;
    }

    @Test
    public void testSourceTargetComputation() throws UnknownHostException
    {
        final int[] clusterSizes = new int[] { 1, 3, 5, 10, 100};
        for (String table : Schema.instance.getNonSystemTables())
        {
            int replicationFactor = Table.open(table).getReplicationStrategy().getReplicationFactor();
            for (int clusterSize : clusterSizes)
                if (clusterSize >= replicationFactor)
                    testSourceTargetComputation(table, clusterSize, replicationFactor);
        }
    }

    private RangeStreamer testSourceTargetComputation(String table, int numOldNodes, int replicationFactor) throws UnknownHostException
    {
        StorageService ss = StorageService.instance;

        generateFakeEndpoints(numOldNodes);
        Token myToken = StorageService.getPartitioner().getRandomToken();
        InetAddress myEndpoint = InetAddress.getByName("127.0.0.1");

        TokenMetadata tmd = ss.getTokenMetadata();
        assertEquals(numOldNodes, tmd.sortedTokens().size());
        RangeStreamer s = new RangeStreamer(tmd, myEndpoint, OperationType.BOOTSTRAP);
        IFailureDetector mockFailureDetector = new IFailureDetector()
        {
            public boolean isAlive(InetAddress ep)
            {
                return true;
            }

            public void interpret(InetAddress ep) { throw new UnsupportedOperationException(); }
            public void report(InetAddress ep) { throw new UnsupportedOperationException(); }
            public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
            public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
            public void remove(InetAddress ep) { throw new UnsupportedOperationException(); }
            public void forceConviction(InetAddress ep) { throw new UnsupportedOperationException(); }
            public void clear(InetAddress ep) { throw new UnsupportedOperationException(); }
        };
        s.addSourceFilter(new RangeStreamer.FailureDetectorSourceFilter(mockFailureDetector));
        s.addRanges(table, Table.open(table).getReplicationStrategy().getPendingAddressRanges(tmd, myToken, myEndpoint));

        Collection<Map.Entry<InetAddress, Collection<Range<Token>>>> toFetch = s.toFetch().get(table);

        // Check we get get RF new ranges in total
        Set<Range<Token>> ranges = new HashSet<Range<Token>>();
        for (Map.Entry<InetAddress, Collection<Range<Token>>> e : toFetch)
            ranges.addAll(e.getValue());

        assertEquals(replicationFactor, ranges.size());

        // there isn't any point in testing the size of these collections for any specific size.  When a random partitioner
        // is used, they will vary.
        assert toFetch.iterator().next().getValue().size() > 0;
        assert !toFetch.iterator().next().getKey().equals(myEndpoint);
        return s;
    }

    @Test
    public void testException() throws UnknownHostException
    {
        String table = Schema.instance.getNonSystemTables().iterator().next();
        int replicationFactor = Table.open(table).getReplicationStrategy().getReplicationFactor();
        RangeStreamer streamer = testSourceTargetComputation(table, replicationFactor, replicationFactor);
        streamer.latch = new CountDownLatch(4);
        streamer.convict(streamer.toFetch().get(table).iterator().next().getKey(), Double.MAX_VALUE);
        assertNotNull("Exception message not set, test failed", streamer.exceptionMessage);
    }

    private void generateFakeEndpoints(int numOldNodes) throws UnknownHostException
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner<?> p = StorageService.getPartitioner();

        for (int i = 1; i <= numOldNodes; i++)
        {
            // leave .1 for myEndpoint
            tmd.updateNormalToken(p.getRandomToken(), InetAddress.getByName("127.0.0." + (i + 1)));
        }
    }
}
