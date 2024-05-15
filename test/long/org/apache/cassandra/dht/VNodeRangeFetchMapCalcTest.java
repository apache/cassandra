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

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.base.Predicates;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamOperation;

import static org.junit.Assert.assertTrue;

public class VNodeRangeFetchMapCalcTest
{
    public static InetAddressAndPort REPLACED;
    public static InetAddressAndPort REPLACEMENT;
    public static String KEYSPACE = "ks_vnode_range_fetch_map";
    public static Map<InetAddressAndPort, Host> hosts;
    static
    {
        try
        {
            REPLACED = InetAddressAndPort.getByName("127.99.99.99:7012");
            REPLACEMENT = InetAddressAndPort.getByName("127.0.0.1:7012");
            hosts = tokensFromFile();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void setupCluster() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        SchemaLoader.startGossiper();
        SchemaLoader.prepareServer();
        SchemaLoader.schemaDefinition("BootStrapperTest");
        RangeStreamer.ALIVE_PREDICATE = Predicates.alwaysTrue();
    }

    @Test
    public void testRangeFetchMapCalcFromLog() throws IOException
    {
        DatabaseDescriptor.setEndpointSnitch(snitch(hosts));
        IFailureDetector mockFailureDetector = new IFailureDetector()
        {
            public boolean isAlive(InetAddressAndPort ep)
            {
                return !ep.equals(REPLACED);
            }

            public void interpret(InetAddressAndPort ep) { throw new UnsupportedOperationException(); }
            public void report(InetAddressAndPort ep) { throw new UnsupportedOperationException(); }
            public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
            public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
            public void remove(InetAddressAndPort ep) { throw new UnsupportedOperationException(); }
            public void forceConviction(InetAddressAndPort ep) { throw new UnsupportedOperationException(); }
        };


        Set<String> dcs = hosts.values().stream().map(h -> h.dc).collect(Collectors.toSet());
        Object [] ntsParams = new Object[dcs.size() * 2];
        int i = 0;
        for (String dc : dcs)
        {
            ntsParams[i++] = dc;
            ntsParams[i++] = 3;
        }

        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();

        for (Map.Entry<InetAddressAndPort, Host> entry : hosts.entrySet())
        {
            tmd.updateHostId(UUID.randomUUID(), entry.getKey());
            tmd.updateNormalTokens(entry.getValue().tokens, entry.getKey());
        }
        Host replaced = hosts.get(REPLACED);
        hosts.put(REPLACEMENT, new Host(replaced.tokens, replaced.dc, replaced.rack));
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.nts(ntsParams),
                                    SchemaLoader.standardCFMD(KEYSPACE, "table"));
        AbstractReplicationStrategy strategy = Keyspace.open(KEYSPACE).getReplicationStrategy();
        tmd.addReplaceTokens(hosts.get(REPLACED).tokens, REPLACEMENT, REPLACED);
        PendingRangeCalculatorService.instance.update();

        tmd.updateHostId(UUID.randomUUID(), REPLACEMENT);

        RangeStreamer rangeStreamer = new RangeStreamer(tmd, hosts.get(REPLACED).tokens, REPLACEMENT, StreamOperation.BOOTSTRAP, false, snitch(hosts), new StreamStateStore(), mockFailureDetector, false, 1);

        rangeStreamer.addRanges(KEYSPACE, strategy.getPendingAddressRanges(tmd, replaced.tokens, REPLACEMENT));
        assertTrue(rangeStreamer.toFetch().get(KEYSPACE).size() > 190);
    }

    static IEndpointSnitch snitch(Map<InetAddressAndPort, Host> hosts)
    {
        return new AbstractNetworkTopologySnitch()
        {
            @Override
            public String getRack(InetAddressAndPort endpoint)
            {
                if (endpoint.equals(REPLACEMENT))
                    return hosts.get(REPLACED).rack;
                return hosts.get(endpoint).rack;
            }

            @Override
            public String getDatacenter(InetAddressAndPort endpoint)
            {
                if (endpoint.equals(REPLACEMENT))
                    return hosts.get(REPLACED).dc;
                return hosts.get(endpoint).dc;
            }
        };
    }

    private static Map<InetAddressAndPort, Host> tokensFromFile() throws IOException
    {
        List<String> lines = Files.readAllLines(Path.of("test/data/vnodecluster/hosts"));
        Map<InetAddressAndPort, Host> hosts = new HashMap<>();
        for (String line : lines)
        {
            String [] lineParts = line.split(";");
            hosts.put(InetAddressAndPort.getByName(lineParts[0].replace("/", "")), Host.fromFile(lineParts[1], lineParts[2], lineParts[3].split(",")));
        }
        return hosts;
    }

    private static class Host
    {
        final Set<Token> tokens;
        final String dc;
        final String rack;

        private Host(Set<Token> tokens, String dc, String rack)
        {
            this.tokens = tokens;
            this.dc = dc;
            this.rack = rack;
        }

        public static Host fromFile(String dc, String rack, String[] tokens)
        {
            Set<Token> ts = new HashSet<>();
            // reduce the number of tokens to 64 to make the test quicker
            for (int i = 0; i < tokens.length; i += 2)
                ts.add(Murmur3Partitioner.instance.getTokenFactory().fromString(tokens[i]));

            return new Host(ts, dc, rack);
        }
    }

}
