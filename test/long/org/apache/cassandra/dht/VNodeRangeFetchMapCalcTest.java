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
import java.util.stream.Collectors;

import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.IEndpointSnitch;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.MovementMap;
import org.apache.cassandra.tcm.ownership.PlacementProvider;
import org.apache.cassandra.tcm.ownership.PlacementTransitionPlan;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.sequences.BootstrapAndReplace;
import org.apache.cassandra.tcm.transformations.UnsafeJoin;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertTrue;

public class VNodeRangeFetchMapCalcTest
{
    public static InetAddressAndPort REPLACED = InetAddressAndPort.getByNameUnchecked("127.99.99.99:7012");
    public static InetAddressAndPort REPLACEMENT = InetAddressAndPort.getByNameUnchecked("127.0.0.1:7012");
    public static String KEYSPACE = "ks_vnode_range_fetch_map";

    @BeforeClass
    public static void setupCluster() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ClusterMetadataService.setInstance(ClusterMetadataTestHelper.syncInstanceForTest());
        ClusterMetadataService.instance().log().bootstrap(FBUtilities.getBroadcastAddressAndPort());
    }

    @Test
    public void testRangeFetchMapCalcFromLog() throws IOException
    {
        IFailureDetector mockFailureDetector = new IFailureDetector()
        {
            public boolean isAlive(InetAddressAndPort ep)
            {
                return true;
            }

            public void interpret(InetAddressAndPort ep) { throw new UnsupportedOperationException(); }
            public void report(InetAddressAndPort ep) { throw new UnsupportedOperationException(); }
            public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
            public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener) { throw new UnsupportedOperationException(); }
            public void remove(InetAddressAndPort ep) { throw new UnsupportedOperationException(); }
            public void forceConviction(InetAddressAndPort ep) { throw new UnsupportedOperationException(); }
        };

        Map<InetAddressAndPort, Host> hosts = tokensFromFile();
        Set<String> dcs = hosts.values().stream().map(h -> h.dc).collect(Collectors.toSet());
        Object [] ntsParams = new Object[dcs.size() * 2];
        int i = 0;
        for (String dc : dcs)
        {
            ntsParams[i++] = dc;
            ntsParams[i++] = 3;
        }
        ClusterMetadataTestHelper.addOrUpdateKeyspace(KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.nts(ntsParams)));
        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId beingReplaced = metadata.directory.peerId(REPLACED);
        Location replacedLoc = metadata.directory.location(beingReplaced);
        NodeId replacement = ClusterMetadataTestHelper.register(REPLACEMENT, replacedLoc.datacenter, replacedLoc.rack);
        hosts = new HashMap<>(hosts);
        hosts.put(REPLACEMENT, new Host(hosts.get(REPLACED).tokens, replacedLoc.datacenter, replacedLoc.rack));
        metadata = ClusterMetadata.current();
        PlacementProvider placementProvider = new UniformRangePlacement();
        PlacementTransitionPlan ptp = placementProvider.planForReplacement(metadata, beingReplaced, replacement, metadata.schema.getKeyspaces());
        MovementMap movementMap = BootstrapAndReplace.movementMap(metadata.directory.endpoint(beingReplaced), ptp.addToWrites());
        RangeStreamer rangeStreamer = new RangeStreamer(metadata, StreamOperation.BOOTSTRAP, false, snitch(hosts), new StreamStateStore(), mockFailureDetector, false, 1, movementMap, null);
        rangeStreamer.addKeyspaceToFetch(KEYSPACE);
        assertTrue(rangeStreamer.toFetch().get(KEYSPACE).size() > 190);
    }

    static IEndpointSnitch snitch(Map<InetAddressAndPort, Host> hosts)
    {
        return new AbstractNetworkTopologySnitch()
        {
            @Override
            public String getRack(InetAddressAndPort endpoint)
            {
                return hosts.get(endpoint).rack;
            }

            @Override
            public String getDatacenter(InetAddressAndPort endpoint)
            {
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
            hosts.put(InetAddressAndPort.getByNameUnchecked(lineParts[0].replace("/", "")), Host.fromFile(lineParts[1], lineParts[2], lineParts[3].split(",")));
        }
        PlacementProvider placementProvider = new UniformRangePlacement();
        for (Map.Entry<InetAddressAndPort, Host> entry : hosts.entrySet())
        {
            InetAddressAndPort ep = entry.getKey();
            Host host = entry.getValue();
            NodeId nodeId = ClusterMetadataTestHelper.register(ep, host.dc, host.rack);
            ClusterMetadataService.instance().commit(new UnsafeJoin(nodeId, host.tokens, placementProvider));
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
