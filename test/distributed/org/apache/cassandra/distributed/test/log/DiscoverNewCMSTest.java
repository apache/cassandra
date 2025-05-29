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

package org.apache.cassandra.distributed.test.log;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;

import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DiscoverNewCMSTest extends TestBaseImpl
{

    @Before
    public void disableAccord()
    {
        CassandraRelevantProperties.DTEST_ACCORD_ENABLED.setBoolean(false);
    }

    @Test
    public void singleNodeCMSAddressChangeTest() throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .createWithoutStarting())
        {
            test(cluster, 1);
        }
    }

    @Test
    public void multiNodeCMSOnlyClusterAddressChangeTest() throws IOException, ExecutionException, InterruptedException
    {

        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .createWithoutStarting())
        {
            test(cluster, 3);
        }
    }

    @Test
    public void multiNodeCMSAllAddressesChangeTest() throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = builder().withNodes(6)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .createWithoutStarting())
        {
            test(cluster, 3);
        }
    }

    private void test(Cluster cluster, int cmsSize) throws IOException, ExecutionException, InterruptedException
    {
        ExecutorService executor = Executors.newFixedThreadPool(cluster.size());
        cluster.setUncaughtExceptionsFilter((node, t) -> {
            Throwable rootCause = Throwables.getRootCause(t);
            // Some fetchCMSLog operations might temporarily fail and be retried during address changes
            return rootCause.getMessage() != null
                   && rootCause.getMessage().startsWith("Cannot achieve consistency level SERIAL");

        });
        cluster.startup();
        init(cluster);
        IInvokableInstance n1 = cluster.get(1);
        if (cmsSize > 1)
            n1.nodetoolResult("cms", "reconfigure", "" + cmsSize).asserts().success();
        ClusterUtils.waitForCMSToQuiesce(cluster, n1);

        // Set up the expectations for what address changes are going to happen
        Map<InetSocketAddress, Pair<NodeId, InetSocketAddress>> addressMapping = new HashMap<>(cluster.size());
        for (IInvokableInstance inst : cluster)
        {
            InetSocketAddress starting = inst.config().broadcastAddress();
            InetSocketAddress expected = new InetSocketAddress(bumpAddress(starting.getAddress()), starting.getPort());
            NodeId id = ClusterUtils.getNodeId(inst);
            addressMapping.put(starting, Pair.create(id, expected));
        }

        // Check the CMS membership at the start of the test & predict what it should be at the end
        Set<InetSocketAddress> startingCMS = new HashSet<>(cmsSize);
        Set<InetSocketAddress> expectedCMS = new HashSet<>(cmsSize);
        for (InetSocketAddress s : ClusterUtils.getCMSMemberAddresses(n1))
        {
            startingCMS.add(s);
            expectedCMS.add(addressMapping.get(s).right);
        }
        Set<NodeId> cmsNodes = ClusterUtils.getCMSMemberIds(n1);

        // Shut down all nodes, modify each one's broadcast address and reconfigure seeds as these
        // will be used to rediscover peers. Seed config does not need to be uniform across the cluster
        // but there must be enough intersection to enable the CMS members to rediscover each other
        for (int i = 1; i <= cluster.size(); i++)
        {
            IInvokableInstance inst = cluster.get(i);
            inst.shutdown().get();
            InetAddress newBroadcastAddress = addressMapping.get(inst.config().broadcastAddress()).right.getAddress();
            byte[] bytes = newBroadcastAddress.getAddress();
            ClusterUtils.updateAddress(inst, addrString(bytes));
            String seed1 = addrString(bytes[0], bytes[1], bytes[2], (byte) i);
            String seed2 = addrString(bytes[0], bytes[1], bytes[2], (byte) ((i < cluster.size()) ? i + 1 : 1));
            ClusterUtils.updateSeed(inst, seed1, seed2);
        }

        // Start everything up and wait for state to cluster state to quiesce
        List<Future<?>> startups = new ArrayList<>(cluster.size());
        for (IInvokableInstance inst : cluster)
        {
            Future<Boolean> f = executor.submit(() -> {
                inst.startup();
                return true;
            });
            startups.add(f);
        }

        FBUtilities.waitOnFutures(startups, 60, TimeUnit.SECONDS);
        ClusterUtils.waitForCMSToQuiesce(cluster, n1);

        // wait until each node's STARTUP transformation has been enacted by all nodes
        Awaitility.waitAtMost(30, TimeUnit.SECONDS).until(() -> allAddressChangesEnacted(cluster));
        Epoch afterAllAddressChanges = getEpochAfterAllAddressChanges(cluster);
        ClusterUtils.waitForCMSToQuiesce(cluster, afterAllAddressChanges, true);

        // Assert that:
        // * The membership of the CMS (i.e. which node ids) remains the same
        // * The set of CMS addresses  matches the prediction made at the start
        // * Every node has successfully changed its address. The previous check
        //   is a logical consequence of this, but it doesn't hurt to verify both
        for (IInvokableInstance inst : cluster)
        {
            assertEquals(cmsNodes, ClusterUtils.getCMSMemberIds(inst));
            Set<InetSocketAddress> finalCMS = ClusterUtils.getCMSMemberAddresses(inst);
            assertEquals(startingCMS.size(), finalCMS.size());
            assertEquals(expectedCMS.size(), finalCMS.size());
            assertTrue(expectedCMS.containsAll(finalCMS));
            for (Pair<NodeId, InetSocketAddress> peer : addressMapping.values())
            {
                InetSocketAddress fromInst = ClusterUtils.getEndpoint(inst, peer.left);
                assertEquals("Check failed on instance " + inst.config().num(), peer.right, fromInst);
            }
        }
    }

    private boolean allAddressChangesEnacted(Cluster cluster)
    {
        return getEpochAfterAllAddressChanges(cluster).isAfter(Epoch.FIRST);
    }

    private Epoch getEpochAfterAllAddressChanges(Cluster cluster)
    {
        int nodes = cluster.size();
        long epochAfterAllStartups = cluster.get(1).callOnInstance(() -> {
            UntypedResultSet rs = QueryProcessor.executeInternal("SELECT epoch, kind from system_views.cluster_metadata_log where kind = 'STARTUP'");
            if (rs == null || rs.isEmpty() || rs.size() < nodes)
                return -1;

            long epoch = rs.stream().mapToLong(r -> r.getLong("epoch")).max().orElse(-1);
            return epoch;
        }).longValue();
        return Epoch.create(epochAfterAllStartups);
    }

    private InetAddress bumpAddress(InetAddress address) throws UnknownHostException
    {
        // ipv4 addresses for this test
        assert address.getAddress().length == 4;
        byte[] bytes = address.getAddress();
        bytes[2]++;
        return InetAddress.getByAddress(bytes);
    }

    private String addrString(byte...b) throws UnknownHostException
    {
        assert b.length == 4;
        return InetAddress.getByAddress(new byte[] { b[0], b[1], b[2], b[3] }).getHostAddress();
    }

}
