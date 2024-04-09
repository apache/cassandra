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
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.distributed.Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN;
import static org.apache.cassandra.distributed.Constants.KEY_DTEST_FULL_STARTUP;
import static org.apache.cassandra.distributed.api.TokenSupplier.evenlyDistributedTokens;
import static org.apache.cassandra.distributed.shared.ClusterUtils.addInstance;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getCMSMembers;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

// Tests for operations such as remove, decomission, and replace triggering CMS reconfiguration
public class TriggeredReconfigureCMSTest extends FuzzTestBase
{
    @Test
    public void testRemoveCMSMember() throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = Cluster.build(5)
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(5, "dc0", "rack0"))
                                      .withConfig(conf -> conf.set("hinted_handoff_enabled", "false")
                                                              .with(Feature.NETWORK, Feature.GOSSIP))
                                      .start())
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            Set<String> cms = getCMSMembers(cluster.get(1));
            assertEquals(3, cms.size());

            String instanceToRemove =  cms.stream().filter(addr -> !addr.contains("/127.0.0.1")).findFirst().get();
            IInvokableInstance nodeToRemove = cluster.stream().filter(i -> i.config().broadcastAddress().toString().contains(instanceToRemove)).findFirst().get();
            String nodeId = nodeToRemove.callOnInstance(() -> ClusterMetadata.current().myNodeId().toUUID().toString());
            nodeToRemove.shutdown().get();

            cluster.get(1).runOnInstance(() -> {
                try
                {
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                    while (FailureDetector.instance.isAlive(InetAddressAndPort.getByName(instanceToRemove.replace("/", ""))) &&
                           System.nanoTime() < deadline)
                        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
            });
            cluster.get(1).nodetoolResult("removenode", nodeId, "--force").asserts().success();

            cms = getCMSMembers(cluster.get(1));
            assertEquals(3, cms.size());
            assertTrue(cms.contains("/127.0.0.1"));
            assertFalse(cms.contains(instanceToRemove));
        }
    }

    @Test
    public void testDecommissionCMSMember() throws IOException
    {
        try (Cluster cluster = Cluster.build(5)
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(5, "dc0", "rack0"))
                                      .withConfig(conf -> conf.set("hinted_handoff_enabled", "false")
                                                              .with(Feature.NETWORK, Feature.GOSSIP))
                                      .start())
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            Set<String> cms = getCMSMembers(cluster.get(1));
            assertEquals(3, cms.size());
            cluster.get(1).nodetoolResult("decommission").asserts().success();
            cms = getCMSMembers(cluster.get(1));
            assertEquals(3, cms.size());
        }
    }

    @Test
    public void testReplaceCMSMember() throws IOException
    {
        try (Cluster cluster = Cluster.build(5)
                                      .withTokenSupplier(evenlyDistributedTokens(6, 1))
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(5, "dc0", "rack0"))
                                      .withConfig(conf -> conf.set("hinted_handoff_enabled", "false")
                                                              .with(Feature.NETWORK, Feature.GOSSIP))
                                      .start())
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            Set<String> cms = getCMSMembers(cluster.get(1));
            assertEquals(3, cms.size());
            assertTrue(cms.contains("/127.0.0.1"));

            IInvokableInstance toReplace = cluster.get(2);
            Collection<String> replacedTokens = ClusterUtils.getLocalTokens(toReplace);
            toReplace.shutdown(false);
            IInvokableInstance replacement = addInstance(cluster,
                                                         toReplace.config(),
                                                         c -> c.set("auto_bootstrap", true)
                                                               .set(KEY_DTEST_FULL_STARTUP, false)
                                                               .set(KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false));
            try (WithProperties replacementProps = new WithProperties())
            {
                replacementProps.set(CassandraRelevantProperties.REPLACE_ADDRESS_FIRST_BOOT,
                                     toReplace.config().broadcastAddress().getAddress().getHostAddress());
                replacement.startup();
            }

            Collection<String> replacementTokens = ClusterUtils.getLocalTokens(replacement);
            assertEquals(replacedTokens, replacementTokens);

            cms = getCMSMembers(cluster.get(1));
            assertEquals(3, cms.size());
            assertTrue(cms.contains("/127.0.0.1"));
            assertFalse(cms.contains("/127.0.0.2"));
        }
    }
}
