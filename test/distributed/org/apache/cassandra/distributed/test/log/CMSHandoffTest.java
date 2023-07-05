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
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.distributed.Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN;
import static org.apache.cassandra.distributed.Constants.KEY_DTEST_FULL_STARTUP;
import static org.apache.cassandra.distributed.api.TokenSupplier.evenlyDistributedTokens;
import static org.apache.cassandra.distributed.shared.ClusterUtils.addInstance;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getCMSMembers;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CMSHandoffTest extends FuzzTestBase
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
            cluster.get(2).nodetoolResult("addtocms").asserts().success();
            cluster.get(3).nodetoolResult("addtocms").asserts().success();
            Set<String> cms = getCMSMembers(cluster.get(1));
            assertEquals(3, cms.size());
            assertTrue(cms.contains("/127.0.0.1"));
            assertTrue(cms.contains("/127.0.0.2"));
            assertTrue(cms.contains("/127.0.0.3"));

            String nodeId = cluster.get(2).callOnInstance(() -> ClusterMetadata.current().myNodeId().toUUID().toString());
            cluster.get(2).shutdown().get();
            cluster.get(1).nodetoolResult("removenode", nodeId, "--force").asserts().success();

            cms = getCMSMembers(cluster.get(1));
            assertEquals(3, cms.size());
            assertTrue(cms.contains("/127.0.0.1"));
            assertFalse(cms.contains("/127.0.0.2"));
            assertTrue(cms.contains("/127.0.0.3"));
            assertTrue(cms.contains("/127.0.0.4") ^ cms.contains("/127.0.0.5"));
        }
    }

    @Test
    public void testDecommissionCMSMember() throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = Cluster.build(5)
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(5, "dc0", "rack0"))
                                      .withConfig(conf -> conf.set("hinted_handoff_enabled", "false")
                                                              .with(Feature.NETWORK, Feature.GOSSIP))
                                      .start())
        {
            cluster.get(2).nodetoolResult("addtocms").asserts().success();
            cluster.get(3).nodetoolResult("addtocms").asserts().success();
            Set<String> cms = getCMSMembers(cluster.get(1));
            assertEquals(3, cms.size());
            assertTrue(cms.contains("/127.0.0.1"));
            assertTrue(cms.contains("/127.0.0.2"));
            assertTrue(cms.contains("/127.0.0.3"));

            cluster.get(2).nodetoolResult("decommission").asserts().success();
            cms = getCMSMembers(cluster.get(1));
            assertEquals(3, cms.size());
            assertTrue(cms.contains("/127.0.0.1"));
            assertFalse(cms.contains("/127.0.0.2"));
            assertTrue(cms.contains("/127.0.0.3"));
            assertTrue(cms.contains("/127.0.0.4") ^ cms.contains("/127.0.0.5"));
        }
    }

    @Test
    public void testReplaceCMSMember() throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = Cluster.build(5)
                                      .withTokenSupplier(evenlyDistributedTokens(6, 1))
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(5, "dc0", "rack0"))
                                      .withConfig(conf -> conf.set("hinted_handoff_enabled", "false")
                                                              .with(Feature.NETWORK, Feature.GOSSIP))
                                      .start())
        {
            cluster.get(2).nodetoolResult("addtocms").asserts().success();
            cluster.get(3).nodetoolResult("addtocms").asserts().success();
            Set<String> cms = getCMSMembers(cluster.get(1));
            assertEquals(3, cms.size());
            assertTrue(cms.contains("/127.0.0.1"));
            assertTrue(cms.contains("/127.0.0.2"));
            assertTrue(cms.contains("/127.0.0.3"));

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
            assertTrue(cms.contains("/127.0.0.3"));
            assertTrue(cms.contains("/127.0.0.4") ^ cms.contains("/127.0.0.5"));
        }
    }
}
