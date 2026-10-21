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

import java.util.Random;
import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.transformations.Startup;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.fail;

public class CommitStartupByNonCMSNodeTest extends FuzzTestBase
{
    @Test
    public void commitStartupByNonCMSNode() throws Throwable
    {
        try (Cluster cluster = Cluster.build(4)
                                      .withConfig(conf -> conf.set("request_timeout", "1000ms") // for TCM commit
                                                              .set("cms_retry_delay", "10ms,retries=1") // avoid retries by CMS node to avoid triggering log fetch
                                                              .set("write_request_timeout", "100ms") // time out paxos writes quickly
                                                              .with(Feature.NETWORK, Feature.GOSSIP))
                                      .start())
        {
            cluster.setUncaughtExceptionsFilter(t -> t.getMessage() != null && t.getMessage().contains("There are not enough nodes in dc0 datacenter to satisfy replication factor"));
            Random rnd = new Random(2);
            Supplier<Integer> nodeSelector = () -> rnd.nextInt(cluster.size() - 1) + 1;
            cluster.get(nodeSelector.get()).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            for (int i = 2; i <= 3; i++)
                ClusterUtils.stopUnchecked(cluster.get(i));

            Thread startNode2 = new Thread(() -> {
                cluster.get(2).startup();
            });

            Thread commitStartupTranformation = new Thread(() -> {
                cluster.get(4).runOnInstance(() -> {
                    try
                    {
                        ClusterMetadataService.instance().commit(new Startup(ClusterMetadata.current().myNodeId(),
                                                                             new NodeAddresses(FBUtilities.getBroadcastAddressAndPort()),
                                                                             NodeVersion.CURRENT));
                    }
                    catch (Throwable t)
                    {
                        fail("Should not happen");
                    }
                });
            });
            commitStartupTranformation.start();
            Thread.sleep(10_000);
            startNode2.start();

            commitStartupTranformation.join();
            startNode2.join();
        }
    }
}
