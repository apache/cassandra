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

package org.apache.cassandra.distributed.test.ring;

import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.log.FuzzTestBase;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.sequences.AddToCMS;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.tcm.transformations.cms.RemoveFromCMS;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.shared.ClusterUtils.getCMSMembers;

public class CMSMembershipTest extends FuzzTestBase
{
    @Test
    public void joinCmsTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3).start())
        {
            for (int idx : new int[]{ 2, 3 })
            {
                cluster.get(idx).runOnInstance(() -> {
                    AddToCMS.initiate();
                    ClusterMetadataService.instance().commit(CustomTransformation.make(idx));
                });
            }
        }
    }

    @Test
    public void expandCmsTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3).withConfig(c -> c.with(Feature.NETWORK)).start())
        {
            cluster.get(1).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                for (NodeId nodeId : metadata.directory.peerIds())
                {
                    if (nodeId.equals(metadata.myNodeId()))
                        continue;
                    AddToCMS.initiate(nodeId, metadata.directory.getNodeAddresses(nodeId).broadcastAddress);
                }
            });

            for (int idx : new int[]{ 1, 2, 3 })
            {
                cluster.get(idx).runOnInstance(() -> {
                    ClusterMetadataService.instance().commit(CustomTransformation.make(idx));
                });
            }

            for (int idx : new int[]{ 1, 2, 3 })
            {
                cluster.get(idx).runOnInstance(() -> {
                    ClusterMetadataService.instance().processor().fetchLogAndWait();
                    ClusterMetadata metadata = ClusterMetadata.current();
                    Assert.assertTrue(metadata.fullCMSMembers().contains(FBUtilities.getBroadcastAddressAndPort()));
                });
            }
        }
    }

    @Test
    public void shrinkCmsTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .appendConfig(c -> c.with(Feature.NETWORK)).start())
        {
            cluster.get(1).runOnInstance(() -> {
                try
                {
                    // When there's only one CMS node left, we should reject even `forced` transformatiosn
                    ClusterMetadataService.instance().commit(new RemoveFromCMS(FBUtilities.getBroadcastAddressAndPort(), true));
                }
                catch (IllegalStateException e)
                {
                    if (!e.getMessage().contains("would leave no members in CMS"))
                        throw new AssertionError(e.getMessage());
                }
            });

            cluster.get(1).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                for (NodeId nodeId : metadata.directory.peerIds())
                {
                    if (nodeId.equals(metadata.myNodeId()))
                        continue;
                    AddToCMS.initiate(nodeId, metadata.directory.getNodeAddresses(nodeId).broadcastAddress);
                }
            });

            Set<String> initialCMS = getCMSMembers(cluster.get(1));
            Assert.assertEquals(3, initialCMS.size());
            for (int i=2; i<=3; i++)
            {
                cluster.get(i).runOnInstance(() -> ClusterMetadataService.instance().processor().fetchLogAndWait());
                Assert.assertEquals(initialCMS, getCMSMembers(cluster.get(i)));
            }

            // Without the force option set, removing a node from the CMS should
            // be rejected if it would cause the size to go below 3 nodes
            cluster.get(1).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                for (InetAddressAndPort addr : metadata.directory.allAddresses())
                {
                    if (addr.toString().contains("127.0.0.3"))
                    {
                        try
                        {
                            ClusterMetadataService.instance().commit(new RemoveFromCMS(addr, false));
                        }
                        catch (IllegalStateException e)
                        {
                            if (!e.getMessage().contains("resubmit with force=true"))
                                throw new AssertionError(e.getMessage());
                        }
                        return;
                    }
                }
            });

            // expect no changes to the CMS membership yet
            for (int i=1; i<=3; i++)
            {
                cluster.get(i).runOnInstance(() -> ClusterMetadataService.instance().processor().fetchLogAndWait());
                Assert.assertEquals(initialCMS, getCMSMembers(cluster.get(i)));
            }

            // Run again, this time with the force option
            cluster.get(1).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                for (InetAddressAndPort addr : metadata.directory.allAddresses())
                {
                    if (addr.toString().contains("127.0.0.3"))
                    {
                        ClusterMetadataService.instance().commit(new RemoveFromCMS(addr, true));
                        return;
                    }
                }
            });

            // node3 should have been removed from the CMS
            Set<String> updatedCMS = initialCMS.stream().filter(s -> !s.contains("127.0.0.3")).collect(Collectors.toSet());
            for (int i=1; i<=3; i++)
            {
                cluster.get(i).runOnInstance(() -> ClusterMetadataService.instance().processor().fetchLogAndWait());
                Assert.assertEquals(updatedCMS, getCMSMembers(cluster.get(i)));
            }

            // Commit some transformations as a smoke test
            for (int idx : new int[]{ 1, 2, 3 })
            {
                cluster.get(idx).runOnInstance(() -> {
                    ClusterMetadataService.instance().commit(CustomTransformation.make(idx));
                });
            }
        }
    }
}
