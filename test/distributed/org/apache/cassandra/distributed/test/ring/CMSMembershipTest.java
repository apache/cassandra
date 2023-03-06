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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.test.log.FuzzTestBase;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.sequences.AddToCMS;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.tcm.transformations.cms.RemoveFromCMS;
import org.apache.cassandra.utils.FBUtilities;

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
        try (Cluster cluster = builder().withNodes(3).start())
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
                    ClusterMetadataService.instance().replayAndWait();
                    ClusterMetadata metadata = ClusterMetadata.current();
                    Assert.assertTrue(metadata.fullCMSMembers().contains(FBUtilities.getBroadcastAddressAndPort()));
                });
            }
        }
    }

    @Test
    public void shrinkCmsTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3).start())
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

            IMessageFilters.Filter filter = cluster.filters().allVerbs().to(3).drop();

            cluster.get(1).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                for (InetAddressAndPort addr : metadata.directory.allAddresses())
                {
                    if (addr.toString().contains("127.0.0.3"))
                    {
                        ClusterMetadataService.instance().commit(new RemoveFromCMS(addr));
                        return;
                    }

                }
            });

            filter.off();

            for (int idx : new int[]{ 1, 2, 3 })
            {
                cluster.get(idx).runOnInstance(() -> {
                    ClusterMetadataService.instance().commit(CustomTransformation.make(idx));
                });
            }

            for (int idx : new int[]{ 1, 2, 3 })
            {
                cluster.get(idx).runOnInstance(() -> {
                    ClusterMetadataService.instance().replayAndWait();
                    ClusterMetadata metadata = ClusterMetadata.current();
                    Assert.assertEquals(idx != 3, metadata.fullCMSMembers().contains(FBUtilities.getBroadcastAddressAndPort()));
                });
            }
        }
    }
}
