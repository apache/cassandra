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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.locator.Replicas;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

public class GetEndpointsTest extends TestBaseImpl
{
    @Test
    public void testGetEndpointsForLocalTable() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .start()))
        {
            for (IInvokableInstance i : cluster)
            {
                i.runOnInstance(() -> {
                    List<String> endpoints = StorageService.instance.getNaturalEndpointsWithPort("system", "compaction_history", "7d431310-43c9-11ef-bd50-53ff742309a9");
                    assertEquals(1, endpoints.size());
                    assertEquals(FBUtilities.getBroadcastAddressAndPort().getHostAddressAndPort(), endpoints.get(0));
                });
            }
        }
    }

    @Test
    public void testGetEndpointsForMetadataTables() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.with(Feature.NETWORK))
                                           .start()))
        {
            for (IInvokableInstance i : cluster)
            {
                i.runOnInstance(() -> {
                    List<String> endpoints = StorageService.instance.getNaturalEndpointsWithPort("system", "local_metadata_log", "1");
                    assertEquals(1, endpoints.size());
                    assertEquals(FBUtilities.getBroadcastAddressAndPort().getHostAddressAndPort(), endpoints.get(0));
                });

                i.runOnInstance(() -> {
                    List<String> endpoints = StorageService.instance.getNaturalEndpointsWithPort("system_cluster_metadata", "distributed_metadata_log", "1");
                    assertEquals(1, endpoints.size());
                    assertEquals(Replicas.stringify(ClusterMetadata.current().fullCMSMembersAsReplicas(), true), endpoints);
                });
            }
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            for (IInvokableInstance i : cluster)
            {
                i.runOnInstance(() -> {
                    List<String> endpoints = StorageService.instance.getNaturalEndpointsWithPort("system_cluster_metadata", "distributed_metadata_log", "1");
                    assertEquals(endpoints.toString(), 3, endpoints.size());
                    assertEquals(Replicas.stringify(ClusterMetadata.current().fullCMSMembersAsReplicas(), true), endpoints);
                });

            }
        }
    }
}
