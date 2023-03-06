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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.transformations.CustomTransformation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RequestCurrentEpochTest extends FuzzTestBase
{
    @Test
    public void testRequestingPeerWatermarks() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3).start())
        {
            init(cluster);
            IInvokableInstance cmsNode = cluster.get(1);
            ClusterUtils.waitForCMSToQuiesce(cluster, cmsNode);
            assertEpochs(cluster);
            cluster.schemaChange(withKeyspace("create table %s.t1 (id int primary key, x int)"));
            ClusterUtils.waitForCMSToQuiesce(cluster, cmsNode);
            assertEpochs(cluster);
            // This is very bad and wouldn't possible on a real node, but we just
            // want to affect the epoch on one node so we can assert that it reports
            // it properly when asked. A better way would be to block replication and
            // replay messages to that node, but that's a bit more work.
            IInvokableInstance inst = cluster.get(3);
            Epoch newEpoch = ClusterUtils.getNextEpoch(inst);
            inst.runOnInstance(() -> {
                ClusterMetadataService.instance()
                                      .log()
                                      .append(new Entry(Entry.Id.NONE,
                                                        newEpoch,
                                                        CustomTransformation.make("DANGER")));
                try
                {
                    ClusterMetadataService.instance().awaitAtLeast(newEpoch);
                }
                catch (InterruptedException | TimeoutException e)
                {
                    throw new RuntimeException(e);
                }
            });
            Map<String, Epoch> canonicalEpochs = getEpochsDirectly(cluster);
            assertTrue(canonicalEpochs.get(inst.broadcastAddress().toString())
                                      .isAfter(canonicalEpochs.get(cmsNode.broadcastAddress().toString())));
            assertEpochs(cluster);
        }
    }

    private void assertEpochs(Cluster cluster)
    {
        assertEpochs(cluster, getEpochsDirectly(cluster));
    }

    private void assertEpochs(Cluster cluster, Map<String, Epoch> canonicalEpochs)
    {
        cluster.forEach(inst -> assertEquals(canonicalEpochs, getEpochsIndirectly(inst)));
    }

    private Map<String, Epoch> getEpochsDirectly(Cluster cluster)
    {
        Map<String, Epoch> epochs = new HashMap<>();
        cluster.forEach(inst -> epochs.put(inst.broadcastAddress().toString(), ClusterUtils.getCurrentEpoch(inst)));
        return epochs;
    }

    private Map<String, Epoch> getEpochsIndirectly(IInvokableInstance requester)
    {
        Map<String, Epoch> epochs = ClusterUtils.getPeerEpochs(requester);
        return epochs;
    }
}
