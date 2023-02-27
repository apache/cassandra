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

package org.apache.cassandra.distributed.test.tcm;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.extensions.ExtensionValue;
import org.apache.cassandra.tcm.extensions.IntValue;
import org.apache.cassandra.tcm.transformations.CustomTransformation;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogReplicationSmokeTest extends TestBaseImpl
{
    @Test
    public void testRequestingPeerWatermarks() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                        .start())
        {
            init(cluster);
            IInvokableInstance cmsNode = cluster.get(1);
            ClusterUtils.waitForCMSToQuiesce(cluster, cmsNode);
            Epoch initialEpoch = getConsistentEpoch(cluster);

            int initialVal = getConsistentValue(cluster);
            assertEquals(-1, initialVal);

            Epoch expectedEpoch = initialEpoch.nextEpoch();
            final int expectedVal = new Random(System.nanoTime()).nextInt();
            cluster.get(3).runOnInstance(() -> {
                ClusterMetadataService.instance().commit(CustomTransformation.make(expectedVal));
            });

            ClusterUtils.waitForCMSToQuiesce(cluster, cmsNode);
            Epoch currentEpoch = getConsistentEpoch(cluster);
            assertTrue(currentEpoch.is(expectedEpoch));
            int currentVal = getConsistentValue(cluster);
            assertEquals(expectedVal, currentVal);
        }
    }

    private int getConsistentValue(Cluster cluster)
    {
        Set<Integer> values = new HashSet<>();
        cluster.forEach(inst -> values.add( inst.callOnInstance(() -> {
            ExtensionValue<?> v = ClusterMetadata.current().extensions.get(CustomTransformation.PokeInt.METADATA_KEY);
            return v == null ? -1 : ((IntValue) v).getValue();
        })));
        assertEquals(1, values.size());
        return values.iterator().next();
    }

    private Epoch getConsistentEpoch(Cluster cluster)
    {
        Map<String, Epoch> epochs = getEpochsDirectly(cluster);
        assertEquals(1, new HashSet<>(epochs.values()).size());
        return epochs.values().iterator().next();
    }

    private Map<String, Epoch> getEpochsDirectly(Cluster cluster)
    {
        Map<String, Epoch> epochs = new HashMap<>();
        cluster.forEach(inst -> epochs.put(inst.broadcastAddress().toString(), ClusterUtils.getClusterMetadataVersion(inst)));
        return epochs;
    }

}
