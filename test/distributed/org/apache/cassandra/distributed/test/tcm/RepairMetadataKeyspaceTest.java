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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.DistributedMetadataLogKeyspace;
import org.apache.cassandra.tcm.Epoch;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getDataDirectories;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RepairMetadataKeyspaceTest extends TestBaseImpl
{
    @Test
    public void testRepairMetadataKeyspace() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                        .start())
        {
            init(cluster);
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
            Epoch currentEpoch = getConsistentEpoch(cluster);

            for (IInvokableInstance nodeToTest : cluster)
                assertTrue(canReadCompleteLog(nodeToTest, currentEpoch));

            IInvokableInstance toRepair = cluster.get(3);
            stopUnchecked(toRepair);
            DatabaseDescriptor.clientInitialization();
            String targetDir = DistributedMetadataLogKeyspace.TABLE_NAME + '-' + DistributedMetadataLogKeyspace.LOG_TABLE_ID.toHexString();
            for (File datadir : getDataDirectories(toRepair))
            {
                List<Path> tabledirs = Files.find(datadir.toPath(),
                                                  Integer.MAX_VALUE,
                                                  (filePath, fileAttr) -> fileAttr.isDirectory() && filePath.getFileName().toString().equals(targetDir))
                                            .collect(Collectors.toList());
                for (Path tabledir : tabledirs)
                {
                    Files.find(tabledir,
                               Integer.MAX_VALUE,
                               (path, attr) -> attr.isRegularFile())
                         .map(File::new)
                         .forEach(File::delete);
                }
            }

            toRepair.startup();
            // Stopping and restarting a node commits further epochs (e.g. AccordChangeNodeInfo status updates),
            // so the epoch captured before the bounce is no longer the tip of the log.
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
            currentEpoch = getConsistentEpoch(cluster);
            assertFalse(canReadCompleteLog(toRepair, currentEpoch));

            toRepair.nodetoolResult("repair", "--full", "system_cluster_metadata").asserts().success();
            for (IInvokableInstance nodeToTest : cluster)
                assertTrue(canReadCompleteLog(nodeToTest, currentEpoch));
        }
    }

    private boolean canReadCompleteLog(IInvokableInstance instance, Epoch currentEpoch)
    {
        // The log is complete if it contains every epoch from Epoch.FIRST up to currentEpoch. Later epochs may
        // legitimately have been committed in the meantime (e.g. node status updates), so don't require an exact size.
        Object[][] res = instance.executeInternal("SELECT epoch FROM system_cluster_metadata.distributed_metadata_log");
        Set<Long> fromLog = new HashSet<>();
        for (Object[] row : res)
            fromLog.add((long) row[0]);

        for (long epoch = Epoch.FIRST.getEpoch(); epoch <= currentEpoch.getEpoch(); epoch++)
        {
            if (!fromLog.contains(epoch))
                return false;
        }
        return true;
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
