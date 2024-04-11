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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.distributed.test.ExecUtil;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.FetchCMSLog;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.SystemKeyspace.SNAPSHOT_TABLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SystemKeyspaceStorageTest extends CoordinatorPathTestBase
{
    @Test
    public void testLogStateQuery() throws Throwable
    {
        cmsNodeTest((cluster, simulatedCluster) -> {
            // Disable periodic snapshotting
            DatabaseDescriptor.setMetadataSnapshotFrequency(Integer.MAX_VALUE);
            cluster.get(1).runOnInstance(() -> DatabaseDescriptor.setMetadataSnapshotFrequency(Integer.MAX_VALUE));
            Random rng = new Random(1L);

            // Generate some epochs
            int cnt = 0;
            int nextSnapshotIn = rng.nextInt(10);
            List<Epoch> allEpochs = new ArrayList<>();
            List<Epoch> allSnapshots = new ArrayList<>();

            for (int i = 0; i < 500; i++)
            {
                try
                {
                    if (nextSnapshotIn == 0)
                    {
                        cluster.get(1).runOnInstance(() -> ClusterMetadataService.instance().triggerSnapshot());
                        ClusterMetadata metadata = ClusterMetadataService.instance().processor().fetchLogAndWait();
                        allEpochs.add(metadata.epoch);
                        allSnapshots.add(metadata.epoch);
                        nextSnapshotIn = rng.nextInt(10);
                    }
                    else
                    {
                        nextSnapshotIn--;
                    }
                    ClusterMetadata metadata = ClusterMetadataService.instance().commit(CustomTransformation.make(cnt++));
                    allEpochs.add(metadata.epoch);
                }
                catch (Throwable e)
                {
                    throw new AssertionError(e);
                }
            }

            ClusterMetadataService.instance().processor().fetchLogAndWait();

            List<Epoch> remainingSnapshots = new ArrayList<>(allSnapshots);

            // Delete about a half (but potentially up to 100%) of all possible snapshots
            for (int i = 0; i < allSnapshots.size(); i++)
            {
                if (rng.nextBoolean())
                {
                    // pick a snapshot to delete
                    Epoch toRemoveSnapshot = remainingSnapshots.remove(rng.nextInt(remainingSnapshots.size()));
                    cluster.get(1).runOnInstance(() -> deleteSnapshot(toRemoveSnapshot.getEpoch()));
                }
            }
            Epoch latestSnapshot = remainingSnapshots.get(remainingSnapshots.size() - 1);
            Epoch lastEpoch =  allEpochs.stream().max(Comparator.naturalOrder()).get();
            repeat(10, () -> {
                repeat(100, () -> {
                    Epoch since = allEpochs.get(rng.nextInt(allEpochs.size()));
                    for (boolean consistentReplay : new boolean[]{ true, false })
                    {
                        LogState logState = simulatedCluster.node(2).requestResponse(new FetchCMSLog(since, consistentReplay));
                        // if we return a snapshot it is always the most recent one
                        // we don't return a snapshot if there is only 1 snapshot after `since`
                        Epoch start = since;
                        if (logState.baseState == null)
                        {
                            if (logState.entries.isEmpty()) // requesting an epoch after the last known epoch -> null + empty entries
                                assertTrue(since.getEpoch() >= lastEpoch.getEpoch());
                            else
                                // first entry should be epoch since + 1
                                assertEquals(start.nextEpoch(), logState.entries.get(0).epoch);
                        }
                        else
                        {
                            assertEquals(latestSnapshot, logState.baseState.epoch);
                            start = logState.baseState.epoch;
                            if (logState.entries.isEmpty()) // no entries, snapshot should have the same epoch as since
                                assertEquals(since, start);
                            else // first epoch in entries should be snapshot epoch + 1
                                assertEquals(start.nextEpoch(), logState.entries.get(0).epoch);
                        }

                        for (Entry entry : logState.entries)
                        {
                            start = start.nextEpoch();
                            assertEquals(start, entry.epoch);
                        }
                        assertEquals(lastEpoch, start);
                    }
                });
            });
        });
    }

    @Test
    public void bounceNodeBootrappedFromSnapshot() throws Throwable
    {
        coordinatorPathTest(new TokenPlacementModel.SimpleReplicationFactor(3), (cluster, simulatedCluster) -> {
            ClusterMetadataService.instance().triggerSnapshot();
            ClusterMetadataService.instance().log().waitForHighestConsecutive();
            ClusterMetadataService.instance().snapshotManager().storeSnapshot(ClusterMetadata.current());

            for (int i = 0; i < 5; i++)
                ClusterMetadataService.instance().commit(CustomTransformation.make(i));

            ClusterMetadataService.instance().log().waitForHighestConsecutive();

            cluster.startup();
            cluster.get(1).runOnInstance(() -> {
                ClusterMetadataService.instance().log().waitForHighestConsecutive();
            });
            cluster.get(1).nodetool("flush");
            FBUtilities.waitOnFuture(cluster.get(1).shutdown());
            cluster.get(1).startup();
            // TODO: currently, we do not have means of stopping traffic between the CMS and node1
            // When we do, we need to prevent replay handler from returning results, and assert node1 still catches up
        }, false);
    }

    public static void repeat(int num, ExecUtil.ThrowingSerializableRunnable r)
    {
        for (int i = 0; i < num; i++)
        {
            try
            {
                r.run();
            }
            catch (Throwable throwable)
            {
                throw new AssertionError(throwable);
            }
        }
    }

    public static void deleteSnapshot(long epoch)
    {
        String query = String.format("DELETE FROM %s.%s WHERE epoch = ?", SchemaConstants.SYSTEM_KEYSPACE_NAME, SNAPSHOT_TABLE_NAME);
        QueryProcessor.executeInternal(query, epoch);
    }
}
