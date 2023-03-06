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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.distributed.test.ExecUtil;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Sealed;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.Replay;
import org.apache.cassandra.tcm.log.Replication;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.BiMultiValMap;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.SystemKeyspace.SNAPSHOT_TABLE_NAME;

public class SystemKeyspaceStorageTest extends CoordinatorPathTestBase
{
    @Test
    public void testLogStateQuery() throws Throwable
    {
        cmsNodeTest((cluster, simulatedCluster) -> {
            // Disable snapshots on CMS "client" nodes
            DatabaseDescriptor.setMetadataSnapshotFrequency(Integer.MAX_VALUE);
            cluster.get(1).runOnInstance(() -> DatabaseDescriptor.setMetadataSnapshotFrequency(Integer.MAX_VALUE));
            Random rng = new Random(1L);

            // Map of epoch to period
            BiMultiValMap<Epoch, Long> epochToPeriod = new BiMultiValMap<>();
            // Generate some epochs
            int cnt = 0;
            int periodSize = rng.nextInt(10);
            // set up a few epochs
            for (int i = 0; i < 500; i++)
            {
                try
                {
                    if (periodSize == 0)
                    {
                        cluster.get(1).runOnInstance(() -> ClusterMetadataService.instance().sealPeriod());
                        ClusterMetadata metadata = ClusterMetadataService.instance().replayAndWait();
                        epochToPeriod.put(metadata.epoch, metadata.period);
                        periodSize = rng.nextInt(10);
                    }
                    else
                    {
                        periodSize--;
                    }
                    ClusterMetadata metadata = ClusterMetadataService.instance().commit(CustomTransformation.make(cnt++));
                    epochToPeriod.put(metadata.epoch, metadata.period);
                }
                catch (Throwable e)
                {
                    throw new AssertionError(e);
                }
            }

            ClusterMetadataService.instance().replayAndWait();

            List<Epoch> allEpochs = new ArrayList<>(epochToPeriod.keySet());
            List<Long> allPeriods = cluster.get(1).callOnInstance(() -> getAllSnapshots());
            List<Long> remainingPeriods = new ArrayList<>(allPeriods);

            // Delete about a half (but potentially up to 100%) of all possible snapshots
            for (int i = 0; i < allPeriods.size(); i++)
            {
                if (rng.nextBoolean())
                {
                    // pick a sealed period for which we'll delete the snapshot
                    long toRemovePeriod = remainingPeriods.remove(rng.nextInt(remainingPeriods.size()));
                    // lookup the max epoch for the period, this is the key to the snapshots table
                    long toRemoveEpoch = maxEpochInPeriod(epochToPeriod, toRemovePeriod).getEpoch();
                    cluster.get(1).runOnInstance(() -> deleteSnapshot(toRemoveEpoch));
                }
            }

            repeat(10, () -> {
                long lastSealed = remainingPeriods.isEmpty() ? -1 : remainingPeriods.get(rng.nextInt(remainingPeriods.size()));
                if (lastSealed > 0)
                {
                    Epoch maxInPeriod = maxEpochInPeriod(epochToPeriod, lastSealed);
                    cluster.get(1).runOnInstance(() -> forceLastSealed(lastSealed, maxInPeriod.getEpoch()));
                }

                repeat(100, () -> {
                    Epoch since = allEpochs.get(rng.nextInt(allEpochs.size()));
                    for (boolean consistentReplay : new boolean[]{ true, false })
                    {
                        LogState logState = simulatedCluster.node(2).requestResponse(new Replay(since, consistentReplay));
                        // Either:
                        //  * we have no snapshots to catch up from which may
                        //  * we've requested an epoch that's past last sealed snapshot
                        //  * the epoch we're requesting is the max in the last sealed snapshot, in which case we
                        //    do not send it
                        // so the case is equivalent to the above replication test.
                        if (remainingPeriods.isEmpty()
                            || epochToPeriod.get(since) > lastSealed
                            || since.is(maxEpochInPeriod(epochToPeriod, lastSealed)))
                        {
                            Assert.assertNull(logState.baseState);
                            assertReplication(since, allEpochs, logState.transformations);
                        }
                        // We have a snapshot to catch up from, so it has to be returned
                        else
                        {
                            Assert.assertEquals(lastSealed, logState.baseState.period);
                            Assert.assertTrue(logState.baseState.lastInPeriod);
                            assertReplication(logState.baseState.epoch, allEpochs, logState.transformations);
                        }
                    }
                });
            });
        });
    }

    private Epoch maxEpochInPeriod(BiMultiValMap<Epoch, Long> epochToPeriod, long period)
    {
        return epochToPeriod.inverse()
                            .get(period)
                            .stream()
                            .max(Comparator.naturalOrder())
                            .get();
    }

    @Test
    public void bounceNodeBootrappedFromSnapshot() throws Throwable
    {
        coordinatorPathTest((cluster, simulatedCluster) -> {
            ClusterMetadataService.instance().sealPeriod();
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

    public static void assertReplication(Epoch since, List<Epoch> allEpochs, Replication replication)
    {
        // +1 because we assume we already know the `since` epoch
        int offset = Collections.binarySearch(allEpochs, since) + 1;

        if (since.equals(allEpochs.get(allEpochs.size() - 1)))
            Assert.assertEquals(0, replication.entries().size());
        else
            Assert.assertEquals(since.getEpoch() + 1, replication.entries().get(0).epoch.getEpoch());

        Assert.assertEquals(allEpochs.get(allEpochs.size() - 1).getEpoch() - since.getEpoch(),
                            replication.entries().size());
        for (int i = 0; i < replication.entries().size(); i++)
        {
            Assert.assertEquals(String.format("Got mismatch while replication starting with %s", since),
                                allEpochs.get(offset + i), replication.entries().get(i).epoch);
        }
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

    public static List<Long> getAllSnapshots()
    {
        List<Long> allPeriods = new ArrayList<>();
        String query = String.format("SELECT period FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SNAPSHOT_TABLE_NAME);
        for (UntypedResultSet.Row row : QueryProcessor.executeInternal(query))
            allPeriods.add(row.getLong("period"));

        return allPeriods;
    }

    public static void deleteSnapshot(long epoch)
    {
        String query = String.format("DELETE FROM %s.%s WHERE epoch = ?", SchemaConstants.SYSTEM_KEYSPACE_NAME, SNAPSHOT_TABLE_NAME);
        QueryProcessor.executeInternal(query, epoch);
    }

    public static void forceLastSealed(long period, long epoch)
    {
        Sealed.unsafeClearLookup();
        Sealed.recordSealedPeriod(period, Epoch.create(epoch));
    }
}
