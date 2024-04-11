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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.log.SystemKeyspaceStorage;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.tcm.transformations.TriggerSnapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ReplayPersistedTest extends TestBaseImpl
{
    @Test
    public void testGetPersistedLogState() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(1)
                                             .withConfig((config) -> config.set("metadata_snapshot_frequency", 10))
                                             .start()))
        {
            cluster.get(1).runOnInstance(() -> {
                for (int i = 0; i < 50; i++)
                {
                    ClusterMetadataService.instance().commit(new CustomTransformation(CustomTransformation.PokeInt.NAME, new CustomTransformation.PokeInt(i)));
                    maybeWaitForSnapshot();
                }
                Random r = new Random(1L);
                for (int i = 0; i < 100; i++)
                {
                    for (int j = 0; j < r.nextInt(15); j++)
                    {
                        int poke = i * 1000 + j;
                        ClusterMetadataService.instance().commit(new CustomTransformation(CustomTransformation.PokeInt.NAME, new CustomTransformation.PokeInt(poke)));
                        maybeWaitForSnapshot();
                    }
                    ClusterMetadata cur = ClusterMetadata.current();

                    LogState state = SystemKeyspaceStorage.SystemKeyspace.getPersistedLogState();
                    verifyReplication(cur, state);

                    // make sure we survive losing the last snapshot:
                    ClusterMetadata latest = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots().getLatestSnapshot();
                    if (latest != null)
                    {
                        QueryProcessor.executeInternal("delete from system.metadata_snapshots where epoch = ?", latest.epoch.getEpoch());
                        state = SystemKeyspaceStorage.SystemKeyspace.getPersistedLogState();
                        verifyReplication(cur, state);
                    }

                    MetadataSnapshots.SystemKeyspaceMetadataSnapshots snapshots = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots();
                    UntypedResultSet res = QueryProcessor.executeInternal("select epoch from system.metadata_snapshots");
                    List<Long> allEpochs = new ArrayList<>();
                    for (UntypedResultSet.Row row : res)
                        allEpochs.add(row.getLong("epoch"));
                    allEpochs.sort(Long::compare);
                    for (int x = 0; x < allEpochs.size(); x++)
                    {
                        long epoch = allEpochs.get(x);
                        long epochBefore = epoch - 1;
                        ClusterMetadata metadata = snapshots.getSnapshotBefore(Epoch.create(epochBefore));
                        if (x == 0)
                            assertNull(metadata);
                        else
                            assertEquals((long)allEpochs.get(x - 1), metadata.epoch.getEpoch());

                        assertEquals(epoch, snapshots.getSnapshotBefore(Epoch.create(epoch)).epoch.getEpoch());

                        long nextEpoch = epoch + 1;
                        assertEquals(epoch, snapshots.getSnapshotBefore(Epoch.create(nextEpoch)).epoch.getEpoch());
                    }
                }
            });
        }
    }

    private static void maybeWaitForSnapshot()
    {
        try
        {
            ScheduledExecutors.nonPeriodicTasks.submit(() -> null).get(); // we schedule the seal period on nonPeriodicTasks, just wait for any scheduled tasks to finish
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void verifyReplication(ClusterMetadata cur, LogState state)
    {
        ImmutableList<Entry> entries = state.entries;
        if (entries.isEmpty())
        {
            assertEquals(cur.epoch, state.baseState.epoch);
        }
        else
        {
            Entry last = entries.get(entries.size() - 1);
            // race, we might have got a SealPeriod since we grabbed ClusterMetadata.current (we don't block commit on that)
            if (last.transform instanceof TriggerSnapshot &&
                last.epoch.is(cur.epoch.nextEpoch()))
            {
                entries = state.entries.subList(0, state.entries.size() - 1);
                last = entries.get(entries.size() - 1);
            }
            assertEquals(cur.epoch, last.epoch);
        }
        ClusterMetadata built = state.baseState == null ? new ClusterMetadata(DatabaseDescriptor.getPartitioner()) : state.baseState;
        for (Entry entry : entries)
        {
            Transformation.Result res = entry.transform.execute(built);
            assertTrue(res.isSuccess());
            built = res.success().metadata;
        }
        assertEquals(cur, built);
    }
}
