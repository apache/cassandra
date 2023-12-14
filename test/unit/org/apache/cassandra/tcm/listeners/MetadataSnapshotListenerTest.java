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

package org.apache.cassandra.tcm.listeners;

import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.tcm.AtomicLongBackedProcessor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.ownership.OwnershipUtils;
import org.apache.cassandra.tcm.transformations.ForceSnapshot;
import org.apache.cassandra.tcm.transformations.TriggerSnapshot;

import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.minimalForTesting;
import static org.apache.cassandra.tcm.sequences.SequencesUtils.epoch;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MetadataSnapshotListenerTest
{
    private static final Logger logger = LoggerFactory.getLogger(MetadataSnapshotListenerTest.class);
    private IPartitioner partitioner = Murmur3Partitioner.instance;
    private Random r;

    @BeforeClass
    public static void disableSortedReplicaGroups()
    {
        // Set this so that we don't attempt to sort the random placements as this depends on a populated
        // TokenMap. This is a temporary element of ClusterMetadata, at least in the current form
        CassandraRelevantProperties.TCM_SORT_REPLICA_GROUPS.setBoolean(false);
    }

    @Before
    public void setup()
    {
        long seed = System.nanoTime();
        r = new Random(seed);
        logger.info("SEED: {}", seed);
    }

    @Test
    public void forceSnapshotTriggersSnapshot()
    {
        // ForceSnapshot contains a complete ClusterMetadata which is what we expect to be
        // stored as the snapshot. The input to its execute method is the previous ClusterMetadata
        // and isn't relevant here.
        MetadataSnapshots snapshots = init();
        ClusterMetadata toSnapshot = metadataForSnapshot();
        Entry entry = new Entry(Entry.Id.NONE,
                                toSnapshot.epoch,
                                new ForceSnapshot(toSnapshot));

        ClusterMetadata previous = minimalForTesting(Epoch.FIRST, partitioner);
        Transformation.Result result = entry.transform.execute(previous);
        MetadataSnapshotListener listener = new MetadataSnapshotListener();

        // The payload of the transformation should be retrievable by its epoch
        assertNull(snapshots.getSnapshot(toSnapshot.epoch));
        listener.notify(entry, result);
        assertEquals(toSnapshot, snapshots.getSnapshot(toSnapshot.epoch));
    }

    @Test
    public void triggerSnapshotTest()
    {
        // TriggerSnapshot has no payload itself, but stores the preceding ClusterMetadata state as a snapshot.
        MetadataSnapshots snapshots = init();
        ClusterMetadata toSnapshot = metadataForSnapshot();

        Epoch nextEpoch = toSnapshot.nextEpoch();
        Entry entry = new Entry(Entry.Id.NONE, nextEpoch, TriggerSnapshot.instance);

        Transformation.Result result = entry.transform.execute(toSnapshot);
        MetadataSnapshotListener listener = new MetadataSnapshotListener();

        assertNull(snapshots.getSnapshot(nextEpoch));
        listener.notify(entry, result);
        ClusterMetadata snapshot = snapshots.getSnapshot(nextEpoch);
        assertEquals(nextEpoch, snapshot.epoch);
        assertEquals(toSnapshot.placements, snapshot.placements);
    }

    private MetadataSnapshots init()
    {
        MetadataSnapshots snapshots = new AtomicLongBackedProcessor.InMemoryMetadataSnapshots();
        StubClusterMetadataService service = StubClusterMetadataService.builder(partitioner)
                                                                       .withSnapshots(snapshots)
                                                                       .build();
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(service);
        return snapshots;
    }

    private ClusterMetadata metadataForSnapshot()
    {
        return minimalForTesting(epoch(r), partitioner)
               .transformer()
               .with(OwnershipUtils.randomPlacements(r)).build()
               .metadata;
    }

}
