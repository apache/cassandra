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

package org.apache.cassandra.tcm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.extensions.ExtensionValue;
import org.apache.cassandra.tcm.listeners.MetadataSnapshotListener;
import org.apache.cassandra.tcm.listeners.SchemaListener;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.LogStorage;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.tcm.transformations.TriggerSnapshot;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

public class LogStateTest
{
    @Before
    public void setup() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.cleanupAndLeaveDirs();
        CommitLog.instance.start();
        LogStorage logStorage = LogStorage.SystemKeyspace;
        MetadataSnapshots snapshots = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots();
        ClusterMetadata initial = new ClusterMetadata(DatabaseDescriptor.getPartitioner());
        LocalLog.LogSpec logSpec = LocalLog.logSpec()
                                           .sync()
                                           .withInitialState(initial)
                                           .withStorage(logStorage)
                                           .withLogListener(new MetadataSnapshotListener())
                                           .withListener(new SchemaListener(true));
        LocalLog log = logSpec.createLog();
        ClusterMetadataService cms = new ClusterMetadataService(new UniformRangePlacement(),
                                                                snapshots,
                                                                log,
                                                                new AtomicLongBackedProcessor(log),
                                                                Commit.Replicator.NO_OP,
                                                                false);
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(cms);
        log.readyUnchecked();
        log.unsafeBootstrapForTesting(FBUtilities.getBroadcastAddressAndPort());
    }

    @Test
    public void testRevertEpoch()
    {
        ClusterMetadataService.instance().triggerSnapshot();
        List<Epoch> customEpochs = new ArrayList<>(40);
        for (int i=0; i < 10; i++)
        {
            for (int j = 0; j < 4; j++)
            {
                ClusterMetadataService.instance()
                                      .commit(new CustomTransformation(CustomTransformation.PokeInt.NAME,
                                                                       new CustomTransformation.PokeInt((int) ClusterMetadata.current().epoch.getEpoch())));
                customEpochs.add(ClusterMetadata.current().epoch);
            }
            ClusterMetadataService.instance().commit(TriggerSnapshot.instance);
        }

        for (Epoch epoch : customEpochs)
        {
            ClusterMetadataService.instance().revertToEpoch(epoch);
            ExtensionValue<?> val = ClusterMetadata.current().extensions.get(CustomTransformation.PokeInt.METADATA_KEY);
            // -1 since we poke the previous int to the extension:
            assertEquals((int)(epoch.getEpoch() - 1),  ClusterMetadata.current().extensions.get(CustomTransformation.PokeInt.METADATA_KEY).getValue());
        }
    }
}
