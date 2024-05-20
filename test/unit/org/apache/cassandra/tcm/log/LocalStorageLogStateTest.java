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

package org.apache.cassandra.tcm.log;

import java.io.IOException;

import org.junit.BeforeClass;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.tcm.transformations.TriggerSnapshot;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.db.SystemKeyspace.METADATA_LOG;
import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;

public class LocalStorageLogStateTest extends LogStateTestBase
{
    @BeforeClass
    public static void setupClass() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServerNoRegister();
        CommitLog.instance.start();
    }

    public LocalStorageLogStateTest()
    {
    }

    @Override
    LogStateSUT getSystemUnderTest(MetadataSnapshots snapshots)
    {
        return new LogStateSUT()
        {
            SystemKeyspaceStorage storage = new SystemKeyspaceStorage(() -> snapshots);
            Epoch epoch = Epoch.FIRST;

            @Override
            public void cleanup()
            {
                ColumnFamilyStore.getIfExists(SYSTEM_KEYSPACE_NAME, METADATA_LOG).truncateBlockingWithoutSnapshot();
            }

            @Override
            public void insertRegularEntry() throws IOException
            {
                // somewhat of a hack, but a "real" log as used by the DistributedMetadataKeyspace equivalent of this
                // test will bootstrap the PreInitialize entry at Epoch.FIRST. SystemKeyspaceStorage doesn't do that,
                // so fake an extra entry here to keep the test data in sync.
                if (epoch.is(Epoch.FIRST))
                {
                    storage.append(new Entry(new Entry.Id(epoch.getEpoch()), epoch, CustomTransformation.make((int) epoch.getEpoch())));
                    epoch = epoch.nextEpoch();
                }
                storage.append(new Entry(new Entry.Id(epoch.getEpoch()), epoch, CustomTransformation.make((int) epoch.getEpoch())));
                epoch = epoch.nextEpoch();
            }

            @Override
            public void snapshotMetadata() throws IOException
            {
                storage.append(new Entry(new Entry.Id(epoch.getEpoch()), epoch, TriggerSnapshot.instance));
                epoch = epoch.nextEpoch();
            }

            @Override
            public LogState getLogState(Epoch since)
            {
                return storage.getLogState(since);
            }

            @Override
            public void dumpTables() throws IOException
            {
                UntypedResultSet r = executeInternal("SELECT epoch, entry_id, kind FROM system.local_metadata_log");
                r.forEach(row -> {
                    long e = row.getLong("epoch");
                    long i = row.getLong("entry_id");
                    String s = row.getString("kind");
                    System.out.println(String.format("(%d, %d, %s)", e, i, s));
                });
            }
        };
    }

}
