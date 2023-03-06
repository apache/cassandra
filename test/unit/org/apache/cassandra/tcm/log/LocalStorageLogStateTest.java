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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.Period;
import org.apache.cassandra.tcm.Sealed;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.tcm.transformations.SealPeriod;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.db.SystemKeyspace.METADATA_LOG;
import static org.apache.cassandra.db.SystemKeyspace.SEALED_PERIODS_TABLE_NAME;
import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;

@RunWith(Parameterized.class)
public class LocalStorageLogStateTest extends LogStateTestBase
{
    @BeforeClass
    public static void setupClass() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.cleanupAndLeaveDirs();
        CommitLog.instance.start();
    }

    public LocalStorageLogStateTest(boolean truncateIndexTable, boolean truncateInMemoryIndex)
    {
        this.truncateIndexTable = truncateIndexTable;
        this.truncateInMemoryIndex = truncateInMemoryIndex;
    }

    @Override
    LogStateSUT getSystemUnderTest(MetadataSnapshots snapshots)
    {
        return new LogStateSUT()
        {
            SystemKeyspaceStorage storage = new SystemKeyspaceStorage(() -> snapshots);
            Epoch epoch = Epoch.FIRST;
            long period = Period.FIRST;

            @Override
            public void cleanup() throws IOException
            {
                ColumnFamilyStore.getIfExists(SYSTEM_KEYSPACE_NAME, METADATA_LOG).truncateBlockingWithoutSnapshot();
                ColumnFamilyStore.getIfExists(SYSTEM_KEYSPACE_NAME, SEALED_PERIODS_TABLE_NAME).truncateBlockingWithoutSnapshot();
                Sealed.unsafeClearLookup();
            }

            @Override
            public void insertRegularEntry() throws IOException
            {
                // somewhat of a hack, but a "real" log as used by the DistributedMetadataKeyspace equivalent of this
                // test will bootstrap the PreInitialize entry at Epoch.FIRST. SystemKeyspaceStorage doesn't do that,
                // so fake an extra entry here to keep the test data in sync.
                if (epoch.is(Epoch.FIRST))
                {
                    storage.append(period, new Entry(new Entry.Id(epoch.getEpoch()), epoch, CustomTransformation.make((int) epoch.getEpoch())));
                    epoch = epoch.nextEpoch();
                }
                storage.append(period, new Entry(new Entry.Id(epoch.getEpoch()), epoch, CustomTransformation.make((int) epoch.getEpoch())));
                epoch = epoch.nextEpoch();
            }

            @Override
            public void sealPeriod() throws IOException
            {
                storage.append(period, new Entry(new Entry.Id(epoch.getEpoch()), epoch, SealPeriod.instance));
                Sealed.recordSealedPeriod(period, epoch);
                epoch = epoch.nextEpoch();
                period += 1;
                // required so we have a starting point for finding the right period to build
                // replication from _if_ the max_epoch -> period table is lost
                ClusterMetadataTestHelper.forceCurrentPeriodTo(period);
            }

            @Override
            public LogState getLogState(Epoch since)
            {
                return storage.getLogState(since);
            }

            @Override
            public void dumpTables() throws IOException
            {
                UntypedResultSet r = executeInternal("SELECT period, epoch, entry_id, kind FROM system.local_metadata_log");
                r.forEach(row -> {
                    long p = row.getLong("period");
                    long e = row.getLong("epoch");
                    long i = row.getLong("entry_id");
                    String s = row.getString("kind");
                    System.out.println(String.format("(%d, %d, %d, %s)", p, e, i, s));
                });

                String query = String.format("SELECT max_epoch, period FROM system.metadata_sealed_periods");
                r = executeInternal(query);
                r.forEach(row -> {
                    long p = row.getLong("period");
                    long e = row.getLong("max_epoch");
                    System.out.println(String.format("(%d, %d)", e, p));
                });
            }
        };
    }

}
