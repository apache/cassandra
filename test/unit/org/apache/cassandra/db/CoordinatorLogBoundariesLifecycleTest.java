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

package org.apache.cassandra.db;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.MemtableParams;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.assertj.core.api.Assertions;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(Parameterized.class)
public class CoordinatorLogBoundariesLifecycleTest
{
    private static final AtomicInteger keyspaceNumber = new AtomicInteger();

    static
    {
        DatabaseDescriptor.daemonInitialization();
        MutationJournal.instance.start();
    }

    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
    }

    @Parameter
    public String memtableConfig;

    @Parameters(name="{0}")
    public static Collection<String> memtableConfigs()
    {
        List<String> memtableConfigs = new ArrayList<>();
        for (String config : DatabaseDescriptor.getMemtableConfigurations().keySet())
        {
            try
            {
                MemtableParams.get(config);
                memtableConfigs.add(config);
            }
            catch (ConfigurationException e)
            {
                // skip if the config is meant to validate config parsing
            }
        }

        memtableConfigs.sort(String.CASE_INSENSITIVE_ORDER);
        return memtableConfigs;
    }

    private static Mutation createMutation(TableMetadata tableMetadata, int k, int v)
    {
        DecoratedKey key = tableMetadata.partitioner.decorateKey(ByteBufferUtil.bytes(k));
        MutationId mutationId = MutationTrackingService.instance.nextMutationId(tableMetadata.keyspace, key.getToken());
        SimpleBuilders.MutationBuilder builder = new SimpleBuilders.MutationBuilder(mutationId, tableMetadata.keyspace, key);
        PartitionUpdate.SimpleBuilder partition = builder.update(tableMetadata);
        partition.row().add("v", v);
        Mutation mutation = builder.build();
        Assert.assertFalse(mutation.id().isNone());
        return mutation;
    }

    private static MutationId applyMutation(TableMetadata tableMetadata, int k, int v)
    {
        Mutation mutation = createMutation(tableMetadata, k, v);
        mutation.apply();
        return mutation.id();
    }

    private static void assertEmptyMemtable(View view)
    {
        Assert.assertEquals(0, view.getCurrentMemtable().operationCount());
    }

    private static void assertNonEmptyMemtable(View view)
    {
        Assert.assertTrue(view.getCurrentMemtable().operationCount() > 0);
    }

    private static void assertNumSSTables(View view, int numSSTables)
    {
        Assert.assertEquals(numSSTables, view.liveSSTables().size());
    }

    private static String nextKeyspaceName()
    {
        return "ks_" + keyspaceNumber.getAndIncrement();
    }

    /**
     * Test that mutation ids go from the memtable to sstables, and are combined during compaction
     */
    @Test
    public void mutationIdLifecycleTest()
    {
        String ks = nextKeyspaceName();
        String tbl = "tbl";
        TableMetadata tableMetadata = TableMetadata.builder(ks, tbl)
                                                   .addPartitionKeyColumn("k", Int32Type.instance)
                                                   .addRegularColumn("v", Int32Type.instance)
                                                   .build();

        TableParams tableParams = tableMetadata.params;
        tableParams = tableParams.unbuild().memtable(MemtableParams.get(memtableConfig)).build();
        tableMetadata = tableMetadata.withSwapped(tableParams);

        SchemaLoader.createKeyspace(ks, KeyspaceParams.simple(1, ReplicationType.tracked), tableMetadata);

        // TODO: check against all memtable types (David Capwell style)
        Keyspace keyspace = Keyspace.open(ks);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(tbl);
        cfs.disableAutoCompaction();

        // pre-apply
        {
            View view = cfs.getTracker().getView();
            assertEmptyMemtable(view);
            assertNumSSTables(view, 0);
        }

        MutationId id1;
        MutationId id2;
        // apply 1
        {
            id1 = applyMutation(cfs.metadata(), 1, 1);
            id2 = applyMutation(cfs.metadata(), 2, 2);

            View view = cfs.getTracker().getView();
            assertNonEmptyMemtable(view);
            assertNumSSTables(view, 0);

            Memtable memtable = view.getCurrentMemtable();
            CoordinatorLogBoundaries boundaries = memtable.getFlushSet(null, null).coordinatorLogBoundaries();
            Assertions.assertThat(boundaries.size()).isEqualTo(1);
            Assertions.assertThat(boundaries.max(id2.logId())).isEqualTo(id2);
        }

        // flush 1
        {
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

            View view = cfs.getTracker().getView();
            assertEmptyMemtable(view);
            assertNumSSTables(view, 1);

            SSTableReader sstable = Iterables.getOnlyElement(view.liveSSTables());
            CoordinatorLogBoundaries boundaries = sstable.getCoordinatorLogBoundaries();
            Assertions.assertThat(boundaries.size()).isEqualTo(1);
            Assertions.assertThat(boundaries.max(id2.logId())).isEqualTo(id2);
        }

        MutationId id3;
        MutationId id4;
        // apply 2
        {
            id3 = applyMutation(cfs.metadata(), 3, 3);
            id4 = applyMutation(cfs.metadata(), 4, 4);

            View view = cfs.getTracker().getView();
            assertNonEmptyMemtable(view);
            assertNumSSTables(view, 1);

            Memtable memtable = view.getCurrentMemtable();
            CoordinatorLogBoundaries boundaries = memtable.getFlushSet(null, null).coordinatorLogBoundaries();
            Assertions.assertThat(boundaries.size()).isEqualTo(1);
            Assertions.assertThat(boundaries.max(id4.logId())).isEqualTo(id4);
        }

        // flush 2
        {
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

            View view = cfs.getTracker().getView();
            assertEmptyMemtable(view);
            assertNumSSTables(view, 2);

            List<SSTableReader> sstables = Lists.newArrayList(view.liveSSTables());
            sstables.sort(Comparator.comparing(sst -> sst.descriptor.id.asBytes()));
            {
                CoordinatorLogBoundaries boundaries = sstables.get(0).getCoordinatorLogBoundaries();
                Assertions.assertThat(boundaries.size()).isEqualTo(1);
                Assertions.assertThat(boundaries.max(id2.logId())).isEqualTo(id2);
            }
            {

                CoordinatorLogBoundaries boundaries = sstables.get(1).getCoordinatorLogBoundaries();
                Assertions.assertThat(boundaries.size()).isEqualTo(1);
                Assertions.assertThat(boundaries.max(id4.logId())).isEqualTo(id4);
            }
        }

        // compaction
        {
            cfs.forceMajorCompaction();

            View view = cfs.getTracker().getView();
            assertEmptyMemtable(view);
            assertNumSSTables(view, 1);

            SSTableReader sstable = Iterables.getOnlyElement(view.liveSSTables());
            CoordinatorLogBoundaries boundaries = sstable.getCoordinatorLogBoundaries();
            Assertions.assertThat(boundaries.size()).isEqualTo(1);
            Assertions.assertThat(boundaries.max(id4.logId())).isEqualTo(id4);
        }
    }
}
