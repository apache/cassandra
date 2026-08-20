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

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.AssertUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link ReadExecutionController}
 */
public class ReadExecutionControllerTest
{
    private static final String KEYSPACE = "read_execution_controller_test_ks";
    private static final String TABLE = "table1";
    private static final String INDEX_TABLE = "table1_index";

    private static ColumnFamilyStore cfs;
    private static ColumnFamilyStore indexCfs;

    @BeforeClass
    public static void init() throws Exception
    {
        ServerTestUtils.prepareServerNoRegister();
        MutationJournal.start();

        TableMetadata table = TableMetadata.builder(KEYSPACE, TABLE)
                                           .addPartitionKeyColumn("pk", Int32Type.instance)
                                           .addRegularColumn("v1", Int32Type.instance)
                                           .build();
        TableMetadata indexTable = TableMetadata.builder(KEYSPACE, INDEX_TABLE)
                                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                                .addRegularColumn("v1", Int32Type.instance)
                                                .build();
        KeyspaceMetadata ks = KeyspaceMetadata.create(KEYSPACE,
                                                      KeyspaceParams.simple(1, ReplicationType.tracked),
                                                      Tables.of(table, indexTable));
        ClusterMetadataTestHelper.addOrUpdateKeyspace(ks);
        ServerTestUtils.markCMS();
        StorageService.instance.unsafeSetInitialized();

        cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        indexCfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(INDEX_TABLE);
    }

    @Test
    public void forCommandDoesNotLeakOpOrderGroupWhenRejectingTrackedTable()
    {
        ReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfs.metadata(), FBUtilities.nowInSeconds(), Util.dk(1));

        try
        {
            ReadExecutionController.forCommand(command, true);
            fail("Expected forCommand to throw because tracking the repaired status is unsupported for tracked reads");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue("Unexpected exception message: " + e.getMessage(),
                       e.getMessage().contains("Tracking repaired status is not supported for tracked reads"));
        }

        AssertUtil.assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            OpOrder.Barrier barrier = cfs.readOrdering.newBarrier();
            barrier.issue();
            barrier.await();
        });
    }

    @Test
    public void forCommandDoesNotLeakOpOrderGroupWhenRejectingTrackedTableWithIndex()
    {
        Index index = new BackedStubIndex(cfs, IndexMetadata.fromSchemaMetadata("stub_index", IndexMetadata.Kind.CUSTOM, Collections.emptyMap()));
        Index.QueryPlan queryPlan = new SingleIndexQueryPlan(index);

        ReadCommand command = SinglePartitionReadCommand.create(cfs.metadata(),
                                                                FBUtilities.nowInSeconds(),
                                                                ColumnFilter.all(cfs.metadata()),
                                                                RowFilter.none(),
                                                                DataLimits.NONE,
                                                                Util.dk(1),
                                                                new ClusteringIndexSliceFilter(Slices.ALL, false),
                                                                queryPlan);

        try
        {
            ReadExecutionController.forCommand(command, true);
            fail("Expected forCommand to throw because tracking the repaired status is unsupported for tracked reads");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue("Unexpected exception message: " + e.getMessage(),
                       e.getMessage().contains("Tracking repaired status is not supported for tracked reads"));
        }

        // baseOp must have been released despite the exception
        AssertUtil.assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            OpOrder.Barrier barrier = cfs.readOrdering.newBarrier();
            barrier.issue();
            barrier.await();
        });
        // ensure the index's OpOrder is released too
        AssertUtil.assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            OpOrder.Barrier barrier = indexCfs.readOrdering.newBarrier();
            barrier.issue();
            barrier.await();
        });
    }

    private static class BackedStubIndex extends StubIndex
    {
        BackedStubIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        @Override
        public Optional<ColumnFamilyStore> getBackingTable()
        {
            return Optional.of(indexCfs);
        }
    }

    private static class SingleIndexQueryPlan implements Index.QueryPlan
    {
        private final Index index;

        SingleIndexQueryPlan(Index index)
        {
            this.index = index;
        }

        @Override
        public Set<Index> getIndexes()
        {
            return Collections.singleton(index);
        }

        @Override
        public Index.Searcher searcherFor(ReadCommand command)
        {
            return index.searcherFor(command);
        }

        @Override
        public RowFilter postIndexQueryFilter()
        {
            return RowFilter.none();
        }
    }
}
