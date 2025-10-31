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

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadCommand.PotentialTxnConflicts;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.CoordinatorBehindException;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.FBUtilities;
import org.hamcrest.core.StringContains;

import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.broadcastAddress;
import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.bytesToken;
import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.node1;
import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.randomInt;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MutationTrackingMutationVerbHandlerMigrationTest
{
    private static final String TEST_NAME = "mutation_migration_test_";
    private static final String TRACKED_KEYSPACE = TEST_NAME + "tracked_ks";
    private static final String UNTRACKED_KEYSPACE = TEST_NAME + "untracked_ks";
    private static final String TABLE = "table1";

    private MutationVerbHandler handler;
    private long startingCoordinatorBehindCount;

    @BeforeClass
    public static void init() throws Exception
    {
        ServerTestUtils.prepareServerNoRegister();
        MutationJournal.instance.start();

        TableMetadata trackedTable = TableMetadata.builder(TRACKED_KEYSPACE, TABLE)
                                                  .addPartitionKeyColumn("pk", Int32Type.instance)
                                                  .addRegularColumn("v1", Int32Type.instance)
                                                  .build();
        KeyspaceMetadata trackedKs = KeyspaceMetadata.create(
                TRACKED_KEYSPACE,
                KeyspaceParams.simple(1, ReplicationType.tracked),
                Tables.of(trackedTable)
        );

        TableMetadata untrackedTable = TableMetadata.builder(UNTRACKED_KEYSPACE, TABLE)
                                                    .addPartitionKeyColumn("pk", Int32Type.instance)
                                                    .addRegularColumn("v1", Int32Type.instance)
                                                    .build();
        KeyspaceMetadata untrackedKs = KeyspaceMetadata.create(
                UNTRACKED_KEYSPACE,
                KeyspaceParams.simple(1, ReplicationType.untracked),
                Tables.of(untrackedTable)
        );

        // Register the keyspaces
        ClusterMetadataTestHelper.addOrUpdateKeyspace(trackedKs);
        ClusterMetadataTestHelper.addOrUpdateKeyspace(untrackedKs);

        ServerTestUtils.markCMS();
        StorageService.instance.unsafeSetInitialized();
        DatabaseDescriptor.setMutationTrackingEnabled(true);
    }

    @Before
    public void setup() throws Exception
    {
        ServerTestUtils.resetCMS();
        ClusterMetadataTestHelper.addEndpoint(broadcastAddress, bytesToken(100));
        ClusterMetadataTestHelper.addEndpoint(node1, bytesToken(0));

        MessagingService.instance().inboundSink.clear();
        MessagingService.instance().outboundSink.clear();

        handler = new MutationVerbHandler();
        startingCoordinatorBehindCount = TCMMetrics.instance.coordinatorBehindReplication.getCount();
    }

    @Test
    public void testAcceptMutationWithMatchingRouting() throws Exception
    {
        Epoch currentEpoch = ClusterMetadata.current().epoch;

        Mutation trackedMutation = createTrackedMutation(TRACKED_KEYSPACE);
        handleWithEpoch(trackedMutation, currentEpoch);

        Mutation untrackedMutation = createUntrackedMutation(UNTRACKED_KEYSPACE);
        handleWithEpoch(untrackedMutation, currentEpoch);

        assertEquals(startingCoordinatorBehindCount, TCMMetrics.instance.coordinatorBehindReplication.getCount());
    }

    @Test
    public void testCoordinatorBehindThrowsException() throws Exception
    {
        Epoch currentEpoch = ClusterMetadata.current().epoch;
        Epoch oldEpoch = Epoch.create(currentEpoch.getEpoch() - 1);
        Mutation mutation = createUntrackedMutation(TRACKED_KEYSPACE);

        try
        {
            handleWithEpoch(mutation, oldEpoch);
            fail("Expected CoordinatorBehindException");
        }
        catch (CoordinatorBehindException e)
        {
            assertEquals(startingCoordinatorBehindCount + 1, TCMMetrics.instance.coordinatorBehindReplication.getCount());
        }
    }

    @Test
    public void testSameEpochDifferentRoutingThrowsException() throws Exception
    {
        Epoch currentEpoch = ClusterMetadata.current().epoch;
        Mutation mutation = createTrackedMutation(UNTRACKED_KEYSPACE);

        try
        {
            handleWithEpoch(mutation, currentEpoch);
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException e)
        {
            assert e.getMessage().startsWith("Inconsistent mutation routing");
        }
    }

    @Test
    public void testCoordinatorAhead() throws Exception
    {
        Epoch currentEpoch = ClusterMetadata.current().epoch;
        Epoch futureEpoch = Epoch.create(currentEpoch.getEpoch() + 1);

        Mutation mutation = createUntrackedMutation(TRACKED_KEYSPACE);

        // since this is a unit test, we can't actually fetch newer epochs, so we just
        // make sure that the attempted fetch throws an exception
        try
        {
            handleWithEpoch(mutation, futureEpoch);
            fail("Expected IllegalStateException due CMS fetch timeout");
        }
        catch (IllegalStateException e)
        {
            Assert.assertThat(e.getMessage(), StringContains.containsString("Could not catch up to epoch"));
        }

        assertEquals(startingCoordinatorBehindCount, TCMMetrics.instance.coordinatorBehindReplication.getCount());
    }

    private void handleWithEpoch(Mutation mutation, Epoch epoch) throws Exception
    {
        handler.doVerb(Message.builder(Verb.MUTATION_REQ, mutation)
                             .from(node1)
                             .withId(randomInt())
                             .withEpoch(epoch)
                             .build());
    }

    private Mutation createMutation(String keyspace, int key, int columnValue, MutationId mutationId)
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(TABLE);
        TableMetadata cfm = Schema.instance.getTableMetadata(keyspace, TABLE);
        DecoratedKey dk = cfs.decorateKey(bytes(key));
        ColumnMetadata col = cfs.metadata().getColumn(bytes("v1"));
        Cell cell = BufferCell.live(col, FBUtilities.timestampMicros(), bytes(columnValue));
        Row row = BTreeRow.singleCellRow(Clustering.EMPTY, cell);
        PartitionUpdate update = PartitionUpdate.singleRowUpdate(cfm, dk, row);
        return new Mutation(mutationId, update, PotentialTxnConflicts.DISALLOW);
    }

    private Mutation createTrackedMutation(String keyspace)
    {
        return createMutation(keyspace, 50, 1, new MutationId(1L, 1L));
    }

    private Mutation createUntrackedMutation(String keyspace)
    {
        return createMutation(keyspace, 51, 1, MutationId.none());
    }
}
