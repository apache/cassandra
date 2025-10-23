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
package org.apache.cassandra.service;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationMutationHelper;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationMutationHelper.SplitMutations;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.transformations.AlterSchema;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for ConsensusMigrationMutationHelper mutation splitting logic.
 *
 * Focuses on tracked vs untracked keyspace separation without testing Accord integration.
 */
public class ConsensusMigrationMutationHelperTest
{
    private static final String TRACKED_KS = "tracked_ks";
    private static final String UNTRACKED_KS = "untracked_ks";
    private static final String TABLE = "test_table";

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp() throws Exception
    {
        // Initialize cluster metadata service for each test
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(ClusterMetadataTestHelper.syncInstanceForTest());
        ClusterMetadataService.instance().log().unsafeBootstrapForTesting(FBUtilities.getBroadcastAddressAndPort());

        // Create tracked keyspace with table
        TableMetadata trackedTable = TableMetadata.builder(TRACKED_KS, TABLE)
                                                  .addPartitionKeyColumn("pk", UTF8Type.instance)
                                                  .addRegularColumn("value", UTF8Type.instance)
                                                  .build();
        ClusterMetadataTestHelper.createKeyspace(TRACKED_KS, KeyspaceParams.simple(3, ReplicationType.tracked));
        ClusterMetadataTestHelper.commit(new AlterSchema(SchemaTransformations.addTable(trackedTable, false)));

        // Create untracked keyspace with table
        TableMetadata untrackedTable = TableMetadata.builder(UNTRACKED_KS, TABLE)
                                                    .addPartitionKeyColumn("pk", UTF8Type.instance)
                                                    .addRegularColumn("value", UTF8Type.instance)
                                                    .build();
        ClusterMetadataTestHelper.createKeyspace(UNTRACKED_KS, KeyspaceParams.simple(3));
        ClusterMetadataTestHelper.commit(new AlterSchema(SchemaTransformations.addTable(untrackedTable, false)));
    }

    @Test
    public void testSplitTrackedOnly()
    {
        ClusterMetadata cm = ClusterMetadata.current();
        List<Mutation> mutations = new ArrayList<>();

        // Create 3 mutations to tracked keyspace
        mutations.add(createMutation(TRACKED_KS, "key1", "value1"));
        mutations.add(createMutation(TRACKED_KS, "key2", "value2"));
        mutations.add(createMutation(TRACKED_KS, "key3", "value3"));

        SplitMutations<Mutation> split = ConsensusMigrationMutationHelper.splitMutations(cm, mutations);

        // All mutations should go to tracked bucket
        assertNotNull(split.trackedMutations());
        assertEquals(3, split.trackedMutations().size());

        // Other buckets should be null
        assertNull(split.untrackedMutations());
        assertNull(split.accordMutations());
    }

    @Test
    public void testSplitUntrackedOnly()
    {
        ClusterMetadata cm = ClusterMetadata.current();
        List<Mutation> mutations = new ArrayList<>();

        // Create 3 mutations to untracked keyspace
        mutations.add(createMutation(UNTRACKED_KS, "key1", "value1"));
        mutations.add(createMutation(UNTRACKED_KS, "key2", "value2"));
        mutations.add(createMutation(UNTRACKED_KS, "key3", "value3"));

        SplitMutations<Mutation> split = ConsensusMigrationMutationHelper.splitMutations(cm, mutations);

        // All mutations should go to untracked bucket
        assertNotNull(split.untrackedMutations());
        assertEquals(3, split.untrackedMutations().size());

        // Other buckets should be null
        assertNull(split.trackedMutations());
        assertNull(split.accordMutations());
    }

    @Test
    public void testSplitMixedTrackedUntracked()
    {
        ClusterMetadata cm = ClusterMetadata.current();
        List<Mutation> mutations = new ArrayList<>();

        // Create mixed mutations: 2 tracked, 2 untracked
        mutations.add(createMutation(TRACKED_KS, "key1", "value1"));
        mutations.add(createMutation(UNTRACKED_KS, "key2", "value2"));
        mutations.add(createMutation(TRACKED_KS, "key3", "value3"));
        mutations.add(createMutation(UNTRACKED_KS, "key4", "value4"));

        SplitMutations<Mutation> split = ConsensusMigrationMutationHelper.splitMutations(cm, mutations);

        // Check tracked bucket
        assertNotNull(split.trackedMutations());
        assertEquals(2, split.trackedMutations().size());
        assertEquals("key1", UTF8Type.instance.compose(split.trackedMutations().get(0).key().getKey()));
        assertEquals("key3", UTF8Type.instance.compose(split.trackedMutations().get(1).key().getKey()));

        // Check untracked bucket
        assertNotNull(split.untrackedMutations());
        assertEquals(2, split.untrackedMutations().size());
        assertEquals("key2", UTF8Type.instance.compose(split.untrackedMutations().get(0).key().getKey()));
        assertEquals("key4", UTF8Type.instance.compose(split.untrackedMutations().get(1).key().getKey()));

        // Accord should be null
        assertNull(split.accordMutations());
    }

    private Mutation createMutation(String keyspace, String partitionKey, String value)
    {
        TableMetadata table = Schema.instance.getTableMetadata(keyspace, TABLE);
        return new RowUpdateBuilder(table, 0, partitionKey)
               .add("value", value)
               .build();
    }
}
