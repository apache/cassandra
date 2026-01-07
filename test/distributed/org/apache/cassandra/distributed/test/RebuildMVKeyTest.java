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

package org.apache.cassandra.distributed.test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.transport.Dispatcher;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.TokenSupplier.evenlyDistributedTokens;
import static org.apache.cassandra.distributed.shared.NetworkTopology.singleDcNetworkTopology;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Distributed test for MV rebuild key functionality.
 * Tests MV consistency and rebuild with various inconsistency types in a 3-node cluster.
 */
public class RebuildMVKeyTest extends TestBaseImpl
{
    private static final String KEYSPACE = "rebuild_mv_key_test";
    private static final String BASE_TABLE = "base_table";
    private static final String MV_NON_PK = "mv_non_pk";
    private static final String MV_NON_PK_UNSELECT = "mv_non_pk_unselect";
    private static final String MV_SAME_PK = "mv_same_pk";
    private static final String MV_SAME_PK_UNSELECT = "mv_same_pk_unselect";

    public enum InconsistencyType
    {
        MISSING_MV_ROW,             // Base table has data, MV doesn't
        EXTRA_MV_ROW,               // MV has data, base table doesn't (entire row deleted)
        EXPIRING_MISSING_MV_ROW,    // Base table has row with exipring TTL, MV doesn't
        STALE_MV_DATA,              // MV has outdated regular column (v2)
        NONE,                       // No inconsistency - tests idempotent rebuild
        // Only for case that MV has non-PK column in primary key
        STALE_MV_KEY,               // MV has outdated view key (v1) - requires rebuilding both old and new keys
        EXTRA_MV_KEY,               // MV has row with old v1 key, but v1 cell deleted from base table
        EXPIRING_MISSING_MV_KEY,    // Base row with v1 cell expiring, missing from MV
        MISSING_MV_KEY              // Base row with v1 cell, missing from MV
    }

    /**
     * Represents a single row with inconsistency type for testing.
     */
    private static class TestRow
    {
        final int pk;
        final int ck;
        final int v1;
        final Integer oldV1; // Only set for STALE_MV_KEY type
        final String v2;
        final InconsistencyType inconsistencyType;

        TestRow(int pk, int ck, int v1, String v2, InconsistencyType type)
        {
            this(pk, ck, v1, null, v2, type);
        }

        TestRow(int pk, int ck, int v1, Integer oldV1, String v2, InconsistencyType type)
        {
            this.pk = pk;
            this.ck = ck;
            this.v1 = v1;
            this.oldV1 = oldV1;
            this.v2 = v2;
            this.inconsistencyType = type;
        }

        public String toString()
        {
            return String.format("TestRow{pk=%d, ck=%d, v1=%d, oldV1=%s, v2=%s, type=%s}",
                                 pk, ck, v1, oldV1 == null ? "null" : oldV1.toString(), v2, inconsistencyType.name());
        }
    }

    /**
     * Test rebuild with multiple inconsistencies
     */
    @Test
    public void testRebuildWithMultipleInconsistencies() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withTokenSupplier(evenlyDistributedTokens(3, 1))
                                           .withNodeIdTopology(singleDcNetworkTopology(3, "dc0", "rack0"))
                                           .withConfig(config -> config.with(Feature.NETWORK, Feature.GOSSIP)
                                                                       .set("materialized_views_enabled", true)
                                                                       .set("materialized_view_auto_backfill_enabled", false)
                                                                       .set("direct_materialized_view_modification_enabled", true))
                                           .start()))
        {
            createSchema(cluster);
            // Test with a batch of rows with mixed inconsistency types
            int batchSize = 50;
            // Test 1;
            cluster.schemaChange(String.format(
            "CREATE MATERIALIZED VIEW %s.%s AS SELECT * FROM %s.%s " +
            "WHERE pk IS NOT NULL AND ck IS NOT NULL " +
            "PRIMARY KEY (ck, pk)", KEYSPACE, MV_SAME_PK, KEYSPACE, BASE_TABLE));
            testRebuildWithBatch(cluster, batchSize, 1000, MV_SAME_PK);
            cluster.schemaChange(String.format("DROP MATERIALIZED VIEW %s.%s", KEYSPACE, MV_SAME_PK));

            // Test 2;
            cluster.schemaChange(String.format(
            "CREATE MATERIALIZED VIEW %s.%s AS SELECT ck, pk, v1 FROM %s.%s " +
            "WHERE pk IS NOT NULL AND ck IS NOT NULL " +
            "PRIMARY KEY (ck, pk)", KEYSPACE, MV_SAME_PK_UNSELECT, KEYSPACE, BASE_TABLE));
            testRebuildWithBatch(cluster, batchSize, 2000, MV_SAME_PK_UNSELECT);
            cluster.schemaChange(String.format("DROP MATERIALIZED VIEW %s.%s", KEYSPACE, MV_SAME_PK_UNSELECT));

            // Test 3;
            cluster.schemaChange(String.format(
            "CREATE MATERIALIZED VIEW %s.%s AS SELECT * FROM %s.%s " +
            "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v1 IS NOT NULL " +
            "PRIMARY KEY (v1, ck, pk)", KEYSPACE, MV_NON_PK, KEYSPACE, BASE_TABLE));
            testRebuildWithBatch(cluster, batchSize, 3000, MV_NON_PK);
            cluster.schemaChange(String.format("DROP MATERIALIZED VIEW %s.%s", KEYSPACE, MV_NON_PK));

            // Test 4;
            cluster.schemaChange(String.format(
            "CREATE MATERIALIZED VIEW %s.%s AS SELECT v1, ck, pk FROM %s.%s " +
            "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v1 IS NOT NULL " +
            "PRIMARY KEY (v1, ck, pk)", KEYSPACE, MV_NON_PK_UNSELECT, KEYSPACE, BASE_TABLE));
            testRebuildWithBatch(cluster, batchSize, 4000, MV_NON_PK_UNSELECT);
            cluster.schemaChange(String.format("DROP MATERIALIZED VIEW %s.%s", KEYSPACE, MV_NON_PK_UNSELECT));
        }
    }

    /**
     * Test that MV rebuild must use serial reads when strict_mv_consistency is enabled.
     * <p>
     * This test demonstrates a bug where MV rebuild uses non-serial reads, causing it to miss
     * uncommitted paxos state. The scenario creates a situation where:
     * 1. A paxos transaction is left in Accepted (uncommitted) state
     * 2. Node 1 has the MV mutation applied locally, but other nodes don't, hence not applying base mutation
     * 3. MV rebuild uses non-serial read, sees no base data, and deletes the MV row
     * 4. Paxos repair later commits the transaction
     * 5. Result: Base table has data but MV doesn't, because MV rebuild's deletion shadows the writes
     * <p>
     * The fix is to use LOCAL_SERIAL reads in MV rebuild, which triggers paxos repair before
     * reading, ensuring uncommitted state is committed first.
     */
    @Test
    public void testRebuildWithMVRequiresSerialReadForStrictMV() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withTokenSupplier(evenlyDistributedTokens(3, 1))
                                           .withNodeIdTopology(singleDcNetworkTopology(3, "dc0", "rack0"))
                                           .withConfig(config -> config.with(Feature.NETWORK, Feature.GOSSIP)
                                                                       .set("materialized_views_enabled", true)
                                                                       .set("materialized_view_strict_consistency_enabled", true)
                                                                       .set("paxos_variant", "v2")
                                                                       .set("direct_materialized_view_modification_enabled", true)
                                                                       .set("view_key_rebuild_config.rebuild_on_deletion_enabled", true)
                                                                       .set("view_key_rebuild_config.apply_mutations_enabled", true)
                                                                       .set("view_key_rebuild_config.verbose_logging_enabled", true)
                                                                       .set("materialized_views_per_table_fail_threshold", 1)
                                                                       .set("paxos_repair_enabled", false))
                                           .withInstanceInitializer(BB::install)
                                           .start()))
        {
            createSchema(cluster);
            cluster.schemaChange(String.format("ALTER TABLE %s.%s WITH strict_mv_consistency = true", KEYSPACE, BASE_TABLE));
            cluster.schemaChange(String.format(
            "CREATE MATERIALIZED VIEW %s.%s AS SELECT * FROM %s.%s " +
            "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v1 IS NOT NULL " +
            "PRIMARY KEY (v1, ck, pk) WITH read_repair='NONE'", KEYSPACE, MV_NON_PK, KEYSPACE, BASE_TABLE));
            testRebuildWithMVRequiresSerialReadForStrictMVImpl(cluster, false);

            // disable auto paxos repair
            cluster.get(1).runOnInstance(() -> StorageService.instance.setPaxosRepairEnabled(false));
            cluster.coordinator(1).execute(String.format("TRUNCATE %s.%s", KEYSPACE, BASE_TABLE), ConsistencyLevel.ALL);
            cluster.schemaChange(String.format("DROP MATERIALIZED VIEW %s.%s", KEYSPACE, MV_NON_PK));
            cluster.schemaChange(String.format(
            "CREATE MATERIALIZED VIEW %s.%s AS SELECT * FROM %s.%s " +
            "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v1 IS NOT NULL " +
            "PRIMARY KEY (v1, ck, pk) WITH read_repair='NONE'", KEYSPACE, MV_NON_PK, KEYSPACE, BASE_TABLE));
            testRebuildWithMVRequiresSerialReadForStrictMVImpl(cluster, true);
        }
    }

    private void testRebuildWithMVRequiresSerialReadForStrictMVImpl(Cluster cluster, boolean serialRead) throws Exception
    {
        int pk = 100;
        int ck = 1;
        int v1 = 1000;
        String v2 = "test_value";

        // Step 1: Perform LWT with dropped MUTATION_REQ messages to simulate MV mutation failure
        // Drop MUTATION_REQ from node 1 to nodes 2 & 3, so only node 1 receives MV mutations
        // This will cause MV mutations to fail quorum, preventing paxos commit
        IMessageFilters.Filter dropMutationFilter = cluster.filters().verbs(Verb.MUTATION_REQ.id).from(1).to(2, 3).drop();
        try
        {
            cluster.coordinator(1).execute(
            String.format("INSERT INTO %s.%s (pk, ck, v1, v2) VALUES (?, ?, ?, ?) IF NOT EXISTS", KEYSPACE, BASE_TABLE),
            ConsistencyLevel.ALL, pk, ck, v1, v2);
        }
        catch (Throwable t)
        {
            // Expected: WriteTimeoutException because MV mutations can't achieve quorum
            assertTrue("Expected WriteTimeoutException but got: " + t.getClass().getSimpleName(),
                       t.getClass().getSimpleName().contains("WriteTimeoutException"));
        }

        // Step 2: Verify the state after failed LWT
        // Paxos is in Accepted (uncommitted) state - base table has NO data on any node
        SimpleQueryResult baseResult = cluster.coordinator(1).executeWithResult(
        String.format("SELECT * FROM %s.%s WHERE pk = ? AND ck = ?", KEYSPACE, BASE_TABLE),
        ConsistencyLevel.ALL, pk, ck);
        assertFalse("Base table should NOT have the row (paxos not committed)", baseResult.hasNext());

        // One of the nodes has the MV mutation
        SimpleQueryResult mvResult = cluster.coordinator(1).executeWithResult(
        String.format("SELECT * FROM %s.%s WHERE v1 = ? AND ck = ? AND pk = ?", KEYSPACE, MV_NON_PK),
        ConsistencyLevel.ALL, v1, ck, pk);
        assertTrue("MV should have the row read from ALL (applied locally despite quorum failure)", mvResult.hasNext());

        // Step 3: Trigger MV rebuild by deleting from the MV (BEFORE paxos repair!)
        // Delete from MV triggers rebuild, which reads base table to determine correct MV state (node 2 use non-serial read)
        dropMutationFilter.off();
        if (serialRead)
            cluster.coordinator(1).execute(String.format("DELETE FROM %s.%s WHERE v1 = ? AND ck = ? AND pk = ?", KEYSPACE, MV_NON_PK), ConsistencyLevel.LOCAL_QUORUM, v1, ck, pk);
        else
            cluster.coordinator(2).execute(String.format("DELETE FROM %s.%s WHERE v1 = ? AND ck = ? AND pk = ?", KEYSPACE, MV_NON_PK), ConsistencyLevel.LOCAL_QUORUM, v1, ck, pk);

        // Step 4: Run paxos repair to commit the accepted transaction
        // Paxos repair detects the uncommitted Accepted state and commits it
        // Now the base table has the row on all nodes
        cluster.get(1).runOnInstance(() -> {
            StorageService.instance.setPaxosRepairEnabled(true);
            try
            {
                TableId tableId = Schema.instance.getTableMetadata(KEYSPACE, BASE_TABLE).id;
                StorageService.instance.autoRepairPaxos(tableId).get();
            }
            catch (Exception e)
            {
                throw new RuntimeException("Paxos repair failed", e);
            }
        });

        // Step 5: Verify final state - demonstrates the bug
        baseResult = cluster.coordinator(1).executeWithResult(
        String.format("SELECT * FROM %s.%s WHERE pk = ? AND ck = ?", KEYSPACE, BASE_TABLE),
        ConsistencyLevel.ALL, pk, ck);
        int baseRowCount = countRows(baseResult);

        mvResult = cluster.coordinator(1).executeWithResult(
        String.format("SELECT * FROM %s.%s WHERE v1 = ? AND ck = ? AND pk = ?", KEYSPACE, MV_NON_PK),
        ConsistencyLevel.ALL, v1, ck, pk);
        int mvRowCount = countRows(mvResult);

        if (serialRead)
        {
            assertEquals(baseRowCount, mvRowCount);
        }
        else
        {
            // Base table has 1 row (committed by paxos repair)
            // MV has 0 rows (incorrectly deleted by rebuild that used non-serial read)
            assertEquals(1, baseRowCount);
            assertEquals(0, mvRowCount);
        }
    }

    private void createSchema(Cluster cluster)
    {
        cluster.schemaChange(String.format(
            "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = " +
            "{'class': 'NetworkTopologyStrategy', 'dc0': 3}", KEYSPACE));

        cluster.schemaChange(String.format(
            "CREATE TABLE %s.%s (" +
            "  pk int," +
            "  ck int," +
            "  v1 int," +
            "  v2 text," +
            "  PRIMARY KEY (pk, ck)" +
            ") WITH read_repair='NONE'" , KEYSPACE, BASE_TABLE));
    }

    private void testRebuildWithBatch(Cluster cluster, int batchSize, int baseOffset, String mvName) throws Exception
    {
        // Truncate tables - both base table AND the materialized view
        cluster.coordinator(1).execute(String.format("TRUNCATE %s.%s", KEYSPACE, BASE_TABLE), ConsistencyLevel.ALL);

        // Generate test rows with random inconsistency types
        List<TestRow> testRows = generateTestRows(batchSize, baseOffset, mvName);

        // Insert all data into base table
        for (TestRow row : testRows)
        {
            // For STALE_MV_KEY, insert with OLD v1 first (will be updated later to NEW v1)
            int insertV1 = (row.inconsistencyType == InconsistencyType.STALE_MV_KEY)
                           ? row.oldV1 : row.v1;
            // Skip insert here (will be injected later to not create related MV row)
            if (row.inconsistencyType == InconsistencyType.MISSING_MV_ROW
                || row.inconsistencyType == InconsistencyType.MISSING_MV_KEY
                || row.inconsistencyType == InconsistencyType.EXPIRING_MISSING_MV_ROW
                || row.inconsistencyType == InconsistencyType.EXPIRING_MISSING_MV_KEY)
                continue;
            cluster.coordinator(1).execute(
                String.format("INSERT INTO %s.%s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", KEYSPACE, BASE_TABLE),
                ConsistencyLevel.ALL, row.pk, row.ck, insertV1, row.v2);
        }
        cluster.forEach(instance -> {
            instance.runOnInstance(() -> {
                try
                {
                    Field field = ColumnFamilyStore.class.getDeclaredField("TEST_SKIP_VIEW_UPDATE");
                    field.setAccessible(true);
                    field.setBoolean(null, true);
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            });
        });

        // Inject inconsistencies
        for (TestRow row : testRows)
        {
            injectInconsistencyOnNode(cluster, 1, row);
            injectInconsistencyOnNode(cluster, 2, row);
            injectInconsistencyOnNode(cluster, 3, row);
        }
        // Verify inconsistencies exist
        verifyInconsistency(cluster, mvName, testRows);


        // Enable rebuild on all nodes
        cluster.forEach(instance -> {
            instance.runOnInstance(() -> {
                try
                {
                    Field field = ColumnFamilyStore.class.getDeclaredField("TEST_SKIP_VIEW_UPDATE");
                    field.setAccessible(true);
                    field.setBoolean(null, false);
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
                DatabaseDescriptor.setViewKeyRebuildOnDeletionEnabled(true);
                DatabaseDescriptor.setViewKeyRebuildApplyMutationsEnabled(true);
                DatabaseDescriptor.setViewKeyRebuildVerboseLoggingEnabled(true);
            });
        });

        // Rebuild all affected rows by issuing DELETE operations to the MV through node 1
        for (TestRow row : testRows)
        {
            if (mvName.equals(MV_SAME_PK) || mvName.equals(MV_SAME_PK_UNSELECT))
            {
                cluster.coordinator(1)
                       .execute(String.format("DELETE FROM %s.%s WHERE pk = ? AND ck = ?", KEYSPACE, mvName),
                                ConsistencyLevel.LOCAL_QUORUM, row.pk, row.ck);
            }
            else
            {
                cluster.coordinator(1)
                       .execute(String.format("DELETE FROM %s.%s WHERE v1 = ? AND ck = ? AND pk = ?", KEYSPACE, mvName),
                                ConsistencyLevel.LOCAL_QUORUM, row.v1, row.ck, row.pk);
                // Trigger rebuild with old v1 key as well to remove the old MV row
                if (row.inconsistencyType == InconsistencyType.STALE_MV_KEY)
                    cluster.coordinator(1)
                           .execute(String.format("DELETE FROM %s.%s WHERE v1 = ? AND ck = ? AND pk = ?", KEYSPACE, mvName),
                                    ConsistencyLevel.LOCAL_QUORUM, row.oldV1, row.ck, row.pk);
            }
        }

        // Verify data content matches
        verifyDataConsistency(cluster, mvName, testRows);

        Thread.sleep(10_000);
        // should see rows still match after TTL expiry
        verifyDataConsistency(cluster, mvName, testRows);
    }

    private List<TestRow> generateTestRows(int batchSize, int baseOffset, String mvName)
    {
        boolean hasNonPK = mvName.equals(MV_NON_PK) || mvName.equals(MV_NON_PK_UNSELECT);

        List<TestRow> testRows = new ArrayList<>();
        Random random = new Random(baseOffset);
        InconsistencyType[] allTypes;

        if (hasNonPK)
        {
            allTypes = InconsistencyType.values();
        }
        else
        {
            // Exclude STALE_MV_KEY, EXTRA_MV_KEY, EXPIRING_MISSING_MV_KEY for same-PK MVs
            allTypes = new InconsistencyType[] {
                InconsistencyType.MISSING_MV_ROW,
                InconsistencyType.EXPIRING_MISSING_MV_ROW,
                InconsistencyType.EXTRA_MV_ROW,
                InconsistencyType.STALE_MV_DATA,
                InconsistencyType.NONE
            };
        }

        for (int i = 0; i < batchSize; i++)
        {
            int pk = baseOffset + i;
            int oldV1 = baseOffset * 100 + i * 10;
            String v2 = "value_" + baseOffset + '_' + i;
            InconsistencyType type = allTypes[random.nextInt(allTypes.length)];

            if (type == InconsistencyType.STALE_MV_KEY)
            {
                int newV1 = oldV1 + 50000;
                testRows.add(new TestRow(pk, i, newV1, oldV1, v2, type));
            }
            else
            {
                testRows.add(new TestRow(pk, i, oldV1, v2, type));
            }
        }

        return testRows;
    }

    private void injectInconsistencyOnNode(Cluster cluster, int nodeNum, TestRow row)
    {
        int pk = row.pk;
        int ck = row.ck;
        int v1 = row.v1;
        String v2 = row.v2;
        InconsistencyType type = row.inconsistencyType;

        cluster.get(nodeNum).runOnInstance(() -> {
            switch (type)
            {
                case MISSING_MV_ROW:
                    createMissingMVRow(pk, ck, v1, v2);
                    break;
                case EXTRA_MV_ROW:
                    deleteBaseTableRowDirectly(pk, ck);
                    break;
                case EXPIRING_MISSING_MV_ROW:
                    createMissingMVRow(pk, ck, v1, v2, 8, -1); // row with TTL
                    break;
                case STALE_MV_DATA:
                    updateBaseTableV2Directly(pk, ck);
                    break;
                case STALE_MV_KEY:
                    updateBaseTableV1Directly(pk, ck, v1, v2);
                    break;
                case EXTRA_MV_KEY:
                    deleteBaseTableV1CellDirectly(pk, ck);
                    break;
                case MISSING_MV_KEY:
                    createMissingMVKey(pk, ck, v1);
                    break;
                case EXPIRING_MISSING_MV_KEY:
                    createMissingMVKey(pk, ck, v1, 8); // v1 with TTL
                case NONE:
                    // No inconsistency - data remains consistent
                    break;
            }
        });
    }

    private void verifyInconsistency(Cluster cluster, String mvName, List<TestRow> testRows)
    {
        for (TestRow row : testRows)
        {
            SimpleQueryResult baseResult = cluster.coordinator(1).executeWithResult(
                String.format("SELECT * FROM %s.%s WHERE pk = ? AND ck = ?", KEYSPACE, BASE_TABLE),
                ConsistencyLevel.ALL, row.pk, row.ck);

            boolean isNonPKMV = mvName.equals(MV_NON_PK) || mvName.equals(MV_NON_PK_UNSELECT);
            SimpleQueryResult mvResult = isNonPKMV
                                         ? cluster.coordinator(1)
                                                  .executeWithResult(String.format("SELECT * FROM %s.%s WHERE v1 = ? AND ck = ? AND pk = ?", KEYSPACE, mvName),
                                                                     ConsistencyLevel.ALL, row.v1, row.ck, row.pk)
                                         : cluster.coordinator(1)
                                                  .executeWithResult(String.format("SELECT * FROM %s.%s WHERE pk = ? AND ck = ?", KEYSPACE, mvName),
                                                                     ConsistencyLevel.ALL, row.pk, row.ck);
            switch (row.inconsistencyType)
            {
                case MISSING_MV_ROW:
                case EXPIRING_MISSING_MV_ROW:
                    // Base should have row, MV should not
                    assertEquals("Base table should have row for MISSING_MV_ROW", 1, countRows(baseResult));
                    assertEquals("MV should NOT have row for MISSING_MV_ROW", 0, countRows(mvResult));
                    break;
                case EXTRA_MV_ROW:
                    // Base should not have row, MV should
                    assertEquals("Base table should NOT have row for EXTRA_MV_ROW", 0, countRows(baseResult));
                    assertEquals("MV should have row for EXTRA_MV_ROW", 1, countRows(mvResult));
                    break;

                case STALE_MV_DATA:
                    // Both should exist, but MV should have outdated v2 (old value would be from initial insert)
                    assertTrue("Base table should have row for STALE_MV_DATA", baseResult.hasNext());
                    if (mvName.equals(MV_NON_PK) || mvName.equals(MV_SAME_PK))
                    {
                        assertTrue("MV should have row for STALE_MV_DATA", mvResult.hasNext());
                        assertNotEquals("v2 should be different between base and MV for STALE_MV_DATA",
                                     baseResult.next().getString("v2"), mvResult.next().getString("v2"));
                    }
                    else
                    {
                        // v2 not selected in MV, so just check row existence
                        assertTrue("MV should have row for STALE_MV_DATA", mvResult.hasNext());
                        mvResult.next();
                    }
                    assertFalse(baseResult.hasNext());
                    assertFalse(mvResult.hasNext());
                    break;

                case STALE_MV_KEY:
                    // Only applicable for non-PK MVs
                    // Base should have new v1, MV should have old v1 key
                    if (isNonPKMV)
                    {
                        assertEquals("Base table should have row for STALE_MV_KEY", 1, countRows(baseResult));
                        SimpleQueryResult mvOldKeyResult = cluster.coordinator(1)
                                                                  .executeWithResult(String.format("SELECT * FROM %s.%s WHERE v1 = ? AND ck = ? AND pk = ?", KEYSPACE, mvName),
                                                                                     ConsistencyLevel.ALL, row.oldV1, row.ck, row.pk);
                        assertEquals("MV should have row with OLD v1 for STALE_MV_KEY", 1, countRows(mvOldKeyResult));
                        assertEquals("MV should NOT have row with NEW v1 for STALE_MV_KEY", 0, countRows(mvResult));
                    }
                    break;
                case EXTRA_MV_KEY:
                    // Only applicable for non-PK MVs
                    // Base should have row but v1 is null/deleted, MV should have v1 key
                    if (isNonPKMV)
                    {
                        assertTrue("Base table should have row for EXTRA_MV_KEY", baseResult.hasNext());
                        org.apache.cassandra.distributed.api.Row baseRow = baseResult.next();
                        assertNull("Base v1 should be null for EXTRA_MV_KEY", baseRow.getInteger("v1"));
                        assertEquals("MV should have row with v1 for EXTRA_MV_KEY", 1, countRows(mvResult));
                    }
                    break;
                case MISSING_MV_KEY:
                    // Only applicable for non-PK MVs
                    if (isNonPKMV)
                    {
                        assertEquals("Base table should have row for MISSING_MV_KEY", 1, countRows(baseResult));
                        assertEquals("MV should NOT have row for MISSING_MV_KEY", 0, countRows(mvResult));
                    }
                    break;
                case EXPIRING_MISSING_MV_KEY:
                    // Only applicable for non-PK MVs
                    // Base should have row with expiring v1, MV should not have it
                    if (isNonPKMV)
                    {
                        assertEquals("Base table should have row for EXPIRING_MISSING_MV_KEY", 1, countRows(baseResult));
                        assertEquals("MV should NOT have row for EXPIRING_MISSING_MV_KEY", 0, countRows(mvResult));
                    }
                    break;
                case NONE:
                    // Both should be consistent - row exists in both or neither
                    assertEquals("Base and MV should be consistent for NONE type", countRows(baseResult), countRows(mvResult));
                    break;
            }
        }
    }

    private void verifyDataConsistency(Cluster cluster, String mvName, List<TestRow> testRows)
    {
        for (TestRow row : testRows)
        {
            SimpleQueryResult baseResult = cluster.coordinator(1).executeWithResult(
                String.format("SELECT v1, v2 FROM %s.%s WHERE pk = ? AND ck = ?", KEYSPACE, BASE_TABLE),
                ConsistencyLevel.ALL, row.pk, row.ck);

            if (baseResult.hasNext())
            {
                org.apache.cassandra.distributed.api.Row baseRow = baseResult.next();
                Integer baseV1 = baseRow.getInteger("v1");
                String baseV2 = baseRow.getString("v2");

                if (mvName.equals(MV_NON_PK) || mvName.equals(MV_NON_PK_UNSELECT))
                {
                    if (baseV1 != null)
                    {
                        // Row has v1, so it should exist in MV (for non-PK column MVs)
                        SimpleQueryResult mvResult = cluster.coordinator(1).executeWithResult(
                        String.format("SELECT * FROM %s.%s WHERE v1 = ? AND ck = ? AND pk = ?", KEYSPACE, mvName),
                        ConsistencyLevel.ALL, baseV1, row.ck, row.pk);
                        int rowCnt = 0;
                        while (mvResult.hasNext())
                        {
                            org.apache.cassandra.distributed.api.Row mvRow = mvResult.next();
                            rowCnt++;
                            assertEquals("MV v1 should match base table v1", baseV1, mvRow.getInteger("v1"));
                            if (mvName.equals(MV_NON_PK))
                                assertEquals("MV v2 should match base table v2", baseV2, mvRow.getString("v2"));
                        }
                        assertEquals("There should be exactly one matching row in MV", 1, rowCnt);
                    }
                    else
                    {
                        // Row doesn't have v1, so it shouldn't exist in MV
                        SimpleQueryResult mvResult = cluster.coordinator(1).executeWithResult(
                        String.format("SELECT * FROM %s.%s WHERE v1 = ? AND ck = ? AND pk = ?", KEYSPACE, mvName),
                        ConsistencyLevel.ALL, row.v1, row.ck, row.pk);
                        assertEquals("Row should not exist in MV if v1 is null in base table", 0, countRows(mvResult));
                    }
                }
                else
                {
                    // For same-PK MVs
                    SimpleQueryResult mvResult = cluster.coordinator(1).executeWithResult(
                        String.format("SELECT * FROM %s.%s WHERE pk = ? AND ck = ?", KEYSPACE, mvName),
                        ConsistencyLevel.ALL, row.pk, row.ck);
                    assertTrue("MV should have row", mvResult.hasNext());
                    org.apache.cassandra.distributed.api.Row mvRow = mvResult.next();
                    // v1 and v2 should match
                    assertEquals("MV v1 should match base table v1", baseV1, mvRow.getInteger("v1"));
                    if (mvName.equals(MV_SAME_PK))
                        assertEquals("MV v2 should match base table v2", baseV2, mvRow.getString("v2"));
                    // should be exactly one row
                    assertFalse("Should only have exactly one row", mvResult.hasNext());
                }
            }
            else
            {
                // If row doesn't exist in base table, it shouldn't exist in MV either
                SimpleQueryResult mvResult = (mvName.equals(MV_NON_PK) || mvName.equals(MV_NON_PK_UNSELECT))
                                             ? cluster.coordinator(1).executeWithResult(String.format("SELECT * FROM %s.%s WHERE v1 = ? AND ck = ? AND pk = ?", KEYSPACE, mvName), ConsistencyLevel.ALL, row.v1, row.ck, row.pk)
                                             : cluster.coordinator(1).executeWithResult(String.format("SELECT * FROM %s.%s WHERE pk = ? AND ck = ?", KEYSPACE, mvName), ConsistencyLevel.ALL, row.pk, row.ck);
                assertEquals("Test row " + row + " should not exist in MV if it doesn't exist in base table", 0, countRows(mvResult));
            }
        }
    }

    private int countRows(SimpleQueryResult result)
    {
        int count = 0;
        while (result.hasNext())
        {
            result.next();
            count++;
        }
        return count;
    }

    // ===== Inconsistency injection methods =====

    private static void createMissingMVRow(int pk, int ck, int v1, String v2)
    {
        createMissingMVRow(pk, ck, v1, v2, -1, -1);
    }

    private static void createMissingMVRow(int pk, int ck, int v1, String v2, int rowTTL, int v2TTL)
    {
        ColumnFamilyStore baseCfs = Keyspace.open(RebuildMVKeyTest.KEYSPACE).getColumnFamilyStore(RebuildMVKeyTest.BASE_TABLE);
        TableMetadata baseMetadata = baseCfs.metadata();

        ByteBuffer partitionKey = baseMetadata.partitionKeyType.fromString(String.valueOf(pk));
        ByteBuffer clusteringKey = baseMetadata.clusteringColumns().get(0).type.fromString(String.valueOf(ck));

        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(
            baseMetadata,
            baseMetadata.partitioner.decorateKey(partitionKey),
            baseMetadata.regularAndStaticColumns(),
            1
        );

        Row.Builder rowBuilder = BTreeRow.unsortedBuilder();
        rowBuilder.newRow(baseMetadata.comparator.make(clusteringKey));
        if (rowTTL == -1)
            rowBuilder.addPrimaryKeyLivenessInfo(LivenessInfo.create(FBUtilities.timestampMicros(), LivenessInfo.NO_TTL, FBUtilities.nowInSeconds()));
        else
            rowBuilder.addPrimaryKeyLivenessInfo(LivenessInfo.expiring(FBUtilities.timestampMicros(), rowTTL, FBUtilities.nowInSeconds()));

        ColumnMetadata v1Col = baseMetadata.getColumn(ByteBufferUtil.bytes("v1"));
        rowBuilder.addCell(BufferCell.live(v1Col, FBUtilities.timestampMicros(), ByteBufferUtil.bytes(v1)));
        ColumnMetadata v2Col = baseMetadata.getColumn(ByteBufferUtil.bytes("v2"));
        if (v2TTL == -1)
            rowBuilder.addCell(BufferCell.live(v2Col, FBUtilities.timestampMicros(), ByteBufferUtil.bytes(v2)));
        else
            rowBuilder.addCell(BufferCell.expiring(v2Col, FBUtilities.timestampMicros(), v2TTL, FBUtilities.nowInSeconds(), ByteBufferUtil.bytes(v2)));

        builder.add(rowBuilder.build());

        Mutation mutation = new Mutation(builder.build());
        Keyspace.open(RebuildMVKeyTest.KEYSPACE).apply(mutation, false, false);
        baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    private static void createMissingMVKey(int pk, int ck, int v1)
    {
        createMissingMVKey(pk, ck, v1, -1);
    }

    private static void createMissingMVKey(int pk, int ck, int v1, int v1TTL)
    {
        ColumnFamilyStore baseCfs = Keyspace.open(RebuildMVKeyTest.KEYSPACE).getColumnFamilyStore(RebuildMVKeyTest.BASE_TABLE);
        TableMetadata baseMetadata = baseCfs.metadata();

        ByteBuffer partitionKey = baseMetadata.partitionKeyType.fromString(String.valueOf(pk));
        ByteBuffer clusteringKey = baseMetadata.clusteringColumns().get(0).type.fromString(String.valueOf(ck));

        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(
        baseMetadata,
        baseMetadata.partitioner.decorateKey(partitionKey),
        baseMetadata.regularAndStaticColumns(),
        1
        );

        Row.Builder rowBuilder = BTreeRow.unsortedBuilder();
        rowBuilder.newRow(baseMetadata.comparator.make(clusteringKey));
        // no row liveness info (update cell only)
        rowBuilder.addPrimaryKeyLivenessInfo(LivenessInfo.EMPTY);

        ColumnMetadata v1Col = baseMetadata.getColumn(ByteBufferUtil.bytes("v1"));
        if (v1TTL == -1)
            rowBuilder.addCell(BufferCell.live(v1Col, FBUtilities.timestampMicros(), ByteBufferUtil.bytes(v1)));
        else
            rowBuilder.addCell(BufferCell.expiring(v1Col, FBUtilities.timestampMicros(), v1TTL, FBUtilities.nowInSeconds(), ByteBufferUtil.bytes(v1)));

        builder.add(rowBuilder.build());
        Mutation mutation = new Mutation(builder.build());
        Keyspace.open(RebuildMVKeyTest.KEYSPACE).apply(mutation, true, false);
    }

    private static void deleteBaseTableRowDirectly(int pk, int ck)
    {
        ColumnFamilyStore baseCfs = Keyspace.open(RebuildMVKeyTest.KEYSPACE).getColumnFamilyStore(RebuildMVKeyTest.BASE_TABLE);
        TableMetadata baseMetadata = baseCfs.metadata();

        ByteBuffer partitionKey = baseMetadata.partitionKeyType.fromString(String.valueOf(pk));
        ByteBuffer clusteringKey = baseMetadata.clusteringColumns().get(0).type.fromString(String.valueOf(ck));

        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(
            baseMetadata,
            baseMetadata.partitioner.decorateKey(partitionKey),
            baseMetadata.regularAndStaticColumns(),
            1
        );

        Row.Builder rowBuilder = BTreeRow.unsortedBuilder();
        rowBuilder.newRow(baseMetadata.comparator.make(clusteringKey));
        rowBuilder.addRowDeletion(Row.Deletion.regular(new DeletionTime(FBUtilities.timestampMicros(), FBUtilities.nowInSeconds())));

        builder.add(rowBuilder.build());
        Mutation mutation = new Mutation(builder.build());
        Keyspace.open(RebuildMVKeyTest.KEYSPACE).apply(mutation, true, false);
    }

    private static void updateBaseTableV2Directly(int pk, int ck)
    {
        ColumnFamilyStore baseCfs = Keyspace.open(RebuildMVKeyTest.KEYSPACE).getColumnFamilyStore(RebuildMVKeyTest.BASE_TABLE);
        TableMetadata baseMetadata = baseCfs.metadata();

        ByteBuffer partitionKey = baseMetadata.partitionKeyType.fromString(String.valueOf(pk));
        ByteBuffer clusteringKey = baseMetadata.clusteringColumns().get(0).type.fromString(String.valueOf(ck));

        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(
            baseMetadata,
            baseMetadata.partitioner.decorateKey(partitionKey),
            baseMetadata.regularAndStaticColumns(),
            1
        );

        Row.Builder rowBuilder = BTreeRow.unsortedBuilder();
        rowBuilder.newRow(baseMetadata.comparator.make(clusteringKey));
        long timestamp = FBUtilities.timestampMicros();

        ColumnMetadata v2Col = baseMetadata.getColumn(ByteBufferUtil.bytes("v2"));
        if (v2Col != null)
        {
            Cell<?> cell = BufferCell.live(v2Col, timestamp, ByteBufferUtil.bytes("newV2"));
            rowBuilder.addCell(cell);
        }

        builder.add(rowBuilder.build());
        Mutation mutation = new Mutation(builder.build());
        Keyspace.open(RebuildMVKeyTest.KEYSPACE).apply(mutation, true, false);
    }

    private static void updateBaseTableV1Directly(int pk, int ck, int newV1, String v2)
    {
        ColumnFamilyStore baseCfs = Keyspace.open(RebuildMVKeyTest.KEYSPACE).getColumnFamilyStore(RebuildMVKeyTest.BASE_TABLE);
        TableMetadata baseMetadata = baseCfs.metadata();

        ByteBuffer partitionKey = baseMetadata.partitionKeyType.fromString(String.valueOf(pk));
        ByteBuffer clusteringKey = baseMetadata.clusteringColumns().get(0).type.fromString(String.valueOf(ck));

        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(
            baseMetadata,
            baseMetadata.partitioner.decorateKey(partitionKey),
            baseMetadata.regularAndStaticColumns(),
            1
        );

        Row.Builder rowBuilder = BTreeRow.unsortedBuilder();
        rowBuilder.newRow(baseMetadata.comparator.make(clusteringKey));
        long timestamp = FBUtilities.timestampMicros();

        ColumnMetadata v1Col = baseMetadata.getColumn(ByteBufferUtil.bytes("v1"));
        if (v1Col != null)
        {
            ByteBuffer v1Value = ByteBufferUtil.bytes(newV1);
            Cell<?> v1Cell = BufferCell.live(v1Col, timestamp, v1Value);
            rowBuilder.addCell(v1Cell);
        }

        ColumnMetadata v2Col = baseMetadata.getColumn(ByteBufferUtil.bytes("v2"));
        if (v2Col != null)
        {
            Cell<?> v2Cell = BufferCell.live(v2Col, timestamp, ByteBufferUtil.bytes(v2));
            rowBuilder.addCell(v2Cell);
        }

        builder.add(rowBuilder.build());
        Mutation mutation = new Mutation(builder.build());
        Keyspace.open(RebuildMVKeyTest.KEYSPACE).apply(mutation, true, false);
    }

    private static void deleteBaseTableV1CellDirectly(int pk, int ck)
    {
        ColumnFamilyStore baseCfs = Keyspace.open(RebuildMVKeyTest.KEYSPACE).getColumnFamilyStore(RebuildMVKeyTest.BASE_TABLE);
        TableMetadata baseMetadata = baseCfs.metadata();

        ByteBuffer partitionKey = baseMetadata.partitionKeyType.fromString(String.valueOf(pk));
        ByteBuffer clusteringKey = baseMetadata.clusteringColumns().get(0).type.fromString(String.valueOf(ck));

        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(
            baseMetadata,
            baseMetadata.partitioner.decorateKey(partitionKey),
            baseMetadata.regularAndStaticColumns(),
            1
        );

        Row.Builder rowBuilder = BTreeRow.unsortedBuilder();
        rowBuilder.newRow(baseMetadata.comparator.make(clusteringKey));
        long timestamp = FBUtilities.timestampMicros();

        ColumnMetadata v1Col = baseMetadata.getColumn(ByteBufferUtil.bytes("v1"));
        if (v1Col != null)
        {
            Cell<?> v1Cell = BufferCell.tombstone(v1Col, timestamp, FBUtilities.nowInSeconds());
            rowBuilder.addCell(v1Cell);
        }

        builder.add(rowBuilder.build());
        Mutation mutation = new Mutation(builder.build());
        Keyspace.open(RebuildMVKeyTest.KEYSPACE).apply(mutation, true, false);
    }

    public static class BB
    {
        public static void install(ClassLoader classLoader, Integer num)
        {
            // Only install on node 2
            if (num != 2)
                return;

            new ByteBuddy().rebase(StorageProxy.class)
                           .method(named("readOne"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static RowIterator readOne(SinglePartitionReadCommand command,
                                          org.apache.cassandra.db.ConsistencyLevel consistencyLevel,
                                          Dispatcher.RequestTime requestTime)
        throws Exception
        {
            return PartitionIterators.getOnlyElement(
                StorageProxy.read(SinglePartitionReadCommand.Group.one(command),
                                 org.apache.cassandra.db.ConsistencyLevel.LOCAL_QUORUM,
                                 requestTime),
                command);
        }
    }
}
