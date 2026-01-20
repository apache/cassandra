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

package org.apache.cassandra.cql3;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.psjava.util.AssertStatus.assertNotNull;

public class RebuildMVKeyTest extends ViewAbstractTest
{
    @Before
    @Override
    public void beforeTest() throws Throwable
    {
        super.beforeTest();
        createTable("CREATE TABLE %s (pk int, ck int, v1 int, v2 text, PRIMARY KEY (pk, ck))");
    }

    @After
    public void afterTest() throws Throwable
    {
        DatabaseDescriptor.setDirectMaterializedViewModification(false);
        DatabaseDescriptor.setViewKeyRebuildOnDeletionEnabled(false);
        DatabaseDescriptor.setViewKeyRebuildApplyMutationsEnabled(false);
        DatabaseDescriptor.setViewKeyRebuildViewReadEnabled(false);
        DatabaseDescriptor.setViewKeyRebuildVerboseLoggingEnabled(false);
    }

    @Test
    public void testRebuildKeyOnMVMetric() throws Throwable
    {
        DatabaseDescriptor.setDirectMaterializedViewModification(true);
        DatabaseDescriptor.setViewKeyRebuildOnDeletionEnabled(true);
        DatabaseDescriptor.setViewKeyRebuildApplyMutationsEnabled(true);
        String mvName = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                   "WHERE pk IS NOT NULL AND ck IS NOT NULL " +
                                   "PRIMARY KEY (pk, ck)");

        execute("TRUNCATE %s");
        String mvQuery = String.format("DELETE FROM %s.%s WHERE pk = ? AND ck = ?", keyspace(), mvName);
        executeNet(mvQuery, 1, 1);

        TableMetadata viewMetadata = Schema.instance.getTableMetadata(keyspace(), mvName);
        assertNotNull(viewMetadata);
        ColumnFamilyStore viewCfs = Schema.instance.getColumnFamilyStoreInstance(viewMetadata.id);
        assertNotNull(viewCfs.metric.viewRebuildKeyTime);
        assertEquals(1, viewCfs.metric.viewRebuildKeyTime.cf.getCount());
        // non-zero write latency as mutation is applied
        assertTrue(viewCfs.metric.writeLatency.latencyMeter.getCount() > 0);
    }

    @Test
    public void testRebuildKeyWithoutMutation() throws Throwable
    {
        DatabaseDescriptor.setDirectMaterializedViewModification(true);
        DatabaseDescriptor.setViewKeyRebuildOnDeletionEnabled(true);
        DatabaseDescriptor.setViewKeyRebuildApplyMutationsEnabled(false);
        String mvName = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                   "WHERE pk IS NOT NULL AND ck IS NOT NULL " +
                                   "PRIMARY KEY (pk, ck)");

        execute("TRUNCATE %s");
        String mvQuery = String.format("DELETE FROM %s.%s WHERE pk = ? AND ck = ?", keyspace(), mvName);
        executeNet(mvQuery, 1, 1);

        TableMetadata viewMetadata = Schema.instance.getTableMetadata(keyspace(), mvName);
        assertNotNull(viewMetadata);
        ColumnFamilyStore viewCfs = Schema.instance.getColumnFamilyStoreInstance(viewMetadata.id);
        assertNotNull(viewCfs.metric.viewRebuildKeyTime);
        assertEquals(1, viewCfs.metric.viewRebuildKeyTime.cf.getCount());
        // zero write latency as no mutation is applied
        assertEquals(0, viewCfs.metric.writeLatency.latencyMeter.getCount());
    }

    @Test
    public void testRebuildKeyOnMVValidation() throws Throwable
    {
        DatabaseDescriptor.setDirectMaterializedViewModification(true);
        DatabaseDescriptor.setViewKeyRebuildOnDeletionEnabled(true);
        DatabaseDescriptor.setViewKeyRebuildApplyMutationsEnabled(true);
        String mvName = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                   "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v1 IS NOT NULL " +
                                   "PRIMARY KEY (pk, ck, v1)");

        execute("TRUNCATE %s");

        try
        {
            executeNet(String.format("DELETE FROM %s.%s WHERE pk = 1 AND ck = 1 AND v1 IN (1,2)", keyspace(), mvName));
            fail("expect InvalidQueryException");
        }
        catch (InvalidQueryException e)
        {
            assertTrue(e.getMessage().contains("Cannot use IN restrictions in rebuildMVKey"));
        }

        try
        {
            executeNet(String.format("DELETE FROM %s.%s WHERE pk = 1 AND ck = 1", keyspace(), mvName));
            fail("expect InvalidQueryException");
        }
        catch (InvalidQueryException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("rebuildMVKey requires all primary key restricted by equalities"));
        }

        try
        {
            executeNet(String.format("INSERT INTO %s.%s (pk, ck, v1) VALUES (1, 1, 1)", keyspace(), mvName));
            fail("expect InvalidQueryException");
        }
        catch (InvalidQueryException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("Can only use DELETE statements to rebuildMVKey"));
        }

        try
        {
            executeNet(String.format("UPDATE %s.%s SET v2 = 'test' WHERE pk = 1 AND ck = 1 AND v1 = 1", keyspace(), mvName));
            fail("expect InvalidQueryException");
        }
        catch (InvalidQueryException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("Can only use DELETE statements to rebuildMVKey"));
        }

    }

    /**
     * Test rebuildMVKey with view comparison enabled.
     * Verifies that the comparison code path is exercised without errors.
     * Detailed comparison logic is tested in ViewUtilsRowComparisonTest.
     */
    @Test
    public void testRebuildKeyWithViewComparisonEnabled() throws Throwable
    {
        DatabaseDescriptor.setDirectMaterializedViewModification(true);
        DatabaseDescriptor.setViewKeyRebuildOnDeletionEnabled(true);
        DatabaseDescriptor.setViewKeyRebuildApplyMutationsEnabled(true);
        DatabaseDescriptor.setViewKeyRebuildViewReadEnabled(true);
        DatabaseDescriptor.setViewKeyRebuildVerboseLoggingEnabled(true);

        String mvName = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                   "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v1 IS NOT NULL " +
                                   "PRIMARY KEY (v1, pk, ck)");

        // Insert data into base table - view will be populated automatically
        execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (1, 1, 100, 'test')");

        // rebuildMVKey exercises the comparison code path
        String mvQuery = String.format("DELETE FROM %s.%s WHERE v1 = ? AND pk = ? AND ck = ?", keyspace(), mvName);
        executeNet(mvQuery, 100, 1, 1);

        // Verify metric was recorded (confirms the code path was executed)
        TableMetadata viewMetadata = Schema.instance.getTableMetadata(keyspace(), mvName);
        ColumnFamilyStore viewCfs = Schema.instance.getColumnFamilyStoreInstance(viewMetadata.id);
        assertEquals(1, viewCfs.metric.viewRebuildKeyTime.cf.getCount());
    }

    /**
     * Test rebuildMVKey with viewAhead scenario - view row has newer timestamp than base.
     */
    @Test
    public void testRebuildKeyWithViewAhead() throws Throwable
    {
        DatabaseDescriptor.setDirectMaterializedViewModification(true);
        DatabaseDescriptor.setViewKeyRebuildOnDeletionEnabled(true);
        DatabaseDescriptor.setViewKeyRebuildApplyMutationsEnabled(true);
        DatabaseDescriptor.setViewKeyRebuildViewReadEnabled(true);
        DatabaseDescriptor.setViewKeyRebuildVerboseLoggingEnabled(true);

        String mvName = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                   "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v1 IS NOT NULL " +
                                   "PRIMARY KEY (v1, pk, ck)");

        // Insert into base table with a specific timestamp
        long baseTimestamp = System.currentTimeMillis() * 1000; // microseconds
        execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (1, 1, 100, 'test') USING TIMESTAMP " + baseTimestamp);
        TableMetadata viewMetadata = Schema.instance.getTableMetadata(keyspace(), mvName);
        long futureTimestamp = baseTimestamp + 10000000; // 10 seconds ahead
        int nowInSec = (int) (System.currentTimeMillis() / 1000);

        // Build a mutation for the view table with a future timestamp
        Row.Builder rowBuilder = BTreeRow.sortedBuilder();
        rowBuilder.newRow(Clustering.make(
            ByteBufferUtil.bytes(1),   // pk (clustering in view)
            ByteBufferUtil.bytes(1))); // ck (clustering in view)
        rowBuilder.addPrimaryKeyLivenessInfo(LivenessInfo.create(futureTimestamp, nowInSec));
        ColumnMetadata v2Col = viewMetadata.getColumn(ByteBufferUtil.bytes("v2"));
        rowBuilder.addCell(BufferCell.live(v2Col, futureTimestamp, ByteBufferUtil.bytes("test")));

        PartitionUpdate update = PartitionUpdate.singleRowUpdate(
            viewMetadata,
            ByteBufferUtil.bytes(100), // v1 (partition key in view)
            rowBuilder.build());

        new Mutation(update).apply();

        String mvQuery = String.format("DELETE FROM %s.%s WHERE v1 = ? AND pk = ? AND ck = ?", keyspace(), mvName);
        executeNet(mvQuery, 100, 1, 1);

        // Verify metric was recorded
        ColumnFamilyStore viewCfs = Schema.instance.getColumnFamilyStoreInstance(viewMetadata.id);
        assertEquals(1, viewCfs.metric.viewRebuildKeyTime.cf.getCount());
    }
}
