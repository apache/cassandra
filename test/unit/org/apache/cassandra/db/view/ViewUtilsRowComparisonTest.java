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

package org.apache.cassandra.db.view;

import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ViewAbstractTest;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for ViewUtils.ViewRowComparison.compare method.
 */
public class ViewUtilsRowComparisonTest extends ViewAbstractTest
{
    private static final String KEYSPACE = "view_utils_row_comparison_test";

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = " +
                                   "{'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
    }

    @Before
    @Override
    public void beforeTest() throws Throwable
    {
        super.beforeTest();
        execute("USE " + KEYSPACE);
    }

    // ==================== Test Context Helper ====================

    /**
     * Common test context that encapsulates the standard test setup pattern.
     */
    private static class TestContext
    {
        final ColumnFamilyStore baseCfs;
        final View view;
        final TableMetadata baseMetadata;
        final TableMetadata viewMetadata;
        final DecoratedKey basePartitionKey;
        final int nowInSec;
        final long timestamp;

        TestContext(ColumnFamilyStore baseCfs, String viewName, int partitionKeyValue)
        {
            this.baseCfs = baseCfs;
            this.view = baseCfs.keyspace.viewManager.getByName(viewName);
            this.baseMetadata = baseCfs.metadata();
            this.viewMetadata = view.getDefinition().metadata;
            this.basePartitionKey = baseMetadata.partitioner.decorateKey(ByteBufferUtil.bytes(partitionKeyValue));
            this.nowInSec = FBUtilities.nowInSeconds();
            this.timestamp = System.currentTimeMillis() * 1000;
        }

        ViewUtils.ViewRowComparison.Result compare(Row baseRow, Row viewRow, ByteBuffer nonPKValue)
        {
            return ViewUtils.ViewRowComparison.compare(view, baseRow, viewRow, basePartitionKey, nonPKValue, nowInSec);
        }
    }

    private TestContext setupStandardView(String viewName) throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 text)");
        createView(viewName, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                            "WHERE k IS NOT NULL AND v1 IS NOT NULL PRIMARY KEY (v1, k)");
        return new TestContext(getCurrentColumnFamilyStore(), viewName, 1);
    }

    private TestContext setupClusteringView(String viewName) throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v1 int, v2 text, PRIMARY KEY (k, c))");
        createView(viewName, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                            "WHERE k IS NOT NULL AND c IS NOT NULL AND v1 IS NOT NULL PRIMARY KEY (v1, k, c)");
        return new TestContext(getCurrentColumnFamilyStore(), viewName, 1);
    }

    private TestContext setupMapView(String viewName) throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, m map<text, text>)");
        createView(viewName, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                            "WHERE k IS NOT NULL AND v1 IS NOT NULL PRIMARY KEY (v1, k)");
        return new TestContext(getCurrentColumnFamilyStore(), viewName, 1);
    }

    private TestContext setupSamePKView(String viewName) throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        createView(viewName, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                            "WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, k)");
        return new TestContext(getCurrentColumnFamilyStore(), viewName, 1);
    }

    // ==================== Assertion Helpers ====================

    private void assertStatus(ViewUtils.ViewRowComparison.Result result, ViewUtils.ViewRowComparison.Status expected)
    {
        assertEquals(expected, result.status);
    }

    private void assertStatusAndContains(ViewUtils.ViewRowComparison.Result result,
                                         ViewUtils.ViewRowComparison.Status expected,
                                         String... containsStrings)
    {
        assertEquals(expected, result.status);
        for (String s : containsStrings)
        {
            assertTrue("Summary should contain '" + s + "' but was: " + result.summary,
                      result.summary.contains(s));
        }
    }

    // ==================== IDENTICAL Cases ====================

    /** Test IDENTICAL status when both base and view rows are null. */
    @Test
    public void testIdentical_BothRowsNull() throws Throwable
    {
        TestContext ctx = setupStandardView("test_view");
        ViewUtils.ViewRowComparison.Result result = ctx.compare(null, null, ByteBufferUtil.bytes(100));
        assertStatus(result, ViewUtils.ViewRowComparison.Status.IDENTICAL);
        assertEquals("base row is NULL, view row is NULL", result.summary);
    }

    /**
     * Test IDENTICAL status when base row is null and view row exists but is dead.
     * This covers the case where view row has a tombstone or all cells are dead.
     */
    @Test
    public void testIdentical_BaseRowNullViewRowDead() throws Throwable
    {
        TestContext ctx = setupStandardView("test_view_dead");
        // Create a view row that exists but is dead (all cells are tombstones, no PK liveness)
        Row viewRow = viewRowBuilder(ctx).withK(1).withV2Tombstone().withoutPKLiveness().build();
        ViewUtils.ViewRowComparison.Result result = ctx.compare(null, viewRow, ByteBufferUtil.bytes(100));
        assertStatus(result, ViewUtils.ViewRowComparison.Status.IDENTICAL);
        assertEquals("base row is NULL, view row is dead", result.summary);
    }

    /** Test IDENTICAL status when both base and view rows exist and match perfectly. */
    @Test
    public void testIdentical_BothRowsMatch() throws Throwable
    {
        TestContext ctx = setupStandardView("matching_view");
        Row baseRow = baseRowBuilder(ctx).withV1(100).withV2("test").build();
        Row viewRow = viewRowBuilder(ctx).withK(1).withV2("test").build();
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, viewRow, ByteBufferUtil.bytes(100));
        assertStatus(result, ViewUtils.ViewRowComparison.Status.IDENTICAL);
    }

    // ==================== STALE Cases ====================

    /** Test STALE_BASE_ABSENT status when base row is null but view row exists (orphaned view row). */
    @Test
    public void testStale_BaseRowNullViewRowExists() throws Throwable
    {
        TestContext ctx = setupStandardView("stale_view1");
        Row viewRow = viewRowBuilder(ctx).withK(1).withV2("test").build();
        ViewUtils.ViewRowComparison.Result result = ctx.compare(null, viewRow, ByteBufferUtil.bytes(100));
        assertStatusAndContains(result, ViewUtils.ViewRowComparison.Status.STALE_BASE_ABSENT, "base row is NULL but view row exists");
    }

    /** Test STALE_BASE_EXCLUDED status when non-PK column is null/dead but view row exists. */
    @Test
    public void testStale_NonPKColumnDeadViewRowExists() throws Throwable
    {
        TestContext ctx = setupStandardView("stale_view2");
        Row baseRow = baseRowBuilder(ctx).withV2("test").build(); // no v1
        Row viewRow = viewRowBuilder(ctx).withK(1).withV2("test").build();
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, viewRow, ByteBufferUtil.bytes(100));
        assertStatusAndContains(result, ViewUtils.ViewRowComparison.Status.STALE_BASE_EXCLUDED, "filter failed", "v1");
    }

    /** Test STALE_VALUE_CHANGED status when non-PK column value changed but stale view row exists at old clustering. */
    @Test
    public void testStale_NonPKColumnValueMismatch() throws Throwable
    {
        TestContext ctx = setupStandardView("stale_view3");
        Row baseRow = baseRowBuilder(ctx).withV1(200).withV2("test").build(); // v1=200
        Row viewRow = viewRowBuilder(ctx).withK(1).withV2("test").build(); // stale at v1=100
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, viewRow, ByteBufferUtil.bytes(100));
        assertStatusAndContains(result, ViewUtils.ViewRowComparison.Status.STALE_VALUE_CHANGED, "NonPkCol", "stale");
    }

    // ==================== MISSING Cases ====================

    /** Test MISSING status when base row matches filter but view row is null. */
    @Test
    public void testMissing_BaseRowMatchesFilterViewRowNull() throws Throwable
    {
        TestContext ctx = setupStandardView("missing_view");
        Row baseRow = baseRowBuilder(ctx).withV1(100).withV2("test").build();
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, null, ByteBufferUtil.bytes(100));
        assertStatusAndContains(result, ViewUtils.ViewRowComparison.Status.MISSING, "view row not found");
    }

    // ==================== MISMATCH Cases ====================

    /** Test MISMATCH when both rows exist but have different cell values. */
    @Test
    public void testMismatch_DifferentCellValues() throws Throwable
    {
        TestContext ctx = setupStandardView("mismatch_view");
        Row baseRow = baseRowBuilder(ctx).withV1(100).withV2("updated").build();
        Row viewRow = viewRowBuilder(ctx).withK(1).withV2("original").build();
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, viewRow, ByteBufferUtil.bytes(100));
        assertStatus(result, ViewUtils.ViewRowComparison.Status.MISMATCH);
        assertTrue(!result.summary.isEmpty());
    }

    /** Test MISMATCH when both rows exist but have different timestamps (view older). */
    @Test
    public void testMismatch_DifferentTimestamps() throws Throwable
    {
        TestContext ctx = setupStandardView("mismatch_view2");
        Row baseRow = baseRowBuilder(ctx).withV1(100).withV2("test").build();
        Row viewRow = viewRowBuilder(ctx).withK(1).withV2("test").withTimestamp(ctx.timestamp - 10000000).build();
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, viewRow, ByteBufferUtil.bytes(100));
        assertStatus(result, ViewUtils.ViewRowComparison.Status.MISMATCH);
        assertFalse("viewAhead should be false when view is older", result.viewAhead);
    }

    /** Test MISMATCH with viewAhead=true when view row has NEWER timestamp than expected from base. */
    @Test
    public void testMismatch_ViewAhead() throws Throwable
    {
        TestContext ctx = setupStandardView("view_ahead_view");
        Row baseRow = baseRowBuilder(ctx).withV1(100).withV2("test").build();
        Row viewRow = viewRowBuilder(ctx).withK(1).withV2("test").withTimestamp(ctx.timestamp + 10000000).build();
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, viewRow, ByteBufferUtil.bytes(100));
        assertStatus(result, ViewUtils.ViewRowComparison.Status.MISMATCH);
        assertTrue("viewAhead should be true when view has newer timestamp", result.viewAhead);
    }

    /**
     * Test IDENTICAL when view row has liveness but expected doesn't - neither is expiring.
     * Per our design: if neither row is expiring, liveness differences don't affect user-visible consistency.
     */
    @Test
    public void testIdentical_LivenessExpectedEmptyActualHasNonExpiringLiveness() throws Throwable
    {
        TestContext ctx = setupSamePKView("liveness_empty_view");
        Row baseRow = baseRowBuilder(ctx).withClustering(10).withV("test").withoutPKLiveness().build();
        Row viewRow = viewRowSamePKBuilder(ctx).withC(10).withK(1).withV("test").build();
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, viewRow, null);
        // Neither is expiring, so no liveness diff - cells keep both alive
        assertStatus(result, ViewUtils.ViewRowComparison.Status.IDENTICAL);
    }

    /**
     * Test IDENTICAL when expected liveness has value but actual view row has empty liveness - neither is expiring.
     * Per our design: if neither row is expiring, liveness differences don't affect user-visible consistency.
     * Note: Uses setupSamePKView because it doesn't enforce strict liveness, allowing rows with live cells
     * but no PK liveness to be considered alive.
     */
    @Test
    public void testIdentical_LivenessExpectedHasActualEmpty_NeitherExpiring() throws Throwable
    {
        TestContext ctx = setupSamePKView("liveness_actual_empty_view");
        Row baseRow = baseRowBuilder(ctx).withClustering(10).withV("test").build();
        Row viewRow = viewRowSamePKBuilder(ctx).withC(10).withK(1).withV("test").withoutPKLiveness().build();
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, viewRow, null);
        // Neither is expiring, so no liveness diff - cells keep both alive
        assertStatus(result, ViewUtils.ViewRowComparison.Status.IDENTICAL);
    }

    /**
     * Test MISMATCH when one row is expiring and the other isn't.
     * This is a real inconsistency - one row will expire and the other won't.
     */
    @Test
    public void testMismatch_OneExpiringOneNot() throws Throwable
    {
        TestContext ctx = setupStandardView("ttl_view");
        Row baseRow = baseRowBuilder(ctx).withV1(100).withV2("test").withTTL(3600).build();
        Row viewRow = viewRowBuilder(ctx).withK(1).withV2("test").build();
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, viewRow, ByteBufferUtil.bytes(100));
        assertStatusAndContains(result, ViewUtils.ViewRowComparison.Status.MISMATCH, "liveness.expiring");
    }

    /**
     * Test that cell timestamp differences are still detected.
     * Note: Liveness timestamp differences are ignored, but cell timestamps still matter
     * because users can query via WRITETIME(column). This test verifies cells with different
     * timestamps report MISMATCH.
     */
    @Test
    public void testMismatch_DifferentCellTimestamps() throws Throwable
    {
        TestContext ctx = setupStandardView("liveness_view");
        Row baseRow = baseRowBuilder(ctx).withV1(100).withV2("test").build();
        Row viewRow = viewRowBuilder(ctx).withK(1).withV2("test").withTimestamp(ctx.timestamp - 5000000).build();
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, viewRow, ByteBufferUtil.bytes(100));
        // Cell timestamps differ - this is still a mismatch (queryable via WRITETIME)
        assertStatusAndContains(result, ViewUtils.ViewRowComparison.Status.MISMATCH, "v2:", "ts=");
    }

    /** Test MISMATCH when map columns have different values. */
    @Test
    public void testMismatch_MapColumnDifference() throws Throwable
    {
        TestContext ctx = setupMapView("map_view");
        Row baseRow = baseRowWithMapBuilder(ctx).withV1(100).withMapEntry("key1", "value1").build();
        Row viewRow = viewRowWithMapBuilder(ctx).withK(1).withMapEntry("key1", "value2").build();
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, viewRow, ByteBufferUtil.bytes(100));
        assertStatusAndContains(result, ViewUtils.ViewRowComparison.Status.MISMATCH, "m");
    }

    /** Test MISMATCH when map column has extra entry in view. */
    @Test
    public void testMismatch_MapColumnExtraInView() throws Throwable
    {
        TestContext ctx = setupMapView("map_view2");
        Row baseRow = baseRowBuilder(ctx).withV1(100).build(); // no map
        Row viewRow = viewRowWithMapBuilder(ctx).withK(1).withMapEntry("key1", "value1").build();
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, viewRow, ByteBufferUtil.bytes(100));
        assertStatusAndContains(result, ViewUtils.ViewRowComparison.Status.MISMATCH, "m", "extra");
    }

    /** Test MISMATCH when map columns have different complex deletions. */
    @Test
    public void testMismatch_MapComplexDeletionDifference() throws Throwable
    {
        TestContext ctx = setupMapView("map_del_view");
        Row baseRow = baseRowWithMapBuilder(ctx).withV1(100).withMapEntry("key1", "value1").build();
        Row viewRow = viewRowWithMapBuilder(ctx).withK(1).withMapEntry("key1", "value1").withComplexDeletion(ctx.timestamp - 1000).build();
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, viewRow, ByteBufferUtil.bytes(100));
        assertStatusAndContains(result, ViewUtils.ViewRowComparison.Status.MISMATCH, "m", "deletion");
    }

    /** Test MISMATCH when view row has extra column not present in expected. */
    @Test
    public void testMismatch_ColumnExtraInView() throws Throwable
    {
        TestContext ctx = setupStandardView("extra_col_view");
        Row baseRow = baseRowBuilder(ctx).withV1(100).build(); // no v2
        Row viewRow = viewRowBuilder(ctx).withK(1).withV2("extra_value").build();
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, viewRow, ByteBufferUtil.bytes(100));
        assertStatusAndContains(result, ViewUtils.ViewRowComparison.Status.MISMATCH, "v2", "extra");
    }

    /** Test MISMATCH when expected row has column but view row doesn't. */
    @Test
    public void testMismatch_ColumnMissingInView() throws Throwable
    {
        TestContext ctx = setupStandardView("missing_col_view");
        Row baseRow = baseRowBuilder(ctx).withV1(100).withV2("test_value").build();
        Row viewRow = viewRowBuilder(ctx).withK(1).build(); // no v2
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, viewRow, ByteBufferUtil.bytes(100));
        assertStatusAndContains(result, ViewUtils.ViewRowComparison.Status.MISMATCH, "v2", "missing");
    }

    /** Test MISMATCH when cell is tombstone in view but live in expected. */
    @Test
    public void testMismatch_CellTombstoneInView() throws Throwable
    {
        TestContext ctx = setupStandardView("tombstone_view");
        Row baseRow = baseRowBuilder(ctx).withV1(100).withV2("test_value").build();
        Row viewRow = viewRowBuilder(ctx).withK(1).withV2Tombstone().build();
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, viewRow, ByteBufferUtil.bytes(100));
        assertStatusAndContains(result, ViewUtils.ViewRowComparison.Status.MISMATCH, "v2");
    }

    // ==================== FILTERED Cases ====================

    /** Test FILTERED when base row doesn't match filter (v1 null) and view row is correctly null. */
    @Test
    public void testFiltered_BaseRowDoesNotMatchFilterViewRowNull() throws Throwable
    {
        TestContext ctx = setupStandardView("filtered_view");
        Row baseRow = baseRowBuilder(ctx).withV2("test").build(); // no v1
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, null, ByteBufferUtil.bytes(100));
        assertStatus(result, ViewUtils.ViewRowComparison.Status.CONSISTENT_FILTERED_NONPK_COLUMN);
    }

    /** Test FILTERED when non-PK column (v1) has expired due to TTL and view row is correctly null. */
    @Test
    public void testFiltered_ExpiredNonPKColumn() throws Throwable
    {
        TestContext ctx = setupStandardView("filtered_view2");
        Row baseRow = baseRowBuilder(ctx).withExpiredV1(100).withV2("test").build();
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, null, ByteBufferUtil.bytes(100));
        assertStatus(result, ViewUtils.ViewRowComparison.Status.CONSISTENT_FILTERED_NONPK_COLUMN);
    }

    /** Test FILTERED when clustering key restriction (c > 50) is not satisfied. */
    @Test
    public void testFiltered_ClusteringKeyRestrictionNotSatisfied() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v1 int, v2 text, PRIMARY KEY (k, c))");
        createView("clustering_restrict_view", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                              "WHERE k IS NOT NULL AND c IS NOT NULL AND c > 50 AND v1 IS NOT NULL PRIMARY KEY (v1, k, c)");
        TestContext ctx = new TestContext(getCurrentColumnFamilyStore(), "clustering_restrict_view", 1);
        Row baseRow = baseRowWithClusteringBuilder(ctx).withC(30).withV1(100).withV2("test").build(); // c=30 fails c>50
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, null, ByteBufferUtil.bytes(100));
        assertStatusAndContains(result, ViewUtils.ViewRowComparison.Status.CONSISTENT_FILTERED_CLUSTERING, "clustering not selected");
    }

    // ==================== STALE with Filter Failure Cases ====================

    /** Test STALE when base row has matching row, verifies clustering view behavior. */
    @Test
    public void testStale_ClusteringNotSelected() throws Throwable
    {
        TestContext ctx = setupClusteringView("clustering_view");
        Row baseRow = baseRowWithClusteringBuilder(ctx).withC(10).withV1(100).withV2("test").build();
        Row viewRow = viewRowWithClusteringBuilder(ctx).withV1(100).withK(1).withC(10).withV2("test").build();
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, viewRow, ByteBufferUtil.bytes(100));
        assertStatus(result, ViewUtils.ViewRowComparison.Status.IDENTICAL);
    }

    /** Test STALE_BASE_EXCLUDED when clustering key restriction not satisfied but stale view row exists. */
    @Test
    public void testStale_ClusteringKeyRestrictionNotSatisfied_StaleViewExists() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v1 int, v2 text, PRIMARY KEY (k, c))");
        createView("clustering_stale_view", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                           "WHERE k IS NOT NULL AND c IS NOT NULL AND c > 50 AND v1 IS NOT NULL PRIMARY KEY (v1, k, c)");
        TestContext ctx = new TestContext(getCurrentColumnFamilyStore(), "clustering_stale_view", 1);
        Row baseRow = baseRowWithClusteringBuilder(ctx).withC(30).withV1(100).withV2("test").build(); // c=30 fails c>50
        Row viewRow = viewRowWithClusteringBuilder(ctx).withV1(100).withK(1).withC(30).withV2("test").build();
        ViewUtils.ViewRowComparison.Result result = ctx.compare(baseRow, viewRow, ByteBufferUtil.bytes(100));
        assertStatusAndContains(result, ViewUtils.ViewRowComparison.Status.STALE_BASE_EXCLUDED, "filter failed", "clustering not selected");
    }

    // ==================== Row Builders ====================

    private BaseRowBuilder baseRowBuilder(TestContext ctx)
    {
        return new BaseRowBuilder(ctx.baseMetadata, ctx.timestamp, ctx.nowInSec);
    }

    private BaseRowWithClusteringBuilder baseRowWithClusteringBuilder(TestContext ctx)
    {
        return new BaseRowWithClusteringBuilder(ctx.baseMetadata, ctx.timestamp, ctx.nowInSec);
    }

    private BaseRowWithMapBuilder baseRowWithMapBuilder(TestContext ctx)
    {
        return new BaseRowWithMapBuilder(ctx.baseMetadata, ctx.timestamp, ctx.nowInSec);
    }

    private ViewRowBuilder viewRowBuilder(TestContext ctx)
    {
        return new ViewRowBuilder(ctx.viewMetadata, ctx.timestamp, ctx.nowInSec);
    }

    private ViewRowWithClusteringBuilder viewRowWithClusteringBuilder(TestContext ctx)
    {
        return new ViewRowWithClusteringBuilder(ctx.viewMetadata, ctx.timestamp, ctx.nowInSec);
    }

    private ViewRowWithMapBuilder viewRowWithMapBuilder(TestContext ctx)
    {
        return new ViewRowWithMapBuilder(ctx.viewMetadata, ctx.timestamp, ctx.nowInSec);
    }

    private ViewRowSamePKBuilder viewRowSamePKBuilder(TestContext ctx)
    {
        return new ViewRowSamePKBuilder(ctx.viewMetadata, ctx.timestamp, ctx.nowInSec);
    }

    // ==================== Builder Classes ====================

    /** Builder for base rows with simple PK (k). */
    private static class BaseRowBuilder
    {
        final TableMetadata metadata;
        long timestamp;
        final int nowInSec;
        Integer v1;
        String v2;
        String v;
        int ttl = 0;
        boolean includePKLiveness = true;
        Long rowDeletion = null;
        boolean v1Expired = false;
        Integer clustering = null;

        BaseRowBuilder(TableMetadata metadata, long timestamp, int nowInSec)
        {
            this.metadata = metadata;
            this.timestamp = timestamp;
            this.nowInSec = nowInSec;
        }

        BaseRowBuilder withV1(int v1) { this.v1 = v1; return this; }
        BaseRowBuilder withV2(String v2) { this.v2 = v2; return this; }
        BaseRowBuilder withV(String v) { this.v = v; return this; }
        BaseRowBuilder withTTL(int ttl) { this.ttl = ttl; return this; }
        BaseRowBuilder withoutPKLiveness() { this.includePKLiveness = false; return this; }
        BaseRowBuilder withRowDeletion(long ts) { this.rowDeletion = ts; return this; }
        BaseRowBuilder withExpiredV1(int v1) { this.v1 = v1; this.v1Expired = true; return this; }
        BaseRowBuilder withClustering(int c) { this.clustering = c; return this; }

        Row build()
        {
            Row.Builder builder = BTreeRow.sortedBuilder();
            Clustering<?> clust = clustering != null
                                  ? Clustering.make(ByteBufferUtil.bytes(clustering))
                                  : Clustering.EMPTY;
            builder.newRow(clust);

            if (includePKLiveness)
            {
                if (ttl > 0)
                    builder.addPrimaryKeyLivenessInfo(LivenessInfo.withExpirationTime(timestamp, ttl, nowInSec + ttl));
                else
                    builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, nowInSec));
            }

            if (rowDeletion != null)
                builder.addRowDeletion(Row.Deletion.regular(new DeletionTime(rowDeletion, nowInSec)));

            addCell(builder, "v1", v1, v1Expired);
            addCell(builder, "v2", v2);
            addCell(builder, "v", v);
            return builder.build();
        }

        void addCell(Row.Builder builder, String name, Object value)
        {
            addCell(builder, name, value, false);
        }

        void addCell(Row.Builder builder, String name, Object value, boolean expired)
        {
            if (value == null) return;
            ColumnMetadata col = metadata.getColumn(ByteBufferUtil.bytes(name));
            if (col == null) return;

            ByteBuffer buf = value instanceof Integer
                             ? ByteBufferUtil.bytes((Integer) value)
                             : ByteBufferUtil.bytes((String) value);
            Cell<?> cell;
            if (expired)
                cell = BufferCell.expiring(col, timestamp, 1, nowInSec - 10, buf);
            else if (ttl > 0)
                cell = BufferCell.expiring(col, timestamp, ttl, nowInSec + ttl, buf);
            else
                cell = BufferCell.live(col, timestamp, buf);
            builder.addCell(cell);
        }
    }

    /** Builder for base rows with clustering column (k, c). */
    private static class BaseRowWithClusteringBuilder
    {
        final TableMetadata metadata;
        final long timestamp;
        final int nowInSec;
        int c;
        Integer v1;
        String v2;

        BaseRowWithClusteringBuilder(TableMetadata metadata, long timestamp, int nowInSec)
        {
            this.metadata = metadata;
            this.timestamp = timestamp;
            this.nowInSec = nowInSec;
        }

        BaseRowWithClusteringBuilder withC(int c) { this.c = c; return this; }
        BaseRowWithClusteringBuilder withV1(int v1) { this.v1 = v1; return this; }
        BaseRowWithClusteringBuilder withV2(String v2) { this.v2 = v2; return this; }

        Row build()
        {
            Row.Builder builder = BTreeRow.sortedBuilder();
            builder.newRow(Clustering.make(ByteBufferUtil.bytes(c)));
            builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, nowInSec));

            if (v1 != null)
            {
                ColumnMetadata col = metadata.getColumn(ByteBufferUtil.bytes("v1"));
                if (col != null) builder.addCell(BufferCell.live(col, timestamp, ByteBufferUtil.bytes(v1)));
            }
            if (v2 != null)
            {
                ColumnMetadata col = metadata.getColumn(ByteBufferUtil.bytes("v2"));
                if (col != null) builder.addCell(BufferCell.live(col, timestamp, ByteBufferUtil.bytes(v2)));
            }
            return builder.build();
        }
    }

    /** Builder for base rows with map column. */
    private static class BaseRowWithMapBuilder
    {
        final TableMetadata metadata;
        final long timestamp;
        final int nowInSec;
        Integer v1;
        String mapKey, mapValue;

        BaseRowWithMapBuilder(TableMetadata metadata, long timestamp, int nowInSec)
        {
            this.metadata = metadata;
            this.timestamp = timestamp;
            this.nowInSec = nowInSec;
        }

        BaseRowWithMapBuilder withV1(int v1) { this.v1 = v1; return this; }
        BaseRowWithMapBuilder withMapEntry(String key, String value) { this.mapKey = key; this.mapValue = value; return this; }

        Row build()
        {
            Row.Builder builder = BTreeRow.sortedBuilder();
            builder.newRow(Clustering.EMPTY);
            builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, nowInSec));

            if (v1 != null)
            {
                ColumnMetadata col = metadata.getColumn(ByteBufferUtil.bytes("v1"));
                if (col != null) builder.addCell(BufferCell.live(col, timestamp, ByteBufferUtil.bytes(v1)));
            }
            if (mapKey != null)
            {
                ColumnMetadata col = metadata.getColumn(ByteBufferUtil.bytes("m"));
                if (col != null)
                {
                    CellPath path = CellPath.create(ByteBufferUtil.bytes(mapKey));
                    builder.addCell(BufferCell.live(col, timestamp, ByteBufferUtil.bytes(mapValue), path));
                }
            }
            return builder.build();
        }
    }

    /** Builder for view rows with standard PK (v1, k). v1 is partition key (not in Row), k is clustering. */
    private static class ViewRowBuilder
    {
        final TableMetadata metadata;
        long timestamp;
        final int nowInSec;
        int k;  // clustering column (base table's partition key)
        String v2;
        boolean includePKLiveness = true;
        Long rowDeletion = null;
        boolean v2Tombstone = false;
        int ttl = 0;  // 0 means no TTL

        ViewRowBuilder(TableMetadata metadata, long timestamp, int nowInSec)
        {
            this.metadata = metadata;
            this.timestamp = timestamp;
            this.nowInSec = nowInSec;
        }

        ViewRowBuilder withK(int k) { this.k = k; return this; }  // k is the clustering column
        ViewRowBuilder withV2(String v2) { this.v2 = v2; return this; }
        ViewRowBuilder withTimestamp(long ts) { this.timestamp = ts; return this; }
        ViewRowBuilder withoutPKLiveness() { this.includePKLiveness = false; return this; }
        ViewRowBuilder withRowDeletion(long ts) { this.rowDeletion = ts; return this; }
        ViewRowBuilder withV2Tombstone() { this.v2Tombstone = true; return this; }
        ViewRowBuilder withTTL(int ttl) { this.ttl = ttl; return this; }

        Row build()
        {
            Row.Builder builder = BTreeRow.sortedBuilder();
            builder.newRow(Clustering.make(ByteBufferUtil.bytes(k)));

            if (includePKLiveness)
            {
                if (ttl > 0)
                    builder.addPrimaryKeyLivenessInfo(LivenessInfo.withExpirationTime(timestamp, ttl, nowInSec + ttl));
                else
                    builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, nowInSec));
            }

            if (rowDeletion != null)
                builder.addRowDeletion(Row.Deletion.regular(new DeletionTime(rowDeletion, nowInSec)));

            ColumnMetadata v2Col = metadata.getColumn(ByteBufferUtil.bytes("v2"));
            if (v2Col != null)
            {
                if (v2Tombstone)
                    builder.addCell(BufferCell.tombstone(v2Col, timestamp, nowInSec));
                else if (v2 != null)
                    builder.addCell(BufferCell.live(v2Col, timestamp, ByteBufferUtil.bytes(v2)));
            }
            return builder.build();
        }
    }

    /** Builder for view rows with clustering PK (v1, k, c). */
    private static class ViewRowWithClusteringBuilder
    {
        final TableMetadata metadata;
        final long timestamp;
        final int nowInSec;
        int v1, k, c;
        String v2;

        ViewRowWithClusteringBuilder(TableMetadata metadata, long timestamp, int nowInSec)
        {
            this.metadata = metadata;
            this.timestamp = timestamp;
            this.nowInSec = nowInSec;
        }

        ViewRowWithClusteringBuilder withV1(int v1) { this.v1 = v1; return this; }
        ViewRowWithClusteringBuilder withK(int k) { this.k = k; return this; }
        ViewRowWithClusteringBuilder withC(int c) { this.c = c; return this; }
        ViewRowWithClusteringBuilder withV2(String v2) { this.v2 = v2; return this; }

        Row build()
        {
            Row.Builder builder = BTreeRow.sortedBuilder();
            builder.newRow(Clustering.make(ByteBufferUtil.bytes(k), ByteBufferUtil.bytes(c)));
            builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, nowInSec));

            if (v2 != null)
            {
                ColumnMetadata col = metadata.getColumn(ByteBufferUtil.bytes("v2"));
                if (col != null) builder.addCell(BufferCell.live(col, timestamp, ByteBufferUtil.bytes(v2)));
            }
            return builder.build();
        }
    }

    /** Builder for view rows with map column. */
    private static class ViewRowWithMapBuilder
    {
        final TableMetadata metadata;
        final long timestamp;
        final int nowInSec;
        int k;
        String mapKey, mapValue;
        Long complexDeletion = null;

        ViewRowWithMapBuilder(TableMetadata metadata, long timestamp, int nowInSec)
        {
            this.metadata = metadata;
            this.timestamp = timestamp;
            this.nowInSec = nowInSec;
        }

        ViewRowWithMapBuilder withK(int k) { this.k = k; return this; }
        ViewRowWithMapBuilder withMapEntry(String key, String value) { this.mapKey = key; this.mapValue = value; return this; }
        ViewRowWithMapBuilder withComplexDeletion(long ts) { this.complexDeletion = ts; return this; }

        Row build()
        {
            Row.Builder builder = BTreeRow.sortedBuilder();
            builder.newRow(Clustering.make(ByteBufferUtil.bytes(k)));
            builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, nowInSec));

            ColumnMetadata mapCol = metadata.getColumn(ByteBufferUtil.bytes("m"));
            if (mapCol != null)
            {
                if (complexDeletion != null)
                    builder.addComplexDeletion(mapCol, new DeletionTime(complexDeletion, nowInSec));
                if (mapKey != null)
                {
                    CellPath path = CellPath.create(ByteBufferUtil.bytes(mapKey));
                    builder.addCell(BufferCell.live(mapCol, timestamp, ByteBufferUtil.bytes(mapValue), path));
                }
            }
            return builder.build();
        }
    }

    /** Builder for view rows with same PK as base (c, k). */
    private static class ViewRowSamePKBuilder
    {
        final TableMetadata metadata;
        final long timestamp;
        final int nowInSec;
        int c, k;
        String v;
        boolean includePKLiveness = true;

        ViewRowSamePKBuilder(TableMetadata metadata, long timestamp, int nowInSec)
        {
            this.metadata = metadata;
            this.timestamp = timestamp;
            this.nowInSec = nowInSec;
        }

        ViewRowSamePKBuilder withC(int c) { this.c = c; return this; }
        ViewRowSamePKBuilder withK(int k) { this.k = k; return this; }
        ViewRowSamePKBuilder withV(String v) { this.v = v; return this; }
        ViewRowSamePKBuilder withoutPKLiveness() { this.includePKLiveness = false; return this; }

        Row build()
        {
            Row.Builder builder = BTreeRow.sortedBuilder();
            builder.newRow(Clustering.make(ByteBufferUtil.bytes(k)));
            if (includePKLiveness)
                builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, nowInSec));

            if (v != null)
            {
                ColumnMetadata col = metadata.getColumn(ByteBufferUtil.bytes("v"));
                if (col != null) builder.addCell(BufferCell.live(col, timestamp, ByteBufferUtil.bytes(v)));
            }
            return builder.build();
        }
    }
}
