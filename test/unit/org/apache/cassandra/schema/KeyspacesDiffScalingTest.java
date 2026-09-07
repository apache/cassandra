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

package org.apache.cassandra.schema;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.sun.management.ThreadMXBean;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.db.marshal.Int32Type;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * A single schema change must cost the same whether the cluster holds a hundred tables or a hundred thousand.
 *
 * <p>{@link Keyspaces#diff} is executed several times for every DDL statement - by the transformation, by
 * {@link DistributedSchema#initializeKeyspaceInstances}, and again when change listeners are notified - so any per-call
 * cost that scales with the size of the schema is paid on every {@code CREATE TABLE} and turns bulk schema creation
 * into an O(n^2) operation.
 *
 * <p>These tests measure allocation rather than elapsed time. Allocation is counted exactly by the JVM rather than
 * sampled, does not depend on GC timing or on what else the machine is doing, and is the quantity that actually
 * matters here: the failure mode users hit is not gradual slowdown but a GC wall, reached because each statement
 * allocates in proportion to the whole schema. A wall-clock assertion at this granularity would be flaky; this is not.
 *
 * <p>Both tests assert a growth <em>ratio</em> rather than an absolute byte count, so they do not need re-tuning for a
 * different JDK, allocator, or machine.
 */
public class KeyspacesDiffScalingTest
{
    /** Table counts differing by 8x. An O(schema-size) diff allocates ~8x more at LARGE; a correct one allocates ~1x. */
    private static final int SMALL = 400;
    private static final int LARGE = 3200;

    /**
     * Generous: the honest expectation is ~1.0. Anything below this is flat enough to rule out per-table work; the
     * behaviour under test allocates ~8x, so the gap between pass and fail is wide and no borderline case exists.
     */
    private static final double MAX_GROWTH = 3.0;

    private static final int WARMUP = 10;
    private static final int ITERATIONS = 20;

    private static final ThreadMXBean THREADS = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    /** Kept live so the diffs under measurement cannot be optimised away. */
    @SuppressWarnings("unused")
    private static volatile Object sink;

    @BeforeClass
    public static void beforeClass()
    {
        ServerTestUtils.prepareServerNoRegister();
        assertTrue("This test requires per-thread allocation counting",
                   THREADS.isThreadAllocatedMemorySupported() && THREADS.isThreadAllocatedMemoryEnabled());
    }

    /**
     * The reported case: many tables in one keyspace. Adding table n+1 must not cost more than adding table 2.
     */
    @Test
    public void addingOneTableDoesNotScaleWithExistingTablesInSameKeyspace()
    {
        long small = bytesPerDiff(oneKeyspaceWithTableAdded(SMALL));
        long large = bytesPerDiff(oneKeyspaceWithTableAdded(LARGE));

        assertGrowthIsFlat("adding one table to a keyspace already holding", small, large);
    }

    /**
     * Tables in <em>other</em> keyspaces must not be touched at all. The changed keyspace holds a fixed 10 tables in
     * both cases; only the unrelated keyspaces grow. CASSANDRA-7444 established this in 2.1.1 ("compare before/after
     * schemas of the affected keyspaces only"); this test pins it.
     */
    @Test
    public void addingOneTableDoesNotScaleWithTablesInOtherKeyspaces()
    {
        long small = bytesPerDiff(manyKeyspacesWithTableAdded(SMALL));
        long large = bytesPerDiff(manyKeyspacesWithTableAdded(LARGE));

        assertGrowthIsFlat("adding one table to a 10-table keyspace alongside unrelated tables numbering", small, large);
    }

    /** The optimisation must not change what the diff reports: one created table, nothing else. */
    @Test
    public void diffReportsExactlyTheCreatedTable()
    {
        Keyspaces[] pair = oneKeyspaceWithTableAdded(50);
        Keyspaces.KeyspacesDiff diff = Keyspaces.diff(pair[0], pair[1]);

        assertEquals("no keyspaces created", 0, size(diff.created));
        assertEquals("no keyspaces dropped", 0, size(diff.dropped));
        assertEquals("exactly one keyspace altered", 1, diff.altered.size());

        KeyspaceMetadata.KeyspaceDiff ks = diff.altered.get(0);
        assertEquals("exactly one table created", 1, size(ks.tables.created));
        assertEquals("no tables dropped", 0, size(ks.tables.dropped));
        assertEquals("no tables altered", 0, ks.tables.altered.size());
        assertEquals("the added table", "table_50", ks.tables.created.iterator().next().name);
    }

    /** Same, for the reverse direction. */
    @Test
    public void diffReportsExactlyTheDroppedTable()
    {
        Keyspaces[] pair = oneKeyspaceWithTableAdded(50);
        Keyspaces.KeyspacesDiff diff = Keyspaces.diff(pair[1], pair[0]);

        assertEquals("exactly one keyspace altered", 1, diff.altered.size());
        KeyspaceMetadata.KeyspaceDiff ks = diff.altered.get(0);
        assertEquals("no tables created", 0, size(ks.tables.created));
        assertEquals("exactly one table dropped", 1, size(ks.tables.dropped));
        assertEquals("the removed table", "table_50", ks.tables.dropped.iterator().next().name);
    }

    /**
     * An altered table must still be detected. This is the case an over-eager identity short-circuit would break: the
     * instance genuinely differs, so it must be compared, not skipped.
     */
    @Test
    public void diffReportsAnAlteredTable()
    {
        List<TableMetadata> tables = tables("ks", 50);
        Keyspaces before = keyspaces(Tables.of(tables), ImmutableList.of());

        TableMetadata altered = tables.get(10).unbuild().comment("changed").build();
        List<TableMetadata> after = new ArrayList<>(tables);
        after.set(10, altered);

        Keyspaces.KeyspacesDiff diff = Keyspaces.diff(before, keyspaces(Tables.of(after), ImmutableList.of()));

        assertEquals("exactly one keyspace altered", 1, diff.altered.size());
        KeyspaceMetadata.KeyspaceDiff ks = diff.altered.get(0);
        assertEquals("no tables created", 0, size(ks.tables.created));
        assertEquals("no tables dropped", 0, size(ks.tables.dropped));
        assertEquals("exactly one table altered", 1, ks.tables.altered.size());
    }

    // ------------------------------------------------------- Keyspaces#withAddedOrUpdated

    /**
     * {@link Keyspaces#withAddedOrUpdated} used to be a {@code without(name)} followed by {@code with(keyspace)}:
     * every table and view of the keyspace removed from the by-{@link TableId} map one at a time, then every one
     * re-added. Adding table n+1 must not cost more than adding table 2.
     */
    @Test
    public void withAddedOrUpdatedDoesNotScaleWithExistingTablesInSameKeyspace()
    {
        long small = bytesPerWithAddedOrUpdated(oneKeyspaceWithTableAdded(SMALL));
        long large = bytesPerWithAddedOrUpdated(oneKeyspaceWithTableAdded(LARGE));

        assertGrowthIsFlat("adding one table via withAddedOrUpdated to a keyspace already holding", small, large);
    }

    /** The added table must be findable by id and by name, and the keyspace entry itself is the new instance. */
    @Test
    public void withAddedOrUpdatedAddedTableFindableByIdAndName()
    {
        Keyspaces[] pair = oneKeyspaceWithTableAdded(50);
        KeyspaceMetadata after = pair[1].getNullable("ks");

        Keyspaces updated = pair[0].withAddedOrUpdated(after);

        TableMetadata added = after.getTableNullable("table_50");
        assertNotNull("fixture sanity: the added table is present in the target definition", added);
        assertSame("found by id", added, updated.getTableOrViewNullable(added.id));
        assertSame("the keyspace entry is the new definition", after, updated.getNullable("ks"));
    }

    /** A table dropped from the new definition must no longer be resolvable by id. */
    @Test
    public void withAddedOrUpdatedRemovedTableIsGone()
    {
        List<TableMetadata> tables = tables("ks", 50);
        Keyspaces before = keyspaces(Tables.of(tables), ImmutableList.of());
        TableMetadata removed = tables.get(10);

        List<TableMetadata> remaining = new ArrayList<>(tables);
        remaining.remove(10);
        KeyspaceMetadata after = KeyspaceMetadata.create("ks", KeyspaceParams.simple(1), Tables.of(remaining));

        Keyspaces updated = before.withAddedOrUpdated(after);

        assertNull("the removed table is no longer resolvable by id", updated.getTableOrViewNullable(removed.id));
        assertSame("a table that survived is still resolvable",
                  remaining.get(0), updated.getTableOrViewNullable(remaining.get(0).id));
    }

    /** A table replaced by a new instance must resolve to the new instance, not the old one. */
    @Test
    public void withAddedOrUpdatedReplacedTableReturnsNewInstance()
    {
        List<TableMetadata> tables = tables("ks", 50);
        Keyspaces before = keyspaces(Tables.of(tables), ImmutableList.of());

        TableMetadata replaced = tables.get(10).unbuild().comment("changed").build();
        List<TableMetadata> after = new ArrayList<>(tables);
        after.set(10, replaced);
        KeyspaceMetadata afterKsm = KeyspaceMetadata.create("ks", KeyspaceParams.simple(1), Tables.of(after));

        Keyspaces updated = before.withAddedOrUpdated(afterKsm);

        assertSame("resolves to the new instance, not the old one",
                  replaced, updated.getTableOrViewNullable(replaced.id));
    }

    /** Views must be reachable by their own id, distinct from the base table's id. */
    @Test
    public void withAddedOrUpdatedViewsByOwnId()
    {
        TableMetadata base = table("ks", 0);
        TableMetadata viewTable = table("ks", 1);
        ViewMetadata view = new ViewMetadata(base.id, base.name, true, WhereClause.empty(), viewTable);

        Keyspaces before = Keyspaces.of(KeyspaceMetadata.create("ks", KeyspaceParams.simple(1), Tables.of(base)));
        KeyspaceMetadata after = KeyspaceMetadata.create("ks", KeyspaceParams.simple(1),
                                                         Tables.of(base), Views.builder().put(view).build(),
                                                         Types.none(), UserFunctions.none());

        Keyspaces updated = before.withAddedOrUpdated(after);

        assertSame("the view, looked up by its own id", viewTable, updated.getTableOrViewNullable(viewTable.id));
        assertSame("the base table is still reachable", base, updated.getTableOrViewNullable(base.id));
    }

    /** Other keyspaces, and every table in them, must be completely untouched by reference. */
    @Test
    public void withAddedOrUpdatedOtherKeyspacesUntouched()
    {
        Keyspaces[] pair = manyKeyspacesWithTableAdded(50);
        KeyspaceMetadata after = pair[1].getNullable("ks");

        Keyspaces updated = pair[0].withAddedOrUpdated(after);

        for (KeyspaceMetadata other : pair[0])
        {
            if (other.name.equals("ks"))
                continue;
            assertSame("keyspace " + other.name + " is untouched", other, updated.getNullable(other.name));
            for (TableMetadata table : other.tables)
                assertSame("table " + table + " is untouched", table, updated.getTableOrViewNullable(table.id));
        }
    }

    /**
     * Allocation attributable to one {@link Keyspaces#withAddedOrUpdated} call, warmed up so lazy initialisation is
     * not counted.
     */
    private static long bytesPerWithAddedOrUpdated(Keyspaces[] beforeAfter)
    {
        Keyspaces before = beforeAfter[0];
        KeyspaceMetadata after = beforeAfter[1].getNullable("ks");

        for (int i = 0; i < WARMUP; i++)
            sink = before.withAddedOrUpdated(after);

        long id = Thread.currentThread().getId();
        long start = THREADS.getThreadAllocatedBytes(id);
        for (int i = 0; i < ITERATIONS; i++)
            sink = before.withAddedOrUpdated(after);
        return (THREADS.getThreadAllocatedBytes(id) - start) / ITERATIONS;
    }

    // ---------------------------------------------------------------- helpers

    private static void assertGrowthIsFlat(String what, long small, long large)
    {
        double growth = (double) large / small;
        String detail = String.format("%s %,d tables allocated %,d bytes; at %,d tables it allocated %,d bytes " +
                                      "(%.1fx). The schema is %dx larger, so a diff whose cost is independent of " +
                                      "schema size should be near 1.0x.",
                                      what, SMALL, small, LARGE, large, growth, LARGE / SMALL);
        assertTrue(detail, growth < MAX_GROWTH);
    }

    /** Allocation attributable to one {@link Keyspaces#diff} call, warmed up so lazy initialisation is not counted. */
    private static long bytesPerDiff(Keyspaces[] beforeAfter)
    {
        Keyspaces before = beforeAfter[0];
        Keyspaces after = beforeAfter[1];

        for (int i = 0; i < WARMUP; i++)
            sink = Keyspaces.diff(before, after);

        long id = Thread.currentThread().getId();
        long start = THREADS.getThreadAllocatedBytes(id);
        for (int i = 0; i < ITERATIONS; i++)
            sink = Keyspaces.diff(before, after);
        return (THREADS.getThreadAllocatedBytes(id) - start) / ITERATIONS;
    }

    /** {before, after} where a single keyspace holds {@code existing} tables and one more is added. */
    private static Keyspaces[] oneKeyspaceWithTableAdded(int existing)
    {
        List<TableMetadata> tables = tables("ks", existing);
        Keyspaces before = keyspaces(Tables.of(tables), ImmutableList.of());
        Keyspaces after = keyspaces(Tables.of(tables).with(table("ks", existing)), ImmutableList.of());
        return new Keyspaces[]{ before, after };
    }

    /**
     * {before, after} where the changed keyspace always holds 10 tables, and {@code unrelated} further tables are
     * spread across other keyspaces that do not change at all.
     */
    private static Keyspaces[] manyKeyspacesWithTableAdded(int unrelated)
    {
        int others = 10;
        int perOther = unrelated / others;

        // Built once and shared by both sides. A schema change rebuilds only the keyspace it touches and carries the
        // rest over by reference, so sharing these instances is what the production path actually produces - a fixture
        // that rebuilt them independently would be measuring a situation that never occurs.
        List<KeyspaceMetadata> untouched = new ArrayList<>(others);
        for (int i = 0; i < others; i++)
        {
            String name = "other_" + i;
            untouched.add(KeyspaceMetadata.create(name, KeyspaceParams.simple(1), Tables.of(tables(name, perOther))));
        }

        List<TableMetadata> tables = tables("ks", 10);
        Keyspaces before = keyspaces(Tables.of(tables), untouched);
        Keyspaces after = keyspaces(Tables.of(tables).with(table("ks", 10)), untouched);
        return new Keyspaces[]{ before, after };
    }

    /** Keyspace "ks" holding {@code tables}, alongside the given untouched keyspaces. */
    private static Keyspaces keyspaces(Tables tables, List<KeyspaceMetadata> untouched)
    {
        return Keyspaces.of(KeyspaceMetadata.create("ks", KeyspaceParams.simple(1), tables)).with(untouched);
    }

    private static List<TableMetadata> tables(String keyspace, int count)
    {
        List<TableMetadata> tables = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
            tables.add(table(keyspace, i));
        return tables;
    }

    private static TableMetadata table(String keyspace, int i)
    {
        return TableMetadata.builder(keyspace, "table_" + i)
                            .addPartitionKeyColumn("pk", Int32Type.instance)
                            .addClusteringColumn("ck", Int32Type.instance)
                            .addRegularColumn("v", Int32Type.instance)
                            .build();
    }

    private static int size(Iterable<?> iterable)
    {
        int n = 0;
        for (Object ignored : iterable) n++;
        return n;
    }
}
