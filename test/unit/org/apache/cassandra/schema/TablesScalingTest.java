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
import java.util.Collections;
import java.util.List;

import com.sun.management.ThreadMXBean;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.db.marshal.Int32Type;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Adding, replacing or removing one table must not cost in proportion to the tables already present.
 *
 * <p>{@link Tables} used to keep three {@link com.google.common.collect.ImmutableMap}s and rebuild all of them on
 * every mutation - {@code with} was {@code builder().add(this).add(table).build()} - so a single {@code CREATE TABLE}
 * copied every existing table into three fresh maps and recomputed index metadata for every indexed table while doing
 * so. Several such rebuilds happen per DDL statement, which is what kept bulk schema creation quadratic even once
 * diffing was cheap. The maps are persistent now, and these tests are what holds that.
 *
 * <p>As in {@link KeyspacesDiffScalingTest}, these assert allocation rather than elapsed time: it is counted exactly
 * rather than sampled, is independent of GC timing and machine load, and is the quantity that produces the failure
 * users hit. A persistent map allocates O(log n) per update rather than O(1), so the bar is "grows far slower than the
 * collection" rather than "perfectly flat".
 */
public class TablesScalingTest
{
    private static final int SMALL = 400;
    private static final int LARGE = 3200;

    /**
     * An O(n) rebuild allocates ~8x more at LARGE. A persistent map allocates O(log n), i.e. ~1.3x across this range.
     * 3.0 sits far from both, so neither a pass nor a failure is borderline.
     */
    private static final double MAX_GROWTH = 3.0;

    private static final int WARMUP = 10;
    private static final int ITERATIONS = 20;

    private static final ThreadMXBean THREADS = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    @SuppressWarnings("unused")
    private static volatile Object sink;

    @BeforeClass
    public static void beforeClass()
    {
        ServerTestUtils.prepareServerNoRegister();
        assertTrue("This test requires per-thread allocation counting",
                   THREADS.isThreadAllocatedMemorySupported() && THREADS.isThreadAllocatedMemoryEnabled());
    }

    @Test
    public void addingATableDoesNotScaleWithExistingTables()
    {
        long small = bytesPerOp(fixture(SMALL), t -> t.with(table("ks", SMALL)));
        long large = bytesPerOp(fixture(LARGE), t -> t.with(table("ks", LARGE)));
        assertGrowthIsSublinear("adding one table to", small, large);
    }

    @Test
    public void replacingATableDoesNotScaleWithExistingTables()
    {
        TableMetadata smallSwap = altered(fixture(SMALL), 7);
        TableMetadata largeSwap = altered(fixture(LARGE), 7);
        long small = bytesPerOp(fixture(SMALL), t -> t.withSwapped(smallSwap));
        long large = bytesPerOp(fixture(LARGE), t -> t.withSwapped(largeSwap));
        assertGrowthIsSublinear("replacing one table in", small, large);
    }

    @Test
    public void removingATableDoesNotScaleWithExistingTables()
    {
        long small = bytesPerOp(fixture(SMALL), t -> t.without("table_7"));
        long large = bytesPerOp(fixture(LARGE), t -> t.without("table_7"));
        assertGrowthIsSublinear("removing one table from", small, large);
    }

    // -------------------------------------------------------------- correctness guards

    @Test
    public void withAddsExactlyOneTableAndPreservesTheRest()
    {
        Tables before = fixture(50);
        Tables after = before.with(table("ks", 50));

        assertEquals(51, size(after));
        assertNotNull("added table is resolvable by name", after.getNullable("table_50"));
        for (TableMetadata t : before)
        {
            assertNotNull("pre-existing table still resolvable by name: " + t.name, after.getNullable(t.name));
            assertNotNull("pre-existing table still resolvable by id: " + t.name, after.getNullable(t.id));
        }
    }

    @Test
    public void withoutRemovesExactlyOneTable()
    {
        Tables after = fixture(50).without("table_7");

        assertEquals(49, size(after));
        assertNull("removed table is gone by name", after.getNullable("table_7"));
        assertNotNull("neighbours survive", after.getNullable("table_6"));
        assertNotNull("neighbours survive", after.getNullable("table_8"));
    }

    @Test
    public void withSwappedReplacesInPlace()
    {
        Tables before = fixture(50);
        TableMetadata swapped = altered(before, 7);
        Tables after = before.withSwapped(swapped);

        assertEquals(50, size(after));
        assertEquals("the replacement is what resolves", "changed", after.getNullable("table_7").params.comment);
        assertEquals("lookup by id agrees with lookup by name",
                     after.getNullable("table_7"), after.getNullable(swapped.id));
    }

    /**
     * Pins what callers may rely on about iteration. It is no longer insertion order - the backing map is sorted by
     * table name - so this asserts the properties that actually have consequences: iteration is repeatable, it depends
     * on the set of tables rather than on the order they were added, it agrees with lookup by name and by id, and
     * changing one table leaves the relative order of the others alone. Serialization walks this collection, so two
     * nodes holding the same schema must walk it the same way; the schema version itself is derived from the TCM
     * epoch rather than hashed from content ({@link DistributedSchema}), so ordering cannot shift a digest.
     */
    @Test
    public void iterationIsDeterministicAndAgreesWithLookup()
    {
        Tables tables = fixture(50);

        List<String> first = names(tables);
        assertEquals("iteration order is repeatable", first, names(tables));

        List<TableMetadata> reversed = new ArrayList<>();
        for (TableMetadata t : tables)
            reversed.add(t);
        Collections.reverse(reversed);
        assertEquals("iteration order is a function of the tables, not of the order they were added",
                     first, names(Tables.of(reversed)));

        for (String name : first)
        {
            TableMetadata table = tables.getNullable(name);
            assertNotNull("iterated table resolves by name: " + name, table);
            assertSame("lookup by id agrees with iteration: " + name, table, tables.getNullable(table.id));
        }

        List<String> afterAdd = names(tables.with(table("ks", 50)));
        assertTrue("the added table is iterated", afterAdd.contains("table_50"));
        assertEquals("adding a table does not reorder the existing ones", first, discard(afterAdd, "table_50"));

        assertEquals("removing a table does not reorder the others",
                     discard(first, "table_7"), names(tables.without("table_7")));
    }

    // -------------------------------------------------------------- helpers

    private interface Op
    {
        Tables apply(Tables tables);
    }

    private static void assertGrowthIsSublinear(String what, long small, long large)
    {
        double growth = (double) large / small;
        String detail = String.format("%s %,d tables allocated %,d bytes; at %,d tables it allocated %,d bytes " +
                                      "(%.1fx). The collection is %dx larger, so an update that does not copy it " +
                                      "should be far below that.",
                                      what, SMALL, small, LARGE, large, growth, LARGE / SMALL);
        assertTrue(detail, growth < MAX_GROWTH);
    }

    private static long bytesPerOp(Tables tables, Op op)
    {
        for (int i = 0; i < WARMUP; i++)
            sink = op.apply(tables);

        long id = Thread.currentThread().getId();
        long start = THREADS.getThreadAllocatedBytes(id);
        for (int i = 0; i < ITERATIONS; i++)
            sink = op.apply(tables);
        return (THREADS.getThreadAllocatedBytes(id) - start) / ITERATIONS;
    }

    private static Tables fixture(int count)
    {
        List<TableMetadata> tables = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
            tables.add(table("ks", i));
        return Tables.of(tables);
    }

    private static TableMetadata altered(Tables tables, int i)
    {
        return tables.getNullable("table_" + i).unbuild().comment("changed").build();
    }

    private static TableMetadata table(String keyspace, int i)
    {
        return TableMetadata.builder(keyspace, "table_" + i)
                            .addPartitionKeyColumn("pk", Int32Type.instance)
                            .addClusteringColumn("ck", Int32Type.instance)
                            .addRegularColumn("v", Int32Type.instance)
                            .build();
    }

    private static List<String> discard(List<String> names, String name)
    {
        List<String> rest = new ArrayList<>(names);
        assertTrue("expected " + name + " to be present", rest.remove(name));
        return rest;
    }

    private static List<String> names(Tables tables)
    {
        List<String> names = new ArrayList<>();
        for (TableMetadata t : tables)
            names.add(t.name);
        return names;
    }

    private static int size(Tables tables)
    {
        int n = 0;
        for (TableMetadata ignored : tables) n++;
        return n;
    }
}
