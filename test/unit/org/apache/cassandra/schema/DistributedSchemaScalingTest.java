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

import com.sun.management.ThreadMXBean;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.tcm.Epoch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Constructing a {@link DistributedSchema} must cost the same whether the cluster holds a hundred tables or a hundred
 * thousand.
 *
 * <p>A {@code DistributedSchema} is built at least twice for every DDL statement - once by the transformation itself,
 * and again by {@link DistributedSchema#withLastModified} when the resulting {@link org.apache.cassandra.tcm.Transformer}
 * stamps the new epoch onto it. Any per-construction cost that scales with the size of the schema is therefore paid
 * repeatedly on every {@code CREATE TABLE}, which turns bulk schema creation into an O(n^2) operation and eventually
 * exhausts the heap.
 *
 * <p>The {@link Keyspaces} handed to the constructor already maintains a by-{@link TableId} index of every table and
 * view it holds, so re-deriving one is duplicated work as well as duplicated memory.
 *
 * <p>These tests measure allocation rather than elapsed time. Allocation is counted exactly by the JVM rather than
 * sampled, does not depend on GC timing or on what else the machine is doing, and is the quantity that actually
 * matters here: the failure mode users hit is not gradual slowdown but a GC wall, reached because each statement
 * allocates in proportion to the whole schema. A wall-clock assertion at this granularity would be flaky; this is not.
 *
 * <p>The scaling tests assert a growth <em>ratio</em> rather than an absolute byte count, so they do not need
 * re-tuning for a different JDK, allocator, or machine.
 */
public class DistributedSchemaScalingTest
{
    /** Table counts differing by 8x. An O(schema-size) constructor allocates ~8x more at LARGE; a correct one ~1x. */
    private static final int SMALL = 400;
    private static final int LARGE = 3200;

    /** Keyspaces the tables are spread across in the multi-keyspace fixture; held constant as the table count grows. */
    private static final int KEYSPACES = 10;

    /**
     * Generous: the honest expectation is ~1.0. Anything below this is flat enough to rule out per-table work; the
     * behaviour under test allocates ~8x, so the gap between pass and fail is wide and no borderline case exists.
     */
    private static final double MAX_GROWTH = 3.0;

    private static final int WARMUP = 10;
    private static final int ITERATIONS = 20;

    private static final ThreadMXBean THREADS = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    /** Kept live so the schemas under measurement cannot be optimised away. */
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
     * The shape a real cluster has: tables spread over several keyspaces. Stamping a new epoch onto the schema must
     * not touch tables at all.
     */
    @Test
    public void withLastModifiedDoesNotScaleWithNumberOfTables()
    {
        long small = bytesPerWithLastModified(schemaOverManyKeyspaces(SMALL));
        long large = bytesPerWithLastModified(schemaOverManyKeyspaces(LARGE));

        assertGrowthIsFlat("stamping an epoch onto a schema of", small, large);
    }

    /**
     * The reported case: one keyspace holding very many tables. Kept separate from the multi-keyspace fixture because
     * a per-keyspace cost and a per-table cost are different bugs, and only this shape isolates the latter.
     */
    @Test
    public void withLastModifiedDoesNotScaleWithTablesInOneKeyspace()
    {
        long small = bytesPerWithLastModified(schemaOverOneKeyspace(SMALL));
        long large = bytesPerWithLastModified(schemaOverOneKeyspace(LARGE));

        assertGrowthIsFlat("stamping an epoch onto a single keyspace holding", small, large);
    }

    // ------------------------------------------------------- correctness guards

    /** Every table must still be reachable by id, and nothing else must be. */
    @Test
    public void everyTableIsReachableById()
    {
        Keyspaces keyspaces = manyKeyspaces(50);
        DistributedSchema schema = new DistributedSchema(keyspaces, Epoch.FIRST);

        int seen = 0;
        for (KeyspaceMetadata ksm : keyspaces)
        {
            for (TableMetadata table : ksm.tables)
            {
                assertSame("table " + table + " looked up by id", table, schema.getTableMetadata(table.id));
                seen++;
            }
        }
        assertEquals("every table in the fixture was checked", 50, seen);
        assertNull("an unknown id resolves to null", schema.getTableMetadata(TableId.generate()));
    }

    /**
     * Views are reachable by id too. This is what the by-id index was built from ({@code tablesAndViews()}), so it is
     * the guard that a delegating implementation must not silently narrow to base tables only.
     */
    @Test
    public void viewsAreReachableById()
    {
        TableMetadata base = table("ks", 0);
        TableMetadata viewTable = table("ks", 1);
        ViewMetadata view = new ViewMetadata(base.id, base.name, true, WhereClause.empty(), viewTable);

        KeyspaceMetadata ksm = KeyspaceMetadata.create("ks", KeyspaceParams.simple(1),
                                                       Tables.of(base), Views.builder().put(view).build(),
                                                       Types.none(), UserFunctions.none());
        DistributedSchema schema = new DistributedSchema(Keyspaces.of(ksm), Epoch.FIRST);

        assertSame("the base table", base, schema.getTableMetadata(base.id));
        assertSame("the view, looked up by its own id", viewTable, schema.getTableMetadata(viewTable.id));
    }

    /** Stamping an epoch changes the epoch and the version, and nothing about the tables. */
    @Test
    public void withLastModifiedPreservesTableLookups()
    {
        Keyspaces keyspaces = manyKeyspaces(50);
        DistributedSchema before = new DistributedSchema(keyspaces, Epoch.FIRST);
        DistributedSchema after = before.withLastModified(Epoch.FIRST.nextEpoch());

        assertEquals("epoch was advanced", Epoch.FIRST.nextEpoch(), after.lastModified());
        assertSame("keyspaces carried over by reference", before.getKeyspaces(), after.getKeyspaces());

        for (KeyspaceMetadata ksm : keyspaces)
            for (TableMetadata table : ksm.tables)
                assertSame(table, after.getTableMetadata(table.id));
    }

    /**
     * The by-id view must have the same lifetime as the schema that exposes it: a table dropped from the keyspaces
     * must not remain resolvable through a schema built from the smaller {@link Keyspaces}, and must stay resolvable
     * through the older schema that still holds the larger one.
     */
    @Test
    public void droppedTablesDisappearWithoutDisturbingTheOlderSchema()
    {
        Keyspaces keyspaces = manyKeyspaces(50);
        KeyspaceMetadata dropped = keyspaces.iterator().next();
        TableMetadata table = dropped.tables.iterator().next();

        DistributedSchema before = new DistributedSchema(keyspaces, Epoch.FIRST);
        DistributedSchema after = new DistributedSchema(keyspaces.without(dropped.name), Epoch.FIRST.nextEpoch());

        assertNotNull("still present in the schema that predates the drop", before.getTableMetadata(table.id));
        assertNull("gone from the schema that follows it", after.getTableMetadata(table.id));
    }

    /** Validation still rejects a keyspace whose table claims to live somewhere else. */
    @Test
    public void mismatchedKeyspaceIsStillRejected()
    {
        KeyspaceMetadata ksm = KeyspaceMetadata.create("ks", KeyspaceParams.simple(1), Tables.of(table("other", 0)));
        try
        {
            new DistributedSchema(Keyspaces.of(ksm), Epoch.FIRST);
            throw new AssertionError("expected construction to reject a table pointing at the wrong keyspace");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("points to keyspace"));
        }
    }

    // ---------------------------------------------------------------- helpers

    private static void assertGrowthIsFlat(String what, long small, long large)
    {
        double growth = (double) large / small;
        String detail = String.format("%s %,d tables allocated %,d bytes; at %,d tables it allocated %,d bytes " +
                                      "(%.1fx). The schema is %dx larger, so a construction whose cost is " +
                                      "independent of schema size should be near 1.0x.",
                                      what, SMALL, small, LARGE, large, growth, LARGE / SMALL);
        assertTrue(detail, growth < MAX_GROWTH);
    }

    /**
     * Allocation attributable to one {@link DistributedSchema#withLastModified} call, warmed up so lazy initialisation
     * is not counted.
     */
    private static long bytesPerWithLastModified(DistributedSchema schema)
    {
        Epoch epoch = Epoch.FIRST.nextEpoch();

        for (int i = 0; i < WARMUP; i++)
            sink = schema.withLastModified(epoch);

        long id = Thread.currentThread().getId();
        long start = THREADS.getThreadAllocatedBytes(id);
        for (int i = 0; i < ITERATIONS; i++)
            sink = schema.withLastModified(epoch);
        return (THREADS.getThreadAllocatedBytes(id) - start) / ITERATIONS;
    }

    private static DistributedSchema schemaOverManyKeyspaces(int tables)
    {
        return new DistributedSchema(manyKeyspaces(tables), Epoch.FIRST);
    }

    private static DistributedSchema schemaOverOneKeyspace(int tables)
    {
        return new DistributedSchema(Keyspaces.of(keyspace("ks", tables)), Epoch.FIRST);
    }

    /** {@code total} tables spread evenly over a fixed number of keyspaces. */
    private static Keyspaces manyKeyspaces(int total)
    {
        List<KeyspaceMetadata> keyspaces = new ArrayList<>(KEYSPACES);
        for (int i = 0; i < KEYSPACES; i++)
            keyspaces.add(keyspace("ks_" + i, total / KEYSPACES));
        return Keyspaces.of(keyspaces);
    }

    private static KeyspaceMetadata keyspace(String name, int tableCount)
    {
        List<TableMetadata> tables = new ArrayList<>(tableCount);
        for (int i = 0; i < tableCount; i++)
            tables.add(table(name, i));
        return KeyspaceMetadata.create(name, KeyspaceParams.simple(1), Tables.of(tables));
    }

    private static TableMetadata table(String keyspace, int i)
    {
        return TableMetadata.builder(keyspace, "table_" + i)
                            .addPartitionKeyColumn("pk", Int32Type.instance)
                            .addClusteringColumn("ck", Int32Type.instance)
                            .addRegularColumn("v", Int32Type.instance)
                            .build();
    }
}
