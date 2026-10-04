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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterators;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Exercises byte paging when replicas disagree about the size and liveness of collection rows. A byte limit is
 * applied independently by each replica, so the deliberately different collection sizes below make replicas stop
 * their pages after different clustering rows.
 */
@RunWith(Parameterized.class)
public class BytePagingCollectionsTest extends TestBaseImpl
{
    private static final int NODES = 3;
    private static final int ROWS = 12;
    private static final int[] PAGE_SIZES = { 1, 64, 127, 256, 511, 1024, 4096 };
    private static final long[] SEEDS = { 0x11745L, 0xC011EC7L };
    private static final AtomicInteger sequence = new AtomicInteger();

    private static Cluster cluster;

    @Parameterized.Parameter
    public CollectionKind kind;

    @Parameterized.Parameters(name = "collection={0}")
    public static CollectionKind[] collectionKinds()
    {
        return CollectionKind.values();
    }

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = init(Cluster.build(NODES)
                              .withConfig(config -> config.set("hinted_handoff_enabled", false))
                              .start());
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void fuzzReplicaCollectionPageBoundaries()
    {
        for (long seed : SEEDS)
        {
            String table = createTable(kind, "NONE");
            try
            {
                populateDivergentCollections(table, kind, seed, ROWS);
                if ((seed & 1) == 0)
                    cluster.stream().forEach(node -> node.flush(KEYSPACE));

                String query = select(table, "");
                Object[][] expected = cluster.coordinator(1).execute(query, ALL);
                for (int pageSize : PAGE_SIZES)
                    assertPagedRows(query, expected, pageSize, kind, seed);
            }
            finally
            {
                dropTable(table);
            }
        }
    }

    @Test
    public void fuzzCollectionPagingWithBlockingReadRepair()
    {
        long seed = SEEDS[kind.ordinal() & 1];
        String table = createTable(kind, "BLOCKING");
        try
        {
            populateDivergentCollections(table, kind, seed, ROWS);
            if ((kind.ordinal() & 1) == 0)
                cluster.stream().forEach(node -> node.flush(KEYSPACE));

            String query = select(table, "");
            long repairsBefore = readRepairRequests(table);
            Object[][] repaired = page(query, 256);
            long repairs = readRepairRequests(table) - repairsBefore;
            assertTrue(format("Expected byte-paged %s read to trigger read repair", kind), repairs > 0);

            for (int node = 1; node <= NODES; node++)
            {
                Object[][] localRows = cluster.get(node).executeInternal(query);
                assertRowsWithContext(localRows, repaired, kind, seed, 256, "local node " + node);
            }
        }
        finally
        {
            dropTable(table);
        }
    }

    @Test
    public void fuzzCollectionPagingWithShortReadProtection()
    {
        long seed = SEEDS[(kind.ordinal() + 1) & 1];
        String table = createTable(kind, "NONE");
        try
        {
            populateDivergentCollections(table, kind, seed, ROWS + 6);

            // Each replica counts a different subset of these rows as live and also sees a different byte size.
            // Reconciliation removes all nine rows, forcing short-read retries to reach the LIMIT.
            for (int ck = 0; ck < 9; ck++)
                cluster.get(1 + ck % NODES).executeInternal(format("DELETE FROM %s USING TIMESTAMP 1000 WHERE pk=0 AND ck=%d", table, ck));

            String query = select(table, " LIMIT 5");
            Object[][] expected = cluster.coordinator(1).execute(query, ALL);
            assertEquals(5, expected.length);
            for (int i = 0; i < expected.length; i++)
                assertEquals(9 + i, expected[i][1]);
            for (int pageSize : PAGE_SIZES)
                assertPagedRows(query, expected, pageSize, kind, seed);
        }
        finally
        {
            dropTable(table);
        }
    }

    private static String createTable(CollectionKind kind, String readRepair)
    {
        String table = KEYSPACE + ".byte_paging_collections_" + sequence.getAndIncrement();
        cluster.schemaChange(format("CREATE TABLE %s (pk int, ck int, marker int, v %s, PRIMARY KEY (pk, ck)) " +
                                    "WITH read_repair='%s'", table, kind.cqlType, readRepair));
        return table;
    }

    private static void dropTable(String table)
    {
        cluster.schemaChange("DROP TABLE IF EXISTS " + table);
    }

    private static void populateDivergentCollections(String table, CollectionKind kind, long seed, int rows)
    {
        for (int ck = 0; ck < rows; ck++)
        {
            cluster.coordinator(1).execute(format("INSERT INTO %s (pk, ck, marker) VALUES (0, ?, ?) USING TIMESTAMP 1", table),
                                           ALL, ck, ck);
        }

        Random random = new Random(seed ^ (0x9E3779B97F4A7C15L * kind.ordinal()));
        for (int node = 1; node <= NODES; node++)
        {
            for (int ck = 0; ck < rows; ck++)
            {
                int elements = ck == 0 ? new int[]{ 12, 5, 1 }[node - 1] : 1 + random.nextInt(6);
                int elementSize = ck == 0 ? 96 : 16 + random.nextInt(160);
                String literal = kind.literal(node, ck, elements, elementSize);
                String assignment = kind.frozen ? "v = " + literal : "v = v + " + literal;
                cluster.get(node).executeInternal(format("UPDATE %s USING TIMESTAMP %d SET %s WHERE pk=0 AND ck=%d",
                                                         table, 10 + node, assignment, ck));
            }
        }
    }

    private static String select(String table, String suffix)
    {
        return format("SELECT pk, ck, marker, v FROM %s WHERE pk=0%s", table, suffix);
    }

    private static void assertPagedRows(String query,
                                        Object[][] expected,
                                        int pageSize,
                                        CollectionKind kind,
                                        long seed)
    {
        assertRowsWithContext(page(query, pageSize), expected, kind, seed, pageSize, "distributed result");
    }

    private static Object[][] page(String query, int pageSize)
    {
        Iterator<Object[]> iterator = cluster.coordinator(1).executeWithPagingInBytes(query, ALL, pageSize);
        return Iterators.toArray(iterator, Object[].class);
    }

    private static void assertRowsWithContext(Object[][] actual,
                                              Object[][] expected,
                                              CollectionKind kind,
                                              long seed,
                                              int pageSize,
                                              String source)
    {
        try
        {
            assertRows(actual, expected);
        }
        catch (AssertionError e)
        {
            throw new AssertionError(format("%s mismatch for kind=%s seed=%d pageSizeInBytes=%d",
                                            source, kind, seed, pageSize), e);
        }
    }

    private static long readRepairRequests(String table)
    {
        String tableName = table.substring(table.indexOf('.') + 1);
        return cluster.get(1).callOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
            return cfs.metric.readRepairRequests.getCount();
        });
    }

    public enum CollectionKind
    {
        LIST("list<text>", false),
        SET("set<text>", false),
        MAP("map<text, text>", false),
        FROZEN_LIST("frozen<list<text>>", true),
        FROZEN_SET("frozen<set<text>>", true),
        FROZEN_MAP("frozen<map<text, text>>", true);

        private final String cqlType;
        private final boolean frozen;

        CollectionKind(String cqlType, boolean frozen)
        {
            this.cqlType = cqlType;
            this.frozen = frozen;
        }

        private String literal(int node, int ck, int elements, int elementSize)
        {
            List<String> values = new ArrayList<>(elements);
            for (int element = 0; element < elements; element++)
            {
                String id = format("n%d_c%d_e%d", node, ck, element);
                String value = id + '_' + repeat((char) ('a' + node), elementSize);
                values.add(this == MAP || this == FROZEN_MAP
                           ? format("'%s':'%s'", id, value)
                           : format("'%s'", value));
            }

            String joined = String.join(",", values);
            return this == LIST || this == FROZEN_LIST ? '[' + joined + ']' : '{' + joined + '}';
        }

        private static String repeat(char value, int count)
        {
            char[] chars = new char[count];
            java.util.Arrays.fill(chars, value);
            return new String(chars);
        }
    }
}
