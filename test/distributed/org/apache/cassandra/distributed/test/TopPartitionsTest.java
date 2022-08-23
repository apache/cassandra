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
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.PreviewRepairTest.logMark;
import static org.junit.Assert.assertEquals;
import static org.psjava.util.AssertStatus.assertTrue;

@RunWith(Parameterized.class)
public class TopPartitionsTest extends TestBaseImpl
{
    public enum Repair
    {
        Incremental, Full, FullPreview
    }

    private static AtomicInteger COUNTER = new AtomicInteger(0);
    private static Cluster CLUSTER;

    private final Repair repair;

    public TopPartitionsTest(Repair repair)
    {
        this.repair = repair;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> messages()
    {
        return Stream.of(Repair.values())
                     .map(a -> new Object[]{ a })
                     .collect(Collectors.toList());
    }

    @BeforeClass
    public static void setup() throws IOException
    {
        CLUSTER = init(Cluster.build(2).withConfig(config ->
                                                   config.set("min_tracked_partition_size", "0MiB")
                                                         .set("min_tracked_partition_tombstone_count", 0)
                                                         .with(GOSSIP, NETWORK))
                              .start());
    }

    @AfterClass
    public static void cleanup()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Before
    public void before()
    {
        setCount(10, 10);
    }

    @Test
    public void basicPartitionSizeTest() throws TimeoutException
    {
        String name = "tbl" + COUNTER.getAndIncrement();
        String table = KEYSPACE + "." + name;
        CLUSTER.schemaChange("create table " + table + " (id int, ck int, t int, primary key (id, ck))");
        for (int i = 0; i < 100; i++)
            for (int j = 0; j < i; j++)
                CLUSTER.coordinator(1).execute("insert into " + table + " (id, ck, t) values (?,?,?)", ConsistencyLevel.ALL, i, j, i * j + 100);

        repair();
        CLUSTER.forEach(inst -> inst.runOnInstance(() -> {
            // partitions 99 -> 90 are the largest, make sure they are in the map;
            Map<String, Long> sizes = Keyspace.open(KEYSPACE).getColumnFamilyStore(name).getTopSizePartitions();
            for (int i = 99; i >= 90; i--)
                assertTrue(sizes.containsKey(String.valueOf(i)));

            Map<String, Long> tombstones = Keyspace.open(KEYSPACE).getColumnFamilyStore(name).getTopTombstonePartitions();
            assertEquals(10, tombstones.size());
            assertTrue(tombstones.values().stream().allMatch(l -> l == 0));
        }));

        // make sure incremental repair doesn't change anything;
        CLUSTER.get(1).nodetool("repair", KEYSPACE);
        CLUSTER.forEach(inst -> inst.runOnInstance(() -> {
            Map<String, Long> sizes = Keyspace.open(KEYSPACE).getColumnFamilyStore(name).getTopSizePartitions();
            for (int i = 99; i >= 90; i--)
                assertTrue(sizes.containsKey(String.valueOf(i)));
        }));
    }

    @Test
    public void configChangeTest() throws TimeoutException
    {
        String name = "tbl" + COUNTER.getAndIncrement();
        String table = KEYSPACE + "." + name;
        CLUSTER.schemaChange("create table " + table + " (id int, ck int, t int, primary key (id, ck))");
        for (int i = 0; i < 100; i++)
        {
            for (int j = 0; j < i; j++)
            {
                CLUSTER.coordinator(1).execute("insert into " + table + " (id, ck, t) values (?,?,?)", ConsistencyLevel.ALL, i, j, i * j + 100);
                CLUSTER.coordinator(1).execute("DELETE FROM " + table + " where id = ? and ck = ?", ConsistencyLevel.ALL, i, -j);
            }
        }

        // top should have 10 elements
        repair();
        CLUSTER.get(1).runOnInstance(() -> {
            ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(name);
            Assertions.assertThat(store.getTopTombstonePartitions()).hasSize(10);
            Assertions.assertThat(store.getTopTombstonePartitions()).hasSize(10);
        });

        // reconfigure and repair; top should have 20 elements
        setCount(20, 20);
        repair();
        CLUSTER.get(1).runOnInstance(() -> {
            ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(name);
            Assertions.assertThat(store.getTopTombstonePartitions()).hasSize(20);
            Assertions.assertThat(store.getTopTombstonePartitions()).hasSize(20);
        });

        // test shrinking config
        setCount(5, 5);
        repair();
        CLUSTER.get(1).runOnInstance(() -> {
            ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(name);
            Assertions.assertThat(store.getTopTombstonePartitions()).hasSize(5);
            Assertions.assertThat(store.getTopTombstonePartitions()).hasSize(5);
        });
    }

    @Test
    public void basicRowTombstonesTest() throws InterruptedException, TimeoutException
    {
        String name = "tbl" + COUNTER.getAndIncrement();
        String table = KEYSPACE + "." + name;
        CLUSTER.schemaChange("create table " + table + " (id int, ck int, t int, primary key (id, ck)) with gc_grace_seconds = 1");
        for (int i = 0; i < 100; i++)
            for (int j = 0; j < i; j++)
                CLUSTER.coordinator(1).execute("DELETE FROM " + table + " where id = ? and ck = ?", ConsistencyLevel.ALL, i, j);
        repair();
        // tombstones not purgeable
        CLUSTER.get(1).runOnInstance(() -> {
            Map<String, Long> tombstones = Keyspace.open(KEYSPACE).getColumnFamilyStore(name).getTopTombstonePartitions();
            for (int i = 99; i >= 90; i--)
            {
                assertTrue(tombstones.containsKey(String.valueOf(i)));
                assertEquals(i, (long) tombstones.get(String.valueOf(i)));
            }
        });
        Thread.sleep(2000);
        // count purgeable tombstones;
        repair();
        CLUSTER.get(1).runOnInstance(() -> {
            Map<String, Long> tombstones = Keyspace.open(KEYSPACE).getColumnFamilyStore(name).getTopTombstonePartitions();
            for (int i = 99; i >= 90; i--)
            {
                assertTrue(tombstones.containsKey(String.valueOf(i)));
                assertEquals(i, (long) tombstones.get(String.valueOf(i)));
            }
        });
        CLUSTER.get(1).forceCompact(KEYSPACE, name);
        // all tombstones actually purged;
        repair();
        CLUSTER.get(1).runOnInstance(() -> {
            Map<String, Long> tombstones = Keyspace.open(KEYSPACE).getColumnFamilyStore(name).getTopTombstonePartitions();
            assertTrue(tombstones.values().stream().allMatch(l -> l == 0));
        });
    }

    @Test
    public void basicRegularTombstonesTest() throws InterruptedException, TimeoutException
    {
        String name = "tbl" + COUNTER.getAndIncrement();
        String table = KEYSPACE + "." + name;
        CLUSTER.schemaChange("create table " + table + " (id int, ck int, t int, primary key (id, ck)) with gc_grace_seconds = 1");
        for (int i = 0; i < 100; i++)
            for (int j = 0; j < i; j++)
                CLUSTER.coordinator(1).execute("UPDATE " + table + " SET t = null where id = ? and ck = ?", ConsistencyLevel.ALL, i, j);
        repair();
        // tombstones not purgeable
        CLUSTER.get(1).runOnInstance(() -> {
            Map<String, Long> tombstones = Keyspace.open(KEYSPACE).getColumnFamilyStore(name).getTopTombstonePartitions();
            for (int i = 99; i >= 90; i--)
            {
                assertTrue(tombstones.containsKey(String.valueOf(i)));
                assertEquals(i, (long) tombstones.get(String.valueOf(i)));
            }
        });
        Thread.sleep(2000);
        // count purgeable tombstones;
        repair();
        CLUSTER.get(1).runOnInstance(() -> {
            Map<String, Long> tombstones = Keyspace.open(KEYSPACE).getColumnFamilyStore(name).getTopTombstonePartitions();
            for (int i = 99; i >= 90; i--)
            {
                assertTrue(tombstones.containsKey(String.valueOf(i)));
                assertEquals(i, (long) tombstones.get(String.valueOf(i)));
            }
        });

        CLUSTER.get(1).forceCompact(KEYSPACE, name);
        // all tombstones actually purged;
        repair();
        CLUSTER.get(1).runOnInstance(() -> {
            Map<String, Long> tombstones = Keyspace.open(KEYSPACE).getColumnFamilyStore(name).getTopTombstonePartitions();
            assertTrue(tombstones.values().stream().allMatch(l -> l == 0));
        });
    }

    private static void setCount(int size, int tombstone)
    {
        CLUSTER.forEach(i -> i.runOnInstance(() -> {
            DatabaseDescriptor.setMaxTopSizePartitionCount(size);
            DatabaseDescriptor.setMaxTopTombstonePartitionCount(tombstone);
        }));
    }

    private void repair() throws TimeoutException
    {
        switch (repair)
        {
            case Incremental:
            {
                // IR will not populate, as it only looks at non-repaired data
                // to trigger this patch, we need IR + --validate
                long[] marks = logMark(CLUSTER);
                NodeToolResult res = CLUSTER.get(1).nodetoolResult("repair", KEYSPACE);
                res.asserts().success();
                PreviewRepairTest.waitLogsRepairFullyFinished(CLUSTER, marks);
                res = CLUSTER.get(1).nodetoolResult("repair", "--validate", KEYSPACE);
                res.asserts().success();
                res.asserts().notificationContains("Repaired data is in sync");
            }
            break;
            case Full:
            {
                CLUSTER.get(1).nodetoolResult("repair", "-full", KEYSPACE).asserts().success();
            }
            break;
            case FullPreview:
            {
                CLUSTER.get(1).nodetoolResult("repair", "-full", "--preview", KEYSPACE).asserts().success();
            }
            break;
            default:
                throw new AssertionError("Unknown repair type: " + repair);
        }
    }

    @Test
    public void basicRangeTombstonesTest() throws Throwable
    {
        String name = "tbl" + COUNTER.getAndIncrement();
        String table = KEYSPACE + "." + name;
        CLUSTER.schemaChange("create table " + table + " (id int, ck int, t int, primary key (id, ck)) with gc_grace_seconds = 1");
        for (int i = 0; i < 100; i++)
            for (int j = 0; j < i; j++)
                CLUSTER.coordinator(1).execute("DELETE FROM " + table + " WHERE id = ? and ck >= ? and ck <= ?", ConsistencyLevel.ALL, i, j, j);
        repair();
        // tombstones not purgeable
        CLUSTER.get(1).runOnInstance(() -> {
            Map<String, Long> tombstones = Keyspace.open(KEYSPACE).getColumnFamilyStore(name).getTopTombstonePartitions();
            // note that we count range tombstone markers - so the count will be double the number of deletions we did above
            for (int i = 99; i >= 90; i--)
                assertEquals(i * 2, (long)tombstones.get(String.valueOf(i)));
        });
        Thread.sleep(2000);
        // count purgeable tombstones;
        repair();
        CLUSTER.get(1).runOnInstance(() -> {
            Map<String, Long> tombstones = Keyspace.open(KEYSPACE).getColumnFamilyStore(name).getTopTombstonePartitions();
            for (int i = 99; i >= 90; i--)
                assertEquals(i * 2, (long)tombstones.get(String.valueOf(i)));
        });

        CLUSTER.get(1).forceCompact(KEYSPACE, name);
        // all tombstones actually purged;
        repair();
        CLUSTER.get(1).runOnInstance(() -> {
            Map<String, Long> tombstones = Keyspace.open(KEYSPACE).getColumnFamilyStore(name).getTopTombstonePartitions();
            assertTrue(tombstones.values().stream().allMatch( l -> l == 0));
        });
    }
}
