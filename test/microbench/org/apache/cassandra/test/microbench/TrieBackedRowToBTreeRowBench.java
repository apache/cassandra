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

package org.apache.cassandra.test.microbench;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.partitions.TrieBackedPartition;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.TrieBackedRow;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.memory.HeapCloner;

/// Measures the copy of `TrieMemtable` rows to the heap as `BTreeRow`s, for rows with simple and complex columns.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = { "-Xmx2G", "-Xms2G" })
@Threads(1)
@State(Scope.Benchmark)
public class TrieBackedRowToBTreeRowBench extends CQLTester
{
    private static final int ROWS = 1000;

    public enum Shape
    {
        /// Five simple columns.
        SIMPLE,
        /// One simple column and a map with ten elements, appended without a complex deletion.
        COMPLEX,
        /// One simple column and a map with ten elements, inserted with a complex deletion.
        COMPLEX_DELETED,
        /// Five simple columns, and a map and a set with ten elements each, inserted with complex deletions.
        MIXED
    }

    @Param({ "SIMPLE", "COMPLEX", "COMPLEX_DELETED", "MIXED" })
    Shape shape;

    @Param({ "heap_buffers", "offheap_objects" })
    String allocationType;

    TableMetadata metadata;
    List<Clustering<?>> clusterings = new ArrayList<>();
    List<DeletionAwareTrie<Object, TrieTombstoneMarker>> rowTries = new ArrayList<>();

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
        // Set before any memtable (and its memory pool) is created.
        DatabaseDescriptor.getRawConfig().memtable_allocation_type = Config.MemtableAllocationType.valueOf(allocationType);
        CQLTester.setUpClass();
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s (pk int, ck int, a int, b int, c int, d text, e text, " +
                                             "m map<int, text>, s set<int>, PRIMARY KEY (pk, ck)) WITH memtable = 'trie'");
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        String qualified = keyspace + '.' + table;

        for (int i = 0; i < ROWS; ++i)
        {
            switch (shape)
            {
                case SIMPLE:
                    execute("INSERT INTO " + qualified + " (pk, ck, a, b, c, d, e) VALUES (0, ?, ?, ?, ?, ?, ?)",
                            i, i, i + 1, i + 2, "d" + i, "e" + i);
                    break;
                case COMPLEX:
                    execute("INSERT INTO " + qualified + " (pk, ck, a) VALUES (0, ?, ?)", i, i);
                    execute("UPDATE " + qualified + " SET m = m + ? WHERE pk = 0 AND ck = ?", map(i), i);
                    break;
                case COMPLEX_DELETED:
                    execute("INSERT INTO " + qualified + " (pk, ck, a, m) VALUES (0, ?, ?, ?)", i, i, map(i));
                    break;
                case MIXED:
                    execute("INSERT INTO " + qualified + " (pk, ck, a, b, c, d, e, m, s) VALUES (0, ?, ?, ?, ?, ?, ?, ?, ?)",
                            i, i, i + 1, i + 2, "d" + i, "e" + i, map(i), set(i));
                    break;
            }
        }

        TrieMemtable memtable = (TrieMemtable) cfs.getCurrentMemtable();
        metadata = cfs.metadata();
        // The partitions of the flush set give direct access to the memtable trie.
        TrieBackedPartition partition = Iterables.getOnlyElement(memtable.getFlushSet(null, null));
        DeletionAwareTrie<Object, TrieTombstoneMarker> partitionTrie = partition.trie();
        for (Iterator<Row> it = partition.rowIterator(); it.hasNext(); )
        {
            Clustering<?> clustering = it.next().clustering();
            clusterings.add(clustering);
            rowTries.add(partitionTrie.tailTrie(partition.path(clustering), false));
        }
        if (rowTries.size() != ROWS)
            throw new AssertionError("Expected " + ROWS + " rows, found " + rowTries.size());

        // Check that both copies agree before measuring them.
        for (int i = 0; i < ROWS; ++i)
        {
            BTreeRow expected = cloneTrieThenConvert(i);
            if (!expected.equals(walk(i)))
                throw new AssertionError("Copies disagree on row " + clusterings.get(i).toString(metadata));
        }
    }

    @TearDown(Level.Trial)
    public void teardown() throws Throwable
    {
        CQLTester.tearDownClass();
        CQLTester.cleanup();
    }

    private static Map<Integer, String> map(int i)
    {
        Map<Integer, String> map = new HashMap<>();
        for (int j = 0; j < 10; ++j)
            map.put(j, "value " + i + '.' + j);
        return map;
    }

    private static Set<Integer> set(int i)
    {
        Set<Integer> set = new HashSet<>();
        for (int j = 0; j < 10; ++j)
            set.add(i * 10 + j);
        return set;
    }

    private BTreeRow walk(int i)
    {
        return TrieBackedRow.toBTreeRow(metadata, clusterings.get(i), rowTries.get(i), HeapCloner.instance);
    }

    /// The copy done before `toBTreeRow`: clone the row as a new on-heap trie, then convert it to a `BTreeRow`.
    private BTreeRow cloneTrieThenConvert(int i)
    {
        return BTreeRow.asBTreeRow(TrieBackedRow.create(metadata, clusterings.get(i), rowTries.get(i)).clone(HeapCloner.instance));
    }

    @Benchmark
    public void toBTreeRow(Blackhole bh)
    {
        for (int i = 0; i < ROWS; ++i)
            bh.consume(walk(i));
    }

    @Benchmark
    public void cloneTrieThenConvert(Blackhole bh)
    {
        for (int i = 0; i < ROWS; ++i)
            bh.consume(cloneTrieThenConvert(i));
    }
}
