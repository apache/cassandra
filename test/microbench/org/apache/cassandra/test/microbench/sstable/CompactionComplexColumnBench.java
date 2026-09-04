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

package org.apache.cassandra.test.microbench.sstable;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.test.microbench.CompactionBench;

/**
 * Measures compaction of a table that holds multi-cell columns: a map and a set. It therefore
 * measures how the cursor path reads, merges and writes a complex column, and not only how it
 * handles a single-cell column.
 *
 * The data makes the merge work on single cells, and not on whole columns. Half of the element
 * keys of each collection are the same in every sstable, so those cells meet each other by cell
 * path. The other half belong to one sstable only, so they merge as separate cells. Every fourth
 * row also takes an update of one map element, which splits a column across two sstables.
 *
 * To compare the two compaction paths, add {@code -p isCursor=true,false}. To compare a change
 * against its baseline, run the same command on both trees.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 25, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class CompactionComplexColumnBench extends CompactionBench
{
    /** elements written into each collection column of each row */
    @Param("8")
    int elementsPerCollection = 8;

    protected void createSStables()
    {
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, ck bigint, m map<text, bigint>, s set<int>, v bigint, PRIMARY KEY(userid, ck))");
        execute("use " + keyspace + ";");
        writeStatement = "INSERT INTO " + table + "(userid,ck,m,s,v)VALUES(?,?,?,?,?)";
        String updateStatement = "UPDATE " + table + " SET m[?] = ? WHERE userid = ? AND ck = ?";

        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(c -> c.disableAutoCompaction()));

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        for (int j = 0; j < sstableCount; j++)
        {
            int pPrefix = overlap.startsWith("PK") ? 0 : j * rowCount;
            int rPrefix = overlap.startsWith("PK.ROW") ? 0 : j * rowCount;
            for (long i = 0; i < rowCount; i++)
            {
                Map<String, Long> m = new LinkedHashMap<>();
                Set<Integer> s = new LinkedHashSet<>();
                for (int e = 0; e < elementsPerCollection; e++)
                {
                    // An even element uses the same path in every sstable, so the merge compares
                    // those cells. An odd element belongs to this sstable only.
                    if (e % 2 == 0)
                    {
                        m.put("shared" + e, i + e);
                        s.add(e);
                    }
                    else
                    {
                        m.put("s" + j + '-' + e, i + e);
                        s.add(j * elementsPerCollection + e);
                    }
                }
                execute(writeStatement, (pPrefix + i), (rPrefix + i), m, s, j * rowCount + i);
                // This update splits the map of the row across sstables, so the column has more
                // than one source in the merge.
                if (i % 4 == 0)
                    execute(updateStatement, "extra", i, (pPrefix + i), (rPrefix + i));
            }

            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        }
    }
}
