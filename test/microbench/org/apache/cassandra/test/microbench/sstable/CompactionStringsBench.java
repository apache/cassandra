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


import java.util.Random;
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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 25, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class CompactionStringsBench extends CompactionBench
{
    @Param("128")
    int stringSizeMin = 128;
    @Param("256")
    int stringSizeMax = 256;

    protected void createSStables()
    {
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace, "CREATE TABLE %s ( pk ascii, ck ascii, c ascii, PRIMARY KEY(pk, ck))");
        execute("use "+keyspace+";");
        writeStatement = "INSERT INTO "+table+"(pk,ck,c)VALUES(?,?,?)";

        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(c -> c.disableAutoCompaction()));

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        for (int j = 0; j < sstableCount; j++)
        {
            Random r = new Random(42L);
            int pPrefix = overlap.startsWith("PK") ? 0 : j * rowCount;
            int rPrefix = overlap.startsWith("PK.ROW") ? 0 : j * rowCount;
            for (long i = 0; i < rowCount; i++)
            {
                execute(writeStatement, (pPrefix + i) + getNextString(r), (rPrefix + i) + getNextString(r), getNextString(r));
            }

            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        }
    }

    private String getNextString(Random r)
    {
        int rangeDelta = stringSizeMax - stringSizeMin;
        int rangeRandom = rangeDelta > 0 ? r.nextInt(rangeDelta) : 0;
        byte[] blob = new byte[stringSizeMin + rangeRandom];
        r.nextBytes(blob);
        for (int i = 0; i < blob.length; i++)
        {
            if (blob[i] < 0)
                blob[i] = 0;
        }
        String nextString = new String(blob);
        return nextString;
    }
}
