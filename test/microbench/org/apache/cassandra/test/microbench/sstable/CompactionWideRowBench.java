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

import java.util.Arrays;
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
public class CompactionWideRowBench extends CompactionBench
{
    @Param("1")
    int rowPerPkCount = 1;

    @Param("1")
    int ckCount = 1;

    @Param("1")
    int colCount = 1;

    protected void createSStables()
    {
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String tableCreate = "CREATE TABLE %s ( userid bigint";
        for (int i=0;i<ckCount;i++)
            tableCreate += ",ck"+i+ " bigint";
        for (int i=0;i<colCount;i++)
            tableCreate += ",c"+i+ " bigint";
        tableCreate += ", PRIMARY KEY(userid";
        for (int i=0;i<ckCount;i++)
            tableCreate += ",ck"+i;
        tableCreate +="))";
        table = createTable(keyspace, tableCreate);
        execute("use "+keyspace+";");
        writeStatement = "INSERT INTO "+table+"(userid";
        for (int i=0;i<ckCount;i++)
            writeStatement += ",ck"+i;
        for (int i=0;i<colCount;i++)
            writeStatement += ",c"+i;
        writeStatement += ")VALUES(?";
        for (int i=0;i<ckCount+colCount;i++)
            writeStatement += ",?";
        writeStatement += ")";

        Object[] values = new Object[1+ckCount+colCount];
        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(c -> c.disableAutoCompaction()));

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        int pkCount = rowCount/rowPerPkCount;
        for (int j = 0; j < sstableCount; j++)
        {
            int pPrefix = overlap.startsWith("PK") ? 0 : j * rowCount;
            int rPrefix = overlap.startsWith("PK.ROW") ? 0 : j * rowCount;
            for (long pkIndex = 0; pkIndex < pkCount; pkIndex++)
            {
                for (long rowIndex = 0; rowIndex < rowPerPkCount; rowIndex++)
                {
                    values[0] = (pPrefix + pkIndex);
                    Arrays.fill(values, 1, ckCount + 1, (rPrefix + rowIndex));
                    Arrays.fill(values, 1 + ckCount, values.length, j * rowCount + pkIndex);
                    execute(writeStatement, values);
                }
            }

            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        }
    }
}
