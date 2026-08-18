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
 * Isolates the cost of SSTableCursorReader.CellCursor's dropped-column check
 * (sstableHasDroppedColumns) on an otherwise identical row/cell shape to the base
 * CompactionBench: same schema, same row count, same value columns, differing only in whether
 * a column was dropped from the table AFTER these sstables were flushed (so their on-disk
 * header still carries it, and every cell merged from them pays the per-cell drop check).
 *
 * dropColumn=false is the CompactionBench baseline shape with 5 extra value columns instead of
 * 1, so the two dropColumn values are apples-to-apples on data volume/shape; only the drop
 * horizon differs.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 25, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class CompactionDroppedColumnBench extends CompactionBench
{
    private static final int VALUE_COLUMN_COUNT = 5;

    @Param({"false", "true"})
    boolean dropColumn = false;

    protected void createSStables()
    {
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        StringBuilder tableCreate = new StringBuilder("CREATE TABLE %s ( userid bigint, picid bigint");
        for (int i = 0; i < VALUE_COLUMN_COUNT; i++)
            tableCreate.append(", c").append(i).append(" bigint");
        tableCreate.append(", PRIMARY KEY(userid, picid))");
        table = createTable(keyspace, tableCreate.toString());
        execute("use " + keyspace + ";");

        StringBuilder insert = new StringBuilder("INSERT INTO " + table + "(userid,picid");
        for (int i = 0; i < VALUE_COLUMN_COUNT; i++)
            insert.append(",c").append(i);
        insert.append(")VALUES(?,?");
        for (int i = 0; i < VALUE_COLUMN_COUNT; i++)
            insert.append(",?");
        insert.append(")");
        writeStatement = insert.toString();

        Object[] values = new Object[2 + VALUE_COLUMN_COUNT];

        Keyspace.system().forEach(k -> k.getColumnFamilyStores().forEach(c -> c.disableAutoCompaction()));

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        for (int j = 0; j < sstableCount; j++)
        {
            int pPrefix = overlap.startsWith("PK") ? 0 : j * rowCount;
            int rPrefix = overlap.startsWith("PK.ROW") ? 0 : j * rowCount;
            for (long i = 0; i < rowCount; i++)
            {
                values[0] = pPrefix + i;
                values[1] = rPrefix + i;
                for (int c = 0; c < VALUE_COLUMN_COUNT; c++)
                    values[2 + c] = j * rowCount + i;
                execute(writeStatement, values);
            }

            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        }

        // Dropped AFTER the sstables above are flushed: their on-disk header still carries c0,
        // so DeserializationHelper.hasDroppedColumns()/sstableHasDroppedColumns is true for
        // every one of them and every cell read back from them pays the per-cell drop check —
        // the exact condition SSTableCursorReader.CellCursor's droppedTimeArray targets.
        if (dropColumn)
            execute("ALTER TABLE " + table + " DROP c0");
    }
}
