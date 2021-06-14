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

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 3, time = 5)
@Fork(value = 1, jvmArgsAppend = "-Xmx4G")
@Threads(1)
@State(Scope.Benchmark)
public class SSTableWriterBench extends AbstractSSTableBench
{
    @Param({ "table", "table_with_clustering" })
    public String tableName;

    @Param({ "BIG", "BTI" })
    public String formatName;

    public final static int KEY_SIZE = 8;
    public final static int P_KEYS = 1000;
    public final static int C_KEYS = 1000;
    public final static int VAL_SIZE = 1;

    public ByteBuffer[] ckeys = new ByteBuffer[C_KEYS];
    public DecoratedKey[] pkeys = new DecoratedKey[P_KEYS];

    private SSTableWriter tableWriter;
    private TableMetadata tableMetadata;
    boolean hasClustering = false;
    private ColumnFamilyStore table;
    private LifecycleTransaction txn;

    @Setup(Level.Trial)
    public void setupTrial()
    {
        Keyspace ks = prepareMetadata();
        pkeys = prepareDecoratedKeys(0, P_KEYS, KEY_SIZE);

        table = ks.getColumnFamilyStore(tableName);
        tableMetadata = table.metadata();
        hasClustering = !tableMetadata.clusteringColumns().isEmpty();

        if (hasClustering)
        {
            ckeys = new ByteBuffer[C_KEYS];
            for (int i = 0; i < ckeys.length; i++)
            {
                ckeys[i] = ByteBuffer.allocate(KEY_SIZE);
                ckeys[i].putInt(0, i);
            }
        }
    }

    @Setup(Level.Invocation)
    public void setupInvocation() throws Exception
    {
        txn = LifecycleTransaction.offline(OperationType.WRITE);
        tableWriter = createWriter(table, getFormat(formatName), txn);
    }

    @Benchmark
    public void writeWithClusteringTest()
    {
        for (int i = 0; i < P_KEYS; i++)
        {
            UpdateBuilder builder = UpdateBuilder.create(tableMetadata, pkeys[i].getKey().duplicate()).withTimestamp(1);
            if (hasClustering)
                for (int j = 0; j < C_KEYS; j++)
                    builder.newRow(ckeys[j].duplicate()).add("val", ByteBuffer.allocate(VAL_SIZE));
            else
                builder.newRow().add("val", ByteBuffer.allocate(VAL_SIZE));

            tableWriter.append(builder.build().unfilteredIterator());
        }
    }

    @TearDown(Level.Invocation)
    public void tearDown()
    {
        tableWriter.abort();
        tableWriter.close();
        txn.close();

        FileUtils.deleteRecursive(tableWriter.descriptor.directory);
    }
}
