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
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.math3.primes.Primes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.util.FileUtils;
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

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 3, time = 5)
@Fork(value = 1, jvmArgsAppend = "-Xmx4G")
@Threads(1)
@State(Scope.Benchmark)
public class SSTableReaderBench extends AbstractSSTableBench
{
    private final static Logger logger = LoggerFactory.getLogger(SSTableReaderBench.class);

    int KEY_SIZE = 8;
    int P_KEYS = 2 << 14;
    int C_KEYS = 2 << 10;
    int VAL_SIZE = 1;

    public ByteBuffer[] ckeys;
    public DecoratedKey[] pkeys;
    public DecoratedKey[] nonpkeys;

    private SSTableReader sstr;
    private SSTableWriter sstw;

    private int idx = 0;

    private final int step = P_KEYS / 2 - 1;

    @Param({ "table", "table_with_clustering" })
    public String tableName;

    @Param({ "BIG", "BTI" })
    public String formatName;
    private ColumnFamilyStore table;
    private LifecycleTransaction txn;

    @Setup(Level.Trial)
    public void setup() throws Exception
    {
        assert Integer.highestOneBit(P_KEYS) == Integer.lowestOneBit(P_KEYS);
        assert Integer.highestOneBit(C_KEYS) == Integer.lowestOneBit(C_KEYS);
        Keyspace ks = prepareMetadata();
        table = ks.getColumnFamilyStore(tableName);
        pkeys = prepareDecoratedKeys(0, P_KEYS, KEY_SIZE);
        nonpkeys = prepareDecoratedKeys(P_KEYS, P_KEYS * 2, KEY_SIZE);
        ckeys = prepareBuffers(0, C_KEYS, KEY_SIZE);

        txn = LifecycleTransaction.offline(OperationType.WRITE);
        sstw = prepareTable(getFormat(formatName), table, txn);
    }

    @Setup(Level.Iteration)
    public void setupIteration() throws Exception
    {
        sstr = prepareReader(getFormat(formatName), sstw);
    }

    /**
     * Generates a quasi random walk over keys but adding a little less than a half and wrapping around.
     */
    private int nextIdx() {
        idx += step;
        if (idx >= P_KEYS)
            idx -= P_KEYS;
        return idx;
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void getEQPosition()
    {
        sstr.getPosition(pkeys[nextIdx()], SSTableReader.Operator.EQ);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void getEQPositionNonExisting()
    {
        sstr.getPosition(nonpkeys[nextIdx()], SSTableReader.Operator.EQ);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void getGTPosition()
    {
        sstr.getPosition(pkeys[nextIdx()], SSTableReader.Operator.GT);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void getGTPositionNonExisting()
    {
        sstr.getPosition(nonpkeys[nextIdx()], SSTableReader.Operator.GT);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void iterateOverAllKeys() throws Exception
    {
        try (PartitionIndexIterator it = sstr.allKeysIterator())
        {
            while (!it.isExhausted()) it.advance();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void fullScanTest()
    {
        try (ISSTableScanner scanner = sstr.getScanner())
        {
            while (scanner.hasNext())
            {
                UnfilteredRowIterator rowIt = scanner.next();
                while (rowIt.hasNext())
                {
                    rowIt.next();
                }
            }
        }
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration()
    {
        sstr.selfRef().release();
    }

    @TearDown(Level.Trial)
    public void tearDown()
    {
        txn.finish();
        txn.close();
        FileUtils.deleteRecursive(sstr.descriptor.directory);
    }

    private SSTableWriter prepareTable(SSTableFormat format, ColumnFamilyStore table, LifecycleTransaction txn) throws Exception
    {
        try (SSTableWriter tableWriter = createWriter(table, format, txn))
        {
            for (int i = 0; i < P_KEYS; i++)
            {
                UpdateBuilder builder = UpdateBuilder.create(table.metadata(), pkeys[i].getKey().duplicate()).withTimestamp(1);
                if (table.metadata().clusteringColumns().isEmpty())
                    builder.newRow().add("val", ByteBuffer.allocate(VAL_SIZE));
                else
                    for (int j = 0; j < C_KEYS; j++)
                        builder.newRow(ckeys[j].duplicate()).add("val", ByteBuffer.allocate(VAL_SIZE));

                tableWriter.append(builder.build().unfilteredIterator());
            }

            tableWriter.prepareToCommit();
            Throwable t = tableWriter.commit(null);
            if (t != null)
                throw new Exception(t);

            logger.info("Created the following files: \n{}", Arrays.stream(tableWriter.descriptor.directory.listFiles())
                                                                   .map(f -> f.getName() + " - " + FileUtils.stringifyFileSize(f.length()))
                                                                   .collect(Collectors.joining("\n")));

            return tableWriter;
        }
    }

    private SSTableReader prepareReader(SSTableFormat format, SSTableWriter tableWriter)
    {
        return format.getReaderFactory().open(tableWriter.descriptor, SSTable.componentsFor(tableWriter.descriptor), table.metadata, false, true);
    }
}


