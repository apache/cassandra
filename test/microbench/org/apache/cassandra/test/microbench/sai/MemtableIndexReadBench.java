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

package org.apache.cassandra.test.microbench.sai;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

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
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.memory.ShardedMemtableIndex;
import org.apache.cassandra.index.sai.memory.UnshardedMemtableIndex;
import org.apache.cassandra.index.sai.plan.Expression;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 5)
@Fork(value = 1, jvmArgsAppend = { "-Xmx4G", "-Xms4G", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@State(Scope.Benchmark)
public class MemtableIndexReadBench extends AbstractMemtableIndexBench
{
    private static final int NUMBER_OF_SEARCHES = 1000;
    private static final AbstractBounds<PartitionPosition> ALL_DATA_RANGE = DataRange.allData(Murmur3Partitioner.instance).keyRange();

    @Param({ "1", "4", "8"})
    int shardCount;

    @Param({"1000000" })
    protected int numberOfTerms;

    @Param({ "1", "10", "100"})
    protected int rowsPerPartition;

    private Expression[] stringEqualityExpressions;
    private QueryContext queryContext;

    @State(Scope.Thread)
    public static class ThreadState
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
    }

    @Setup(Level.Trial)
    public void setup()
    {
        super.setup(numberOfTerms, rowsPerPartition);
    }

    @Setup(Level.Iteration)
    public void setupIndexesAndExpressions() {
        memtableIndex = (shardCount > 1)
                        ? new ShardedMemtableIndex(index, cfs, shardCount):
                        new UnshardedMemtableIndex(index);

        populateIndexData();
        populateExpressions();
        // setup a dummy query context, interface signature needs it.
        queryContext = new QueryContext(null, Long.MAX_VALUE);
    }

    private void populateIndexData()
    {
        int rowCount = 0;
        int keyCount = 0;
        for (int i = 0; i < numberOfTerms; i++)
        {
            memtableIndex.index(partitionKeys[keyCount], Clustering.EMPTY, terms[i]);
            if (++rowCount == rowsPerPartition)
            {
                rowCount = 0;
                keyCount++;
            }
        }
    }

    private void populateExpressions()
    {
        Random random = new Random();
        stringEqualityExpressions = new Expression[NUMBER_OF_SEARCHES];
        for (int i = 0; i < NUMBER_OF_SEARCHES; i++)
            stringEqualityExpressions[i] = Expression.create(index).add(Operator.EQ, terms[random.nextInt(terms.length)]);
    }

    @Benchmark
    public long stringEqualitySearch(ThreadState state)
    {
        long size = 0;
        memtableIndex.search(queryContext,
            stringEqualityExpressions[state.random.nextInt(stringEqualityExpressions.length)],
            ALL_DATA_RANGE);
        return size;
    }
}
