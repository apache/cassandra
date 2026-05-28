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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Random;
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

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.index.sai.memory.ShardedMemtableIndex;
import org.apache.cassandra.index.sai.memory.UnshardedMemtableIndex;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 5)
@Fork(value = 1, jvmArgsAppend = { "-Xmx4G", "-Xms4G", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@State(Scope.Benchmark)
public class MemtableIndexFlushBench extends AbstractMemtableIndexBench
{
    // 1 Million partitionKeys
    private static final int NUM_PARTITION_KEYS = 1000000;
    private static final int SMALL_POOL_SIZE = 8 * 1024;

    @Param({ "1", "4", "8"})
    int shardCount;

    @Param({"50", "100"})
    int numberOfTerms;

    private char[] smallPool = new char[SMALL_POOL_SIZE];

    @Setup(Level.Trial)
    public void setup()
    {
        setupServer();
        setupTableAndKeyspace();
        setupCfsAndIndex();
        setupPartitionKeys();
        setupIndexesExpressionsAndTerms();
    }

    public void setupPartitionKeys()
    {
        TableMetadata tableMetadata = cfs.metadata();

        partitionKeys = new DecoratedKey[NUM_PARTITION_KEYS];
        for (int i = 0; i < NUM_PARTITION_KEYS; i++)
            partitionKeys[i] = tableMetadata.partitioner.decorateKey(tableMetadata.partitionKeyType.fromString("partition_" + i));
    }

    public void setupIndexesExpressionsAndTerms() {
        Memtable memtable = cfs.getCurrentMemtable();
        memtableIndex = (shardCount > 1)
                        ? new ShardedMemtableIndex(index, cfs, shardCount, memtable) :
                        new UnshardedMemtableIndex(index, memtable);

        setupTerms(numberOfTerms);
        populateIndexData();
    }

    @Override
    public void setupTerms(int numberOfTerms)
    {
        Random random = new Random();
        for (int i = 0; i < SMALL_POOL_SIZE; i++)
            smallPool[i] = (char)('a' + random.nextInt(26));

        int length = 64;
        terms = new ByteBuffer[numberOfTerms];
        for (int i = 0; i < numberOfTerms; i++)
            terms[i] = UTF8Type.instance.decompose(
            new String(smallPool, random.nextInt(SMALL_POOL_SIZE - length), length));
    }

    private void populateIndexData()
    {
        int termCount = 0;

        for (int i = 0; i < NUM_PARTITION_KEYS; i++)
        {
            DecoratedKey partitionKey = partitionKeys[i];
            memtableIndex.index(partitionKey, Clustering.EMPTY, terms[termCount]);

            if (++termCount == numberOfTerms)
            {
                termCount = 0;
            }
        }
    }

    @Benchmark
    public long flushBench()
    {
        Iterator<Pair<ByteComparable, Iterator<PrimaryKey>>> it = memtableIndex.iterator(null, null);
        long count = 0;
        while (it.hasNext())
        {
            Iterator<PrimaryKey> primaryKeyIterator = it.next().right;
            while (primaryKeyIterator.hasNext())
            {
                primaryKeyIterator.next();
                count++;
            }
        }
        return count;
    }
}
