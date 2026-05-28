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
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.memory.ShardedMemtableIndex;
import org.apache.cassandra.index.sai.memory.UnshardedMemtableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.schema.TableMetadata;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 6, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(value = 1, jvmArgsAppend = { "-Xmx4G", "-Xms4G", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@State(Scope.Benchmark)
public class MemtableIndexPartitionReadBench extends AbstractMemtableIndexBench
{
    // 1 Million partitionKeys
    private static final int NUM_PARTITION_KEYS = 1000000;
    private static final int NUMBER_OF_SEARCHES = 1000;
    private static final int SMALL_POOL_SIZE = 8 * 1024;

    @Param({ "1", "4", "8"})
    int shardCount;

    @Param({"50", "100"})
    int numberOfTerms;

    private char[] smallPool = new char[SMALL_POOL_SIZE];
    private Expression[] stringEqualityExpressions;
    private QueryContext queryContext;
    private AbstractBounds<PartitionPosition>[] keyRanges;

    @State(Scope.Thread)
    public static class ThreadState
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
    }

    @Setup(Level.Trial)
    public void setup()
    {
        setupServer();
        setupTableAndKeyspace();
        setupCfsAndIndex();
        setupPartitionKeys();
        setupQueryContext();
        setupIndexesExpressionsAndTerms();
    }

    private void setupQueryContext()
    {
        // setup a dummy query context, interface signature needs it.
        queryContext = new QueryContext(null, Long.MAX_VALUE);
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
        populateIndexDataAndKeyRanges();
        populateExpressions();
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

    private void populateIndexDataAndKeyRanges()
    {
        int termCount = 0;
        keyRanges = new AbstractBounds[NUM_PARTITION_KEYS];

        for (int i = 0; i < NUM_PARTITION_KEYS; i++)
        {
            DecoratedKey partitionKey = partitionKeys[i];
            memtableIndex.index(partitionKey, Clustering.EMPTY, terms[termCount]);
            keyRanges[i] = new Bounds<>(partitionKey, partitionKey);

            if (++termCount == numberOfTerms)
            {
                termCount = 0;
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
    public long stringEqualityPartitionRestrictedRangeSearch(ThreadState state)
    {
        long size = 0;
        memtableIndex.search(queryContext,
                             stringEqualityExpressions[state.random.nextInt(stringEqualityExpressions.length)],
                             keyRanges[state.random.nextInt(keyRanges.length)]);
        return size;
    }
}
