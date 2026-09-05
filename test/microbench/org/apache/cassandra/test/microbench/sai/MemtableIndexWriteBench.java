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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.index.sai.memory.ShardedMemtableIndex;
import org.apache.cassandra.index.sai.memory.UnshardedMemtableIndex;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 5)
@Fork(value = 1, jvmArgsAppend = { "-Xmx4G", "-Xms4G", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(8)
@State(Scope.Benchmark)
public class MemtableIndexWriteBench extends AbstractMemtableIndexBench
{
    @Param({ "1", "4", "8"})
    int shardCount;

    @Param({"1000000" })
    protected int numberOfTerms;

    @Param({ "1", "10", "100"})
    protected int rowsPerPartition;

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
    public void setupIndexes()
    {
        Memtable memtable = cfs.getCurrentMemtable();
        memtableIndex = (shardCount > 1)
                        ? new ShardedMemtableIndex(index, cfs, shardCount, memtable):
                        new UnshardedMemtableIndex(index, memtable);
    }

    @Benchmark
    public long write(ThreadState state)
    {
        return memtableIndex.index(partitionKeys[state.random.nextInt(partitionKeys.length)],
                                   Clustering.EMPTY,
                                   terms[state.random.nextInt(terms.length)]);
    }
    
    @TearDown(Level.Trial)
    public void teardown() throws InterruptedException
    {
        CommitLog.instance.shutdownBlocking();
        CQLTester.tearDownClass();
        CQLTester.cleanup();
    }
}
