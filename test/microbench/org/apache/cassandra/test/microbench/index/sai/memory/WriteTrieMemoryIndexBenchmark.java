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

package org.apache.cassandra.test.microbench.index.sai.memory;


import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.index.sai.memory.TrieMemoryIndex;
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

@Fork(1)
@Warmup(iterations = 5, time = 3)
@Measurement(iterations = 10, time = 3)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@State(Scope.Thread)
public class WriteTrieMemoryIndexBenchmark extends AbstractTrieMemoryIndexBenchmark
{
    @Param({ "1000", "10000", "100000", "1000000" })
    protected int numberOfTerms;

    @Param({ "1", "10", "100"})
    protected int rowsPerPartition;

    @Setup(Level.Iteration)
    public void initialiseColumnData()
    {
        initialiseColumnData(numberOfTerms, rowsPerPartition);
    }

    @Setup(Level.Invocation)
    public void initialiseIndexes()
    {
        stringIndex = new TrieMemoryIndex(stringContext);
        integerIndex = new TrieMemoryIndex(integerContext);
    }

    @Benchmark
    public long writeStringIndex()
    {
        long size = 0;
        int rowCount = 0;
        int keyCount = 0;
        for (ByteBuffer term : stringTerms)
        {
            stringIndex.add(partitionKeys[keyCount], Clustering.EMPTY, term);
            if (++rowCount == rowsPerPartition)
            {
                rowCount = 0;
                keyCount++;
            }
            size++;
        }
        return size;
    }

    @Benchmark
    public long writeIntegerIndex()
    {
        long size = 0;
        int rowCount = 0;
        int keyCount = 0;
        for (ByteBuffer term : integerTerms)
        {
            integerIndex.add(partitionKeys[keyCount], Clustering.EMPTY, term);
            if (++rowCount == rowsPerPartition)
            {
                rowCount = 0;
                keyCount++;
            }
            size++;
        }
        return size;
    }
}
