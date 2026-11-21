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
package org.apache.cassandra.io.sstable.format.bti;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.PageAware;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Measures the allocation and time cost of building a BTI partition index over a set of sorted keys.  The change under
 * test cuts the repeated key encoding in {@link PartitionIndexBuilder#addEntry}, so the metric to watch is
 * {@code gc.alloc.rate.norm} (bytes per operation) under {@code -prof gc}.  The index build reflects the encoding cost
 * because the file writes allocate little on the Java heap compared with encoding the keys.
 * <p>
 * The single-run norm also includes trie-node allocation, which this change does not touch, so read the before/after
 * <em>delta</em> as the encoding saving, not one run's absolute number.
 * <p>
 * This bench sits in the bti package because {@link PartitionIndexBuilder} is package-private.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = { "-Xmx4G", "-Xms4G" })
@Threads(1)
@State(Scope.Thread)
public class PartitionIndexBuildBench
{
    @Param({ "1000", "100000", "1000000" })
    int keyCount;

    private DecoratedKey[] keys;
    private File file;

    @Setup(Level.Trial)
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        Murmur3Partitioner partitioner = Murmur3Partitioner.instance;
        Random random = new Random(1);
        keys = new DecoratedKey[keyCount];
        for (int i = 0; i < keyCount; i++)
        {
            ByteBuffer bb = ByteBufferUtil.bytes(new UUID(random.nextLong(), random.nextLong()));
            keys[i] = partitioner.decorateKey(bb);
        }
        Arrays.sort(keys);
    }

    @Setup(Level.Invocation)
    public void createFile()
    {
        file = FileUtils.createTempFile("PartitionIndexBuildBench", "");
    }

    @TearDown(Level.Invocation)
    public void deleteFile()
    {
        file.tryDelete();
    }

    @Benchmark
    public long buildIndex() throws IOException
    {
        FileHandle.Builder fhBuilder = new FileHandle.Builder(file).bufferSize(PageAware.PAGE_SIZE);
        try (SequentialWriter writer = new SequentialWriter(file, SequentialWriterOption.newBuilder().finishOnClose(false).build());
             PartitionIndexBuilder builder = new PartitionIndexBuilder(writer, fhBuilder))
        {
            for (int i = 0; i < keys.length; i++)
                builder.addEntry(keys[i], i);
            return builder.complete();
        }
    }
}
