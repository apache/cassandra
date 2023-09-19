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

package org.apache.cassandra.test.microbench;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.MmappedRegionsCache;
import org.apache.cassandra.io.util.RandomAccessReader;
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

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Threads(16)
@Fork(1)
@State(Scope.Benchmark)
public class ChunkCacheBench
{
    private final static Logger logger = LoggerFactory.getLogger(ChunkCacheBench.class);

    private static final long MASK = -Long.BYTES;

    public static final long fileSize = 1L << 30;

    public int bufferSize = 4096;

    @Param({ "ON_HEAP", "OFF_HEAP" })
    public String bufferType = "ON_HEAP";

    @Param({ "true", "false" })
    public boolean chunkCache = false;

    @Param({ "true", "false" })
    public boolean mmaped = true;

    @Param({ "true", "false" })
    public boolean direct = false;

    private static Path tmp;

    private FileHandle.Builder fhBuilder;
    private final CopyOnWriteArrayList<AutoCloseable> closeables = new CopyOnWriteArrayList<>();
    private MmappedRegionsCache mmappedRegionsCache;

    static
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setFileCacheEnabled(true);
        DatabaseDescriptor.getRawConfig().file_cache_size = new DataStorageSpec.IntMebibytesBound(256);

        try
        {
            tmp = Files.createTempFile("test", "chunkcache");
            int writeBufSize = 64 << 10;
            ByteBuffer buf = ByteBuffer.allocate(writeBufSize);
            try (SeekableByteChannel out = Files.newByteChannel(tmp, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))
            {
                for (long pos = 0; pos < fileSize; )
                {
                    buf.putLong(pos);
                    pos += Long.BYTES;
                    if (buf.remaining() < Long.BYTES)
                    {
                        buf.flip();
                        out.write(buf);
                        buf.rewind();
                    }
                }
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Setup(Level.Trial)
    public void preTrial() throws IOException
    {
        BufferType bufferType = BufferType.valueOf(this.bufferType);
        ChunkCache chunkCache = this.chunkCache ? Objects.requireNonNull(ChunkCache.instance) : null;
        mmappedRegionsCache = new MmappedRegionsCache();
        ChannelProxy.direct = direct;

        fhBuilder = new FileHandle.Builder(new File(tmp))
                    .bufferSize(bufferSize)
                    .withChunkCache(chunkCache)
                    .mmapped(mmaped)
                    .withMmappedRegionsCache(mmappedRegionsCache)
                    .bufferType(bufferType);
    }

    @State(Scope.Thread)
    public static class ThreadState
    {
        public ThreadLocalRandom random = ThreadLocalRandom.current();
        public RandomAccessReader reader;
    }

    @Benchmark
    public void test(ThreadState threadState) throws IOException
    {
        if (threadState.reader == null)
        {
            synchronized (mmappedRegionsCache)
            {
                FileHandle fh = fhBuilder.complete();
                closeables.add(fh);
                threadState.reader = fh.createReader();
                closeables.add(threadState.reader);
            }
        }
        long pos = threadState.random.nextLong(fileSize - Long.BYTES) & MASK;
        threadState.reader.seek(pos);
        threadState.reader.reBuffer();
        long v = threadState.reader.readLong();
        assert v == pos : "Expected " + pos + " but got " + v;
    }

    @TearDown(Level.Iteration)
    public void postIteration() throws IOException
    {
        System.out.println(ChunkCache.instance.metrics.snapshot());
        ChunkCache.instance.metrics.reset();
    }

    @TearDown(Level.Trial)
    public void postTrial() throws IOException
    {
        for (int i = closeables.size() - 1; i >= 0; i--)
            FileUtils.closeQuietly(closeables.get(i));
        closeables.clear();
        FileUtils.closeQuietly(mmappedRegionsCache);
        Files.deleteIfExists(tmp);
    }
}
