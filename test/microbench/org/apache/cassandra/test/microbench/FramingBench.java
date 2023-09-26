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

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.net.BufferPoolAllocator;
import org.apache.cassandra.net.FrameDecoder;
import org.apache.cassandra.net.FrameDecoderCrc;
import org.apache.cassandra.net.FrameDecoderLZ4;
import org.apache.cassandra.net.FrameDecoderUnprotected;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.net.FrameEncoderCrc;
import org.apache.cassandra.net.FrameEncoderLZ4;
import org.apache.cassandra.net.FrameEncoderUnprotected;
import org.apache.cassandra.net.GlobalBufferPoolAllocator;
import org.apache.cassandra.net.ShareableBytes;
import org.apache.cassandra.utils.memory.BufferPools;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.IntUnaryOperator;

@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 4, time = 5)
@Threads(4)
@BenchmarkMode(Mode.AverageTime)
public class FramingBench
{
    static
    {
        Config config = DatabaseDescriptor.loadConfig();
        config.networking_cache_size = new DataStorageSpec.IntMebibytesBound("512MiB");
        DatabaseDescriptor.daemonInitialization(() -> config);
    }

    public static IntUnaryOperator payloadLowerBound = size -> (int) (size * 0.95f);

    @State(Scope.Thread)
    public static class EncoderState
    {
        @Param({ "CRC", "CRC32C", "LZ4", "LZ4_CRC32C" })
        public EncoderDecoderType type;
        // We need to reserve some space for the header and trailer of the frame e.g. 16384 - 7 - 4 = 16373
        @Param({"2037", "16373"})
        public int payloadSize;
        public FrameEncoder encoder;
        public ByteBuffer buffer;
        public final List<Object> refs = new CopyOnWriteArrayList<>();

        @Setup(Level.Invocation)
        public void setup()
        {
            refs.clear();
            encoder = createEncoder(type);
            buffer = createPayload(encoder, ThreadLocalRandom.current()
                    .nextInt(payloadLowerBound.applyAsInt(payloadSize), payloadSize)).buffer;
        }

        @TearDown(Level.Invocation)
        public void teardown()
        {
            if (encoder instanceof FrameEncoderLZ4)
                refs.forEach(FramingBench::release);
            else
                release(buffer);
        }
    }

    @Benchmark
    @Fork(value = 1, jvmArgsAppend = { "-Xmx1g", "-Djmh.executor=CUSTOM",
            "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor" })
    public ByteBuf encode(EncoderState state)
    {
        ByteBuf buff = state.encoder.encode(true, state.buffer);
        state.refs.add(buff);
        return buff;
    }

    @State(Scope.Thread)
    public static class DecoderState
    {
        @Param({ "CRC", "CRC32C", "LZ4", "LZ4_CRC32C" })
        public EncoderDecoderType type;
        // We need to reserve some space for the header and trailer of the frame e.g. 16384 - 7 - 4 = 16373
        @Param({"2037", "16373"})
        public int payloadSize;
        public BufferPoolAllocator allocator = GlobalBufferPoolAllocator.instance;
        public FrameDecoder decoder;
        public ShareableBytes encodedBytes;

        @Setup(Level.Invocation)
        public void setup()
        {
            decoder = createDecoder(type, allocator);
            FrameEncoder encoder = createEncoder(type);
            FrameEncoder.Payload payload = createPayload(encoder, ThreadLocalRandom.current()
                    .nextInt(payloadLowerBound.applyAsInt(payloadSize), payloadSize));

            ByteBuf buf = encoder.encode(payload.isSelfContained(), payload.buffer);
            ByteBuffer frames = BufferPools.forNetworking().getAtLeast(buf.readableBytes(), BufferType.OFF_HEAP);
            frames.put(buf.internalNioBuffer(buf.readerIndex(), buf.readableBytes()));
            frames.flip();
            release(buf);
            encodedBytes = ShareableBytes.wrap(frames);
        }

        @TearDown(Level.Invocation)
        public void teardown()
        {
            release(encodedBytes);
        }
    }

    @Benchmark
    @Fork(value = 1, jvmArgsAppend = { "-Xmx1g", "-Djmh.executor=CUSTOM",
            "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor" })
    public Collection<FrameDecoder.Frame> decode(DecoderState state)
    {
        Collection<FrameDecoder.Frame> out = new ArrayList<>();
        state.decoder.decode(out, state.encodedBytes);
        return out;
    }

    private static void release(Object buff)
    {
        if (buff instanceof BufferPoolAllocator.Wrapped)
            ((BufferPoolAllocator.Wrapped) buff).deallocate();
        else if (buff instanceof ByteBuffer)
            BufferPools.forNetworking().put((ByteBuffer) buff);
        else if (buff instanceof ShareableBytes)
            ((ShareableBytes) buff).release();
        else
            throw new IllegalArgumentException("Unknown buffer type: " + buff.getClass());
    }

    private static FrameEncoder.Payload createPayload(FrameEncoder encoder, int size)
    {
        byte[] bytes = new byte[size];
        ThreadLocalRandom.current().nextBytes(bytes);
        FrameEncoder.Payload payload = encoder.allocator().allocate(true, bytes.length);
        payload.buffer.put(bytes);
        payload.finish();
        return payload;
    }

    private static FrameEncoder createEncoder(EncoderDecoderType encoderName)
    {
        switch (encoderName)
        {
            case CRC:
                return FrameEncoderCrc.instance;
            case CRC32C:
                return FrameEncoderCrc.instanceWithCRC32C;
            case LZ4:
                return FrameEncoderLZ4.fastInstance;
            case LZ4_CRC32C:
                return FrameEncoderLZ4.fastInstanceWithCRC32C;
            case UNPROTECTED:
                return FrameEncoderUnprotected.instance;
            default:
                throw new IllegalArgumentException();
        }
    }

    private static FrameDecoder createDecoder(EncoderDecoderType decoderName, BufferPoolAllocator allocator)
    {
        switch (decoderName)
        {
            case CRC:
                return FrameDecoderCrc.create(allocator);
            case CRC32C:
                return FrameDecoderCrc.createWithCRC32C(allocator);
            case LZ4:
                return FrameDecoderLZ4.fast(allocator);
            case LZ4_CRC32C:
                return FrameDecoderLZ4.fastWithCRC32C(allocator);
            case UNPROTECTED:
                return FrameDecoderUnprotected.create(allocator);
            default:
                throw new IllegalArgumentException();
        }
    }

    public static void main(String[] args) throws Exception
    {
        Options opt = new OptionsBuilder()
                .include(FramingBench.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    public enum EncoderDecoderType
    {
        CRC,
        CRC32C,
        LZ4,
        LZ4_CRC32C,
        UNPROTECTED
    }
}
