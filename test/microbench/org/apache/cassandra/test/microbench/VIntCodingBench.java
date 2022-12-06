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

import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.vint.VIntCoding;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3, jvmArgsAppend = "-Xmx512M")
@Threads(1)
@State(Scope.Benchmark)
public class VIntCodingBench
{
    long oneByte = 53;
    long twoBytes = 10201;
    long threeBytes = 1097151L;
    long fourBytes = 168435455L;
    long fiveBytes = 33251130335L;
    long sixBytes = 3281283447775L;
    long sevenBytes = 417672546086779L;
    long eightBytes = 52057592037927932L;
    long nineBytes = 72057594037927937L;

    final static String MONOMORPHIC = "monomorphic";
    final static String BIMORPHIC = "bimorphic";
    final static String MEGAMORPHIC = "megamorphic";

    @Param({ MONOMORPHIC, BIMORPHIC, MEGAMORPHIC})
    String allocation;

    final Random random = new Random(100);
    final PrimitiveIterator.OfLong longs = random.longs().iterator();
    final static int BUFFER_SIZE = 8192;

    ByteBuffer onheap = ByteBuffer.allocate(BUFFER_SIZE);
    ByteBuffer direct = ByteBuffer.allocateDirect(BUFFER_SIZE);
    File mmapedFile = new File(VIntCodingBench.class + "_mmap");
    ByteBuffer mmaped = allocateMmapedByteBuffer(mmapedFile, BUFFER_SIZE);

    @TearDown
    public void tearDown()
    {
        mmapedFile.delete();
    }

    private static ByteBuffer allocateMmapedByteBuffer(File mmapFile, int bufferSize)
    {
        try(RandomAccessFile file = new RandomAccessFile(mmapFile, "rw");
            FileChannel ch = file.getChannel())
        {
            return ch.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private ByteBuffer getByteBuffer(String allocation)
    {
        ByteBuffer buffer;
        if (allocation.equals(MONOMORPHIC))
        {
            buffer = onheap;
        }
        else if (allocation.equals(BIMORPHIC))
        {
            buffer = random.nextBoolean() ? onheap : direct;
        }
        else
        {
            switch(random.nextInt(3))
            {
                case 0:
                    buffer = onheap;
                    break;
                case 1:
                    buffer = direct;
                    break;
                default:
                    buffer = mmaped;
                    break;
            }
        }
        return buffer;
    }

    private DataOutputPlus getBufferedDataOutput(Blackhole bh, ByteBuffer buffer)
    {
        WritableByteChannel wbc = new WritableByteChannel() {

            @Override
            public boolean isOpen()
            {
                return true;
            }

            @Override
            public void close() throws IOException
            {
            }

            @Override
            public int write(ByteBuffer src) throws IOException
            {
                bh.consume(src);
                int remaining = src.remaining();
                src.position(src.limit());
                return remaining;
            }
        };
        return new BufferedDataOutputStreamPlus(wbc, buffer);
    }

    @Benchmark
    public void testWrite1ByteBB(final Blackhole bh)
    {
        ByteBuffer buffer = getByteBuffer(allocation);
        VIntCoding.writeUnsignedVInt(oneByte, buffer);
        bh.consume(buffer);
        buffer.clear();
    }

    @Benchmark
    public void testWrite1ByteDOP(final Blackhole bh) throws IOException
    {
        ByteBuffer buffer = getByteBuffer(allocation);
        DataOutputPlus out = getBufferedDataOutput(bh, buffer);
        VIntCoding.writeUnsignedVInt(oneByte, out);
        bh.consume(out);
        buffer.clear();
    }

    @Benchmark
    public void testWrite2BytesBB(final Blackhole bh)
    {
        ByteBuffer buffer = getByteBuffer(allocation);
        VIntCoding.writeUnsignedVInt(twoBytes, buffer);
        bh.consume(buffer);
        buffer.clear();
    }

    @Benchmark
    public void testWrite2BytesDOP(final Blackhole bh) throws IOException
    {
        ByteBuffer buffer = getByteBuffer(allocation);
        DataOutputPlus out = getBufferedDataOutput(bh, buffer);
        VIntCoding.writeUnsignedVInt(twoBytes, out);
        bh.consume(out);
        buffer.clear();
    }

    @Benchmark
    public void testWrite3BytesBB(final Blackhole bh)
    {
        ByteBuffer buffer = getByteBuffer(allocation);
        VIntCoding.writeUnsignedVInt(threeBytes, buffer);
        bh.consume(buffer);
        buffer.clear();
    }

    @Benchmark
    public void testWrite3BytesDOP(final Blackhole bh) throws IOException
    {
        ByteBuffer buffer = getByteBuffer(allocation);
        DataOutputPlus out = getBufferedDataOutput(bh, buffer);
        VIntCoding.writeUnsignedVInt(threeBytes, out);
        bh.consume(out);
        buffer.clear();
    }

    @Benchmark
    public void testWrite4BytesBB(final Blackhole bh)
    {
        ByteBuffer buffer = getByteBuffer(allocation);
        VIntCoding.writeUnsignedVInt(fourBytes, buffer);
        bh.consume(buffer);
        buffer.clear();
    }

    @Benchmark
    public void testWrite4BytesDOP(final Blackhole bh) throws IOException
    {
        ByteBuffer buffer = getByteBuffer(allocation);
        DataOutputPlus out = getBufferedDataOutput(bh, buffer);
        VIntCoding.writeUnsignedVInt(fourBytes, out);
        bh.consume(out);
        buffer.clear();
    }

    @Benchmark
    public void testWrite5BytesBB(final Blackhole bh)
    {
        ByteBuffer buffer = getByteBuffer(allocation);
        VIntCoding.writeUnsignedVInt(fiveBytes, buffer);
        bh.consume(buffer);
        buffer.clear();
    }

    @Benchmark
    public void testWrite5BytesDOP(final Blackhole bh) throws IOException
    {
        ByteBuffer buffer = getByteBuffer(allocation);
        DataOutputPlus out = getBufferedDataOutput(bh, buffer);
        VIntCoding.writeUnsignedVInt(fiveBytes, out);
        bh.consume(out);
        buffer.clear();
    }

    @Benchmark
    public void testWrite6BytesBB(final Blackhole bh)
    {
        ByteBuffer buffer = getByteBuffer(allocation);
        VIntCoding.writeUnsignedVInt(sixBytes, buffer);
        bh.consume(buffer);
        buffer.clear();
    }

    @Benchmark
    public void testWrite6BytesDOP(final Blackhole bh) throws IOException
    {
        ByteBuffer buffer = getByteBuffer(allocation);
        DataOutputPlus out = getBufferedDataOutput(bh, buffer);
        VIntCoding.writeUnsignedVInt(sixBytes, out);
        bh.consume(out);
        buffer.clear();
    }

    @Benchmark
    public void testWrite7BytesBB(final Blackhole bh)
    {
        ByteBuffer buffer = getByteBuffer(allocation);
        VIntCoding.writeUnsignedVInt(sevenBytes, buffer);
        bh.consume(buffer);
        buffer.clear();
    }

    @Benchmark
    public void testWrite7BytesDOP(final Blackhole bh) throws IOException
    {
        ByteBuffer buffer = getByteBuffer(allocation);
        DataOutputPlus out = getBufferedDataOutput(bh, buffer);
        VIntCoding.writeUnsignedVInt(sevenBytes, out);
        bh.consume(out);
        buffer.clear();
    }

    @Benchmark
    public void testWrite8BytesBB(final Blackhole bh)
    {
        ByteBuffer buffer = getByteBuffer(allocation);
        VIntCoding.writeUnsignedVInt(eightBytes, buffer);
        bh.consume(buffer);
        buffer.clear();
    }

    @Benchmark
    public void testWrite8BytesDOP(final Blackhole bh) throws IOException
    {
        ByteBuffer buffer = getByteBuffer(allocation);
        DataOutputPlus out = getBufferedDataOutput(bh, buffer);
        VIntCoding.writeUnsignedVInt(eightBytes, out);
        bh.consume(out);
        buffer.clear();
    }

    @Benchmark
    public void testWrite9BytesBB(final Blackhole bh)
    {
        ByteBuffer buffer = getByteBuffer(allocation);
        VIntCoding.writeUnsignedVInt(nineBytes, buffer);
        bh.consume(buffer);
        buffer.clear();
    }

    @Benchmark
    public void testWrite9BytesDOP(final Blackhole bh) throws IOException
    {
        ByteBuffer buffer = getByteBuffer(allocation);
        DataOutputPlus out = getBufferedDataOutput(bh, buffer);
        VIntCoding.writeUnsignedVInt(nineBytes, out);
        bh.consume(out);
        buffer.clear();
    }

    @Benchmark
    public void testWriteRandomLongBB(final Blackhole bh)
    {
        ByteBuffer buffer = getByteBuffer(allocation);
        VIntCoding.writeUnsignedVInt(longs.nextLong(), buffer);
        bh.consume(buffer);
        buffer.clear();
    }

    @Benchmark
    public void testWriteRandomLongDOP(final Blackhole bh) throws IOException
    {
        ByteBuffer buffer = getByteBuffer(allocation);
        DataOutputPlus out = getBufferedDataOutput(bh, buffer);
        VIntCoding.writeUnsignedVInt(longs.nextLong(), out);
        bh.consume(out);
        buffer.clear();
    }

    @Benchmark
    public void testComputeUnsignedVIntSize(final Blackhole bh)
    {
        bh.consume(VIntCoding.computeUnsignedVIntSize(longs.nextLong()));
    }
}
