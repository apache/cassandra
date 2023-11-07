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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.DeletionTime.Serializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3, jvmArgsAppend = "-Xmx512M")
@Threads(1)
@State(Scope.Benchmark)
public class DeletionTimeDeSerBench
{
    final static int BUFFER_SIZE = 1024 * 1024;
    final static int DT_STD_SIZE = 8 + 4; // Long + Int

    final static Random random = new Random(100);
    final static ArrayList<DeletionTime> TEST_DTS_70PC_LIVE = generateDTs(70);
    final static ArrayList<DeletionTime> TEST_DTS_30PC_LIVE = generateDTs(30);

    // Parameters
    final static String nbSstableParam = "NB";
    final static String oaSstableParam = "OA";
    @Param({ nbSstableParam, oaSstableParam })
    String sstableParam;

    final static String live70PcParam = "70PcLive";
    final static String live30PcParam = "30PcLive";
    @Param({ live70PcParam, live30PcParam })
    String liveDTPcParam;

    final static String RAMParam = "RAM";
    final static String diskParam = "Disk";
    @Param({ RAMParam,  diskParam })
    String diskRAMParam;

    // Files
    File serMMapedFile = new File(DeletionTimeDeSerBench.class + "_Sermmap");
    ByteBuffer serMMap = allocateMmapedByteBuffer(serMMapedFile, BUFFER_SIZE);
    File deserMMapedFile = new File(DeletionTimeDeSerBench.class + "_Desermmap");
    ByteBuffer deserMMap = allocateMmapedByteBuffer(deserMMapedFile, BUFFER_SIZE);
    File diskFile = new File(DeletionTimeDeSerBench.class + "_DTBenchTest");

    @TearDown
    public void tearDown()
    {
        serMMapedFile.delete();
        deserMMapedFile.delete();
        diskFile.delete();
    }

    @Benchmark
    public void testE2ESerializeDT(final Blackhole bh) throws IOException
    {
        ByteBuffer buffer = serMMap;
        Serializer serializer = getSerializer(sstableParam);
        ArrayList<DeletionTime> dts = getDTs(liveDTPcParam);
        
        try(DataOutputStreamPlus out = getSerOut(diskRAMParam, serMMap))
        {
            for (int i = 0, m = dts.size(); i < m; i++)
                serializer.serialize(dts.get(i), out);
            bh.consume(out);
        }

        bh.consume(buffer);
        bh.consume(serializer);
        bh.consume(dts);
        buffer.clear();
    }

    @Benchmark
    public void testE2EDeSerializeDT(final Blackhole bh) throws IOException
    {
        ByteBuffer buffer = deserMMap;
        Serializer serializer = getSerializer(sstableParam);
        ArrayList<DeletionTime> dts = getDTs(liveDTPcParam);
        
        try(DataOutputStreamPlus out = getSerOut(diskRAMParam, deserMMap))
        {
            for (int i = 0, m = dts.size(); i < m; i++)
                serializer.serialize(dts.get(i), out);
            bh.consume(out);
        }

        DataInputPlus in = getDeSerIn(diskRAMParam, deserMMap);
        for (int i = 0, m = dts.size(); i < m; i++)
            serializer.deserialize(in);

        bh.consume(buffer);
        bh.consume(serializer);
        bh.consume(dts);
        bh.consume(in);
        buffer.clear();
    }

    /**
     * Test the impact of the different bit ops and write calls. Against memory only.
     */
    @Benchmark
    public void testRawAlgWrites(final Blackhole bh)
    {
        if (diskRAMParam != RAMParam)
            return;

        ArrayList<DeletionTime> dts = getDTs(liveDTPcParam);
        ByteBuffer buffer = ByteBuffer.allocate(DT_STD_SIZE);

        if (sstableParam == oaSstableParam)
        {
            for (int i = 0, m = dts.size(); i < m; i++)
            {
                buffer.clear();
                if (dts.get(i).equals(DeletionTime.LIVE))
                    buffer.put((byte) 0b1000_0000);
                else
                {
                    buffer.putLong(Math.abs(random.nextLong()));
                    buffer.putInt(Math.abs(random.nextInt()));
                }
            }
        }
        else
        {
            for (int i = 0, m = dts.size(); i < m; i++)
            {
                buffer.clear();
                buffer.putLong(Math.abs(random.nextLong()));
                buffer.putInt(Math.abs(random.nextInt()));
            }
        }

        bh.consume(dts);
        bh.consume(buffer);
    }

    /**
     * Test the impact of the different bit ops and read calls. Against memory only.
     */
    @Benchmark
    public void testRawAlgReads(final Blackhole bh)
    {
        if (diskRAMParam != RAMParam)
            return;

        ArrayList<DeletionTime> dts = getDTs(liveDTPcParam);
        ByteBuffer buffer = ByteBuffer.allocate(DT_STD_SIZE);
        buffer.putLong(random.nextLong() & Long.MAX_VALUE); // to prevent Match.abs(MIN_VALUE) == MIN_VALUE
        buffer.putInt(random.nextInt() & Integer.MAX_VALUE);

        if (sstableParam == oaSstableParam)
        {
            for (int i = 0, m = dts.size(); i < m; i++)
            {
                buffer.clear();

                if (dts.get(i).equals(DeletionTime.LIVE))
                {
                    // LIVE: we only read the flags Byte
                    byte b = buffer.get();
                    b = (byte) (b & 0b1000_0000 & 0xFF);
                    bh.consume(b);
                }
                else
                {
                    // The flag
                    byte flag = buffer.get();

                    // 7 bytes mfda
                    int bytes4 = buffer.getInt();
                    int bytes2 = buffer.getShort();
                    int bytes1 = buffer.get();

                    long mfda = flag & 0xFFL;
                    mfda = (mfda << 32) + (bytes4 & 0xFFFFFFFFL);
                    mfda = (mfda << 16) + (bytes2 & 0xFFFFL);
                    mfda = (mfda << 8) + (bytes1 & 0xFFL);
                    
                    // The ldt
                    int ldt = buffer.getInt();

                    bh.consume(flag);
                    bh.consume(bytes4);
                    bh.consume(bytes2);
                    bh.consume(bytes1);
                    bh.consume(mfda);
                    bh.consume(ldt);
                }
            }
        }
        else
        {
            for (int i = 0, m = dts.size(); i < m; i++)
            {
                buffer.clear();
                long mfda = buffer.getLong();
                int ldt = buffer.getInt();

                bh.consume(mfda);
                bh.consume(ldt);
            }
        }

        bh.consume(dts);
        bh.consume(buffer);
    }

    private DataInputPlus getDeSerIn(String value, ByteBuffer maybeBackingBB)
    {
        switch (value)
        {
            case RAMParam:
                maybeBackingBB.rewind();
                return new DataInputBuffer(maybeBackingBB, false);

            case diskParam:
                try
                {
                    return new FileInputStreamPlus(new org.apache.cassandra.io.util.File(diskFile));
                }
                catch(NoSuchFileException e)
                {
                    throw new RuntimeException(e);
                }

            default:
                throw new IllegalStateException("Unknown backing media for deser.");
        }
    }

    private DataOutputStreamPlus getSerOut(String value, ByteBuffer maybeBackingBB)
    {
        switch (value)
        {
            case RAMParam:
                maybeBackingBB.rewind();
                return new DataOutputBuffer(maybeBackingBB);

            case diskParam:
                try
                {
                    return new FileOutputStreamPlus(new org.apache.cassandra.io.util.File(diskFile));
                }
                catch(NoSuchFileException e)
                {
                    throw new RuntimeException(e);
                }

            default:
                throw new IllegalStateException("Unknown backing media for ser.");
        }
    }

    /**
     * Generates DeletionTimes where livePercentage are DeletionTime.LIVE
     * @param livePercentage
     * @return
     */
    private static ArrayList<DeletionTime> generateDTs(int livePercentage)
    {
        ArrayList<DeletionTime> dts = new ArrayList<>(BUFFER_SIZE / DT_STD_SIZE);
        for (int i = 0; i < (BUFFER_SIZE / (DT_STD_SIZE)); i++)
        {
            DeletionTime dt;

            if ((random.nextInt() % 100) < livePercentage)
                dt = DeletionTime.LIVE;
            else
                dt = DeletionTime.build(Math.abs(random.nextLong()), Math.abs(random.nextInt()));

            dts.add(dt);
        }

        return dts;
    }

    private static ArrayList<DeletionTime> getDTs(String value)
    {
        switch (value)
        {
            case live70PcParam:
                return TEST_DTS_70PC_LIVE;

            case live30PcParam:
                return TEST_DTS_30PC_LIVE;

            default:
                throw new IllegalStateException("Unknown live DT PC");
        }
    }

    private static Serializer getSerializer(String value)
    {
        switch (value)
        {
            case nbSstableParam:
                return new DeletionTime.LegacySerializer();

            case oaSstableParam:
                return new DeletionTime.Serializer();

            default:
                throw new IllegalStateException("Unknown serializer");
        }
    }

    private static ByteBuffer allocateMmapedByteBuffer(File mmapFile, int bufferSize)
    {
        try(RandomAccessFile file = new RandomAccessFile(mmapFile, "rw"); FileChannel ch = file.getChannel())
        {
            return ch.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
        }
        catch(IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
