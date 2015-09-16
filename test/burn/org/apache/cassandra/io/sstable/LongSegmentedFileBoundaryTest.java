/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.io.sstable;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import com.google.common.io.Files;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.MmappedSegmentedFile;
import org.apache.cassandra.io.util.MmappedSegmentedFile.Builder.Boundaries;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

public class LongSegmentedFileBoundaryTest
{
    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.cleanupAndLeaveDirs();
        Keyspace.setInitialized();
        StorageService.instance.initServer();
    }

    @AfterClass
    public static void tearDown()
    {
        Config.setClientMode(false);
    }

    @Test
    public void testRandomBoundaries()
    {
        long[] candidates = new long[1 + (1 << 16)];
        int[] indexesToCheck = new int[1 << 8];
        Random random = new Random();

        for (int run = 0; run < 100; run++)
        {

            long seed = random.nextLong();
            random.setSeed(seed);
            System.out.println("Seed: " + seed);

            // at least 1Ki, and as many as 256Ki, boundaries
            int candidateCount = (1 + random.nextInt(candidates.length >> 10)) << 10;
            generateBoundaries(random, candidateCount, candidates, indexesToCheck);

            Boundaries builder = new Boundaries();
            int nextIndexToCheck = indexesToCheck[0];
            int checkCount = 0;
            System.out.printf("[0..%d)", candidateCount);
            for (int i = 1; i < candidateCount - 1; i++)
            {
                if (i == nextIndexToCheck)
                {
                    if (checkCount % 20 == 0)
                        System.out.printf(" %d", i);
                    // grow number of samples logarithmically; work will still increase superlinearly, as size of dataset grows linearly
                    int sampleCount = 1 << (31 - Integer.numberOfLeadingZeros(++checkCount));
                    checkBoundarySample(random, candidates, i, sampleCount, builder);
                    // select out next index to check (there may be dups, so skip them)
                    while ((nextIndexToCheck = checkCount == indexesToCheck.length ? candidateCount : indexesToCheck[checkCount]) == i)
                        checkCount++;
                }

                builder.addCandidate(candidates[i]);
            }
            System.out.println();
            checkBoundaries(candidates, candidateCount - 1, builder, candidates[candidateCount - 1]);
            Assert.assertEquals(candidateCount, nextIndexToCheck);
        }
    }

    private static void generateBoundaries(Random random, int candidateCount, long[] candidates, int[] indexesToCheck)
    {
        // average averageBoundarySize is 4MiB, max 4GiB, min 4KiB
        long averageBoundarySize = (4L << 10) * random.nextInt(1 << 20);
        long prev = 0;
        for (int i = 1 ; i < candidateCount ; i++)
            candidates[i] = prev += Math.max(1, averageBoundarySize + (random.nextGaussian() * averageBoundarySize));

        // generate indexes we will corroborate our behaviour on
        for (int i = 0 ; i < indexesToCheck.length ; i++)
            indexesToCheck[i] = 1 + random.nextInt(candidateCount - 2);
        Arrays.sort(indexesToCheck);
    }

    private static void checkBoundarySample(Random random, long[] candidates, int candidateCount, int sampleCount, Boundaries builder)
    {
        for (int i = 0 ; i < sampleCount ; i++)
        {
            // pick a number exponentially less likely to be near the beginning, since we test that area earlier
            int position = 0 ;
            while (position <= 0)
                position = candidateCount / (Integer.lowestOneBit(random.nextInt()));
            long upperBound = candidates[position];
            long lowerBound = random.nextBoolean() ? (rand(random, 0, upperBound) / (Integer.lowestOneBit(random.nextInt())))
                                                   : candidates[Math.max(0, position - random.nextInt(64))];
            long length = rand(random, lowerBound, upperBound);
            checkBoundaries(candidates, candidateCount, builder, length);
        }
        checkBoundaries(candidates, candidateCount, builder, candidates[candidateCount]);
    }

    private static long rand(Random random, long lowerBound, long upperBound)
    {
        if (upperBound == lowerBound)
            return upperBound;
        return lowerBound + ((random.nextLong() & Long.MAX_VALUE) % (upperBound - lowerBound));
    }

    private static void checkBoundaries(long[] candidates, int candidateCount, Boundaries builder, long length)
    {
        if (length == 0)
            return;

        long[] boundaries = new long[(int) (10 + 2 * (length / Integer.MAX_VALUE))];
        int count = 1;
        int prev = 0;
        while (true)
        {
            int p = candidates[prev + 1] - boundaries[count - 1] >= Integer.MAX_VALUE
                    ? prev + 1
                    : Arrays.binarySearch(candidates, prev, candidateCount, boundaries[count - 1] + Integer.MAX_VALUE);
            if (p < 0) p = -2 -p;
            if (p >= candidateCount - 1 || candidates[p] >= length)
                break;
            boundaries[count++] = candidates[p];
            if (candidates[p + 1] >= length)
                break;
            prev = p;
        }
        if (candidates[candidateCount - 1] < length && length - boundaries[count - 1] >= Integer.MAX_VALUE)
            boundaries[count++] = candidates[candidateCount - 1];
        boundaries[count++] = length;
        final long[] canon = Arrays.copyOf(boundaries, count);
        final long[] check = builder.finish(length, false);
        if (!Arrays.equals(canon, check))
            Assert.assertTrue("\n" + Arrays.toString(canon) + "\n" + Arrays.toString(check), Arrays.equals(canon, check));
    }

    @Test
    public void testBoundariesAndRepairSmall() throws InvalidRequestException, IOException
    {
        testBoundariesAndRepair(1, 1 << 16);
    }

    @Test
    public void testBoundariesAndRepairMedium() throws InvalidRequestException, IOException
    {
        testBoundariesAndRepair(1, 1 << 20);
    }

    @Test
    public void testBoundariesAndRepairLarge() throws InvalidRequestException, IOException
    {
        testBoundariesAndRepair(1, 100 << 20);
    }

    @Test
    public void testBoundariesAndRepairHuge() throws InvalidRequestException, IOException
    {
        testBoundariesAndRepair(1, Integer.MAX_VALUE - 1024);
    }

    @Test
    public void testBoundariesAndRepairTooHuge() throws InvalidRequestException, IOException
    {
        testBoundariesAndRepair(1, Integer.MAX_VALUE);
    }

    @Test
    public void testBoundariesAndRepairHugeIndex() throws InvalidRequestException, IOException
    {
        testBoundariesAndRepair(1 << 7, 1 << 15);
    }

    @Test
    public void testBoundariesAndRepairReallyHugeIndex() throws InvalidRequestException, IOException
    {
        testBoundariesAndRepair(1 << 14, 1 << 15);
    }

    private void testBoundariesAndRepair(int rows, int rowSize) throws InvalidRequestException, IOException
    {
        String KS = "cql_keyspace";
        String TABLE = "table1";

        File tempdir = Files.createTempDir();
        try
        {
            Assert.assertTrue(DatabaseDescriptor.getColumnIndexSize() < rowSize);
            Assert.assertTrue(DatabaseDescriptor.getDiskAccessMode() == Config.DiskAccessMode.mmap);
            Assert.assertTrue(DatabaseDescriptor.getIndexAccessMode() == Config.DiskAccessMode.mmap);
            Assert.assertTrue(StorageService.getPartitioner() instanceof ByteOrderedPartitioner);
            File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
            Assert.assertTrue(dataDir.mkdirs());

            String schema = "CREATE TABLE cql_keyspace.table" + (rows > 1 ? "2" : "1") + " (k bigint, v1 blob, v2 blob, v3 blob, v4 blob, v5 blob, PRIMARY KEY (k" + (rows > 1 ? ", v1" : "") + ")) WITH compression = { 'sstable_compression':'' };";
            String insert = "INSERT INTO cql_keyspace.table" + (rows > 1 ? "2" : "1") + " (k, v1, v2, v3, v4, v5) VALUES (?, ?, ?, ?, ?, ?)";

            CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                      .inDirectory(dataDir)
                                                      .forTable(schema)
                                                      .withPartitioner(StorageService.getPartitioner())
                                                      .using(insert)
                                                      .sorted();
            CQLSSTableWriter writer = builder.build();

            // write 8Gb of decorated keys
            ByteBuffer[] value = new ByteBuffer[rows];
            for (int row = 0 ; row < rows ; row++)
            {
                // if we're using clustering columns, the clustering key is replicated across every other column
                value[row] = ByteBuffer.allocate(rowSize / (rows > 1 ? 8 : 5));
                value[row].putInt(0, row);
            }
            long targetSize = 8L << 30;
            long dk = 0;
            long size = 0;
            long dkSize = rowSize * rows;
            while (size < targetSize)
            {
                for (int row = 0 ; row < rows ; row++)
                    writer.addRow(dk, value[row], value[row], value[row], value[row], value[row]);
                size += dkSize;
                dk++;
            }

            Descriptor descriptor = writer.getCurrentDescriptor().asType(Descriptor.Type.FINAL);
            writer.close();

            // open (and close) the reader so that the summary file is created
            SSTableReader reader = SSTableReader.open(descriptor);
            reader.selfRef().release();

            // then check the boundaries are reasonable, and corrupt them
            checkThenCorruptBoundaries(descriptor, rows * rowSize < Integer.MAX_VALUE);

            // then check that reopening corrects the corruption
            reader = SSTableReader.open(descriptor);
            reader.selfRef().release();
            checkThenCorruptBoundaries(descriptor, rows * rowSize < Integer.MAX_VALUE);
        }
        finally
        {
            FileUtils.deleteRecursive(tempdir);
        }
    }

    private static void checkThenCorruptBoundaries(Descriptor descriptor, boolean expectDataMmappable) throws IOException
    {
        File summaryFile = new File(descriptor.filenameFor(Component.SUMMARY));
        DataInputStream iStream = new DataInputStream(new FileInputStream(summaryFile));
        IndexSummary indexSummary = IndexSummary.serializer.deserialize(iStream, StorageService.getPartitioner(), true, CFMetaData.DEFAULT_MIN_INDEX_INTERVAL, CFMetaData.DEFAULT_MAX_INDEX_INTERVAL);
        ByteBuffer first = ByteBufferUtil.readWithLength(iStream);
        ByteBuffer last = ByteBufferUtil.readWithLength(iStream);
        MmappedSegmentedFile.Builder ibuilder = new MmappedSegmentedFile.Builder();
        MmappedSegmentedFile.Builder dbuilder = new MmappedSegmentedFile.Builder();
        ibuilder.deserializeBounds(iStream);
        dbuilder.deserializeBounds(iStream);
        iStream.close();
        // index file cannot generally be non-mmappable, as index entries cannot be larger than MAX_SEGMENT_SIZE (due to promotedSize being encoded as an int)
        assertBoundaries(descriptor.filenameFor(Component.PRIMARY_INDEX), true, ibuilder.boundaries());
        assertBoundaries(descriptor.filenameFor(Component.DATA), expectDataMmappable, dbuilder.boundaries());

        DataOutputStreamPlus oStream = new DataOutputStreamPlus(new FileOutputStream(summaryFile));
        IndexSummary.serializer.serialize(indexSummary, oStream, true);
        ByteBufferUtil.writeWithLength(first, oStream);
        ByteBufferUtil.writeWithLength(last, oStream);
        oStream.writeInt(1);
        oStream.writeLong(new File(descriptor.filenameFor(Component.PRIMARY_INDEX)).length());
        oStream.writeLong(new File(descriptor.filenameFor(Component.DATA)).length());
        oStream.close();
    }

    private static void assertBoundaries(String path, boolean expectMmappable, long[] boundaries)
    {
        long length = new File(path).length();
        long prev = boundaries[0];
        for (int i = 1 ; i <= boundaries.length && prev < length ; i++)
        {
            long boundary = i == boundaries.length ? length : boundaries[i];
            Assert.assertEquals(String.format("[%d, %d), %d of %d", boundary, prev, i, boundaries.length),
                                expectMmappable, boundary - prev <= Integer.MAX_VALUE);
            prev = boundary;
        }
    }

}
