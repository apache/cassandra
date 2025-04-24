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
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.NativeAllocator;
import org.apache.cassandra.utils.memory.NativePool;

public class NativeCellTest extends CQLTester
{

    private static final Logger logger = LoggerFactory.getLogger(NativeCellTest.class);
    private static final NativeAllocator nativeAllocator = new NativePool(Integer.MAX_VALUE,
                                                                          Integer.MAX_VALUE,
                                                                          1f,
                                                                          () -> ImmediateFuture.success(true)).newAllocator(null);
    private static final OpOrder.Group group = new OpOrder().start();
    private static Random rand;

    @BeforeClass
    public static void setUp()
    {
        long seed = System.currentTimeMillis();
        logger.info("Seed : {}", seed);
        rand = new Random(seed);
    }

    @Test
    public void testCells() throws Exception
    {
        for (int run = 0 ; run < 1000 ; run++)
        {
            Row.Builder builder = BTreeRow.unsortedBuilder();
            builder.newRow(rndclustering());
            int count = 1 + rand.nextInt(10);
            for (int i = 0 ; i < count ; i++)
                rndcd(builder);
            test(builder.build());
        }
    }

    private static Clustering<?> rndclustering()
    {
        int count = 1 + rand.nextInt(100);
        ByteBuffer[] values = new ByteBuffer[count];
        int size = rand.nextInt(65535);
        for (int i = 0 ; i < count ; i++)
        {
            int twiceShare = 1 + (2 * size) / (count - i);
            int nextSize = Math.min(size, rand.nextInt(twiceShare));
            if (nextSize < 10 && rand.nextBoolean())
                continue;

            byte[] bytes = new byte[nextSize];
            rand.nextBytes(bytes);
            values[i] = ByteBuffer.wrap(bytes);
            size -= nextSize;
        }
        return Clustering.make(values);
    }

    private static void rndcd(Row.Builder builder)
    {
        ColumnMetadata col = rndcol();
        if (!col.isComplex())
        {
            builder.addCell(rndcell(col));
        }
        else
        {
            int count = 1 + rand.nextInt(100);
            for (int i = 0 ; i < count ; i++)
                builder.addCell(rndcell(col));
        }
    }

    private static ColumnMetadata rndcol()
    {
        UUID uuid = new UUID(rand.nextLong(), rand.nextLong());
        boolean isComplex = rand.nextBoolean();
        return new ColumnMetadata("",
                                  "",
                                  ColumnIdentifier.getInterned(uuid.toString(), false),
                                    isComplex ? new SetType<>(BytesType.instance, true) : BytesType.instance,
                                  ColumnMetadata.NO_UNIQUE_ID,
                                  -1,
                                  ColumnMetadata.Kind.REGULAR,
                                  null);
    }

    private static Cell<?> rndcell(ColumnMetadata col)
    {
        long timestamp = rand.nextLong();
        int ttl = rand.nextInt();
        long localDeletionTime = ThreadLocalRandom.current().nextLong(Cell.getVersionedMaxDeletiontionTime() + 1);
        byte[] value = new byte[rand.nextInt(sanesize(expdecay()))];
        rand.nextBytes(value);
        CellPath path = null;
        if (col.isComplex())
        {
            byte[] pathbytes = new byte[rand.nextInt(sanesize(expdecay()))];
            rand.nextBytes(value);
            path = CellPath.create(ByteBuffer.wrap(pathbytes));
        }

        return new BufferCell(col, timestamp, ttl, localDeletionTime, ByteBuffer.wrap(value), path);
    }

    private static int expdecay()
    {
        return 1 << Integer.numberOfTrailingZeros(Integer.lowestOneBit(rand.nextInt()));
    }

    private static int sanesize(int randomsize)
    {
        return Math.min(Math.max(1, randomsize), 1 << 26);
    }

    private static void test(Row row)  throws Exception
    {
        Row nrow = row.clone(nativeAllocator.cloner(group));
        Row brow = row.clone(HeapCloner.instance);
        Assert.assertEquals(row, nrow);
        Assert.assertEquals(row, brow);
        Assert.assertEquals(nrow, brow);

        Digest rowDigest = Digest.forReadResponse();
        Digest nativeRowDigest = Digest.forReadResponse();
        Digest byteBufferRowDigest = Digest.forReadResponse();
        row.digest(rowDigest);
        nrow.digest(nativeRowDigest);
        brow.digest(byteBufferRowDigest);
        byte[] rowDigestValue = rowDigest.digest();
        Assert.assertArrayEquals(rowDigestValue, nativeRowDigest.digest());
        Assert.assertArrayEquals(rowDigestValue, byteBufferRowDigest.digest());

        Assert.assertEquals(row.dataSize(), nrow.dataSize());
        Assert.assertEquals(row.dataSize(), brow.dataSize());

        assertClustering(row, brow, nrow);

        assertCellsDataSize(row, nrow);
        assertCellsDataSize(row, brow);

        assertCellsWrittenToOutput(row, nrow);
        assertCellsWrittenToOutput(row, brow);

        assertCellsSlicing(row, nrow);
        assertCellsSlicing(row, brow);
    }

    private static void assertClustering(Row row, Row byteBufferRow, Row nativeRow) throws Exception
    {
        Assert.assertEquals(row.clustering(), nativeRow.clustering());
        Assert.assertEquals(row.clustering(), byteBufferRow.clustering());
        Assert.assertEquals(nativeRow.clustering(), byteBufferRow.clustering());

        ClusteringComparator comparator = new ClusteringComparator(UTF8Type.instance);
        Assert.assertEquals(0, comparator.compare(row.clustering(), nativeRow.clustering()));
        Assert.assertEquals(0, comparator.compare(row.clustering(), byteBufferRow.clustering()));
        Assert.assertEquals(0, comparator.compare(nativeRow.clustering(), byteBufferRow.clustering()));
        Assert.assertEquals(0, comparator.compare(nativeRow.clustering(), row.clustering()));
        Assert.assertEquals(0, comparator.compare(nativeRow.clustering(), nativeRow.clustering()));


        Assert.assertEquals(row.clustering().size(), nativeRow.clustering().size());
        Assert.assertEquals(row.clustering().size(), byteBufferRow.clustering().size());

        assertByteBufferArrayEquals(row.clustering().getBufferArray(), nativeRow.clustering().getBufferArray());
        assertByteBufferArrayEquals(row.clustering().getBufferArray(), byteBufferRow.clustering().getBufferArray());

        assertRawValuesEquals(row.clustering(), nativeRow.clustering());
        assertRawValuesEquals(row.clustering(), byteBufferRow.clustering());


        for (int i = 0; i < row.clustering().size(); i++)
        {
            Assert.assertEquals(row.clustering().isEmpty(i), byteBufferRow.clustering().isEmpty(i));
            Assert.assertEquals(row.clustering().isEmpty(i), nativeRow.clustering().isEmpty(i));

            Assert.assertEquals(row.clustering().isNull(i), byteBufferRow.clustering().isNull(i));
            Assert.assertEquals(row.clustering().isNull(i), nativeRow.clustering().isNull(i));
        }

        assertClusteringElementSizes(row.clustering(), byteBufferRow.clustering());
        assertClusteringElementSizes(row.clustering(), nativeRow.clustering());

        assertClusteringElementWrittenToOutput(row.clustering(), byteBufferRow.clustering());
        assertClusteringElementWrittenToOutput(row.clustering(), nativeRow.clustering());

        assertClusteringSlicing(row.clustering(), byteBufferRow.clustering());
        assertClusteringSlicing(row.clustering(), nativeRow.clustering());

    }

    private static <V1, V2> void assertRawValuesEquals(Clustering<V1> c1, Clustering<V2> c2)
    {
        V1[] rawValues1 = c1.getRawValues();
        V2[] rawValues2 = c2.getRawValues();
        Assert.assertEquals(rawValues1.length, rawValues2.length);
        for (int i = 0; i < c1.size(); i++)
        {
            if (rawValues1[i] != null)
                Assert.assertEquals(0, c1.accessor().compare(rawValues1[i], rawValues2[i], c2.accessor()));
        }
    }

    private static <V1, V2> void assertClusteringElementSizes(Clustering<V1> c1, Clustering<V2> c2)
    {
        for (int i = 0; i < c1.size(); i++)
        {
            if (c1.get(i) != null)
            {
                int sizeC1 = c1.accessor().size(c1.get(i));
                int sizeC2 = c2.accessor().size(c2.get(i));
                Assert.assertEquals(sizeC1, sizeC2);
            }
        }
    }

    private static <V1, V2> void assertClusteringElementWrittenToOutput(Clustering<V1> c1, Clustering<V2> c2) throws IOException
    {
        for (int i = 0; i < c1.size(); i++)
        {
            if (c1.get(i) != null)
            {
                DataOutputBuffer outputC1 = new DataOutputBuffer(c1.dataSize());
                DataOutputBuffer outputC2 = new DataOutputBuffer(c2.dataSize());
                c1.accessor().write(c1.get(i), outputC1);
                c2.accessor().write(c2.get(i), outputC2);
                Assert.assertArrayEquals(outputC1.toByteArray(), outputC2.toByteArray());
            }
        }
    }

    private static <V1, V2> void assertClusteringSlicing(Clustering<V1> c1, Clustering<V2> c2) throws IOException
    {
        for (int i = 0; i < c1.size(); i++)
        {
            if (c1.get(i) != null)
            {
                int offset = c1.accessor().size(c1.get(i)) / 3;
                int length = c1.accessor().size(c1.get(i)) / 2;
                V1 slice1 = c1.accessor().slice(c1.get(i), offset, length);
                V2 slice2 = c2.accessor().slice(c2.get(i), offset, length);
                Assert.assertEquals(0, c1.accessor().compare(slice1, slice2, c2.accessor()));
                Assert.assertEquals(0, c2.accessor().compare(slice2, slice1, c1.accessor()));
            }
        }
    }

    private static void assertByteBufferArrayEquals(ByteBuffer[] array1, ByteBuffer[] array2) {
        Assert.assertEquals(array1.length, array2.length);
        for (int i = 0; i < array1.length; i++) {
            if (array1[i] != null)
                Assert.assertEquals(0, ByteBufferUtil.compareUnsigned(array1[i], array2[i]));
        }
    }

    private static void assertCellsWrittenToOutput(Row row1, Row row2) throws IOException
    {
        Iterator<Cell<?>> row1Iterator = row1.cells().iterator();
        Iterator<Cell<?>> row2Iterator = row2.cells().iterator();
        while (row1Iterator.hasNext())
        {
            Cell cell1 = row1Iterator.next();
            Cell cell2 = row2Iterator.next();
            DataOutputBuffer output1 = new DataOutputBuffer(cell1.dataSize());
            DataOutputBuffer output2 = new DataOutputBuffer(cell2.dataSize());
            cell1.accessor().write(cell1.value(), output1);
            cell2.accessor().write(cell2.value(), output2);
            Assert.assertArrayEquals(output1.toByteArray(), output2.toByteArray());
        }
    }

    private static void assertCellsSlicing(Row row1, Row row2)
    {
        Iterator<Cell<?>> row1Iterator = row1.cells().iterator();
        Iterator<Cell<?>> row2Iterator = row2.cells().iterator();
        while (row1Iterator.hasNext())
        {
            Cell cell1 = row1Iterator.next();
            Cell cell2 = row2Iterator.next();
            int offset = cell1.accessor().size(cell1.value()) / 3;
            int length = cell1.accessor().size(cell1.value()) / 2;
            Object slice1 = cell1.accessor().slice(cell1.value(), offset, length);
            Object slice2 = cell2.accessor().slice(cell2.value(), offset, length);
            Assert.assertEquals(0, cell1.accessor().compare(slice1, slice2, cell2.accessor()));
            Assert.assertEquals(0, cell2.accessor().compare(slice2, slice1, cell1.accessor()));
        }
    }

    private static void assertCellsDataSize(Row row1, Row row2)
    {
        Iterator<Cell<?>> row1Iterator = row1.cells().iterator();
        Iterator<Cell<?>> row2Iterator = row2.cells().iterator();
        while (row1Iterator.hasNext())
        {
            Cell cell1 = row1Iterator.next();
            Cell cell2 = row2Iterator.next();
            Assert.assertEquals(cell1.dataSize(), cell2.dataSize());
        }
    }

}
