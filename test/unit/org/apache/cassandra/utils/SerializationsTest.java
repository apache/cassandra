/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils;

import java.io.IOException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.AbstractSerializationsTester;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.utils.obs.OffHeapBitSet;

public class SerializationsTest extends AbstractSerializationsTester
{
    // Helper function to serialize old Bloomfilter format, should be removed once the old format is not supported
    public static void serializeOldBfFormat(BloomFilter bf, DataOutputPlus out) throws IOException
    {
        out.writeInt(bf.hashCount);
        Assert.assertTrue(bf.bitset instanceof OffHeapBitSet);
        ((OffHeapBitSet) bf.bitset).serializeOldBfFormat(out);
    }

    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static void testBloomFilterWrite1000(boolean oldBfFormat) throws IOException
    {
        try (IFilter bf = FilterFactory.getFilter(1000000, 0.0001))
        {
            for (int i = 0; i < 1000; i++)
                bf.add(Util.dk(Int32Type.instance.decompose(i)));
            try (DataOutputStreamPlus out = getOutput(oldBfFormat ? "3.0" : "4.0", "utils.BloomFilter1000.bin"))
            {
                if (oldBfFormat)
                    serializeOldBfFormat((BloomFilter) bf, out);
                else
                    BloomFilterSerializer.forVersion(false).serialize((BloomFilter) bf, out);
            }
        }
    }

    @Test
    public void testBloomFilterRead1000() throws IOException
    {
        if (EXECUTE_WRITES)
        {
            testBloomFilterWrite1000(false);
            testBloomFilterWrite1000(true);
        }

        try (FileInputStreamPlus in = getInput("4.0", "utils.BloomFilter1000.bin");
             IFilter filter = BloomFilterSerializer.forVersion(false).deserialize(in))
        {
            boolean present;
            for (int i = 0 ; i < 1000 ; i++)
            {
                present = filter.isPresent(Util.dk(Int32Type.instance.decompose(i)));
                Assert.assertTrue(present);
            }
            for (int i = 1000 ; i < 2000 ; i++)
            {
                present = filter.isPresent(Util.dk(Int32Type.instance.decompose(i)));
                Assert.assertFalse(present);
            }
        }

        try (FileInputStreamPlus in = getInput("3.0", "utils.BloomFilter1000.bin");
             IFilter filter = BloomFilterSerializer.forVersion(true).deserialize(in))
        {
            boolean present;
            for (int i = 0 ; i < 1000 ; i++)
            {
                present = filter.isPresent(Util.dk(Int32Type.instance.decompose(i)));
                Assert.assertTrue(present);
            }
            for (int i = 1000 ; i < 2000 ; i++)
            {
                present = filter.isPresent(Util.dk(Int32Type.instance.decompose(i)));
                Assert.assertFalse(present);
            }
        }
    }

    @Test
    public void testBloomFilterTable() throws Exception
    {
        testBloomFilterTable("test/data/bloom-filter/la/foo/la-1-big-Filter.db", true);
    }

    private void testBloomFilterTable(String file, boolean oldBfFormat) throws Exception
    {
        Murmur3Partitioner partitioner = new Murmur3Partitioner();

        try (FileInputStreamPlus in = new File(file).newInputStream();
             IFilter filter = BloomFilterSerializer.forVersion(oldBfFormat).deserialize(in))
        {
            for (int i = 1; i <= 10; i++)
            {
                DecoratedKey decoratedKey = partitioner.decorateKey(Int32Type.instance.decompose(i));
                boolean present = filter.isPresent(decoratedKey);
                Assert.assertTrue(present);
            }

            int positives = 0;
            for (int i = 11; i <= 1000010; i++)
            {
                DecoratedKey decoratedKey = partitioner.decorateKey(Int32Type.instance.decompose(i));
                boolean present = filter.isPresent(decoratedKey);
                if (present)
                    positives++;
            }
            double fpr = positives;
            fpr /= 1000000;
            Assert.assertTrue(fpr <= 0.011d);
        }
    }


    private static void testEstimatedHistogramWrite() throws IOException
    {
        EstimatedHistogram hist0 = new EstimatedHistogram();
        EstimatedHistogram hist1 = new EstimatedHistogram(5000);
        long[] offsets = new long[1000];
        long[] data = new long[offsets.length + 1];
        for (int i = 0; i < offsets.length; i++)
        {
            offsets[i] = i;
            data[i] = 10 * i;
        }
        data[offsets.length] = 100000; // write into the overflow bucket
        EstimatedHistogram hist2 = new EstimatedHistogram(offsets, data);

        try (DataOutputStreamPlus out = getOutput("utils.EstimatedHistogram.bin"))
        {
            EstimatedHistogram.serializer.serialize(hist0, out);
            EstimatedHistogram.serializer.serialize(hist1, out);
            EstimatedHistogram.serializer.serialize(hist2, out);
        }
    }

    @Test
    public void testEstimatedHistogramRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testEstimatedHistogramWrite();

        try (FileInputStreamPlus in = getInput("utils.EstimatedHistogram.bin"))
        {
            Assert.assertNotNull(EstimatedHistogram.serializer.deserialize(in));
            Assert.assertNotNull(EstimatedHistogram.serializer.deserialize(in));
            Assert.assertNotNull(EstimatedHistogram.serializer.deserialize(in));
        }
    }
}
