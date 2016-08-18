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

import java.io.DataInputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.AbstractSerializationsTester;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;

import java.io.File;
import java.io.FileInputStream;

public class SerializationsTest extends AbstractSerializationsTester
{
    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static void testBloomFilterWrite(boolean offheap, boolean oldBfHashOrder) throws IOException
    {
        IPartitioner partitioner = Util.testPartitioner();
        try (IFilter bf = FilterFactory.getFilter(1000000, 0.0001, offheap, oldBfHashOrder))
        {
            for (int i = 0; i < 100; i++)
                bf.add(partitioner.decorateKey(partitioner.getTokenFactory().toByteArray(partitioner.getRandomToken())));
            try (DataOutputStreamPlus out = getOutput(oldBfHashOrder ? "2.1" : "3.0", "utils.BloomFilter.bin"))
            {
                FilterFactory.serialize(bf, out);
            }
        }
    }

    private static void testBloomFilterWrite1000(boolean offheap, boolean oldBfHashOrder) throws IOException
    {
        try (IFilter bf = FilterFactory.getFilter(1000000, 0.0001, offheap, oldBfHashOrder))
        {
            for (int i = 0; i < 1000; i++)
                bf.add(Util.dk(Int32Type.instance.decompose(i)));
            try (DataOutputStreamPlus out = getOutput(oldBfHashOrder ? "2.1" : "3.0", "utils.BloomFilter1000.bin"))
            {
                FilterFactory.serialize(bf, out);
            }
        }
    }

    @Test
    public void testBloomFilterRead1000() throws IOException
    {
        if (EXECUTE_WRITES)
        {
            testBloomFilterWrite1000(true, false);
            testBloomFilterWrite1000(true, true);
        }

        try (DataInputStream in = getInput("3.0", "utils.BloomFilter1000.bin");
             IFilter filter = FilterFactory.deserialize(in, true, false))
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

        try (DataInputStream in = getInput("2.1", "utils.BloomFilter1000.bin");
             IFilter filter = FilterFactory.deserialize(in, true, true))
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

        // eh - reading version version 'ka' (2.1) with 3.0 BloomFilter
        int falsePositive = 0;
        int falseNegative = 0;
        try (DataInputStream in = getInput("2.1", "utils.BloomFilter1000.bin");
             IFilter filter = FilterFactory.deserialize(in, true, false))
        {
            boolean present;
            for (int i = 0 ; i < 1000 ; i++)
            {
                present = filter.isPresent(Util.dk(Int32Type.instance.decompose(i)));
                if (!present)
                    falseNegative ++;
            }
            for (int i = 1000 ; i < 2000 ; i++)
            {
                present = filter.isPresent(Util.dk(Int32Type.instance.decompose(i)));
                if (present)
                    falsePositive ++;
            }
        }
        Assert.assertEquals(1000, falseNegative);
        Assert.assertEquals(0, falsePositive);
    }

    @Test
    public void testBloomFilterTable() throws Exception
    {
        testBloomFilterTable("test/data/bloom-filter/ka/foo/foo-atable-ka-1-Filter.db", true);
        testBloomFilterTable("test/data/bloom-filter/la/foo/la-1-big-Filter.db", false);
    }

    private static void testBloomFilterTable(String file, boolean oldBfHashOrder) throws Exception
    {
        Murmur3Partitioner partitioner = new Murmur3Partitioner();

        try (DataInputStream in = new DataInputStream(new FileInputStream(new File(file)));
             IFilter filter = FilterFactory.deserialize(in, true, oldBfHashOrder))
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

    @Test
    public void testBloomFilterReadMURMUR3() throws IOException
    {
        if (EXECUTE_WRITES)
            testBloomFilterWrite(true, true);

        try (DataInputStream in = getInput("3.0", "utils.BloomFilter.bin");
             IFilter filter = FilterFactory.deserialize(in, true, true))
        {
            Assert.assertNotNull(filter);
        }
    }

    @Test
    public void testBloomFilterReadMURMUR3pre30() throws IOException
    {
        if (EXECUTE_WRITES)
            testBloomFilterWrite(true, false);

        try (DataInputStream in = getInput("2.1", "utils.BloomFilter.bin");
             IFilter filter = FilterFactory.deserialize(in, true, false))
        {
            Assert.assertNotNull(filter);
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
        data[offsets.length] = 100000;
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

        try (DataInputStreamPlus in = getInput("utils.EstimatedHistogram.bin"))
        {
            Assert.assertNotNull(EstimatedHistogram.serializer.deserialize(in));
            Assert.assertNotNull(EstimatedHistogram.serializer.deserialize(in));
            Assert.assertNotNull(EstimatedHistogram.serializer.deserialize(in));
        }
    }
}
