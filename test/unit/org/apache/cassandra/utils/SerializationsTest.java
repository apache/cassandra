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

import org.apache.cassandra.AbstractSerializationsTester;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.service.StorageService;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;

public class SerializationsTest extends AbstractSerializationsTester
{

    private void testBloomFilterWrite(boolean offheap) throws IOException
    {
        IPartitioner partitioner = StorageService.getPartitioner();
        try (IFilter bf = FilterFactory.getFilter(1000000, 0.0001, offheap))
        {
            for (int i = 0; i < 100; i++)
                bf.add(partitioner.decorateKey(partitioner.getTokenFactory().toByteArray(partitioner.getRandomToken())));
            try (DataOutputStreamPlus out = getOutput("utils.BloomFilter.bin"))
            {
                FilterFactory.serialize(bf, out);
            }
        }
    }

    @Test
    public void testBloomFilterReadMURMUR3() throws IOException
    {
        if (EXECUTE_WRITES)
            testBloomFilterWrite(true);

        try (DataInputStream in = getInput("utils.BloomFilter.bin");
             IFilter filter = FilterFactory.deserialize(in, true))
        {
            Assert.assertNotNull(filter);
        }
    }

    private void testEstimatedHistogramWrite() throws IOException
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

        try (DataInputStream in = getInput("utils.EstimatedHistogram.bin"))
        {
            Assert.assertNotNull(EstimatedHistogram.serializer.deserialize(in));
            Assert.assertNotNull(EstimatedHistogram.serializer.deserialize(in));
            Assert.assertNotNull(EstimatedHistogram.serializer.deserialize(in));
        }
    }
}
