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
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.BloomFilterSerializer;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.SerializationsTest;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 4, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2)
@State(Scope.Benchmark)
public class BloomFilterSerializerBench
{
    @Param({"1", "10", "100", "1024"})
    private long numElemsInK;

    @Param({"true", "false"})
    public boolean oldBfFormat;

    static final IFilter.FilterKey wrap(ByteBuffer buf)
    {
        return new BufferDecoratedKey(new Murmur3Partitioner.LongToken(0L), buf);
    }

    private ByteBuffer testVal = ByteBuffer.wrap(new byte[] { 0, 1});

    private static final BloomFilterSerializer serializer = BloomFilterSerializer.forVersion(false);

    @Benchmark
    public void serializationTest() throws IOException
    {
        File file = FileUtils.createTempFile("bloomFilterTest-", ".dat");
        try
        {
            BloomFilter filter = (BloomFilter) FilterFactory.getFilter(numElemsInK * 1024, 0.01d);
            filter.add(wrap(testVal));
            FileOutputStreamPlus out = new FileOutputStreamPlus(file);
            if (oldBfFormat)
                SerializationsTest.serializeOldBfFormat(filter, out);
            else
                serializer.serialize(filter, out);
            out.close();
            filter.close();

            FileInputStreamPlus in = new FileInputStreamPlus(file);
            BloomFilter filter2 = BloomFilterSerializer.forVersion(oldBfFormat).deserialize(in);
            FileUtils.closeQuietly(in);
            filter2.close();
        }
        finally
        {
            file.tryDelete();
        }
    }

}
