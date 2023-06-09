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

package org.apache.cassandra.index.sai.disk.hnsw;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.jctools.maps.NonBlockingHashMapLong;

public class ConcurrentVectorValues implements RamAwareVectorValues
{
    private final int dimensions;
    private final NonBlockingHashMapLong<float[]> values = new NonBlockingHashMapLong<>();

    public ConcurrentVectorValues(int dimensions)
    {
        this.dimensions = dimensions;
    }

    @Override
    public int size()
    {
        return values.size();
    }

    @Override
    public int dimension()
    {
        return dimensions;
    }

    @Override
    public float[] vectorValue(int i)
    {
        return values.get(i);
    }

    /** return approximate bytes used by the new vector */
    public long add(int ordinal, float[] vector)
    {
        values.put(ordinal, vector);
        return RamEstimation.concurrentHashMapRamUsed(1) + oneVectorBytesUsed();
    }

    @Override
    public RandomAccessVectorValues<float[]> copy()
    {
        return this;
    }

    public long write(SequentialWriter writer) throws IOException
    {
        writer.writeInt(size());
        writer.writeInt(dimension());

        // we will re-use this buffer
        var byteBuffer = ByteBuffer.allocate(dimension() * Float.BYTES);
        var floatBuffer = byteBuffer.asFloatBuffer();

        for (var i = 0; i < size(); i++) {
            floatBuffer.put(vectorValue(i));
            // bytebuffer and floatBuffer track their positions separately, and we never changed BB's, so don't need to rewind it
            floatBuffer.rewind();
            writer.write(byteBuffer);
        }

        return writer.position();
    }

    public long ramBytesUsed()
    {
        long REF_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        return 2 * REF_BYTES
               + RamEstimation.concurrentHashMapRamUsed(values.size())
               + values.size() * oneVectorBytesUsed();
    }

    private long oneVectorBytesUsed()
    {
        return Integer.BYTES + Integer.BYTES + (long) dimension() * Float.BYTES;
    }
}
