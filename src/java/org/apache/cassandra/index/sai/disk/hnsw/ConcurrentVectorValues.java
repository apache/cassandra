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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

public class ConcurrentVectorValues implements RandomAccessVectorValues<float[]>
{
    private final VectorType.Serializer serializer;
    private final Map<Integer, float[]> values = new ConcurrentHashMap<>();

    public ConcurrentVectorValues(VectorType.Serializer serializer)
    {
        this.serializer = serializer;
    }

    @Override
    public int size()
    {
        return values.size();
    }

    @Override
    public int dimension()
    {
        return serializer.getDimensions();
    }

    @Override
    public float[] vectorValue(int i)
    {
        return values.get(i);
    }

    public void add(int ordinal, float[] vector)
    {
        values.put(ordinal, vector);
    }

    @Override
    public RandomAccessVectorValues<float[]> copy()
    {
        return this;
    }

    public void write(SequentialWriter writer) throws IOException
    {
        writer.writeInt(size());
        writer.writeInt(dimension());

        for (var i = 0; i < size(); i++) {
            var buffer = serializer.serialize(vectorValue(i));
            writer.write(buffer);
        }
    }

    public long ramBytesUsed()
    {
        long REF_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        return 2 * REF_BYTES
               + RamEstimation.concurrentHashMapRamUsed(values.size())
               + values.size() * (Integer.BYTES + Integer.BYTES + (long) dimension() * Float.BYTES);
    }
}
