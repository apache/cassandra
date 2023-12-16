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

package org.apache.cassandra.index.sai.disk.vector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.util.function.IntUnaryOperator;

import com.google.common.annotations.VisibleForTesting;

import io.github.jbellis.jvector.util.RamUsageEstimator;
import org.apache.cassandra.io.util.SequentialWriter;
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
    public boolean isValueShared()
    {
        return false;
    }

    @Override
    public ConcurrentVectorValues copy()
    {
        // no actual copy required because we always return distinct float[] for distinct vector ordinals
        return this;
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

    @VisibleForTesting
    public long write(SequentialWriter writer, IntUnaryOperator ordinalMapper) throws IOException
    {
        writer.writeInt(size());
        writer.writeInt(dimension());

        for (var i = 0; i < size(); i++) {
            int ord = ordinalMapper.applyAsInt(i);
            var fb = FloatBuffer.wrap(values.get(ord));
            var bb = ByteBuffer.allocate(fb.capacity() * Float.BYTES);
            bb.asFloatBuffer().put(fb);
            writer.write(bb);
        }

        return writer.position();
    }
}
