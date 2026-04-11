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

package org.apache.cassandra.index.sai.disk.v1.vector;

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

    private long oneVectorBytesUsed()
    {
        return Integer.BYTES + Integer.BYTES + (long) dimension() * Float.BYTES;
    }
}
