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


import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.utils.ObjectSizes;

public abstract class AbstractArrayClusteringPrefix extends AbstractClusteringPrefix<byte[]>
{
    public static final byte[][] EMPTY_VALUES_ARRAY = new byte[0][];
    private static final long EMPTY_SIZE = ObjectSizes.measure(new ArrayClustering(EMPTY_VALUES_ARRAY));
    protected final Kind kind;
    protected final byte[][] values;

    public AbstractArrayClusteringPrefix(Kind kind, byte[][] values)
    {
        this.kind = kind;
        this.values = values;
    }

    public Kind kind()
    {
        return kind;
    }

    public ValueAccessor<byte[]> accessor()
    {
        return ByteArrayAccessor.instance;
    }

    public ClusteringPrefix clustering()
    {
        return this;
    }

    public int size()
    {
        return values.length;
    }

    public byte[] get(int i)
    {
        return values[i];
    }

    public byte[][] getRawValues()
    {
        return values;
    }

    public ByteBuffer[] getBufferArray()
    {
        ByteBuffer[] out = new ByteBuffer[values.length];
        for (int i=0; i<values.length; i++)
            out[i] = ByteBuffer.wrap(values[i]);
        return out;
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOfArray(values) + values.length;
    }

    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOfArray(values);
    }

    public ClusteringPrefix<byte[]> minimize()
    {
        return this;
    }
}
