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

import org.apache.cassandra.utils.ObjectSizes;

public class SimpleClustering extends Clustering
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new SimpleClustering(new ByteBuffer[0]));

    private final ByteBuffer[] values;

    public SimpleClustering(ByteBuffer... values)
    {
        this.values = values;
    }

    public SimpleClustering(ByteBuffer value)
    {
        this(new ByteBuffer[]{ value });
    }

    public int size()
    {
        return values.length;
    }

    public ByteBuffer get(int i)
    {
        return values[i];
    }

    public ByteBuffer[] getRawValues()
    {
        return values;
    }

    @Override
    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOnHeapOf(values);
    }

    @Override
    public Clustering takeAlias()
    {
        return this;
    }

    public static Builder builder(int size)
    {
        return new Builder(size);
    }

    public static class Builder implements Writer
    {
        private final ByteBuffer[] values;
        private int idx;

        private Builder(int size)
        {
            this.values = new ByteBuffer[size];
        }

        public void writeClusteringValue(ByteBuffer value)
        {
            values[idx++] = value;
        }

        public SimpleClustering build()
        {
            assert idx == values.length;
            return new SimpleClustering(values);
        }
    }
}
