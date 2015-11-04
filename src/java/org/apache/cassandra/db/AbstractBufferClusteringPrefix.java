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

public abstract class AbstractBufferClusteringPrefix extends AbstractClusteringPrefix
{
    public static final ByteBuffer[] EMPTY_VALUES_ARRAY = new ByteBuffer[0];
    private static final long EMPTY_SIZE = ObjectSizes.measure(Clustering.make(EMPTY_VALUES_ARRAY));

    protected final Kind kind;
    protected final ByteBuffer[] values;

    protected AbstractBufferClusteringPrefix(Kind kind, ByteBuffer[] values)
    {
        this.kind = kind;
        this.values = values;
    }

    public Kind kind()
    {
        return kind;
    }

    public ClusteringPrefix clustering()
    {
        return this;
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

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOnHeapOf(values);
    }

    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOnHeapExcludingData(values);
    }
}
