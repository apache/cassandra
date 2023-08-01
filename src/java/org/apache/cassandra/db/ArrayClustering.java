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

import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.utils.ObjectSizes;

public class ArrayClustering extends AbstractArrayClusteringPrefix implements Clustering<byte[]>
{
    public static final long EMPTY_SIZE = ObjectSizes.measure(new ArrayClustering(EMPTY_VALUES_ARRAY));

    public ArrayClustering(byte[]... values)
    {
        super(Kind.CLUSTERING, values);
    }

    public long unsharedHeapSize()
    {
        if (this == ByteArrayAccessor.factory.clustering() || this == ByteArrayAccessor.factory.staticClustering())
            return 0;
        long arrayRefSize = ObjectSizes.sizeOfArray(values);
        long elementsSize = 0;
        for (int i = 0; i < values.length; i++)
            elementsSize += ObjectSizes.sizeOfArray(values[i]);
        return EMPTY_SIZE + arrayRefSize + elementsSize;
    }

    public long unsharedHeapSizeExcludingData()
    {
        if (this == ByteArrayAccessor.factory.clustering() || this == ByteArrayAccessor.factory.staticClustering())
            return 0;
        return EMPTY_SIZE + ObjectSizes.sizeOfArray(values);
    }

    public static ArrayClustering make(byte[]... values)
    {
        return new ArrayClustering(values);
    }
}
