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

package org.apache.cassandra.service.accord;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ObjectSizes;

public class IndexRange implements Comparable<IndexRange>, IMeasurableMemory
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new IndexRange(null, null));

    public final byte[] start, end;

    public IndexRange(byte[] start, byte[] end)
    {
        this.start = start;
        this.end = end;
    }

    @Override
    public int compareTo(IndexRange other)
    {
        int rc = ByteArrayUtil.compareUnsigned(start, 0, other.start, 0, start.length);
        if (rc == 0)
            rc = ByteArrayUtil.compareUnsigned(end, 0, other.end, 0, end.length);
        return rc;
    }

    @Override
    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOfArray(start) * 2;
    }

    public boolean intersects(byte[] start, byte[] end)
    {
        if (ByteArrayUtil.compareUnsigned(this.start, 0, end, 0, end.length) >= 0)
            return false;
        if (ByteArrayUtil.compareUnsigned(this.end, 0, start, 0, start.length) <= 0)
            return false;
        return true;
    }
}
