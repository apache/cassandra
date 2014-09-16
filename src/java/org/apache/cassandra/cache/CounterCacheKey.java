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
package org.apache.cassandra.cache;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.utils.*;

public class CounterCacheKey implements CacheKey
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new CounterCacheKey(null, ByteBufferUtil.EMPTY_BYTE_BUFFER, CellNames.simpleDense(ByteBuffer.allocate(1))))
                                           + ObjectSizes.measure(new UUID(0, 0));

    public final UUID cfId;
    public final byte[] partitionKey;
    public final byte[] cellName;

    private CounterCacheKey(UUID cfId, ByteBuffer partitionKey, CellName cellName)
    {
        this.cfId = cfId;
        this.partitionKey = ByteBufferUtil.getArray(partitionKey);
        this.cellName = ByteBufferUtil.getArray(cellName.toByteBuffer());
    }

    public static CounterCacheKey create(UUID cfId, ByteBuffer partitionKey, CellName cellName)
    {
        return new CounterCacheKey(cfId, partitionKey, cellName);
    }

    public UUID getCFId()
    {
        return cfId;
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE
               + ObjectSizes.sizeOfArray(partitionKey)
               + ObjectSizes.sizeOfArray(cellName);
    }

    @Override
    public String toString()
    {
        return String.format("CounterCacheKey(%s, %s, %s)",
                             cfId,
                             ByteBufferUtil.bytesToHex(ByteBuffer.wrap(partitionKey)),
                             ByteBufferUtil.bytesToHex(ByteBuffer.wrap(cellName)));
    }

    @Override
    public int hashCode()
    {
        return Arrays.deepHashCode(new Object[]{cfId, partitionKey, cellName});
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof CounterCacheKey))
            return false;

        CounterCacheKey cck = (CounterCacheKey) o;

        return cfId.equals(cck.cfId)
            && Arrays.equals(partitionKey, cck.partitionKey)
            && Arrays.equals(cellName, cck.cellName);
    }
}
