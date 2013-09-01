package org.apache.cassandra.cache;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.Assert;

import org.apache.cassandra.db.ColumnIndex;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.utils.ObjectSizes;
import org.github.jamm.MemoryMeter;
import org.junit.Test;

public class ObjectSizeTest
{
    public static final MemoryMeter meter = new MemoryMeter().omitSharedBufferOverhead();

    @Test
    public void testArraySizes()
    {
        long size = ObjectSizes.getArraySize(0, 1);
        long size2 = meter.measureDeep(new byte[0]);
        Assert.assertEquals(size, size2);
    }

    @Test
    public void testBiggerArraySizes()
    {
        long size = ObjectSizes.getArraySize(0, 1);
        long size2 = meter.measureDeep(new byte[0]);
        Assert.assertEquals(size, size2);

        size = ObjectSizes.getArraySize(8, 1);
        size2 = meter.measureDeep(new byte[8]);
        Assert.assertEquals(size, size2);
    }

    @Test
    public void testKeyCacheKey()
    {
        KeyCacheKey key = new KeyCacheKey(null, ByteBuffer.wrap(new byte[0]));
        long size = key.memorySize();
        long size2 = meter.measureDeep(key);
        Assert.assertEquals(size, size2);
    }

    @Test
    public void testKeyCacheValue()
    {
        RowIndexEntry entry = new RowIndexEntry(123);
        long size = entry.memorySize();
        long size2 = meter.measureDeep(entry);
        Assert.assertEquals(size, size2);
    }

    @Test
    public void testKeyCacheValueWithDelInfo()
    {
        RowIndexEntry entry = RowIndexEntry.create(123, new DeletionTime(123, 123), ColumnIndex.nothing());
        long size = entry.memorySize();
        long size2 = meter.measureDeep(entry);
        Assert.assertEquals(size, size2);
    }

    @Test
    public void testRowCacheKey()
    {
        UUID id = UUID.randomUUID();
        RowCacheKey key = new RowCacheKey(id, ByteBuffer.wrap(new byte[11]));
        long size = key.memorySize();
        long size2 = meter.measureDeep(key) - meter.measureDeep(id);
        Assert.assertEquals(size, size2);
    }

    @Test
    public void testRowCacheSentinel()
    {
        RowCacheSentinel sentinel = new RowCacheSentinel(123);
        long size = sentinel.memorySize();
        long size2 = meter.measureDeep(sentinel);
        Assert.assertEquals(size, size2);
    }
}
