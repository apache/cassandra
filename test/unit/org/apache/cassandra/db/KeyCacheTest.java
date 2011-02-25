package org.apache.cassandra.db;
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


import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.utils.ByteBufferUtil;

public class KeyCacheTest extends CleanupHelper
{
    private static final String TABLE1 = "KeyCacheSpace";

    @Test
    public void testKeyCache50() throws IOException, ExecutionException, InterruptedException
    {
        testKeyCache("Standard1", 64);
    }

    @Test
    public void testKeyCache100() throws IOException, ExecutionException, InterruptedException
    {
        testKeyCache("Standard2", 128);
    }

    public void testKeyCache(String cfName, int expectedCacheSize) throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open(TABLE1);
        ColumnFamilyStore store = table.getColumnFamilyStore(cfName);

        // KeyCache should start at size 1 if we're caching X% of zero data.
        int keyCacheSize = store.getKeyCacheCapacity();
        assert keyCacheSize == 1 : keyCacheSize;

        DecoratedKey key1 = Util.dk("key1");
        DecoratedKey key2 = Util.dk("key2");
        RowMutation rm;

        // inserts
        rm = new RowMutation(TABLE1, key1.key);
        rm.add(new QueryPath(cfName, null, ByteBufferUtil.bytes("1")), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.apply();
        rm = new RowMutation(TABLE1, key2.key);
        rm.add(new QueryPath(cfName, null, ByteBufferUtil.bytes("2")), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.apply();

        // deletes
        rm = new RowMutation(TABLE1, key1.key);
        rm.delete(new QueryPath(cfName, null, ByteBufferUtil.bytes("1")), 1);
        rm.apply();
        rm = new RowMutation(TABLE1, key2.key);
        rm.delete(new QueryPath(cfName, null, ByteBufferUtil.bytes("2")), 1);
        rm.apply();

        // After a flush, the cache should expand to be X% of indices * INDEX_INTERVAL.
        store.forceBlockingFlush();
        keyCacheSize = store.getKeyCacheCapacity();
        assert keyCacheSize == expectedCacheSize : keyCacheSize;

        // After a compaction, the cache should expand to be X% of zero data.
        CompactionManager.instance.submitMajor(store, 0, Integer.MAX_VALUE).get();
        keyCacheSize = store.getKeyCacheCapacity();
        assert keyCacheSize == 1 : keyCacheSize;
    }}
