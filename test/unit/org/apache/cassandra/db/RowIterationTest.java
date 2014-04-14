/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.HashSet;

import org.apache.cassandra.Util;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.utils.FBUtilities;
import static org.junit.Assert.assertEquals;
import org.apache.cassandra.utils.ByteBufferUtil;


public class RowIterationTest extends SchemaLoader
{
    public static final String KEYSPACE1 = "Keyspace2";
    public static final InetAddress LOCAL = FBUtilities.getBroadcastAddress();

    @Test
    public void testRowIteration()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Super3");

        final int ROWS_PER_SSTABLE = 10;
        Set<DecoratedKey> inserted = new HashSet<DecoratedKey>();
        for (int i = 0; i < ROWS_PER_SSTABLE; i++) {
            DecoratedKey key = Util.dk(String.valueOf(i));
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            rm.add("Super3", CellNames.compositeDense(ByteBufferUtil.bytes("sc"), ByteBufferUtil.bytes(String.valueOf(i))), ByteBuffer.wrap(new byte[ROWS_PER_SSTABLE * 10 - i * 2]), i);
            rm.apply();
            inserted.add(key);
        }
        store.forceBlockingFlush();
        assertEquals(inserted.toString(), inserted.size(), Util.getRangeSlice(store).size());
    }

    @Test
    public void testRowIterationDeletionTime()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        String CF_NAME = "Standard3";
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_NAME);
        DecoratedKey key = Util.dk("key");

        // Delete row in first sstable
        Mutation rm = new Mutation(KEYSPACE1, key.getKey());
        rm.delete(CF_NAME, 0);
        rm.add(CF_NAME, Util.cellname("c"), ByteBufferUtil.bytes("values"), 0L);
        rm.apply();
        store.forceBlockingFlush();

        // Delete row in second sstable with higher timestamp
        rm = new Mutation(KEYSPACE1, key.getKey());
        rm.delete(CF_NAME, 1);
        rm.add(CF_NAME, Util.cellname("c"), ByteBufferUtil.bytes("values"), 1L);
        DeletionInfo delInfo2 = rm.getColumnFamilies().iterator().next().deletionInfo();
        assert delInfo2.getTopLevelDeletion().markedForDeleteAt == 1L;
        rm.apply();
        store.forceBlockingFlush();

        ColumnFamily cf = Util.getRangeSlice(store).iterator().next().cf;
        assert cf.deletionInfo().equals(delInfo2);
    }

    @Test
    public void testRowIterationDeletion()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        String CF_NAME = "Standard3";
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_NAME);
        DecoratedKey key = Util.dk("key");

        // Delete a row in first sstable
        Mutation rm = new Mutation(KEYSPACE1, key.getKey());
        rm.delete(CF_NAME, 0);
        rm.apply();
        store.forceBlockingFlush();

        ColumnFamily cf = Util.getRangeSlice(store).iterator().next().cf;
        assert cf != null;
    }
}
