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

package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.junit.Test;

import org.apache.cassandra.db.*;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.Util;

public class SSTableTest extends SchemaLoader
{
    @Test
    public void testSingleWrite() throws IOException
    {
        // write test data
        ByteBuffer key = ByteBufferUtil.bytes(Integer.toString(1));
        ByteBuffer cbytes = ByteBuffer.wrap(new byte[1024]);
        new Random().nextBytes(cbytes.array());
        ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard1");
        cf.addColumn(null, new Column(cbytes, cbytes));

        SortedMap<DecoratedKey, ColumnFamily> map = new TreeMap<DecoratedKey, ColumnFamily>();
        map.put(Util.dk(key), cf);
        SSTableReader ssTable = SSTableUtils.prepare().cf("Standard1").write(map);

        // verify
        ByteBuffer bytes = Util.serializeForSSTable(cf);
        verifySingle(ssTable, bytes, key);
        ssTable = SSTableReader.open(ssTable.descriptor); // read the index from disk
        verifySingle(ssTable, bytes, key);
    }

    private void verifySingle(SSTableReader sstable, ByteBuffer bytes, ByteBuffer key) throws IOException
    {
        RandomAccessReader file = sstable.openDataReader(false);
        file.seek(sstable.getPosition(sstable.partitioner.decorateKey(key), SSTableReader.Operator.EQ).position);
        assert key.equals(ByteBufferUtil.readWithShortLength(file));
        int size = (int)SSTableReader.readRowSize(file, sstable.descriptor);
        byte[] bytes2 = new byte[size];
        file.readFully(bytes2);
        assert ByteBuffer.wrap(bytes2).equals(bytes);
    }

    @Test
    public void testManyWrites() throws IOException
    {
        SortedMap<DecoratedKey, ColumnFamily> map = new TreeMap<DecoratedKey, ColumnFamily>();
        SortedMap<ByteBuffer, ByteBuffer> bytesMap = new TreeMap<ByteBuffer, ByteBuffer>();
        //for (int i = 100; i < 1000; ++i)
        for (int i = 100; i < 300; ++i)
        {
            ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard2");
            ByteBuffer bytes = ByteBufferUtil.bytes(("Avinash Lakshman is a good man: " + i));
            cf.addColumn(null, new Column(bytes, bytes));
            map.put(Util.dk(Integer.toString(i)), cf);
            bytesMap.put(ByteBufferUtil.bytes(Integer.toString(i)), Util.serializeForSSTable(cf));
        }

        // write
        SSTableReader ssTable = SSTableUtils.prepare().cf("Standard2").write(map);

        // verify
        verifyMany(ssTable, bytesMap);
        ssTable = SSTableReader.open(ssTable.descriptor); // read the index from disk
        verifyMany(ssTable, bytesMap);

        Set<Component> live = SSTable.componentsFor(ssTable.descriptor);
        assert !live.isEmpty() : "SSTable has no live components";
        Set<Component> temp = SSTable.componentsFor(ssTable.descriptor.asTemporary(true));
        assert temp.isEmpty() : "SSTable has unexpected temp components";
    }

    private void verifyMany(SSTableReader sstable, Map<ByteBuffer, ByteBuffer> map) throws IOException
    {
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>(map.keySet());
        //Collections.shuffle(keys);
        RandomAccessReader file = sstable.openDataReader(false);
        for (ByteBuffer key : keys)
        {
            file.seek(sstable.getPosition(sstable.partitioner.decorateKey(key), SSTableReader.Operator.EQ).position);
            assert key.equals( ByteBufferUtil.readWithShortLength(file));
            int size = (int)SSTableReader.readRowSize(file, sstable.descriptor);
            byte[] bytes2 = new byte[size];
            file.readFully(bytes2);
            assert Arrays.equals(bytes2, map.get(key).array());
        }
    }
}
