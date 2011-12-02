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

import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SSTableTest extends CleanupHelper
{
    @Test
    public void testSingleWrite() throws IOException {
        // write test data
        ByteBuffer key = ByteBufferUtil.bytes(Integer.toString(1));
        ByteBuffer bytes = ByteBuffer.wrap(new byte[1024]);
        new Random().nextBytes(bytes.array());

        Map<ByteBuffer, ByteBuffer> map = new HashMap<ByteBuffer,ByteBuffer>();
        map.put(key, bytes);
        SSTableReader ssTable = SSTableUtils.prepare().cf("Standard1").writeRaw(map);

        // verify
        verifySingle(ssTable, bytes, key);
        ssTable = SSTableReader.open(ssTable.descriptor); // read the index from disk
        verifySingle(ssTable, bytes, key);
    }

    private void verifySingle(SSTableReader sstable, ByteBuffer bytes, ByteBuffer key) throws IOException
    {
        RandomAccessReader file = sstable.openDataReader(false);
        file.seek(sstable.getPosition(sstable.partitioner.decorateKey(key), SSTableReader.Operator.EQ));
        assert key.equals(ByteBufferUtil.readWithShortLength(file));
        int size = (int)SSTableReader.readRowSize(file, sstable.descriptor);
        byte[] bytes2 = new byte[size];
        file.readFully(bytes2);
        assert ByteBuffer.wrap(bytes2).equals(bytes);
    }

    @Test
    public void testManyWrites() throws IOException {
        Map<ByteBuffer, ByteBuffer> map = new HashMap<ByteBuffer,ByteBuffer>();
        for (int i = 100; i < 1000; ++i)
        {
            map.put(ByteBufferUtil.bytes(Integer.toString(i)), ByteBufferUtil.bytes(("Avinash Lakshman is a good man: " + i)));
        }

        // write
        SSTableReader ssTable = SSTableUtils.prepare().cf("Standard2").writeRaw(map);

        // verify
        verifyMany(ssTable, map);
        ssTable = SSTableReader.open(ssTable.descriptor); // read the index from disk
        verifyMany(ssTable, map);

        Set<Component> live = SSTable.componentsFor(ssTable.descriptor);
        assert !live.isEmpty() : "SSTable has no live components";
        Set<Component> temp = SSTable.componentsFor(ssTable.descriptor.asTemporary(true));
        assert temp.isEmpty() : "SSTable has unexpected temp components";
    }

    private void verifyMany(SSTableReader sstable, Map<ByteBuffer, ByteBuffer> map) throws IOException
    {
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>(map.keySet());
        Collections.shuffle(keys);
        RandomAccessReader file = sstable.openDataReader(false);
        for (ByteBuffer key : keys)
        {
            file.seek(sstable.getPosition(sstable.partitioner.decorateKey(key), SSTableReader.Operator.EQ));
            assert key.equals( ByteBufferUtil.readWithShortLength(file));
            int size = (int)SSTableReader.readRowSize(file, sstable.descriptor);
            byte[] bytes2 = new byte[size];
            file.readFully(bytes2);
            assert Arrays.equals(bytes2, map.get(key).array());
        }
    }
}
