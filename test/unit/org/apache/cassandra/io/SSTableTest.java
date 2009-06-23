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
package org.apache.cassandra.io;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.FileStruct;
import org.apache.cassandra.dht.OrderPreservingPartitioner;

public class SSTableTest extends CleanupHelper
{
    @Test
    public void testSingleWrite() throws IOException {
        File f = File.createTempFile("sstable", "-" + SSTable.temporaryFile_);

        // write test data
        SSTable ssTable = new SSTable(f.getParent(), f.getName(), 1, new OrderPreservingPartitioner());
        Random random = new Random();
        byte[] bytes = new byte[1024];
        random.nextBytes(bytes);

        String key = Integer.toString(1);
        ssTable.append(key, bytes);
        ssTable.close();

        // verify
        verifySingle(ssTable.dataFile_, bytes, key);
        SSTable.reopenUnsafe(); // force reloading the index
        verifySingle(ssTable.dataFile_, bytes, key);
    }

    private void verifySingle(String filename, byte[] bytes, String key) throws IOException
    {
        SSTable ssTable = SSTable.open(filename, new OrderPreservingPartitioner());
        FileStruct fs = new FileStruct(SequenceFile.bufferedReader(ssTable.dataFile_, 128 * 1024), new OrderPreservingPartitioner());
        fs.seekTo(key);
        int size = fs.getBufIn().readInt();
        byte[] bytes2 = new byte[size];
        fs.getBufIn().readFully(bytes2);
        assert Arrays.equals(bytes2, bytes);
    }

    @Test
    public void testManyWrites() throws IOException {
        File f = File.createTempFile("sstable", "-" + SSTable.temporaryFile_);

        TreeMap<String, byte[]> map = new TreeMap<String,byte[]>();
        for ( int i = 100; i < 1000; ++i )
        {
            map.put(Integer.toString(i), ("Avinash Lakshman is a good man: " + i).getBytes());
        }

        // write
        SSTable ssTable = new SSTable(f.getParent(), f.getName(), 1000, new OrderPreservingPartitioner());
        for (String key: map.navigableKeySet())
        {
            ssTable.append(key, map.get(key));
        }
        ssTable.close();

        // verify
        verifyMany(ssTable.dataFile_, map);
        SSTable.reopenUnsafe(); // force reloading the index
        verifyMany(ssTable.dataFile_, map);
    }

    private void verifyMany(String filename, TreeMap<String, byte[]> map) throws IOException
    {
        List<String> keys = new ArrayList(map.keySet());
        Collections.shuffle(keys);
        SSTable ssTable = SSTable.open(filename, new OrderPreservingPartitioner());
        FileStruct fs = new FileStruct(SequenceFile.bufferedReader(ssTable.dataFile_, 128 * 1024), new OrderPreservingPartitioner());
        for (String key : keys)
        {
            fs.seekTo(key);
            int size = fs.getBufIn().readInt();
            byte[] bytes2 = new byte[size];
            fs.getBufIn().readFully(bytes2);
            assert Arrays.equals(bytes2, map.get(key));
        }
    }
}
