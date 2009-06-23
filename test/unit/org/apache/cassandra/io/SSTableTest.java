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
import org.apache.cassandra.utils.BloomFilter;

public class SSTableTest extends CleanupHelper
{
    @Test
    public void testSingleWrite() throws IOException {
        File f = File.createTempFile("sstable", "");
        SSTable ssTable;

        // write test data
        ssTable = new SSTable(f.getParent(), f.getName(), new OrderPreservingPartitioner());
        BloomFilter bf = new BloomFilter(1000, 8);
        Random random = new Random();
        byte[] bytes = new byte[1024];
        random.nextBytes(bytes);

        String key = Integer.toString(1);
        ssTable.append(key, bytes);
        bf.add(key);
        ssTable.close(bf);

        // verify
        verifySingle(f, bytes, key);
        SSTable.indexMetadataMap_.clear(); // force reloading the index
        verifySingle(f, bytes, key);
    }

    private void verifySingle(File f, byte[] bytes, String key) throws IOException
    {
        SSTable ssTable = SSTable.open(f.getPath() + "-Data.db", new OrderPreservingPartitioner());
        FileStruct fs = new FileStruct(SequenceFile.bufferedReader(ssTable.dataFile_, 128 * 1024), new OrderPreservingPartitioner());
        fs.seekTo(key);
        int size = fs.getBufIn().readInt();
        byte[] bytes2 = new byte[size];
        fs.getBufIn().readFully(bytes2);
        assert Arrays.equals(bytes2, bytes);
    }

    @Test
    public void testManyWrites() throws IOException {
        File f = File.createTempFile("sstable", "");
        SSTable ssTable;

        TreeMap<String, byte[]> map = new TreeMap<String,byte[]>();
        for ( int i = 100; i < 1000; ++i )
        {
            map.put(Integer.toString(i), ("Avinash Lakshman is a good man: " + i).getBytes());
        }

        // write
        ssTable = new SSTable(f.getParent(), f.getName(), new OrderPreservingPartitioner());
        BloomFilter bf = new BloomFilter(1000, 8);
        for (String key: map.navigableKeySet())
        {
            ssTable.append(key, map.get(key));
        }
        ssTable.close(bf);

        // verify
        verifyMany(f, map);
        SSTable.indexMetadataMap_.clear(); // force reloading the index
        verifyMany(f, map);
    }

    private void verifyMany(File f, TreeMap<String, byte[]> map) throws IOException
    {
        List<String> keys = new ArrayList(map.keySet());
        Collections.shuffle(keys);
        SSTable ssTable = SSTable.open(f.getPath() + "-Data.db", new OrderPreservingPartitioner());
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
