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
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.OrderPreservingPartitioner;

public class SSTableTest extends CleanupHelper
{
    @Test
    public void testSingleWrite() throws IOException {
        File f = tempSSTableFileName();

        // write test data
        SSTableWriter writer = new SSTableWriter(f.getAbsolutePath(), 1, new OrderPreservingPartitioner());
        Random random = new Random();
        byte[] bytes = new byte[1024];
        random.nextBytes(bytes);

        String key = Integer.toString(1);
        writer.append(writer.partitioner.decorateKey(key), bytes);
        SSTableReader ssTable = writer.closeAndOpenReader(0.01);

        // verify
        verifySingle(ssTable, bytes, key);
        SSTableReader.reopenUnsafe(); // force reloading the index
        verifySingle(ssTable, bytes, key);
    }

    private File tempSSTableFileName() throws IOException
    {
        return File.createTempFile("sstable", "-" + SSTable.TEMPFILE_MARKER + "-Data.db");
    }

    private void verifySingle(SSTableReader sstable, byte[] bytes, String key) throws IOException
    {
        BufferedRandomAccessFile file = new BufferedRandomAccessFile(sstable.path, "r");
        file.seek(sstable.getPosition(sstable.partitioner.decorateKey(key)));
        assert key.equals(file.readUTF());
        int size = file.readInt();
        byte[] bytes2 = new byte[size];
        file.readFully(bytes2);
        assert Arrays.equals(bytes2, bytes);
    }

    @Test
    public void testManyWrites() throws IOException {
        File f = tempSSTableFileName();

        TreeMap<String, byte[]> map = new TreeMap<String,byte[]>();
        for ( int i = 100; i < 1000; ++i )
        {
            map.put(Integer.toString(i), ("Avinash Lakshman is a good man: " + i).getBytes());
        }

        // write
        SSTableWriter writer = new SSTableWriter(f.getAbsolutePath(), 1000, new OrderPreservingPartitioner());
        for (String key: map.navigableKeySet())
        {
            writer.append(writer.partitioner.decorateKey(key), map.get(key));
        }
        SSTableReader ssTable = writer.closeAndOpenReader(0.01);

        // verify
        verifyMany(ssTable, map);
        SSTableReader.reopenUnsafe(); // force reloading the index
        verifyMany(ssTable, map);
    }

    private void verifyMany(SSTableReader sstable, TreeMap<String, byte[]> map) throws IOException
    {
        List<String> keys = new ArrayList<String>(map.keySet());
        Collections.shuffle(keys);
        BufferedRandomAccessFile file = new BufferedRandomAccessFile(sstable.path, "r");
        for (String key : keys)
        {
            file.seek(sstable.getPosition(sstable.partitioner.decorateKey(key)));
            assert key.equals(file.readUTF());
            int size = file.readInt();
            byte[] bytes2 = new byte[size];
            file.readFully(bytes2);
            assert Arrays.equals(bytes2, map.get(key));
        }
    }
}
