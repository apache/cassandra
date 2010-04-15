package org.apache.cassandra.io;
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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.service.StorageService;


public class SSTableReaderTest extends CleanupHelper
{
    @Test
    public void testSpannedIndexPositions() throws IOException, ExecutionException, InterruptedException
    {
        SSTableReader.BUFFER_SIZE = 40;

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");

        // insert a bunch of data
        CompactionManager.instance.disableAutoCompaction();
        for (int j = 0; j < 100; j += 2)
        {
            String key = String.valueOf(j);
            RowMutation rm = new RowMutation("Keyspace1", key);
            rm.add(new QueryPath("Standard1", null, "0".getBytes()), new byte[0], j);
            rm.apply();
        }
        store.forceBlockingFlush();
        CompactionManager.instance.submitMajor(store).get();

        // check that all our keys are found correctly
        SSTableReader sstable = store.getSSTables().iterator().next();
        for (int j = 0; j < 100; j += 2)
        {
            String key = String.valueOf(j);
            DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);
            FileDataInput file = sstable.getFileDataInput(dk, DatabaseDescriptor.getIndexedReadBufferSizeInKB() * 1024);
            DecoratedKey keyInDisk = sstable.getPartitioner().convertFromDiskFormat(file.readUTF());
            assert keyInDisk.equals(dk) : String.format("%s != %s in %s", keyInDisk, dk, file.getPath());
        }

        // check no false positives
        for (int j = 1; j < 110; j += 2)
        {
            String key = String.valueOf(j);
            DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);
            assert sstable.getPosition(dk) == null;
        }
    }
}
