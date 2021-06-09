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
package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;

public class ReducingKeyIteratorTest
{
    public static final String KEYSPACE1 = "ReducingKeyIteratorTest";
    public static final String CF_STANDARD = "Standard1";

    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.prepareServer();
        CompactionManager.instance.disableAutoCompaction();

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD));
    }

    @After
    public void afterTest() throws Exception
    {
        ColumnFamilyStore store = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD);
        store.truncateBlocking();
    }

    @Test
    public void testTotalAndReadBytesOneSSTable() throws IOException
    {
        testTotalAndReadBytes(1, 1000);
    }

    @Test
    public void testTotalAndReadBytesManySSTables() throws IOException
    {
        testTotalAndReadBytes(10, 100);
    }

    public void testTotalAndReadBytes(int tableCount, int rowCount) throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD);
        LoggerFactory.getLogger(getClass()).info("Compression {}", store.metadata().params.compression.asMap());

        for (int t = 0; t < tableCount; ++t)
        {
            for (int i = 0; i < rowCount; i++)
            {
                new RowUpdateBuilder(store.metadata(), i, String.valueOf(i))
                .clustering("0")
                .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                .build()
                .applyUnsafe();
            }
            store.forceBlockingFlush(UNIT_TESTS);
        }

        Set<SSTableReader> sstables = store.getLiveSSTables();
        ReducingKeyIterator reducingIterator = new ReducingKeyIterator(sstables);

        while (reducingIterator.hasNext())
        {
            Assert.assertTrue(reducingIterator.getTotalBytes() >= reducingIterator.getBytesRead());
            reducingIterator.next();
        }
        Assert.assertEquals(reducingIterator.getTotalBytes(), reducingIterator.getBytesRead());
    }
}
