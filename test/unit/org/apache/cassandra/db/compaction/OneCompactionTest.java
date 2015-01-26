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
package org.apache.cassandra.db.compaction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.Set;
import java.util.HashSet;

import org.apache.cassandra.Util;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import org.apache.cassandra.db.*;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;


public class OneCompactionTest
{
    public static final String KEYSPACE1 = "OneCompactionTest";
    private static final String CF_STANDARD1 = "Standard1";
    private static final String CF_STANDARD2 = "Standard2";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        Map<String, String> leveledOptions = new HashMap<>();
        leveledOptions.put("sstable_size_in_mb", "1");
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1).compactionStrategyOptions(leveledOptions),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2));
    }

    private void testCompaction(String columnFamilyName, int insertsPerTable)
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(columnFamilyName);

        Set<DecoratedKey> inserted = new HashSet<DecoratedKey>();
        for (int j = 0; j < insertsPerTable; j++) {
            DecoratedKey key = Util.dk(String.valueOf(j));
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            rm.add(columnFamilyName, Util.cellname("0"), ByteBufferUtil.EMPTY_BYTE_BUFFER, j);
            rm.applyUnsafe();
            inserted.add(key);
            store.forceBlockingFlush();
            assertEquals(inserted.size(), Util.getRangeSlice(store).size());
        }
        CompactionManager.instance.performMaximal(store);
        assertEquals(1, store.getSSTables().size());
    }

    @Test
    public void testCompaction1()
    {
        testCompaction("Standard1", 1);
    }

    @Test
    public void testCompaction2()
    {
        testCompaction("Standard2", 2);
    }
}
