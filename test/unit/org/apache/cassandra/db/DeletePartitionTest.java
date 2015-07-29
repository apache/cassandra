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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

public class DeletePartitionTest
{
    private static final String KEYSPACE1 = "RemoveColumnFamilyTest";
    private static final String CF_STANDARD1 = "Standard1";

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @Test
    public void testDeletePartition()
    {
        testDeletePartition(Util.dk("key1"), true, true);
        testDeletePartition(Util.dk("key2"), true, false);
        testDeletePartition(Util.dk("key3"), false, true);
        testDeletePartition(Util.dk("key4"), false, false);
    }

    public void testDeletePartition(DecoratedKey key, boolean flushBeforeRemove, boolean flushAfterRemove)
    {
        ColumnFamilyStore store = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        ColumnDefinition column = store.metadata.getColumnDefinition(ByteBufferUtil.bytes("val"));

        // write
        new RowUpdateBuilder(store.metadata, 0, key.getKey())
                .clustering("Column1")
                .add("val", "asdf")
                .build()
                .applyUnsafe();

        // validate that data's written
        FilteredPartition partition = Util.getOnlyPartition(Util.cmd(store, key).build());
        assertTrue(partition.rowCount() > 0);
        Row r = partition.iterator().next();
        assertTrue(r.getCell(column).value().equals(ByteBufferUtil.bytes("asdf")));

        if (flushBeforeRemove)
            store.forceBlockingFlush();

        // delete the partition
        new Mutation(KEYSPACE1, key)
                .add(PartitionUpdate.fullPartitionDelete(store.metadata, key, 0, FBUtilities.nowInSeconds()))
                .applyUnsafe();

        if (flushAfterRemove)
            store.forceBlockingFlush();

        // validate removal
        ImmutableBTreePartition partitionUnfiltered = Util.getOnlyPartitionUnfiltered(Util.cmd(store, key).build());
        assertFalse(partitionUnfiltered.partitionLevelDeletion().isLive());
        assertFalse(partitionUnfiltered.iterator().hasNext());
    }
}
