package org.apache.cassandra.db;
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
import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.ByteBufferUtil;

public class LongFlushMemtableTest extends SchemaLoader
{
    @Test
    public void testFlushMemtables() throws IOException, ConfigurationException
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        for (int i = 0; i < 100; i++)
        {
            CFMetaData metadata = CFMetaData.denseCFMetaData(keyspace.getName(), "_CF" + i, UTF8Type.instance);
            MigrationManager.announceNewColumnFamily(metadata);
        }

        for (int j = 0; j < 200; j++)
        {
            for (int i = 0; i < 100; i++)
            {
                Mutation rm = new Mutation("Keyspace1", ByteBufferUtil.bytes("key" + j));
                ColumnFamily cf = ArrayBackedSortedColumns.factory.create("Keyspace1", "_CF" + i);
                // don't cheat by allocating this outside of the loop; that defeats the purpose of deliberately using lots of memory
                ByteBuffer value = ByteBuffer.allocate(100000);
                cf.addColumn(new BufferCell(Util.cellname("c"), value));
                rm.add(cf);
                rm.applyUnsafe();
            }
        }

        int flushes = 0;
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            if (cfs.name.startsWith("_CF"))
                flushes += cfs.getMemtableSwitchCount();
        }
        assert flushes > 0;
    }
}

