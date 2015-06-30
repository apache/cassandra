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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;

public class NameSortTest
{
    private static final String KEYSPACE1 = "NameSortTest";
    private static final String CF = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF, 1000, AsciiType.instance));
    }

    @Test
    public void testNameSort1() throws IOException
    {
        // single key
        testNameSort(1);
    }

    @Test
    public void testNameSort10() throws IOException
    {
        // multiple keys, flushing concurrently w/ inserts
        testNameSort(10);
    }

    @Test
    public void testNameSort100() throws IOException
    {
        // enough keys to force compaction concurrently w/ inserts
        testNameSort(100);
    }

    private void testNameSort(int N) throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        for (int i = 0; i < N; i++)
        {
            ByteBuffer key = ByteBufferUtil.bytes(Integer.toString(i));
            RowUpdateBuilder rub = new RowUpdateBuilder(cfs.metadata, 0, key);
            rub.clustering("cc");
            for (int j = 0; j < 8; j++)
                rub.add("val" + j, j % 2 == 0 ? "a" : "b");
            rub.build().applyUnsafe();
        }
        validateNameSort(cfs);
        keyspace.getColumnFamilyStore("Standard1").forceBlockingFlush();
        validateNameSort(cfs);
    }

    private void validateNameSort(ColumnFamilyStore cfs) throws IOException
    {
        for (FilteredPartition partition : Util.getAll(Util.cmd(cfs).build()))
        {
            for (Row r : partition)
            {
                for (ColumnDefinition cd : r.columns())
                {
                    if (r.getCell(cd) == null)
                        continue;
                    int cellVal = Integer.valueOf(cd.name.toString().substring(cd.name.toString().length() - 1));
                    String expected = cellVal % 2 == 0 ? "a" : "b";
                    assertEquals(expected, ByteBufferUtil.string(r.getCell(cd).value()));
                }
            }
        }
    }
}
