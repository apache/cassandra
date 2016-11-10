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

import static org.junit.Assert.assertEquals;
import static org.apache.cassandra.Util.addMutation;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.junit.BeforeClass;
import org.junit.Test;

public class NameSortTest
{
    private static final String KEYSPACE1 = "NameSortTest";
    private static final String CF = "Standard1";
    private static final String CFSUPER = "Super1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF),
                                    SchemaLoader.superCFMD(KEYSPACE1, CFSUPER, LongType.instance));
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

        for (int i = 0; i < N; ++i)
        {
            ByteBuffer key = ByteBufferUtil.bytes(Integer.toString(i));
            Mutation rm;

            // standard
            for (int j = 0; j < 8; ++j)
            {
                ByteBuffer bytes = j % 2 == 0 ? ByteBufferUtil.bytes("a") : ByteBufferUtil.bytes("b");
                rm = new Mutation(KEYSPACE1, key);
                rm.add("Standard1", Util.cellname("Cell-" + j), bytes, j);
                rm.applyUnsafe();
            }

            // super
            for (int j = 0; j < 8; ++j)
            {
                rm = new Mutation(KEYSPACE1, key);
                for (int k = 0; k < 4; ++k)
                {
                    String value = (j + k) % 2 == 0 ? "a" : "b";
                    addMutation(rm, CFSUPER, "SuperColumn-" + j, k, value, k);
                }
                rm.applyUnsafe();
            }
        }

        validateNameSort(keyspace, N);

        keyspace.getColumnFamilyStore("Standard1").forceBlockingFlush();
        keyspace.getColumnFamilyStore(CFSUPER).forceBlockingFlush();
        validateNameSort(keyspace, N);
    }

    private void validateNameSort(Keyspace keyspace, int N) throws IOException
    {
        for (int i = 0; i < N; ++i)
        {
            DecoratedKey key = Util.dk(Integer.toString(i));
            ColumnFamily cf;

            cf = Util.getColumnFamily(keyspace, key, "Standard1");
            Collection<Cell> cells = cf.getSortedColumns();
            for (Cell cell : cells)
            {
                String name = ByteBufferUtil.string(cell.name().toByteBuffer());
                int j = Integer.valueOf(name.substring(name.length() - 1));
                byte[] bytes = j % 2 == 0 ? "a".getBytes() : "b".getBytes();
                assertEquals(new String(bytes), ByteBufferUtil.string(cell.value()));
            }
        }
    }
}
