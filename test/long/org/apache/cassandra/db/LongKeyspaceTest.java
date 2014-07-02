/**
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

package org.apache.cassandra.db;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.WrappedRunnable;
import static org.apache.cassandra.Util.column;

import org.apache.cassandra.Util;

public class LongKeyspaceTest
{
    public static final String KEYSPACE1 = "LongKeyspaceTest";
    public static final String CF_STANDARD = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD));
    }

    @Test
    public void testGetRowMultiColumn() throws Throwable
    {
        final Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final ColumnFamilyStore cfStore = keyspace.getColumnFamilyStore("Standard1");

        for (int i = 1; i < 5000; i += 100)
        {
            Mutation rm = new Mutation(KEYSPACE1, Util.dk("key" + i).getKey());
            ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE1, "Standard1");
            for (int j = 0; j < i; j++)
                cf.addColumn(column("c" + j, "v" + j, 1L));
            rm.add(cf);
            rm.applyUnsafe();
        }

        Runnable verify = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                ColumnFamily cf;
                for (int i = 1; i < 5000; i += 100)
                {
                    for (int j = 0; j < i; j++)
                    {
                        cf = cfStore.getColumnFamily(Util.namesQueryFilter(cfStore, Util.dk("key" + i), "c" + j));
                        KeyspaceTest.assertColumns(cf, "c" + j);
                    }
                }

            }
        };
        KeyspaceTest.reTest(keyspace.getColumnFamilyStore("Standard1"), verify);
    }
}
