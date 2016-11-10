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

import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.cassandra.db.KeyspaceTest.assertColumns;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import static org.apache.cassandra.Util.column;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;

public class MultitableTest
{
    private static final String KEYSPACE1 = "MultitableTest1";
    private static final String KEYSPACE2 = "MultitableTest2";
    private static final String CF1 = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF1));
        SchemaLoader.createKeyspace(KEYSPACE2,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE2, CF1));
    }

    @Test
    public void testSameCFs()
    {
        Keyspace keyspace1 = Keyspace.open(KEYSPACE1);
        Keyspace keyspace2 = Keyspace.open(KEYSPACE2);

        Mutation rm;
        DecoratedKey dk = Util.dk("keymulti");
        ColumnFamily cf;

        cf = ArrayBackedSortedColumns.factory.create(KEYSPACE1, "Standard1");
        cf.addColumn(column("col1", "val1", 1L));
        rm = new Mutation(KEYSPACE1, dk.getKey(), cf);
        rm.applyUnsafe();

        cf = ArrayBackedSortedColumns.factory.create(KEYSPACE2, "Standard1");
        cf.addColumn(column("col2", "val2", 1L));
        rm = new Mutation(KEYSPACE2, dk.getKey(), cf);
        rm.applyUnsafe();

        keyspace1.getColumnFamilyStore("Standard1").forceBlockingFlush();
        keyspace2.getColumnFamilyStore("Standard1").forceBlockingFlush();

        assertColumns(Util.getColumnFamily(keyspace1, dk, "Standard1"), "col1");
        assertColumns(Util.getColumnFamily(keyspace2, dk, "Standard1"), "col2");
    }
}
