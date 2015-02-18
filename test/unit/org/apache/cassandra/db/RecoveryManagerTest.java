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
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogArchiver;

import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.db.KeyspaceTest.assertColumns;
import static org.apache.cassandra.Util.cellname;

@RunWith(OrderedJUnit4ClassRunner.class)
public class RecoveryManagerTest
{
    private static final String KEYSPACE1 = "RecoveryManagerTest1";
    private static final String CF_STANDARD1 = "Standard1";
    private static final String CF_COUNTER1 = "Counter1";

    private static final String KEYSPACE2 = "RecoveryManagerTest2";
    private static final String CF_STANDARD3 = "Standard3";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    CFMetaData.denseCFMetaData(KEYSPACE1, CF_COUNTER1, BytesType.instance).defaultValidator(CounterColumnType.instance));
        SchemaLoader.createKeyspace(KEYSPACE2,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE2, CF_STANDARD3));
    }

    @Test
    public void testNothingToRecover() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
        CommitLog.instance.recover();
    }

    @Test
    public void testOne() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
        Keyspace keyspace1 = Keyspace.open(KEYSPACE1);
        Keyspace keyspace2 = Keyspace.open(KEYSPACE2);

        Mutation rm;
        DecoratedKey dk = Util.dk("keymulti");
        ColumnFamily cf;

        cf = ArrayBackedSortedColumns.factory.create(KEYSPACE1, "Standard1");
        cf.addColumn(column("col1", "val1", 1L));
        rm = new Mutation(KEYSPACE1, dk.getKey(), cf);
        rm.apply();

        cf = ArrayBackedSortedColumns.factory.create(KEYSPACE2, "Standard3");
        cf.addColumn(column("col2", "val2", 1L));
        rm = new Mutation(KEYSPACE2, dk.getKey(), cf);
        rm.apply();

        keyspace1.getColumnFamilyStore("Standard1").clearUnsafe();
        keyspace2.getColumnFamilyStore("Standard3").clearUnsafe();

        CommitLog.instance.resetUnsafe(false); // disassociate segments from live CL
        CommitLog.instance.recover();

        assertColumns(Util.getColumnFamily(keyspace1, dk, "Standard1"), "col1");
        assertColumns(Util.getColumnFamily(keyspace2, dk, "Standard3"), "col2");
    }

    @Test
    public void testRecoverCounter() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
        Keyspace keyspace1 = Keyspace.open(KEYSPACE1);

        Mutation rm;
        DecoratedKey dk = Util.dk("key");
        ColumnFamily cf;

        for (int i = 0; i < 10; ++i)
        {
            cf = ArrayBackedSortedColumns.factory.create(KEYSPACE1, "Counter1");
            cf.addColumn(BufferCounterCell.createLocal(cellname("col"), 1L, 1L, Long.MIN_VALUE));
            rm = new Mutation(KEYSPACE1, dk.getKey(), cf);
            rm.apply();
        }

        keyspace1.getColumnFamilyStore("Counter1").clearUnsafe();

        CommitLog.instance.resetUnsafe(false); // disassociate segments from live CL
        CommitLog.instance.recover();

        cf = Util.getColumnFamily(keyspace1, dk, "Counter1");

        assert cf.getColumnCount() == 1;
        Cell c = cf.getColumn(cellname("col"));

        assert c != null;
        assert ((CounterCell)c).total() == 10L;
    }

    @Test
    public void testRecoverPIT() throws Exception
    {
        CommitLog.instance.resetUnsafe(true);
        Date date = CommitLogArchiver.format.parse("2112:12:12 12:12:12");
        long timeMS = date.getTime() - 5000;

        Keyspace keyspace1 = Keyspace.open(KEYSPACE1);
        DecoratedKey dk = Util.dk("dkey");
        for (int i = 0; i < 10; ++i)
        {
            long ts = TimeUnit.MILLISECONDS.toMicros(timeMS + (i * 1000));
            ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE1, "Standard1");
            cf.addColumn(column("name-" + i, "value", ts));
            Mutation rm = new Mutation(KEYSPACE1, dk.getKey(), cf);
            rm.apply();
        }
        keyspace1.getColumnFamilyStore("Standard1").clearUnsafe();
        CommitLog.instance.resetUnsafe(false); // disassociate segments from live CL
        CommitLog.instance.recover();

        ColumnFamily cf = Util.getColumnFamily(keyspace1, dk, "Standard1");
        Assert.assertEquals(6, cf.getColumnCount());
    }


    @Test
    public void testRecoverPITUnordered() throws Exception
    {
        CommitLog.instance.resetUnsafe(true);
        Date date = CommitLogArchiver.format.parse("2112:12:12 12:12:12");
        long timeMS = date.getTime();

        Keyspace keyspace1 = Keyspace.open(KEYSPACE1);
        DecoratedKey dk = Util.dk("dkey");

        // Col 0 and 9 are the only ones to be recovered
        for (int i = 0; i < 10; ++i)
        {
            long ts;
            if(i==9)
                ts = TimeUnit.MILLISECONDS.toMicros(timeMS - 1000);
            else
                ts = TimeUnit.MILLISECONDS.toMicros(timeMS + (i * 1000));

            ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE1, "Standard1");
            cf.addColumn(column("name-" + i, "value", ts));
            Mutation rm = new Mutation(KEYSPACE1, dk.getKey(), cf);
            rm.apply();
        }

        ColumnFamily cf = Util.getColumnFamily(keyspace1, dk, "Standard1");
        Assert.assertEquals(10, cf.getColumnCount());

        keyspace1.getColumnFamilyStore("Standard1").clearUnsafe();
        CommitLog.instance.resetUnsafe(false); // disassociate segments from live CL
        CommitLog.instance.recover();

        cf = Util.getColumnFamily(keyspace1, dk, "Standard1");
        Assert.assertEquals(2, cf.getColumnCount());
    }
}
