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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.exceptions.ConfigurationException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogArchiver;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;

@RunWith(OrderedJUnit4ClassRunner.class)
public class RecoveryManagerTest
{
    private static Logger logger = LoggerFactory.getLogger(RecoveryManagerTest.class);

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
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    SchemaLoader.counterCFMD(KEYSPACE1, CF_COUNTER1));
        SchemaLoader.createKeyspace(KEYSPACE2,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE2, CF_STANDARD3));
    }

    @Before
    public void clearData()
    {
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1).truncateBlocking();
        Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_COUNTER1).truncateBlocking();
        Keyspace.open(KEYSPACE2).getColumnFamilyStore(CF_STANDARD3).truncateBlocking();
    }

    @Test
    public void testNothingToRecover() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
    }

    @Test
    public void testOne() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
        Keyspace keyspace1 = Keyspace.open(KEYSPACE1);
        Keyspace keyspace2 = Keyspace.open(KEYSPACE2);

        UnfilteredRowIterator upd1 = Util.apply(new RowUpdateBuilder(keyspace1.getColumnFamilyStore(CF_STANDARD1).metadata, 1L, 0, "keymulti")
            .clustering("col1").add("val", "1")
            .build());

        UnfilteredRowIterator upd2 = Util.apply(new RowUpdateBuilder(keyspace2.getColumnFamilyStore(CF_STANDARD3).metadata, 1L, 0, "keymulti")
                                       .clustering("col2").add("val", "1")
                                       .build());

        keyspace1.getColumnFamilyStore("Standard1").clearUnsafe();
        keyspace2.getColumnFamilyStore("Standard3").clearUnsafe();

        CommitLog.instance.resetUnsafe(false);

        DecoratedKey dk = Util.dk("keymulti");
        Assert.assertTrue(Util.equal(upd1, Util.getOnlyPartitionUnfiltered(Util.cmd(keyspace1.getColumnFamilyStore(CF_STANDARD1), dk).build()).unfilteredIterator()));
        Assert.assertTrue(Util.equal(upd2, Util.getOnlyPartitionUnfiltered(Util.cmd(keyspace2.getColumnFamilyStore(CF_STANDARD3), dk).build()).unfilteredIterator()));
    }

    @Test
    public void testRecoverCounter() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
        Keyspace keyspace1 = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace1.getColumnFamilyStore(CF_COUNTER1);

        for (int i = 0; i < 10; ++i)
        {
            new CounterMutation(new RowUpdateBuilder(cfs.metadata, 1L, 0, "key")
                .clustering("cc").add("val", CounterContext.instance().createLocal(1L))
                .build(), ConsistencyLevel.ALL).apply();
        }

        keyspace1.getColumnFamilyStore("Counter1").clearUnsafe();

        int replayed = CommitLog.instance.resetUnsafe(false);

        ColumnDefinition counterCol = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes("val"));
        Row row = Util.getOnlyRow(Util.cmd(cfs).includeRow("cc").columns("val").build());
        assertEquals(10L, CounterContext.instance().total(row.getCell(counterCol).value()));
    }

    @Test
    public void testRecoverPIT() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        CommitLog.instance.resetUnsafe(true);
        Date date = CommitLogArchiver.format.parse("2112:12:12 12:12:12");
        long timeMS = date.getTime() - 5000;

        Keyspace keyspace1 = Keyspace.open(KEYSPACE1);
        for (int i = 0; i < 10; ++i)
        {
            long ts = TimeUnit.MILLISECONDS.toMicros(timeMS + (i * 1000));
            new RowUpdateBuilder(cfs.metadata, ts, "name-" + i)
                .clustering("cc")
                .add("val", Integer.toString(i))
                .build()
                .apply();
        }

        // Sanity check row count prior to clear and replay
        assertEquals(10, Util.getAll(Util.cmd(cfs).build()).size());

        keyspace1.getColumnFamilyStore("Standard1").clearUnsafe();
        CommitLog.instance.resetUnsafe(false);

        assertEquals(6, Util.getAll(Util.cmd(cfs).build()).size());
    }

    @Test
    public void testRecoverPITUnordered() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        CommitLog.instance.resetUnsafe(true);
        Date date = CommitLogArchiver.format.parse("2112:12:12 12:12:12");
        long timeMS = date.getTime();

        Keyspace keyspace1 = Keyspace.open(KEYSPACE1);

        // Col 0 and 9 are the only ones to be recovered
        for (int i = 0; i < 10; ++i)
        {
            long ts;
            if (i == 9)
                ts = TimeUnit.MILLISECONDS.toMicros(timeMS - 1000);
            else
                ts = TimeUnit.MILLISECONDS.toMicros(timeMS + (i * 1000));

            new RowUpdateBuilder(cfs.metadata, ts, "name-" + i)
                .clustering("cc")
                .add("val", Integer.toString(i))
                .build()
                .apply();
        }

        // Sanity check row count prior to clear and replay
        assertEquals(10, Util.getAll(Util.cmd(cfs).build()).size());

        keyspace1.getColumnFamilyStore("Standard1").clearUnsafe();
        CommitLog.instance.resetUnsafe(false);

        assertEquals(2, Util.getAll(Util.cmd(cfs).build()).size());
    }
}
