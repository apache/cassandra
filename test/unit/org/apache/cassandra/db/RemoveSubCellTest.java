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

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;

import static org.apache.cassandra.Util.getBytes;
import org.apache.cassandra.Util;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.google.common.util.concurrent.Uninterruptibles;


public class RemoveSubCellTest
{
    private static final String KEYSPACE1 = "RemoveSubCellTest";
    private static final String CF_SUPER1 = "Super1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.superCFMD(KEYSPACE1, CF_SUPER1, LongType.instance));
    }

    @Test
    public void testRemoveSubColumn()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Super1");
        Mutation rm;
        DecoratedKey dk = Util.dk("key1");

        // add data
        rm = new Mutation(KEYSPACE1, dk.getKey());
        Util.addMutation(rm, "Super1", "SC1", 1, "asdf", 0);
        rm.applyUnsafe();
        store.forceBlockingFlush();

        CellName cname = CellNames.compositeDense(ByteBufferUtil.bytes("SC1"), getBytes(1L));
        // remove
        rm = new Mutation(KEYSPACE1, dk.getKey());
        rm.delete("Super1", cname, 1);
        rm.applyUnsafe();

        ColumnFamily retrieved = store.getColumnFamily(QueryFilter.getIdentityFilter(dk, "Super1", System.currentTimeMillis()));
        assertFalse(retrieved.getColumn(cname).isLive());
        assertNull(Util.cloneAndRemoveDeleted(retrieved, Integer.MAX_VALUE));
    }

    @Test
    public void testRemoveSubColumnAndContainer()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Super1");
        Mutation rm;
        DecoratedKey dk = Util.dk("key2");

        // add data
        rm = new Mutation(KEYSPACE1, dk.getKey());
        Util.addMutation(rm, "Super1", "SC1", 1, "asdf", 0);
        rm.applyUnsafe();
        store.forceBlockingFlush();

        // remove the SC
        ByteBuffer scName = ByteBufferUtil.bytes("SC1");
        CellName cname = CellNames.compositeDense(scName, getBytes(1L));
        rm = new Mutation(KEYSPACE1, dk.getKey());
        rm.deleteRange("Super1", SuperColumns.startOf(scName), SuperColumns.endOf(scName), 1);
        rm.applyUnsafe();

        // Mark current time and make sure the next insert happens at least
        // one second after the previous one (since gc resolution is the second)
        QueryFilter filter = QueryFilter.getIdentityFilter(dk, "Super1", System.currentTimeMillis());
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        // remove the column itself
        rm = new Mutation(KEYSPACE1, dk.getKey());
        rm.delete("Super1", cname, 2);
        rm.applyUnsafe();

        ColumnFamily retrieved = store.getColumnFamily(filter);
        assertFalse(retrieved.getColumn(cname).isLive());
        assertNull(Util.cloneAndRemoveDeleted(retrieved, Integer.MAX_VALUE));
    }
}
