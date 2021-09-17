/*
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

package org.apache.cassandra.service;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.Tables;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.UnsupportedEncodingException;

import static org.apache.cassandra.cql3.QueryProcessor.process;

public class PartitionDenylistTest
{
    final static String ks_cql = "partition_denylist_keyspace";

    @BeforeClass
    public static void init() throws ConfigurationException, RequestExecutionException
    {
        CQLTester.prepareServer();

        KeyspaceMetadata schema = KeyspaceMetadata.create(ks_cql, KeyspaceParams.simple(1), Tables.of(
        CreateTableStatement.parse("CREATE TABLE foofoo ("
                                   + "bar text, "
                                   + "baz text, "
                                   + "qux text, "
                                   + "quz text, "
                                   + "foo text, "
                                   + "PRIMARY KEY((bar, baz), qux, quz) ) ", ks_cql)
                            .build()
        ));
        Schema.instance.load(schema);
        DatabaseDescriptor.setEnablePartitionDenylist(true);
        DatabaseDescriptor.setEnableDenylistRangeReads(true);
        DatabaseDescriptor.setDenylistConsistencyLevel(ConsistencyLevel.ONE);
        DatabaseDescriptor.setdenyDenylistRefreshPeriodSeconds(1);
        StorageService.instance.initServer(0);
    }

    @Before
    public void setup() throws RequestExecutionException, UnsupportedEncodingException, InterruptedException {
        DatabaseDescriptor.setEnablePartitionDenylist(true);
        process("TRUNCATE system_distributed.partition_denylist", ConsistencyLevel.ONE);
        forceReloadDenylist();

        process("INSERT INTO " + ks_cql + ".foofoo (bar, baz, qux, quz, foo) VALUES ('aaa', 'bbb', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("INSERT INTO " + ks_cql + ".foofoo (bar, baz, qux, quz, foo) VALUES ('bbb', 'ccc', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("INSERT INTO " + ks_cql + ".foofoo (bar, baz, qux, quz, foo) VALUES ('ccc', 'ddd', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("INSERT INTO " + ks_cql + ".foofoo (bar, baz, qux, quz, foo) VALUES ('ddd', 'eee', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);

        denylist("" + ks_cql + "", "foofoo", "bbb:ccc");
        forceReloadDenylist();
    }


    private static void denylist(final String ks, final String cf, final String key) throws RequestExecutionException
    {
        StorageProxy.instance.denylistKey(ks, cf, key);
    }

    @Test
    public void testRead() throws RequestExecutionException
    {
        process("SELECT * FROM " + ks_cql + ".foofoo WHERE bar='aaa' and baz='bbb'", ConsistencyLevel.ONE);
    }

    @Test(expected = InvalidRequestException.class)
    public void testReadDenylisted() throws Throwable
    {
        process("SELECT * FROM " + ks_cql + ".foofoo WHERE bar='bbb' and baz='ccc'", ConsistencyLevel.ONE);
    }

    @Test
    public void testReadUndenylisted() throws RequestExecutionException, InterruptedException, UnsupportedEncodingException
    {
        process("TRUNCATE system_distributed.partition_denylist", ConsistencyLevel.ONE);
        forceReloadDenylist();
        process("SELECT * FROM " + ks_cql + ".foofoo WHERE bar='bbb' and baz='ccc'", ConsistencyLevel.ONE);
    }

    @Test
    public void testWrite() throws RequestExecutionException
    {
        process("INSERT INTO " + ks_cql + ".foofoo (bar, baz, qux, quz, foo) VALUES ('eee', 'fff', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("DELETE FROM " + ks_cql + ".foofoo WHERE bar='eee' and baz='fff'", ConsistencyLevel.ONE);
    }

    @Test(expected = InvalidRequestException.class)
    public void testWriteDenylisted() throws Throwable
    {
        process("INSERT INTO " + ks_cql + ".foofoo (bar, baz, qux, quz, foo) VALUES ('bbb', 'ccc', 'eee', 'fff', 'w')", ConsistencyLevel.ONE);
    }

    @Test(expected = InvalidRequestException.class)
    public void testCASWriteDenylisted() throws Throwable
    {
        process("UPDATE " + ks_cql + ".foofoo SET foo='w' WHERE bar='bbb' AND baz='ccc' AND qux='eee' AND quz='fff' IF foo='v'", ConsistencyLevel.LOCAL_SERIAL);
    }

    @Test
    public void testWriteUndenylisted() throws RequestExecutionException, InterruptedException, UnsupportedEncodingException
    {
        process("TRUNCATE system_distributed.partition_denylist", ConsistencyLevel.ONE);
        forceReloadDenylist();
        process("INSERT INTO " + ks_cql + ".foofoo (bar, baz, qux, quz, foo) VALUES ('bbb', 'ccc', 'eee', 'fff', 'w')", ConsistencyLevel.ONE);
    }

    @Test
    public void testRangeSlice() throws RequestExecutionException
    {
        UntypedResultSet rows;
        rows = process("SELECT * FROM " + ks_cql + ".foofoo WHERE token(bar, baz) < token('bbb', 'ccc')", ConsistencyLevel.ONE);
        Assert.assertEquals(1, rows.size());

        rows = process("SELECT * FROM " + ks_cql + ".foofoo WHERE token(bar, baz) > token('bbb', 'ccc')", ConsistencyLevel.ONE);
        Assert.assertEquals(2, rows.size());

        rows = process("SELECT * FROM " + ks_cql + ".foofoo WHERE token(bar, baz) >= token('aaa', 'bbb') and token(bar, baz) < token('bbb', 'ccc')", ConsistencyLevel.ONE);
        Assert.assertEquals(1, rows.size());

        rows = process("SELECT * FROM " + ks_cql + ".foofoo WHERE token(bar, baz) > token('bbb', 'ccc') and token(bar, baz) <= token('ddd', 'eee')", ConsistencyLevel.ONE);
        Assert.assertEquals(2, rows.size());
    }

    @Test(expected = InvalidRequestException.class)
    public void testRangeDenylisted() throws Throwable
    {
        process("SELECT * FROM " + ks_cql + ".foofoo", ConsistencyLevel.ONE);
    }

    @Test(expected = InvalidRequestException.class)
    public void testRangeDenylisted2() throws Throwable
    {
        process("SELECT * FROM " + ks_cql + ".foofoo WHERE token(bar, baz) >= token('aaa', 'bbb') and token (bar, baz) <= token('bbb', 'ccc')", ConsistencyLevel.ONE);
    }

    @Test(expected = InvalidRequestException.class)
    public void testRangeDenylisted3() throws Throwable
    {
        process("SELECT * FROM " + ks_cql + ".foofoo WHERE token(bar, baz) >= token('bbb', 'ccc') and token (bar, baz) <= token('ccc', 'ddd')", ConsistencyLevel.ONE);
    }

    @Test(expected = InvalidRequestException.class)
    public void testRangeDenylisted4() throws Throwable
    {
        process("SELECT * FROM " + ks_cql + ".foofoo WHERE token(bar, baz) > token('aaa', 'bbb') and token (bar, baz) < token('ccc', 'ddd')", ConsistencyLevel.ONE);
    }

    @Test(expected = InvalidRequestException.class)
    public void testRangeDenylisted5() throws Throwable
    {
        process("SELECT * FROM " + ks_cql + ".foofoo WHERE token(bar, baz) > token('aaa', 'bbb')", ConsistencyLevel.ONE);
    }

    @Test(expected = InvalidRequestException.class)
    public void testRangeDenylisted6() throws Throwable
    {
        process("SELECT * FROM " + ks_cql + ".foofoo WHERE token(bar, baz) < token('ddd', 'eee')", ConsistencyLevel.ONE);
    }

    @Test
    public void testReadInvalidCF() throws Exception
    {
        denylist("santa", "claus", "hohoho");
        forceReloadDenylist();
    }

    @Test
    public void testDenylistDisabled() throws Exception
    {
        DatabaseDescriptor.setEnablePartitionDenylist(false);
        process("INSERT INTO " + ks_cql + ".foofoo (bar, baz, qux, quz, foo) VALUES ('bbb', 'ccc', 'eee', 'fff', 'w')", ConsistencyLevel.ONE);
        process("SELECT * FROM " + ks_cql + ".foofoo WHERE bar='bbb' and baz='ccc'", ConsistencyLevel.ONE);
        process("SELECT * FROM " + ks_cql + ".foofoo", ConsistencyLevel.ONE);
    }

    private static void forceReloadDenylist() throws InterruptedException
    {
        StorageProxy.instance.loadPartitionDenylist();
    }
}