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
        DatabaseDescriptor.setDenylistRefreshSeconds(1);
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
        process("INSERT INTO " + ks_cql + ".foofoo (bar, baz, qux, quz, foo) VALUES ('eee', 'fff', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("INSERT INTO " + ks_cql + ".foofoo (bar, baz, qux, quz, foo) VALUES ('fff', 'ggg', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("INSERT INTO " + ks_cql + ".foofoo (bar, baz, qux, quz, foo) VALUES ('ggg', 'hhh', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("INSERT INTO " + ks_cql + ".foofoo (bar, baz, qux, quz, foo) VALUES ('hhh', 'iii', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("INSERT INTO " + ks_cql + ".foofoo (bar, baz, qux, quz, foo) VALUES ('iii', 'jjj', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("INSERT INTO " + ks_cql + ".foofoo (bar, baz, qux, quz, foo) VALUES ('jjj', 'kkk', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);

        denylist("" + ks_cql + "", "foofoo", "bbb:ccc");
        forceReloadDenylist();
    }


    private static void denylist(final String ks, final String table, final String key) throws RequestExecutionException
    {
        StorageProxy.instance.denylistKey(ks, table, key);
    }

    /**
     * @return Whether the *attempt* to remove the denylisted key and refresh succeeded. Doesn't necessarily indicate the key
     * was previously blocked and found.
     */
    private static boolean removeDenylist(final String ks, final String table, final String key) throws RequestExecutionException
    {
        return StorageProxy.instance.removeDenylistKey(ks, table, key);
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

    /**
     * Want to make sure we don't throw anything or explode when people try to remove a key that's not there
     */
    @Test
    public void testRemoveMissingIsGraceful() throws InterruptedException
    {
        confirmDenied("bbb", "ccc");
        Assert.assertTrue(removeDenylist("" + ks_cql + "", "foofoo", "bbb:ccc"));

        // We expect this to silently not find and succeed at *trying* to remove it
        Assert.assertTrue(removeDenylist("" + ks_cql + "", "foofoo", "bbb:ccc"));

        forceReloadDenylist();
        confirmAllowed("bbb", "ccc");
    }

    /**
     * We need to confirm that the entire cache is reloaded rather than being an additive change; we don't want keys to
     * persist after their removal and reload from CQL.
     */
    @Test
    public void testRemoveWorksOnReload() throws InterruptedException
    {
        denyAllKeys();

        confirmDenied("aaa", "bbb");
        confirmDenied("eee", "fff");
        confirmDenied("iii", "jjj");

        // poke a hole in the middle and reload
        removeDenylist("" + ks_cql + "", "foofoo", "eee:fff");
        forceReloadDenylist();

        confirmAllowed("eee", "fff");
        confirmDenied("aaa", "bbb");
        confirmDenied("iii", "jjj");
    }

    /**
     * We go through a few steps here:
     *  1) Add more keys than we're allowed
     *  2) Confirm that the overflow keys are *not* denied
     *  3) Raise the allowable limit
     *  4) Confirm that the overflow keys are now denied (and no longer really "overflow" for that matter)
     */
    @Test
    public void testShrinkAndGrow() throws InterruptedException
    {
        StorageProxy.instance.setMaxDenylistKeysPerTable(5);
        StorageProxy.instance.setMaxDenylistKeysTotal(5);
        denyAllKeys();

        // Confirm overflowed keys are allowed; first come first served
        confirmDenied("aaa", "bbb");
        confirmAllowed("iii", "jjj");

        // Now we raise the limit back up and do nothing else and confirm it's blocked
        StorageProxy.instance.setMaxDenylistKeysPerTable(1000);
        StorageProxy.instance.setMaxDenylistKeysTotal(1000);
        confirmDenied("aaa", "bbb");
        confirmDenied("iii", "jjj");

        StorageProxy.instance.setMaxDenylistKeysPerTable(5);
        StorageProxy.instance.setMaxDenylistKeysTotal(5);
        confirmAllowed("iii", "jjj");

        // Now, we remove the denylist entries for our first 5, drop the limit back down, and confirm those overflowed keys now block
        removeDenylist("" + ks_cql + "", "foofoo", "aaa:bbb");
        removeDenylist("" + ks_cql + "", "foofoo", "bbb:ccc");
        removeDenylist("" + ks_cql + "", "foofoo", "ccc:ddd");
        removeDenylist("" + ks_cql + "", "foofoo", "ddd:eee");
        removeDenylist("" + ks_cql + "", "foofoo", "eee:fff");
        forceReloadDenylist();
        confirmDenied("iii", "jjj");
    }

    private void confirmDenied(String keyOne, String keyTwo)
    {
        boolean denied = false;
        String query = String.format("SELECT * FROM " + ks_cql + ".foofoo WHERE bar='%s' and baz='%s'", keyOne, keyTwo);
        try
        {
            process(query, ConsistencyLevel.ONE);
        }
        catch (InvalidRequestException ire)
        {
            denied = true;
        }
        Assert.assertTrue(String.format("Expected query to be denied but was allowed! %s", query), denied);
    }

    private void confirmAllowed(String keyOne, String keyTwo)
    {
        process(String.format("SELECT * FROM " + ks_cql + ".foofoo WHERE bar='%s' and baz='%s'", keyOne, keyTwo), ConsistencyLevel.ONE);
    }

    private static void forceReloadDenylist() throws InterruptedException
    {
        StorageProxy.instance.loadPartitionDenylist();
    }

    private void denyAllKeys() throws InterruptedException
    {
        denylist("" + ks_cql + "", "foofoo", "aaa:bbb");
        denylist("" + ks_cql + "", "foofoo", "bbb:ccc");
        denylist("" + ks_cql + "", "foofoo", "ccc:ddd");
        denylist("" + ks_cql + "", "foofoo", "ddd:eee");
        denylist("" + ks_cql + "", "foofoo", "eee:fff");
        denylist("" + ks_cql + "", "foofoo", "fff:ggg");
        denylist("" + ks_cql + "", "foofoo", "ggg:hhh");
        denylist("" + ks_cql + "", "foofoo", "hhh:iii");
        denylist("" + ks_cql + "", "foofoo", "iii:jjj");
        forceReloadDenylist();
    }
}