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

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.Tables;

import static org.apache.cassandra.cql3.QueryProcessor.process;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PartitionDenylistTest
{
    private final static String ks_cql = "partition_denylist_keyspace";

    @BeforeClass
    public static void init()
    {
        ServerTestUtils.daemonInitialization();

        CQLTester.prepareServer();

        KeyspaceMetadata schema = KeyspaceMetadata.create(ks_cql,
                                                          KeyspaceParams.simple(1),
                                                          Tables.of(
            CreateTableStatement.parse("CREATE TABLE table1 ("
                                       + "keyone text, "
                                       + "keytwo text, "
                                       + "qux text, "
                                       + "quz text, "
                                       + "foo text, "
                                       + "PRIMARY KEY((keyone, keytwo), qux, quz) ) ", ks_cql).build(),
            CreateTableStatement.parse("CREATE TABLE table2 ("
                                       + "keyone text, "
                                       + "keytwo text, "
                                       + "keythree text, "
                                       + "value text, "
                                       + "PRIMARY KEY((keyone, keytwo), keythree) ) ", ks_cql).build(),
            CreateTableStatement.parse("CREATE TABLE table3 ("
                                       + "keyone text, "
                                       + "keytwo text, "
                                       + "keythree text, "
                                       + "value text, "
                                       + "PRIMARY KEY((keyone, keytwo), keythree) ) ", ks_cql).build()
        ));
        SchemaTestUtil.addOrUpdateKeyspace(schema, false);
        DatabaseDescriptor.setPartitionDenylistEnabled(true);
        DatabaseDescriptor.setDenylistRangeReadsEnabled(true);
        DatabaseDescriptor.setDenylistConsistencyLevel(ConsistencyLevel.ONE);
        DatabaseDescriptor.setDenylistRefreshSeconds(1);
        StorageService.instance.initServer(0);
    }

    @Before
    public void setup()
    {
        DatabaseDescriptor.setPartitionDenylistEnabled(true);
        resetDenylist();

        process("INSERT INTO " + ks_cql + ".table1 (keyone, keytwo, qux, quz, foo) VALUES ('aaa', 'bbb', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("INSERT INTO " + ks_cql + ".table1 (keyone, keytwo, qux, quz, foo) VALUES ('bbb', 'ccc', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("INSERT INTO " + ks_cql + ".table1 (keyone, keytwo, qux, quz, foo) VALUES ('ccc', 'ddd', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("INSERT INTO " + ks_cql + ".table1 (keyone, keytwo, qux, quz, foo) VALUES ('ddd', 'eee', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("INSERT INTO " + ks_cql + ".table1 (keyone, keytwo, qux, quz, foo) VALUES ('eee', 'fff', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("INSERT INTO " + ks_cql + ".table1 (keyone, keytwo, qux, quz, foo) VALUES ('fff', 'ggg', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("INSERT INTO " + ks_cql + ".table1 (keyone, keytwo, qux, quz, foo) VALUES ('ggg', 'hhh', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("INSERT INTO " + ks_cql + ".table1 (keyone, keytwo, qux, quz, foo) VALUES ('hhh', 'iii', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("INSERT INTO " + ks_cql + ".table1 (keyone, keytwo, qux, quz, foo) VALUES ('iii', 'jjj', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("INSERT INTO " + ks_cql + ".table1 (keyone, keytwo, qux, quz, foo) VALUES ('jjj', 'kkk', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);


        for (int i = 0; i < 50; i++)
            process(String.format("INSERT INTO " + ks_cql + ".table2 (keyone, keytwo, keythree, value) VALUES ('%d', '%d', '%d', '%d')", i, i, i, i), ConsistencyLevel.ONE);

        for (int i = 0; i < 50; i++)
            process(String.format("INSERT INTO " + ks_cql + ".table3 (keyone, keytwo, keythree, value) VALUES ('%d', '%d', '%d', '%d')", i, i, i, i), ConsistencyLevel.ONE);

        denylist("table1", "bbb:ccc");
        refreshList();
    }


    private static void denylist(String table, final String key)
    {
        StorageProxy.instance.denylistKey(ks_cql, table, key);
    }

    private static void refreshList()
    {
        StorageProxy.instance.loadPartitionDenylist();
    }

    /**
     * @return Whether the *attempt* to remove the denylisted key and refresh succeeded. Doesn't necessarily indicate the key
     * was previously blocked and found.
     */
    private static boolean removeDenylist(final String ks, final String table, final String key)
    {
        return StorageProxy.instance.removeDenylistKey(ks, table, key);
    }

    @Test
    public void testRead()
    {
        process("SELECT * FROM " + ks_cql + ".table1 WHERE keyone='aaa' and keytwo='bbb'", ConsistencyLevel.ONE);
    }

    @Test
    public void testReadDenylisted()
    {
        assertThatThrownBy(() -> process("SELECT * FROM " + ks_cql + ".table1 WHERE keyone='bbb' and keytwo='ccc'", ConsistencyLevel.ONE))
                           .isInstanceOf(InvalidRequestException.class)
                           .hasMessageContaining("Unable to read denylisted partition");
    }

    @Test
    public void testIsKeyDenylistedAPI()
    {
        Assert.assertTrue(StorageProxy.instance.isKeyDenylisted(ks_cql, "table1", "bbb:ccc"));
        resetDenylist();
        Assert.assertFalse(StorageProxy.instance.isKeyDenylisted(ks_cql, "table1", "bbb:ccc"));

        // Confirm an add mutates cache state
        denylist("table1", "bbb:ccc");
        Assert.assertTrue(StorageProxy.instance.isKeyDenylisted(ks_cql, "table1", "bbb:ccc"));

        // Confirm removal then mutates cache w/out explicit reload
        StorageProxy.instance.removeDenylistKey(ks_cql, "table1", "bbb:ccc");
        Assert.assertFalse(StorageProxy.instance.isKeyDenylisted(ks_cql, "table1", "bbb:ccc"));
    }

    @Test
    public void testReadUndenylisted()
    {
        resetDenylist();
        process("SELECT * FROM " + ks_cql + ".table1 WHERE keyone='ccc' and keytwo='ddd'", ConsistencyLevel.ONE);
    }

    @Test
    public void testWrite()
    {
        process("INSERT INTO " + ks_cql + ".table1 (keyone, keytwo, qux, quz, foo) VALUES ('eee', 'fff', 'ccc', 'ddd', 'v')", ConsistencyLevel.ONE);
        process("DELETE FROM " + ks_cql + ".table1 WHERE keyone='eee' and keytwo='fff'", ConsistencyLevel.ONE);
    }

    @Test
    public void testWriteDenylisted()
    {
        assertThatThrownBy(() -> process("INSERT INTO " + ks_cql + ".table1 (keyone, keytwo, qux, quz, foo) VALUES ('bbb', 'ccc', 'eee', 'fff', 'w')", ConsistencyLevel.ONE))
                           .isInstanceOf(InvalidRequestException.class)
                           .hasMessageContaining("Unable to write to denylisted partition");
    }

    @Test
    public void testCASWriteDenylisted()
    {
        assertThatThrownBy(() -> process("UPDATE " + ks_cql + ".table1 SET foo='w' WHERE keyone='bbb' AND keytwo='ccc' AND qux='eee' AND quz='fff' IF foo='v'", ConsistencyLevel.LOCAL_SERIAL))
                           .isInstanceOf(InvalidRequestException.class)
                           .hasMessageContaining("Unable to CAS write to denylisted partition");
    }

    @Test
    public void testWriteUndenylisted()
    {
        resetDenylist();
        process("INSERT INTO " + ks_cql + ".table1 (keyone, keytwo, qux, quz, foo) VALUES ('bbb', 'ccc', 'eee', 'fff', 'w')", ConsistencyLevel.ONE);
    }

    @Test
    public void testRangeSlice()
    {
        UntypedResultSet rows;
        rows = process("SELECT * FROM " + ks_cql + ".table1 WHERE token(keyone, keytwo) < token('bbb', 'ccc')", ConsistencyLevel.ONE);
        Assert.assertEquals(1, rows.size());

        // 10 entries total in our table
        rows = process("SELECT * FROM " + ks_cql + ".table1 WHERE token(keyone, keytwo) > token('bbb', 'ccc')", ConsistencyLevel.ONE);
        Assert.assertEquals(8, rows.size());

        rows = process("SELECT * FROM " + ks_cql + ".table1 WHERE token(keyone, keytwo) >= token('aaa', 'bbb') and token(keyone, keytwo) < token('bbb', 'ccc')", ConsistencyLevel.ONE);
        Assert.assertEquals(1, rows.size());

        rows = process("SELECT * FROM " + ks_cql + ".table1 WHERE token(keyone, keytwo) > token('bbb', 'ccc') and token(keyone, keytwo) <= token('ddd', 'eee')", ConsistencyLevel.ONE);
        Assert.assertEquals(2, rows.size());
    }

    @Test
    public void testRangeDenylisted()
    {
        assertThatThrownBy(() -> process("SELECT * FROM " + ks_cql + ".table1", ConsistencyLevel.ONE))
                           .isInstanceOf(InvalidRequestException.class)
                           .hasMessageContaining("Attempted to read a range containing 1 denylisted keys");
    }

    @Test
    public void testRangeDenylisted2()
    {
        assertThatThrownBy(() -> process("SELECT * FROM " + ks_cql + ".table1 WHERE token(keyone, keytwo) >= token('aaa', 'bbb') and token (keyone, keytwo) <= token('bbb', 'ccc')", ConsistencyLevel.ONE))
                           .isInstanceOf(InvalidRequestException.class)
                           .hasMessageContaining("Attempted to read a range containing 1 denylisted keys");
    }

    @Test
    public void testRangeDenylisted3()
    {
        assertThatThrownBy(() -> process("SELECT * FROM " + ks_cql + ".table1 WHERE token(keyone, keytwo) >= token('bbb', 'ccc') and token (keyone, keytwo) <= token('ccc', 'ddd')", ConsistencyLevel.ONE))
                           .isInstanceOf(InvalidRequestException.class)
                           .hasMessageContaining("Attempted to read a range containing 1 denylisted keys");
    }

    @Test
    public void testRangeDenylisted4()
    {
        assertThatThrownBy(() -> process("SELECT * FROM " + ks_cql + ".table1 WHERE token(keyone, keytwo) > token('aaa', 'bbb') and token (keyone, keytwo) < token('ccc', 'ddd')", ConsistencyLevel.ONE))
                           .isInstanceOf(InvalidRequestException.class)
                           .hasMessageContaining("Attempted to read a range containing 1 denylisted keys");
    }

    @Test
    public void testRangeDenylisted5()
    {
        assertThatThrownBy(() -> process("SELECT * FROM " + ks_cql + ".table1 WHERE token(keyone, keytwo) > token('aaa', 'bbb')", ConsistencyLevel.ONE))
                           .isInstanceOf(InvalidRequestException.class)
                           .hasMessageContaining("Attempted to read a range containing 1 denylisted keys");
    }

    @Test
    public void testRangeDenylisted6()
    {
        assertThatThrownBy(() -> process("SELECT * FROM " + ks_cql + ".table1 WHERE token(keyone, keytwo) < token('ddd', 'eee')", ConsistencyLevel.ONE))
                           .isInstanceOf(InvalidRequestException.class)
                           .hasMessageContaining("Attempted to read a range containing 1 denylisted keys");
    }

    @Test
    public void testInsertUnknownPKIsGraceful()
    {
        Assert.assertTrue(StorageProxy.instance.denylistKey(ks_cql, "table1", "hohoho"));
    }

    @Test
    public void testInsertInvalidTableIsGraceful()
    {
        Assert.assertFalse(StorageProxy.instance.denylistKey(ks_cql, "asldkfjadlskjf", "alksdjfads"));
    }

    @Test
    public void testInsertInvalidKSIsGraceful()
    {
        Assert.assertFalse(StorageProxy.instance.denylistKey("asdklfjas", "asldkfjadlskjf", "alksdjfads"));
    }

    @Test
    public void testDisabledDenylistThrowsNoExceptions()
    {
        process(String.format("TRUNCATE TABLE %s.table2", ks_cql), ConsistencyLevel.ONE);
        process(String.format("TRUNCATE TABLE %s.table3", ks_cql), ConsistencyLevel.ONE);
        denyAllKeys();
        DatabaseDescriptor.setPartitionDenylistEnabled(false);
        process("INSERT INTO " + ks_cql + ".table1 (keyone, keytwo, qux, quz, foo) VALUES ('bbb', 'ccc', 'eee', 'fff', 'w')", ConsistencyLevel.ONE);
        process("SELECT * FROM " + ks_cql + ".table1 WHERE keyone='bbb' and keytwo='ccc'", ConsistencyLevel.ONE);
        process("SELECT * FROM " + ks_cql + ".table1", ConsistencyLevel.ONE);

        for (int i = 0; i < 50; i++)
        {
            process(String.format("INSERT INTO %s.table2 (keyone, keytwo, keythree, value) VALUES ('%s', '%s', '%s', '%s')", ks_cql, i, i, i, i), ConsistencyLevel.ONE);
            process(String.format("SELECT * FROM %s.table2 WHERE keyone='%s' and keytwo='%s'", ks_cql, i, i), ConsistencyLevel.ONE);
        }

        for (int i = 0; i < 50; i++)
        {
            process(String.format("INSERT INTO %s.table3 (keyone, keytwo, keythree, value) VALUES ('%s', '%s', '%s', '%s')", ks_cql, i, i, i, i), ConsistencyLevel.ONE);
            process(String.format("SELECT * FROM %s.table3 WHERE keyone='%s' and keytwo='%s'", ks_cql, i, i), ConsistencyLevel.ONE);
        }
    }

    /**
     * Want to make sure we don't throw anything or explode when people try to remove a key that's not there
     */
    @Test
    public void testRemoveMissingIsGraceful()
    {
        confirmDenied("table1", "bbb", "ccc");
        Assert.assertTrue(removeDenylist(ks_cql, "table1", "bbb:ccc"));

        // We expect this to silently not find and succeed at *trying* to remove it
        Assert.assertTrue(removeDenylist(ks_cql, "table1", "bbb:ccc"));
        refreshList();

        confirmAllowed("table1", "bbb", "ccc");
    }

    /**
     * We need to confirm that the entire cache is reloaded rather than being an additive change; we don't want keys to
     * persist after their removal and reload from CQL.
     */
    @Test
    public void testRemoveWorksOnReload()
    {
        denyAllKeys();
        refreshList();

        confirmDenied("table1", "aaa", "bbb");
        confirmDenied("table1", "eee", "fff");
        confirmDenied("table1", "iii", "jjj");

        // poke a hole in the middle and reload
        removeDenylist(ks_cql, "table1", "eee:fff");
        refreshList();

        confirmAllowed("table1", "eee", "fff");
        confirmDenied("table1", "aaa", "bbb");
        confirmDenied("table1", "iii", "jjj");
    }

    /**
     * We go through a few steps here:
     *  1) Add more keys than we're allowed
     *  2) Confirm that the overflow keys are *not* denied
     *  3) Raise the allowable limit
     *  4) Confirm that the overflow keys are now denied (and no longer really "overflow" for that matter)
     */
    @Test
    public void testShrinkAndGrow()
    {
        denyAllKeys();
        refreshList();

        // Initial control; check denial of both initial and final keys
        confirmDenied("table1", "aaa", "bbb");
        confirmDenied("table1", "iii", "jjj");

        // Lower our limit to 5 allowable denies and then check and see that things past the limit are ignored
        StorageProxy.instance.setDenylistMaxKeysPerTable(5);
        StorageProxy.instance.setDenylistMaxKeysTotal(5);
        refreshList();

        // Confirm overflowed keys are allowed; first come first served
        confirmDenied("table1", "aaa", "bbb");
        confirmAllowed("table1", "iii", "jjj");

        // Now we raise the limit back up and do nothing else and confirm it's blocked
        StorageProxy.instance.setDenylistMaxKeysPerTable(1000);
        StorageProxy.instance.setDenylistMaxKeysTotal(1000);
        refreshList();
        confirmDenied("table1", "aaa", "bbb");
        confirmDenied("table1", "iii", "jjj");

        // Unblock via overflow the table 1 sentinel we'll check in a second
        StorageProxy.instance.setDenylistMaxKeysPerTable(5);
        StorageProxy.instance.setDenylistMaxKeysTotal(5);
        refreshList();
        confirmAllowed("table1", "iii", "jjj");

        // Now, we remove the denylist entries for our first 5, drop the limit back down, and confirm those overflowed keys now block
        removeDenylist(ks_cql, "table1", "aaa:bbb");
        removeDenylist(ks_cql, "table1", "bbb:ccc");
        removeDenylist(ks_cql, "table1", "ccc:ddd");
        removeDenylist(ks_cql, "table1", "ddd:eee");
        removeDenylist(ks_cql, "table1", "eee:fff");
        refreshList();
        confirmDenied("table1", "iii", "jjj");
    }

    /*
    We need to test that, during a violation of our global allowable limit, we still enforce our limit of keys queried
    on a per-table basis.
     */
    @Test
    public void testTableLimitRespected()
    {
        StorageProxy.instance.setDenylistMaxKeysPerTable(5);
        StorageProxy.instance.setDenylistMaxKeysTotal(12);
        denyAllKeys();
        refreshList();

        // Table 1: expect first 5 denied
        confirmDenied("table1", "aaa", "bbb");
        confirmDenied("table1", "eee", "fff");
        confirmAllowed("table1", "fff", "ggg");

        // Table 2: expect first 5 denied
        for (int i = 0; i < 5; i++)
            confirmDenied("table2", Integer.toString(i), Integer.toString(i));

        // Confirm remainder are allowed because we hit our table limit at 5
        for (int i = 5; i < 50; i++)
            confirmAllowed("table2", Integer.toString(i), Integer.toString(i));

        // Table 3: expect only first 2 denied; global limit enforcement
        confirmDenied("table3", "0", "0");
        confirmDenied("table3", "1", "1");

        // And our final 48 should be allowed
        for (int i = 2; i < 50; i++)
            confirmAllowed("table3", Integer.toString(i), Integer.toString(i));
    }

    /**
     * Test that we don't allow overflowing global limit due to accumulation of allowable table queries
     */
    @Test
    public void testGlobalLimitRespected()
    {
        StorageProxy.instance.setDenylistMaxKeysPerTable(50);
        StorageProxy.instance.setDenylistMaxKeysTotal(15);
        denyAllKeys();
        refreshList();

        // Table 1: expect all 10 denied
        confirmDenied("table1", "aaa", "bbb");
        confirmDenied("table1", "jjj", "kkk");

        // Table 2: expect only 5 denied up to global limit trigger
        for (int i = 0; i < 5; i++)
            confirmDenied("table2", Integer.toString(i), Integer.toString(i));

        // Remainder of Table 2 should be allowed; testing overflow boundary logic
        for (int i = 5; i < 50; i++)
            confirmAllowed("table2", Integer.toString(i), Integer.toString(i));

        // Table 3: expect all allowed as we're past global limit by the time we got to this table load. This confirms that
        // we bypass load completely on tables once we're at our global limit.
        for (int i = 0; i < 50; i++)
            confirmAllowed("table3", Integer.toString(i), Integer.toString(i));
    }

    private void confirmDenied(String table, String keyOne, String keyTwo)
    {
        String query = String.format("SELECT * FROM " + ks_cql + "." + table + " WHERE keyone='%s' and keytwo='%s'", keyOne, keyTwo);
        assertThatThrownBy(() -> process(query, ConsistencyLevel.ONE))
                           .isInstanceOf(InvalidRequestException.class)
                           .hasMessageContaining("Unable to read denylisted partition");
    }

    private void confirmAllowed(String table, String keyOne, String keyTwo)
    {
        process(String.format("SELECT * FROM %s.%s WHERE keyone='%s' and keytwo='%s'", ks_cql, table, keyOne, keyTwo), ConsistencyLevel.ONE);
    }

    private void resetDenylist()
    {
        process("TRUNCATE system_distributed.partition_denylist", ConsistencyLevel.ONE);
        StorageProxy.instance.setDenylistMaxKeysTotal(1000);
        StorageProxy.instance.setDenylistMaxKeysPerTable(1000);
        StorageProxy.instance.loadPartitionDenylist();
    }

    private void denyAllKeys()
    {
        denylist("table1", "aaa:bbb");
        denylist("table1", "bbb:ccc");
        denylist("table1", "ccc:ddd");
        denylist("table1", "ddd:eee");
        denylist("table1", "eee:fff");
        denylist("table1", "fff:ggg");
        denylist("table1", "ggg:hhh");
        denylist("table1", "hhh:iii");
        denylist("table1", "iii:jjj");
        denylist("table1", "jjj:kkk");

        for (int i = 0; i < 50; i++)
        {
            denylist("table2", String.format("%d:%d", i, i));
            denylist("table3", String.format("%d:%d", i, i));
        }

    }
}
