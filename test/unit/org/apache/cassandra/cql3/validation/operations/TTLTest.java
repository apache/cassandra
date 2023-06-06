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

package org.apache.cassandra.cql3.validation.operations;


import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ExpirationDateOverflowHandling;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ExpirationDateOverflowHandling.ExpirationDateOverflowPolicy;
import org.apache.cassandra.db.rows.AbstractCell;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.IScrubber;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.tools.StandaloneScrubber;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.apache.cassandra.utils.Clock;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TTLTest extends CQLTester
{
    public static String NEGATIVE_LOCAL_EXPIRATION_TEST_DIR = "test/data/negative-local-expiration-test/%s";

    public static int MAX_TTL = Attributes.MAX_TTL;

    public static final String SIMPLE_NOCLUSTERING = "table1";
    public static final String SIMPLE_CLUSTERING = "table2";
    public static final String COMPLEX_NOCLUSTERING = "table3";
    public static final String COMPLEX_CLUSTERING = "table4";
    private Config.CorruptedTombstoneStrategy corruptTombstoneStrategy;

    // We should start applying overflow policies depending on supported sstable formats. Either in year 2038 or 2086
    boolean overflowPoliciesApply = (Clock.Global.currentTimeMillis() / 1000) > (Cell.getVersionedMaxDeletiontionTime() - MAX_TTL);

    @Before
    public void before()
    {
        corruptTombstoneStrategy = DatabaseDescriptor.getCorruptedTombstoneStrategy();
    }

    @After
    public void after()
    {
        DatabaseDescriptor.setCorruptedTombstoneStrategy(corruptTombstoneStrategy);
    }

    @Test
    public void testTTLPerRequestLimit() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int)");
        // insert with low TTL should not be denied
        execute("INSERT INTO %s (k, i) VALUES (1, 1) USING TTL ?", 10);

        try
        {
            execute("INSERT INTO %s (k, i) VALUES (1, 1) USING TTL ?", MAX_TTL + 1);
            fail("Expect InvalidRequestException");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage().contains("ttl is too large."));
        }

        try
        {
            execute("INSERT INTO %s (k, i) VALUES (1, 1) USING TTL ?", -1);
            fail("Expect InvalidRequestException");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage().contains("A TTL must be greater or equal to 0"));
        }
        execute("TRUNCATE %s");

        // insert with low TTL should not be denied
        execute("UPDATE %s USING TTL ? SET i = 1 WHERE k = 2", 5);

        try
        {
            execute("UPDATE %s USING TTL ? SET i = 1 WHERE k = 2", MAX_TTL + 1);
            fail("Expect InvalidRequestException");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage().contains("ttl is too large."));
        }

        try
        {
            execute("UPDATE %s USING TTL ? SET i = 1 WHERE k = 2", -1);
            fail("Expect InvalidRequestException");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage().contains("A TTL must be greater or equal to 0"));
        }
    }

    @Test
    public void testTTLDefaultLimit() throws Throwable
    {
        try
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, i int) WITH default_time_to_live=-1");
            fail("Expect Invalid schema");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getCause()
                        .getMessage()
                        .contains("default_time_to_live must be greater than or equal to 0 (got -1)"));
        }
        try
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, i int) WITH default_time_to_live="
                        + (MAX_TTL + 1));
            fail("Expect Invalid schema");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getCause()
                        .getMessage()
                        .contains("default_time_to_live must be less than or equal to " + MAX_TTL + " (got "
                                  + (MAX_TTL + 1) + ")"));
        }

        // table with default low TTL should not be denied
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int) WITH default_time_to_live=" + 5);
        execute("INSERT INTO %s (k, i) VALUES (1, 1)");
    }

    @Test
    public void testCapWarnExpirationOverflowPolicy() throws Throwable
    {
        if (overflowPoliciesApply)
            // We don't test that the actual warn is logged here, only on dtest
            testCapExpirationDateOverflowPolicy(ExpirationDateOverflowHandling.ExpirationDateOverflowPolicy.CAP);
    }

    @Test
    public void testCapNoWarnExpirationOverflowPolicy() throws Throwable
    {
        if (overflowPoliciesApply)
            testCapExpirationDateOverflowPolicy(ExpirationDateOverflowHandling.ExpirationDateOverflowPolicy.CAP_NOWARN);
    }

    @Test
    public void testCapNoWarnExpirationOverflowPolicyDefaultTTL() throws Throwable
    {
        if (overflowPoliciesApply)
        {
            ExpirationDateOverflowPolicy origPolicy = ExpirationDateOverflowHandling.policy;
            ExpirationDateOverflowHandling.policy = ExpirationDateOverflowPolicy.CAP_NOWARN;
            createTable("CREATE TABLE %s (k int PRIMARY KEY, i int) WITH default_time_to_live=" + MAX_TTL);
            execute("INSERT INTO %s (k, i) VALUES (1, 1)");
            checkTTLIsCapped("i");
            ExpirationDateOverflowHandling.policy = origPolicy;
        }
    }

    @Test
    public void testRejectExpirationOverflowPolicy() throws Throwable
    {
        if (overflowPoliciesApply)
        {
            ExpirationDateOverflowPolicy origPolicy = ExpirationDateOverflowHandling.policy;
            ExpirationDateOverflowHandling.policy = ExpirationDateOverflowPolicy.REJECT;

            createTable("CREATE TABLE %s (k int PRIMARY KEY, i int)");
            try
            {
                execute("INSERT INTO %s (k, i) VALUES (1, 1) USING TTL " + MAX_TTL);
                fail();
            }
            catch (InvalidRequestException e)
            {
                assertTrue(e.getMessage().contains("exceeds maximum supported expiration date"));
            }
            try
            {
                createTable("CREATE TABLE %s (k int PRIMARY KEY, i int) WITH default_time_to_live=" + MAX_TTL);
                execute("INSERT INTO %s (k, i) VALUES (1, 1)");
                fail();
            }
            catch (InvalidRequestException e)
            {
                assertTrue(e.getMessage().contains("exceeds maximum supported expiration date"));
            }

            ExpirationDateOverflowHandling.policy = origPolicy;
        }
    }

    @Test
    public void testRecoverOverflowedExpirationWithScrub() throws Throwable
    {
        // this tests writes corrupt tombstones on purpose, disable the strategy:
        DatabaseDescriptor.setCorruptedTombstoneStrategy(Config.CorruptedTombstoneStrategy.disabled);
        baseTestRecoverOverflowedExpiration(false, false, false);
        baseTestRecoverOverflowedExpiration(true, false, false);
        baseTestRecoverOverflowedExpiration(true, false, true);

        baseTestRecoverOverflowedExpiration(false, true, false);
        baseTestRecoverOverflowedExpiration(false, true, true);
        // we reset the corrupted ts strategy after each test in @After above
    }

    public void testCapExpirationDateOverflowPolicy(ExpirationDateOverflowHandling.ExpirationDateOverflowPolicy policy) throws Throwable
    {
        ExpirationDateOverflowPolicy origPolicy = ExpirationDateOverflowHandling.policy;
        ExpirationDateOverflowHandling.policy = policy;

        // simple column, clustering, flush
        testCapExpirationDateOverflowPolicy(true, true, true);
        // simple column, clustering, noflush
        testCapExpirationDateOverflowPolicy(true, true, false);
        // simple column, noclustering, flush
        testCapExpirationDateOverflowPolicy(true, false, true);
        // simple column, noclustering, noflush
        testCapExpirationDateOverflowPolicy(true, false, false);
        // complex column, clustering, flush
        testCapExpirationDateOverflowPolicy(false, true, true);
        // complex column, clustering, noflush
        testCapExpirationDateOverflowPolicy(false, true, false);
        // complex column, noclustering, flush
        testCapExpirationDateOverflowPolicy(false, false, true);
        // complex column, noclustering, noflush
        testCapExpirationDateOverflowPolicy(false, false, false);

        // Return to previous policy
        ExpirationDateOverflowHandling.policy = origPolicy;
    }

    public void testCapExpirationDateOverflowPolicy(boolean simple, boolean clustering, boolean flush) throws Throwable
    {
        // Create Table
        createTable(simple, clustering);

        // Insert data with INSERT and UPDATE
        if (simple)
        {
            execute("INSERT INTO %s (k, a) VALUES (?, ?) USING TTL " + MAX_TTL, 2, 2);
            if (clustering)
                execute("UPDATE %s USING TTL " + MAX_TTL + " SET b = 1 WHERE k = 1 AND a = 1;");
            else
                execute("UPDATE %s USING TTL " + MAX_TTL + " SET a = 1, b = 1 WHERE k = 1;");
        }
        else
        {
            execute("INSERT INTO %s (k, a, b) VALUES (?, ?, ?) USING TTL " + MAX_TTL, 2, 2, set("v21", "v22", "v23", "v24"));
            if (clustering)
                execute("UPDATE  %s USING TTL " + MAX_TTL + " SET b = ? WHERE k = 1 AND a = 1;", set("v11", "v12", "v13", "v14"));
            else
                execute("UPDATE  %s USING TTL " + MAX_TTL + " SET a = 1, b = ? WHERE k = 1;", set("v11", "v12", "v13", "v14"));
        }

        // Maybe Flush
        Keyspace ks = Keyspace.open(keyspace());
        if (flush)
            Util.flush(ks);

        // Verify data
        verifyData(simple);

        // Maybe major compact
        if (flush)
        {
            // Major compact and check data is still present
            ks.getColumnFamilyStore(currentTable()).forceMajorCompaction();

            // Verify data again
            verifyData(simple);
        }
    }

    public void baseTestRecoverOverflowedExpiration(boolean runScrub, boolean runSStableScrub, boolean reinsertOverflowedTTL) throws Throwable
    {
        // simple column, clustering
        testRecoverOverflowedExpirationWithScrub(true, true, runScrub, runSStableScrub, reinsertOverflowedTTL);
        // simple column, noclustering
        testRecoverOverflowedExpirationWithScrub(true, false, runScrub, runSStableScrub, reinsertOverflowedTTL);
        // complex column, clustering
        testRecoverOverflowedExpirationWithScrub(false, true, runScrub, runSStableScrub, reinsertOverflowedTTL);
        // complex column, noclustering
        testRecoverOverflowedExpirationWithScrub(false, false, runScrub, runSStableScrub, reinsertOverflowedTTL);
    }

    private void createTable(boolean simple, boolean clustering)
    {
        if (simple)
        {
            if (clustering)
                createTable("create table %s (k int, a int, b int, primary key(k, a))");
            else
                createTable("create table %s (k int primary key, a int, b int)");
        }
        else
        {
            if (clustering)
                createTable("create table %s (k int, a int, b set<text>, primary key(k, a))");
            else
                createTable("create table %s (k int primary key, a int, b set<text>)");
        }
    }

    private void verifyData(boolean simple) throws Throwable
    {
        if (simple)
        {
            assertRows(execute("SELECT * from %s"), row(1, 1, 1), row(2, 2, null));
        }
        else
        {
            assertRows(execute("SELECT * from %s"), row(1, 1, set("v11", "v12", "v13", "v14")), row(2, 2, set("v21", "v22", "v23", "v24")));
        }
        // Cannot retrieve TTL from collections
        if (simple)
            checkTTLIsCapped("b");
    }

    /**
     * Verify that the computed TTL is equal to the maximum allowed ttl given the
     * {@link AbstractCell#localDeletionTime()} field limitation (CASSANDRA-14092)
     */
    private void checkTTLIsCapped(String field) throws Throwable
    {
        // TTL is computed dynamically from row expiration time, so if it is
        // equal or higher to the minimum max TTL we compute before the query
        // we are fine.
        UntypedResultSet execute = execute("SELECT ttl(" + field + ") FROM %s WHERE k = 1");
        long minMaxTTL = computeMaxTTL();
        for (UntypedResultSet.Row row : execute)
        {
            long ttl = row.getInt("ttl(" + field + ")");
            assert (ttl >= minMaxTTL) : "ttl must be greater than or equal to minMaxTTL, but " + ttl + " is less than " + minMaxTTL;
        }
    }

    /**
     * The max TTL is computed such that the TTL summed with the current time is equal to the maximum
     * allowed expiration time {@link org.apache.cassandra.db.rows.Cell#MAX_DELETION_TIME}
     * when this was an int Integer.MAX_VALUE - 1
     */
    private long computeMaxTTL()
    {
        int nowInSecs = (int) (System.currentTimeMillis() / 1000);
        return Cell.getVersionedMaxDeletiontionTime() - nowInSecs;
    }

    public void testRecoverOverflowedExpirationWithScrub(boolean simple, boolean clustering, boolean runScrub, boolean runSStableScrub,  boolean reinsertOverflowedTTL) throws Throwable
    {
        if (reinsertOverflowedTTL)
        {
            assert runScrub || runSStableScrub;
        }

        createTable(simple, clustering);

        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(currentTable());

        assertEquals(0, cfs.getLiveSSTables().size());

        copySSTablesToTableDir(currentTable(), simple, clustering);

        cfs.loadNewSSTables();

        if (runScrub)
        {
            cfs.scrub(true, IScrubber.options().checkData().reinsertOverflowedTTLRows(reinsertOverflowedTTL).build(), 1);

            if (reinsertOverflowedTTL)
            {
                if (simple)
                    assertRows(execute("SELECT * from %s"), row(1, 1, 1), row(2, 2, null));
                else
                    assertRows(execute("SELECT * from %s"), row(1, 1, set("v11", "v12", "v13", "v14")), row(2, 2, set("v21", "v22", "v23", "v24")));

                cfs.forceMajorCompaction();

                if (simple)
                    assertRows(execute("SELECT * from %s"), row(1, 1, 1), row(2, 2, null));
                else
                    assertRows(execute("SELECT * from %s"), row(1, 1, set("v11", "v12", "v13", "v14")), row(2, 2, set("v21", "v22", "v23", "v24")));
            }
            else
            {
                assertEmpty(execute("SELECT * from %s"));
            }
        }
        if (runSStableScrub)
        {
            try (WithProperties properties = new WithProperties().set(TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST, true))
            {
                ToolResult tool;
                if (reinsertOverflowedTTL)
                    tool = ToolRunner.invokeClass(StandaloneScrubber.class, "-r", KEYSPACE, cfs.name);
                else
                    tool = ToolRunner.invokeClass(StandaloneScrubber.class, KEYSPACE, cfs.name);

                tool.assertOnCleanExit();
                Assertions.assertThat(tool.getStdout()).contains("Pre-scrub sstables snapshotted into");
                if (reinsertOverflowedTTL)
                    Assertions.assertThat(tool.getStdout()).contains("Fixed 2 rows with overflowed local deletion time.");
                else
                    Assertions.assertThat(tool.getStdout()).contains("No valid partitions found while scrubbing");
            }
        }

        try
        {
            cfs.truncateBlocking();
            dropTable("DROP TABLE %s");
        }
        catch (Throwable e)
        {
            // StandaloneScrubber.class should be ran as a tool with a stable env. In a test env there are things moving
            // under its feet such as the async CQLTester.afterTest() operations. We try to sync cleanup of tables here
            // but we need to catch any exceptions we might run into bc of the hack. See CASSANDRA-16546
        }
    }

    private void copySSTablesToTableDir(String table, boolean simple, boolean clustering) throws IOException
    {
        File destDir = Keyspace.open(keyspace()).getColumnFamilyStore(table).getDirectories().getCFDirectories().iterator().next();
        File sourceDir = getTableDir(table, simple, clustering);
        for (File file : sourceDir.tryList())
        {
            copyFile(file, destDir);
        }
    }

    private static File getTableDir(String table, boolean simple, boolean clustering)
    {
        return new File(String.format(NEGATIVE_LOCAL_EXPIRATION_TEST_DIR, getTableName(simple, clustering)));
    }

    private static void copyFile(File src, File dest) throws IOException
    {
        byte[] buf = new byte[65536];
        if (src.isFile())
        {
            File target = new File(dest, src.name());
            int rd;
            FileInputStreamPlus is = new FileInputStreamPlus(src);
            FileOutputStreamPlus os = new FileOutputStreamPlus(target);
            while ((rd = is.read(buf)) >= 0)
                os.write(buf, 0, rd);
            os.close();
        }
    }

    public static String getTableName(boolean simple, boolean clustering)
    {
        if (simple)
            return clustering ? SIMPLE_CLUSTERING : SIMPLE_NOCLUSTERING;
        else
            return clustering ? COMPLEX_CLUSTERING : COMPLEX_NOCLUSTERING;
    }
}