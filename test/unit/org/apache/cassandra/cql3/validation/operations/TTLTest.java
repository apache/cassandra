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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.BufferExpiringCell;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ExpiringCell;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.FBUtilities;

import org.junit.Test;

public class TTLTest extends CQLTester
{
    public static String NEGATIVE_LOCAL_EXPIRATION_TEST_DIR = "test/data/negative-local-expiration-test/%s";

    public static int MAX_TTL = ExpiringCell.MAX_TTL;

    public static final String SIMPLE_NOCLUSTERING = "table1";
    public static final String SIMPLE_CLUSTERING = "table2";
    public static final String COMPLEX_NOCLUSTERING = "table3";
    public static final String COMPLEX_CLUSTERING = "table4";

    @Test
    public void testTTLPerRequestLimit() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int)");
        // insert with low TTL should not be denied
        execute("INSERT INTO %s (k, i) VALUES (1, 1) USING TTL ?", 10); // max ttl

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
        execute("UPDATE %s USING TTL ? SET i = 1 WHERE k = 2", 5); // max ttl

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
                        .contains("default_time_to_live cannot be smaller than 0"));
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
    public void testRejectExpirationDateOverflowPolicy() throws Throwable
    {
        Attributes.policy = Attributes.ExpirationDateOverflowPolicy.REJECT;
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int)");
        try
        {
            execute("INSERT INTO %s (k, i) VALUES (1, 1) USING TTL " + MAX_TTL);
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage().contains("exceeds maximum supported expiration date"));
        }
        try
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, i int) WITH default_time_to_live=" + MAX_TTL);
            execute("INSERT INTO %s (k, i) VALUES (1, 1)");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage().contains("exceeds maximum supported expiration date"));
        }
    }

    @Test
    public void testCapExpirationDatePolicyDefaultTTL() throws Throwable
    {
        Attributes.policy = Attributes.ExpirationDateOverflowPolicy.CAP;
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int) WITH default_time_to_live=" + MAX_TTL);
        execute("INSERT INTO %s (k, i) VALUES (1, 1)");
        checkTTLIsCapped("i");
        Attributes.policy = Attributes.ExpirationDateOverflowPolicy.REJECT;
    }

    @Test
    public void testCapExpirationDatePolicyPerRequest() throws Throwable
    {
        // Test cap policy
        Attributes.policy = Attributes.ExpirationDateOverflowPolicy.CAP;

        // simple column, clustering, flush
        baseCapExpirationDateOverflowTest(true, true, true);
        // simple column, clustering, noflush
        baseCapExpirationDateOverflowTest(true, true, false);
        // simple column, noclustering, flush
        baseCapExpirationDateOverflowTest(true, false, true);
        // simple column, noclustering, noflush
        baseCapExpirationDateOverflowTest(true, false, false);
        // complex column, clustering, flush
        baseCapExpirationDateOverflowTest(false, true, true);
        // complex column, clustering, noflush
        baseCapExpirationDateOverflowTest(false, true, false);
        // complex column, noclustering, flush
        baseCapExpirationDateOverflowTest(false, false, true);
        // complex column, noclustering, noflush
        baseCapExpirationDateOverflowTest(false, false, false);
        // complex column, noclustering, flush
        baseCapExpirationDateOverflowTest(false, false, false);

        // Return to previous policy
        Attributes.policy = Attributes.ExpirationDateOverflowPolicy.REJECT;
    }

    @Test
    public void testRecoverOverflowedExpirationWithScrub() throws Throwable
    {
        baseTestRecoverOverflowedExpiration(false, false);
        baseTestRecoverOverflowedExpiration(true, false);
        baseTestRecoverOverflowedExpiration(true, true);
    }

    public void baseCapExpirationDateOverflowTest(boolean simple, boolean clustering, boolean flush) throws Throwable
    {
        // Create Table
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

        // Insert data with INSERT and UPDATE
        if (simple)
        {
            execute("INSERT INTO %s (k, a, b) VALUES (?, ?, ?) USING TTL " + MAX_TTL, 2, 2, 2);
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
            FBUtilities.waitOnFutures(ks.flush());

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

    public void baseTestRecoverOverflowedExpiration(boolean runScrub, boolean reinsertOverflowedTTL) throws Throwable
    {
        // simple column, clustering
        testRecoverOverflowedExpirationWithScrub(true, true, runScrub, reinsertOverflowedTTL);
        // simple column, noclustering
        testRecoverOverflowedExpirationWithScrub(true, false, runScrub, reinsertOverflowedTTL);
        // complex column, clustering
        testRecoverOverflowedExpirationWithScrub(false, true, runScrub, reinsertOverflowedTTL);
        // complex column, noclustering
        testRecoverOverflowedExpirationWithScrub(false, false, runScrub, reinsertOverflowedTTL);
    }

    private void verifyData(boolean simple) throws Throwable
    {
        if (simple)
        {
            assertRows(execute("SELECT * from %s"), row(1, 1, 1), row(2, 2, 2));
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
     * Verify that the computed TTL is approximately equal to the maximum allowed ttl given the
     * {@link ExpiringCell#getLocalDeletionTime()} field limitation (CASSANDRA-14092)
     */
    private void checkTTLIsCapped(String field) throws Throwable
    {

        // TTL is computed dynamically from row expiration time, so if it is
        // equal or higher to the minimum max TTL we compute before the query
        // we are fine.
        int minMaxTTL = computeMaxTTL();
        UntypedResultSet execute = execute("SELECT ttl(" + field + ") FROM %s");
        for (UntypedResultSet.Row row : execute)
        {
            int ttl = row.getInt("ttl(" + field + ")");
            assertTrue(ttl >= minMaxTTL);
        }
    }

    /**
     * The max TTL is computed such that the TTL summed with the current time is equal to the maximum
     * allowed expiration time {@link BufferExpiringCell#getLocalDeletionTime()} (2038-01-19T03:14:06+00:00)
     */
    private int computeMaxTTL()
    {
        int nowInSecs = (int) (System.currentTimeMillis() / 1000);
        return BufferExpiringCell.MAX_DELETION_TIME - nowInSecs;
    }

    public void testRecoverOverflowedExpirationWithScrub(boolean simple, boolean clustering, boolean runScrub, boolean reinsertOverflowedTTL) throws Throwable
    {
        if (reinsertOverflowedTTL)
        {
            assert runScrub;
        }

        createTable(simple, clustering);

        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(currentTable());

        assertEquals(0, cfs.getSSTables().size());

        copySSTablesToTableDir(currentTable(), simple, clustering);

        cfs.loadNewSSTables();

        if (runScrub)
        {
            cfs.scrub(true, false, false, reinsertOverflowedTTL, 1);
        }

        if (reinsertOverflowedTTL)
        {
            if (simple)
                assertRows(execute("SELECT * from %s"), row(1, 1, 1), row(2, 2, 2));
            else
                assertRows(execute("SELECT * from %s"), row(1, 1, set("v11", "v12", "v13", "v14")), row(2, 2, set("v21", "v22", "v23", "v24")));

            cfs.forceMajorCompaction();

            if (simple)
                assertRows(execute("SELECT * from %s"), row(1, 1, 1), row(2, 2, 2));
            else
                assertRows(execute("SELECT * from %s"), row(1, 1, set("v11", "v12", "v13", "v14")), row(2, 2, set("v21", "v22", "v23", "v24")));
        }
        else
        {
            assertEmpty(execute("SELECT * from %s"));
        }
    }

    private void copySSTablesToTableDir(String table, boolean simple, boolean clustering) throws IOException
    {
        File destDir = Keyspace.open(keyspace()).getColumnFamilyStore(table).directories.getCFDirectories().iterator().next();
        File sourceDir = getTableDir(table, simple, clustering);
        for (File file : sourceDir.listFiles())
        {
            copyFile(file, destDir);
        }
    }

    private static File getTableDir(String table, boolean simple, boolean clustering)
    {
        return new File(String.format(NEGATIVE_LOCAL_EXPIRATION_TEST_DIR, getTableName(simple, clustering)));
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

    private static File getTableDir(boolean simple, boolean clustering)
    {
        return new File(String.format(NEGATIVE_LOCAL_EXPIRATION_TEST_DIR, getTableName(simple, clustering)));
    }

    private static void copyFile(File src, File dest) throws IOException
    {
        byte[] buf = new byte[65536];
        if (src.isFile())
        {
            File target = new File(dest, src.getName());
            int rd;
            FileInputStream is = new FileInputStream(src);
            FileOutputStream os = new FileOutputStream(target);
            while ((rd = is.read(buf)) >= 0)
                os.write(buf, 0, rd);
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
