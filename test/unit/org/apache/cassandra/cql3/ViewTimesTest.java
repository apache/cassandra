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

package org.apache.cassandra.cql3;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/*
 * This test class was too large and used to timeout CASSANDRA-16777. We're splitting it into:
 * - ViewTest
 * - ViewPKTest
 * - ViewRangesTest
 * - ViewTimesTest
 */
public class ViewTimesTest extends ViewAbstractTest
{
    @Test
    public void testRegularColumnTimestampUpdates() throws Throwable
    {
        // Regression test for CASSANDRA-10910

        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "c int, " +
                    "val int)");

        execute("USE " + keyspace());
        executeNet("USE " + keyspace());

        createView("mv_rctstest", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c)");

        updateView("UPDATE %s SET c = ?, val = ? WHERE k = ?", 0, 0, 0);
        updateView("UPDATE %s SET val = ? WHERE k = ?", 1, 0);
        updateView("UPDATE %s SET c = ? WHERE k = ?", 1, 0);
        assertRows(execute("SELECT c, k, val FROM mv_rctstest"), row(1, 0, 1));

        updateView("TRUNCATE %s");

        updateView("UPDATE %s USING TIMESTAMP 1 SET c = ?, val = ? WHERE k = ?", 0, 0, 0);
        updateView("UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE k = ?", 1, 0);
        updateView("UPDATE %s USING TIMESTAMP 2 SET val = ? WHERE k = ?", 1, 0);
        updateView("UPDATE %s USING TIMESTAMP 4 SET c = ? WHERE k = ?", 2, 0);
        updateView("UPDATE %s USING TIMESTAMP 3 SET val = ? WHERE k = ?", 2, 0);

        assertRows(execute("SELECT c, k, val FROM mv_rctstest"), row(2, 0, 2));
        assertRows(execute("SELECT c, k, val FROM mv_rctstest limit 1"), row(2, 0, 2));
    }

    @Test
    public void complexTimestampUpdateTestWithFlush() throws Throwable
    {
        complexTimestampUpdateTest(true);
    }

    @Test
    public void complexTimestampUpdateTestWithoutFlush() throws Throwable
    {
        complexTimestampUpdateTest(false);
    }

    public void complexTimestampUpdateTest(boolean flush) throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY (a, b))");

        execute("USE " + keyspace());
        executeNet("USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, a, b)");
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        //Set initial values TS=0, leaving e null and verify view
        executeNet("INSERT INTO %s (a, b, c, d) VALUES (0, 0, 1, 0) USING TIMESTAMP 0");
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0));

        //update c's timestamp TS=2
        executeNet("UPDATE %s USING TIMESTAMP 2 SET c = ? WHERE a = ? and b = ? ", 1, 0, 0);
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0));

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        // change c's value and TS=3, tombstones c=1 and adds c=0 record
        executeNet("UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE a = ? and b = ? ", 0, 0, 0);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0));

        if(flush)
        {
            ks.getColumnFamilyStore("mv").forceMajorCompaction();
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));
        }


        //change c's value back to 1 with TS=4, check we can see d
        executeNet("UPDATE %s USING TIMESTAMP 4 SET c = ? WHERE a = ? and b = ? ", 1, 0, 0);
        if (flush)
        {
            ks.getColumnFamilyStore("mv").forceMajorCompaction();
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));
        }

        assertRows(execute("SELECT d,e from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0, null));


        //Add e value @ TS=1
        executeNet("UPDATE %s USING TIMESTAMP 1 SET e = ? WHERE a = ? and b = ? ", 1, 0, 0);
        assertRows(execute("SELECT d,e from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0, 1));

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));


        //Change d value @ TS=2
        executeNet("UPDATE %s USING TIMESTAMP 2 SET d = ? WHERE a = ? and b = ? ", 2, 0, 0);
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(2));

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));


        //Change d value @ TS=3
        executeNet("UPDATE %s USING TIMESTAMP 3 SET d = ? WHERE a = ? and b = ? ", 1, 0, 0);
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(1));


        //Tombstone c
        executeNet("DELETE FROM %s WHERE a = ? and b = ?", 0, 0);
        assertRows(execute("SELECT d from mv"));

        //Add back without D
        executeNet("INSERT INTO %s (a, b, c) VALUES (0, 0, 1)");

        //Make sure D doesn't pop back in.
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row((Object) null));


        //New partition
        // insert a row with timestamp 0
        executeNet("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP 0", 1, 0, 0, 0, 0);

        // overwrite pk and e with timestamp 1, but don't overwrite d
        executeNet("INSERT INTO %s (a, b, c, e) VALUES (?, ?, ?, ?) USING TIMESTAMP 1", 1, 0, 0, 0);

        // delete with timestamp 0 (which should only delete d)
        executeNet("DELETE FROM %s USING TIMESTAMP 0 WHERE a = ? AND b = ?", 1, 0);
        assertRows(execute("SELECT a, b, c, d, e from mv WHERE c = ? and a = ? and b = ?", 0, 1, 0),
                   row(1, 0, 0, null, 0)
        );

        executeNet("UPDATE %s USING TIMESTAMP 2 SET c = ? WHERE a = ? AND b = ?", 1, 1, 0);
        executeNet("UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE a = ? AND b = ?", 0, 1, 0);
        assertRows(execute("SELECT a, b, c, d, e from mv WHERE c = ? and a = ? and b = ?", 0, 1, 0),
                   row(1, 0, 0, null, 0)
        );

        executeNet("UPDATE %s USING TIMESTAMP 3 SET d = ? WHERE a = ? AND b = ?", 0, 1, 0);
        assertRows(execute("SELECT a, b, c, d, e from mv WHERE c = ? and a = ? and b = ?", 0, 1, 0),
                   row(1, 0, 0, 0, 0)
        );
    }

    @Test
    public void ttlTest() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b))");

        executeNet("USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)");

        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TTL 3", 1, 1, 1, 1);

        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        updateView("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, 2);

        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        List<Row> results = executeNet("SELECT d FROM mv WHERE c = 2 AND a = 1 AND b = 1").all();
        assertEquals(1, results.size());
        Assert.assertTrue("There should be a null result given back due to ttl expiry", results.get(0).isNull(0));
    }

    @Test
    public void ttlExpirationTest() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b))");

        executeNet("USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)");

        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TTL 3", 1, 1, 1, 1);

        Thread.sleep(TimeUnit.SECONDS.toMillis(4));
        assertEquals(0, executeNet("SELECT * FROM mv WHERE c = 1 AND a = 1 AND b = 1").all().size());
    }

    @Test
    public void conflictingTimestampTest() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "PRIMARY KEY (a, b))");

        executeNet("USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)");

        for (int i = 0; i < 50; i++)
        {
            updateView("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP 1", 1, 1, i);
        }

        ResultSet mvRows = executeNet("SELECT c FROM mv");
        List<Row> rows = executeNet("SELECT c FROM %s").all();
        assertEquals("There should be exactly one row in base", 1, rows.size());
        int expected = rows.get(0).getInt("c");
        assertRowsNet(mvRows, row(expected));
    }

    @Test
    public void testCreateMvWithTTL() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "c int, " +
                    "val int) WITH default_time_to_live = 60");

        execute("USE " + keyspace());
        executeNet("USE " + keyspace());

        // Must NOT include "default_time_to_live" for Materialized View creation
        try
        {
            createView("mv_ttl1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c) WITH default_time_to_live = 30");
            fail("Should fail if TTL is provided for materialized view");
        }
        catch (Exception e)
        {
            assertEquals("Cannot set default_time_to_live for a materialized view. Data in a materialized " +
                         "view always expire at the same time than the corresponding data in the parent table.",
                         e.getMessage());
        }
    }

    @Test
    public void testAlterMvWithNoZeroTTL() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "c int, " +
                    "val int) WITH default_time_to_live = 60");

        createView("mv_ttl2", "CREATE MATERIALIZED VIEW " + keyspace() + ".%s AS SELECT * FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c)");

        // Must NOT include "default_time_to_live" on alter Materialized View
        try
        {
            executeNet("ALTER MATERIALIZED VIEW " + keyspace() + ".mv_ttl2 WITH default_time_to_live = 30");
            fail("Should fail if TTL is provided while altering materialized view");
        }
        catch (Exception e)
        {
            // Make sure the message is clear. See CASSANDRA-16960
            assertEquals("Forbidden default_time_to_live detected for a materialized view. " +
                         "Data in a materialized view always expires at the same time as " +
                         "the corresponding data in the parent table. default_time_to_live " +
                         "must be set to zero, see CASSANDRA-12868 for more information",
                         e.getMessage());
        }
    }

    @Test
    public void testMvWithZeroTTL() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "c int, " +
                    "val int) WITH default_time_to_live = 60");

        try
        {
            createView("mv_ttl3", "CREATE MATERIALIZED VIEW " + keyspace() + ".%s AS SELECT * FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c) WITH default_time_to_live = 0");
        }
        catch (Exception e)
        {
            fail("Should not fail if TTL equal to 0 is provided while altering materialized view");
        }
    }

    @Test
    public void testAlterMvWithZeroTTL() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "c int, " +
                    "val int) WITH default_time_to_live = 60");

        createView("mv_ttl4", "CREATE MATERIALIZED VIEW " + keyspace() + ".%s AS SELECT * FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c)");

        try
        {
            executeNet("ALTER MATERIALIZED VIEW " + keyspace() + ".mv_ttl4 WITH default_time_to_live = 0");
        }
        catch (Exception e)
        {
            fail("Should not fail if TTL equal to 0 is provided while altering materialized view");
        }
    }
}
