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

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.utils.FBUtilities;

/* ViewFilteringTest class has been split into multiple ones because of timeout issues (CASSANDRA-16670, CASSANDRA-17167)
 * Any changes here check if they apply to the other classes
 * - ViewFilteringPKTest
 * - ViewFilteringClustering1Test
 * - ViewFilteringClustering2Test
 * - ViewFilteringTest
 * - ...
 * - ViewFiltering*Test
 */
@RunWith(Parameterized.class)
public class ViewFilteringTest extends ViewFilteringTester
{
    // TODO will revise the non-pk filter condition in MV, see CASSANDRA-11500
    @Ignore
    @Test
    public void testViewFilteringWithFlush() throws Throwable
    {
        testViewFiltering(true);
    }

    // TODO will revise the non-pk filter condition in MV, see CASSANDRA-11500
    @Ignore
    @Test
    public void testViewFilteringWithoutFlush() throws Throwable
    {
        testViewFiltering(false);
    }

    public void testViewFiltering(boolean flush) throws Throwable
    {
        // CASSANDRA-13547: able to shadow entire view row if base column used in filter condition is modified
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a))");

        execute("USE " + keyspace());
        executeNet(version, "USE " + keyspace());

        createView("mv_test1",
                   "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL and c = 1  PRIMARY KEY (a, b)");
        createView("mv_test2",
                   "CREATE MATERIALIZED VIEW %s AS SELECT c, d FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL and c = 1 and d = 1 PRIMARY KEY (a, b)");
        createView("mv_test3",
                   "CREATE MATERIALIZED VIEW %s AS SELECT a, b, c, d FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a, b)");
        createView("mv_test4",
                   "CREATE MATERIALIZED VIEW %s AS SELECT c FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL and c = 1 PRIMARY KEY (a, b)");
        createView("mv_test5",
                   "CREATE MATERIALIZED VIEW %s AS SELECT c FROM %%s WHERE a IS NOT NULL and d = 1 PRIMARY KEY (a, d)");
        createView("mv_test6",
                   "CREATE MATERIALIZED VIEW %s AS SELECT c FROM %%s WHERE a = 1 and d IS NOT NULL PRIMARY KEY (a, d)");

        waitForView(keyspace(), "mv_test1");
        waitForView(keyspace(), "mv_test2");
        waitForView(keyspace(), "mv_test3");
        waitForView(keyspace(), "mv_test4");
        waitForView(keyspace(), "mv_test5");
        waitForView(keyspace(), "mv_test6");

        Keyspace ks = Keyspace.open(keyspace());
        ks.getColumnFamilyStore("mv_test1").disableAutoCompaction();
        ks.getColumnFamilyStore("mv_test2").disableAutoCompaction();
        ks.getColumnFamilyStore("mv_test3").disableAutoCompaction();
        ks.getColumnFamilyStore("mv_test4").disableAutoCompaction();
        ks.getColumnFamilyStore("mv_test5").disableAutoCompaction();
        ks.getColumnFamilyStore("mv_test6").disableAutoCompaction();

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) using timestamp 0", 1, 1, 1, 1);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        // views should be updated.
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test1"), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test2"), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test3"), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test4"), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test5"), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test6"), row(1, 1, 1));

        updateView("UPDATE %s using timestamp 1 set c = ? WHERE a=?", 0, 1);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowCount(execute("SELECT * FROM mv_test1"), 0);
        assertRowCount(execute("SELECT * FROM mv_test2"), 0);
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test3"), row(1, 1, 0, 1));
        assertRowCount(execute("SELECT * FROM mv_test4"), 0);
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test5"), row(1, 1, 0));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test6"), row(1, 1, 0));

        updateView("UPDATE %s using timestamp 2 set c = ? WHERE a=?", 1, 1);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        // row should be back in views.
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test1"), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test2"), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test3"), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test4"), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test5"), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test6"), row(1, 1, 1));

        updateView("UPDATE %s using timestamp 3 set d = ? WHERE a=?", 0, 1);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test1"), row(1, 1, 1, 0));
        assertRowCount(execute("SELECT * FROM mv_test2"), 0);
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test3"), row(1, 1, 1, 0));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test4"), row(1, 1, 1));
        assertRowCount(execute("SELECT * FROM mv_test5"), 0);
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test6"), row(1, 0, 1));

        updateView("UPDATE %s using timestamp 4 set c = ? WHERE a=?", 0, 1);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowCount(execute("SELECT * FROM mv_test1"), 0);
        assertRowCount(execute("SELECT * FROM mv_test2"), 0);
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test3"), row(1, 1, 0, 0));
        assertRowCount(execute("SELECT * FROM mv_test4"), 0);
        assertRowCount(execute("SELECT * FROM mv_test5"), 0);
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test6"), row(1, 0, 0));

        updateView("UPDATE %s using timestamp 5 set d = ? WHERE a=?", 1, 1);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        // should not update as c=0
        assertRowCount(execute("SELECT * FROM mv_test1"), 0);
        assertRowCount(execute("SELECT * FROM mv_test2"), 0);
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test3"), row(1, 1, 0, 1));
        assertRowCount(execute("SELECT * FROM mv_test4"), 0);
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test5"), row(1, 1, 0));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test6"), row(1, 1, 0));

        updateView("UPDATE %s using timestamp 6 set c = ? WHERE a=?", 1, 1);

        // row should be back in views.
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test1"), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test2"), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test3"), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test4"), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test5"), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test6"), row(1, 1, 1));

        updateView("UPDATE %s using timestamp 7 set b = ? WHERE a=?", 2, 1);
        if (flush)
        {
            FBUtilities.waitOnFutures(ks.flush());
            for (String view : views)
                ks.getColumnFamilyStore(view).forceMajorCompaction();
        }
        // row should be back in views.
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test1"), row(1, 2, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test2"), row(1, 2, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test3"), row(1, 2, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test4"), row(1, 2, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test5"), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test6"), row(1, 1, 1));

        updateView("DELETE b, c FROM %s using timestamp 6 WHERE a=?", 1);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT * FROM %s"), row(1, 2, null, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test1"));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test2"));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test3"), row(1, 2, null, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test4"));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test5"), row(1, 1, null));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test6"), row(1, 1, null));

        updateView("DELETE FROM %s using timestamp 8 where a=?", 1);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowCount(execute("SELECT * FROM mv_test1"), 0);
        assertRowCount(execute("SELECT * FROM mv_test2"), 0);
        assertRowCount(execute("SELECT * FROM mv_test3"), 0);
        assertRowCount(execute("SELECT * FROM mv_test4"), 0);
        assertRowCount(execute("SELECT * FROM mv_test5"), 0);
        assertRowCount(execute("SELECT * FROM mv_test6"), 0);

        updateView("UPDATE %s using timestamp 9 set b = ?,c = ? where a=?", 1, 1, 1); // upsert
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test1"), row(1, 1, 1, null));
        assertRows(execute("SELECT * FROM mv_test2"));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test3"), row(1, 1, 1, null));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test4"), row(1, 1, 1));
        assertRows(execute("SELECT * FROM mv_test5"));
        assertRows(execute("SELECT * FROM mv_test6"));

        updateView("DELETE FROM %s using timestamp 10 where a=?", 1);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowCount(execute("SELECT * FROM mv_test1"), 0);
        assertRowCount(execute("SELECT * FROM mv_test2"), 0);
        assertRowCount(execute("SELECT * FROM mv_test3"), 0);
        assertRowCount(execute("SELECT * FROM mv_test4"), 0);
        assertRowCount(execute("SELECT * FROM mv_test5"), 0);
        assertRowCount(execute("SELECT * FROM mv_test6"), 0);

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) using timestamp 11", 1, 1, 1, 1);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        // row should be back in views.
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test1"), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test2"), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test3"), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test4"), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test5"), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test6"), row(1, 1, 1));

        updateView("DELETE FROM %s using timestamp 12 where a=?", 1);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowCount(execute("SELECT * FROM mv_test1"), 0);
        assertRowCount(execute("SELECT * FROM mv_test2"), 0);
        assertRowCount(execute("SELECT * FROM mv_test3"), 0);
        assertRowCount(execute("SELECT * FROM mv_test4"), 0);
        assertRowCount(execute("SELECT * FROM mv_test5"), 0);
        assertRowCount(execute("SELECT * FROM mv_test6"), 0);

        dropView("mv_test1");
        dropView("mv_test2");
        dropView("mv_test3");
        dropView("mv_test4");
        dropView("mv_test5");
        dropView("mv_test6");
        dropTable("DROP TABLE %s");
    }

    // TODO will revise the non-pk filter condition in MV, see CASSANDRA-11500
    @Ignore
    @Test
    public void testMVFilteringWithComplexColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, l list<int>, s set<int>, m map<int,int>, PRIMARY KEY (a, b))");

        execute("USE " + keyspace());
        executeNet(version, "USE " + keyspace());

        createView("mv_test1",
                   "CREATE MATERIALIZED VIEW %s AS SELECT a,b,c FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL "
                   + "and l contains (1) AND s contains (1) AND m contains key (1) PRIMARY KEY (a, b, c)");
        createView("mv_test2",
                   "CREATE MATERIALIZED VIEW %s AS SELECT a,b FROM %%s WHERE a IS NOT NULL and b IS NOT NULL AND l contains (1) PRIMARY KEY (a, b)");
        createView("mv_test3",
                   "CREATE MATERIALIZED VIEW %s AS SELECT a,b FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND s contains (1) PRIMARY KEY (a, b)");
        createView("mv_test4",
                   "CREATE MATERIALIZED VIEW %s AS SELECT a,b FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND m contains key (1) PRIMARY KEY (a, b)");

        waitForView(keyspace(), "mv_test1");
        waitForView(keyspace(), "mv_test2");
        waitForView(keyspace(), "mv_test3");
        waitForView(keyspace(), "mv_test4");

        // not able to drop base column filtered in view
        assertInvalidMessage("Cannot drop column l, depended on by materialized views", "ALTER TABLE %s DROP l");
        assertInvalidMessage("Cannot drop column s, depended on by materialized views", "ALTER TABLE %S DROP s");
        assertInvalidMessage("Cannot drop column m, depended on by materialized views", "ALTER TABLE %s DROP m");

        Keyspace ks = Keyspace.open(keyspace());
        ks.getColumnFamilyStore("mv_test1").disableAutoCompaction();
        ks.getColumnFamilyStore("mv_test2").disableAutoCompaction();
        ks.getColumnFamilyStore("mv_test3").disableAutoCompaction();
        ks.getColumnFamilyStore("mv_test4").disableAutoCompaction();

        execute("INSERT INTO %s (a, b, c, l, s, m) VALUES (?, ?, ?, ?, ?, ?) ",
                1,
                1,
                1,
                list(1, 1, 2),
                set(1, 2),
                map(1, 1, 2, 2));
        FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test1"), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test2"), row(1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test3"), row(1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test4"), row(1, 1));

        execute("UPDATE %s SET l=l-[1] WHERE a = 1 AND b = 1" );
        FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test1"));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test2"));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test3"), row(1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test4"), row(1, 1));

        execute("UPDATE %s SET s=s-{2}, m=m-{2} WHERE a = 1 AND b = 1");
        FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT a,b,c FROM %s"), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test1"));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test2"));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test3"), row(1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test4"), row(1, 1));

        execute("UPDATE %s SET  m=m-{1} WHERE a = 1 AND b = 1");
        FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT a,b,c FROM %s"), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test1"));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test2"));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test3"), row(1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test4"));

        // filter conditions result not changed
        execute("UPDATE %s SET  l=l+[2], s=s-{0}, m=m+{3:3} WHERE a = 1 AND b = 1");
        FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT a,b,c FROM %s"), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test1"));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test2"));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test3"), row(1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM mv_test4"));
    }

    @Test
    public void testMVCreationSelectRestrictions() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY((a, b), c, d))");

        execute("USE " + keyspace());
        executeNet(version, "USE " + keyspace());

        // IS NOT NULL is required on all PK statements that are not otherwise restricted
        List<String> badStatements = Arrays.asList(
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE b IS NOT NULL AND c IS NOT NULL AND d is NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND c IS NOT NULL AND d is NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND d is NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND c is NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = ? AND b IS NOT NULL AND c is NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = blobAsInt(?) AND b IS NOT NULL AND c is NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s PRIMARY KEY (a, b, c, d)"
        );

        for (String badStatement : badStatements)
        {
            try
            {
                createView("mv1_test", badStatement);
                Assert.fail("Create MV statement should have failed due to missing IS NOT NULL restriction: " + badStatement);
            }
            catch (InvalidQueryException exc) {}
        }

        List<String> goodStatements = Arrays.asList(
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = 1 AND b = 1 AND c IS NOT NULL AND d is NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND c = 1 AND d IS NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND c = 1 AND d = 1 PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = 1 AND b = 1 AND c = 1 AND d = 1 PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = 1 AND b = 1 AND c > 1 AND d IS NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = 1 AND b = 1 AND c = 1 AND d IN (1, 2, 3) PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = 1 AND b = 1 AND (c, d) = (1, 1) PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = 1 AND b = 1 AND (c, d) > (1, 1) PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = 1 AND b = 1 AND (c, d) IN ((1, 1), (2, 2)) PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = (int) 1 AND b = 1 AND c = 1 AND d = 1 PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a = blobAsInt(intAsBlob(1)) AND b = 1 AND c = 1 AND d = 1 PRIMARY KEY ((a, b), c, d)"
        );

        for (int i = 0; i < goodStatements.size(); i++)
        {
            try
            {
                createView("mv" + i + "_test", goodStatements.get(i));
            }
            catch (Exception e)
            {
                throw new RuntimeException("MV creation failed: " + goodStatements.get(i), e);
            }

            try
            {
                executeNet(version, "ALTER MATERIALIZED VIEW mv" + i + "_test WITH compaction = { 'class' : 'LeveledCompactionStrategy' }");
            }
            catch (Exception e)
            {
                throw new RuntimeException("MV alter failed: " + goodStatements.get(i), e);
            }
        }
    }

    @Test
    public void testCaseSensitivity() throws Throwable
    {
        createTable("CREATE TABLE %s (\"theKey\" int, \"theClustering\" int, \"the\"\"Value\" int, PRIMARY KEY (\"theKey\", \"theClustering\"))");

        execute("USE " + keyspace());
        executeNet(version, "USE " + keyspace());

        execute("INSERT INTO %s (\"theKey\", \"theClustering\", \"the\"\"Value\") VALUES (?, ?, ?)", 0, 0, 0);
        execute("INSERT INTO %s (\"theKey\", \"theClustering\", \"the\"\"Value\") VALUES (?, ?, ?)", 0, 1, 0);
        execute("INSERT INTO %s (\"theKey\", \"theClustering\", \"the\"\"Value\") VALUES (?, ?, ?)", 1, 0, 0);
        execute("INSERT INTO %s (\"theKey\", \"theClustering\", \"the\"\"Value\") VALUES (?, ?, ?)", 1, 1, 0);

        createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s " +
                              "WHERE \"theKey\" = 1 AND \"theClustering\" = 1 AND \"the\"\"Value\" IS NOT NULL " +
                              "PRIMARY KEY (\"theKey\", \"theClustering\")");

        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test"))
            Thread.sleep(10);
        createView("mv_test2", "CREATE MATERIALIZED VIEW %s AS SELECT \"theKey\", \"theClustering\", \"the\"\"Value\" FROM %%s " +
                               "WHERE \"theKey\" = 1 AND \"theClustering\" = 1 AND \"the\"\"Value\" IS NOT NULL " +
                               "PRIMARY KEY (\"theKey\", \"theClustering\")");
        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test2"))
            Thread.sleep(10);

        for (String mvname : Arrays.asList("mv_test", "mv_test2"))
        {
            assertRowsIgnoringOrder(execute("SELECT \"theKey\", \"theClustering\", \"the\"\"Value\" FROM " + mvname),
                                    row(1, 1, 0)
            );
        }

        executeNet(version, "ALTER TABLE %s RENAME \"theClustering\" TO \"Col\"");

        for (String mvname : Arrays.asList("mv_test", "mv_test2"))
        {
            assertRowsIgnoringOrder(execute("SELECT \"theKey\", \"Col\", \"the\"\"Value\" FROM " + mvname),
                                    row(1, 1, 0)
            );
        }
    }

    @Test
    public void testFilterWithFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");

        execute("USE " + keyspace());
        executeNet(version, "USE " + keyspace());

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 0, 2);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, 3);

        createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s " +
                              "WHERE a = blobAsInt(intAsBlob(1)) AND b IS NOT NULL " +
                              "PRIMARY KEY (a, b)");

        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test"))
            Thread.sleep(10);

        assertRows(execute("SELECT a, b, c FROM mv_test"),
                   row(1, 0, 2),
                   row(1, 1, 3)
        );

        executeNet(version, "ALTER TABLE %s RENAME a TO foo");

        assertRows(execute("SELECT foo, b, c FROM mv_test"),
                   row(1, 0, 2),
                   row(1, 1, 3)
        );
    }

    @Test
    public void testFilterWithTypecast() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");

        execute("USE " + keyspace());
        executeNet(version, "USE " + keyspace());

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 0, 2);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, 3);

        createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s " +
                              "WHERE a = (int) 1 AND b IS NOT NULL " +
                              "PRIMARY KEY (a, b)");

        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test"))
            Thread.sleep(10);

        assertRows(execute("SELECT a, b, c FROM mv_test"),
                   row(1, 0, 2),
                   row(1, 1, 3)
        );

        executeNet(version, "ALTER TABLE %s RENAME a TO foo");

        assertRows(execute("SELECT foo, b, c FROM mv_test"),
                   row(1, 0, 2),
                   row(1, 1, 3)
        );
    } 
}
