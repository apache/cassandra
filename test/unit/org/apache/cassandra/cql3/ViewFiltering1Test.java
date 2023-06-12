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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.config.CassandraRelevantProperties.MV_ALLOW_FILTERING_NONKEY_COLUMNS_UNSAFE;

/* ViewFilteringTest class has been split into multiple ones because of timeout issues (CASSANDRA-16670, CASSANDRA-17167)
 * Any changes here check if they apply to the other classes
 * - ViewFilteringPKTest
 * - ViewFilteringClustering1Test
 * - ViewFilteringClustering2Test
 * - ViewFilteringTest
 * - ...
 * - ViewFiltering*Test
 */
public class ViewFiltering1Test extends ViewAbstractParameterizedTest
{
    @BeforeClass
    public static void startup()
    {
        ViewAbstractParameterizedTest.startup();
        MV_ALLOW_FILTERING_NONKEY_COLUMNS_UNSAFE.setBoolean(true);
    }

    @AfterClass
    public static void tearDown()
    {
        MV_ALLOW_FILTERING_NONKEY_COLUMNS_UNSAFE.setBoolean(false);
    }

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

        String mv1 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                "WHERE a IS NOT NULL AND b IS NOT NULL and c = 1  PRIMARY KEY (a, b)");
        String mv2 = createView("CREATE MATERIALIZED VIEW %s AS SELECT c, d FROM %s " +
                                "WHERE a IS NOT NULL AND b IS NOT NULL and c = 1 and d = 1 PRIMARY KEY (a, b)");
        String mv3 = createView("CREATE MATERIALIZED VIEW %s AS SELECT a, b, c, d FROM %%s " +
                                "WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a, b)");
        String mv4 = createView("CREATE MATERIALIZED VIEW %s AS SELECT c FROM %s " +
                                "WHERE a IS NOT NULL AND b IS NOT NULL and c = 1 PRIMARY KEY (a, b)");
        String mv5 = createView("CREATE MATERIALIZED VIEW %s AS SELECT c FROM %s " +
                                "WHERE a IS NOT NULL and d = 1 PRIMARY KEY (a, d)");
        String mv6 = createView("CREATE MATERIALIZED VIEW %s AS SELECT c FROM %s " +
                                "WHERE a = 1 and d IS NOT NULL PRIMARY KEY (a, d)");

        Keyspace ks = Keyspace.open(keyspace());
        ks.getColumnFamilyStore(mv1).disableAutoCompaction();
        ks.getColumnFamilyStore(mv2).disableAutoCompaction();
        ks.getColumnFamilyStore(mv3).disableAutoCompaction();
        ks.getColumnFamilyStore(mv4).disableAutoCompaction();
        ks.getColumnFamilyStore(mv5).disableAutoCompaction();
        ks.getColumnFamilyStore(mv6).disableAutoCompaction();

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) using timestamp 0", 1, 1, 1, 1);
        if (flush)
            Util.flush(ks);

        // views should be updated.
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv1), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv2), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv3), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv4), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv5), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv6), row(1, 1, 1));

        updateView("UPDATE %s using timestamp 1 set c = ? WHERE a=?", 0, 1);
        if (flush)
            Util.flush(ks);

        assertRowCount(execute("SELECT * FROM " + mv1), 0);
        assertRowCount(execute("SELECT * FROM " + mv2), 0);
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv3), row(1, 1, 0, 1));
        assertRowCount(execute("SELECT * FROM " + mv4), 0);
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv5), row(1, 1, 0));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv6), row(1, 1, 0));

        updateView("UPDATE %s using timestamp 2 set c = ? WHERE a=?", 1, 1);
        if (flush)
            Util.flush(ks);

        // row should be back in views.
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv1), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv2), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv3), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv4), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv5), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv6), row(1, 1, 1));

        updateView("UPDATE %s using timestamp 3 set d = ? WHERE a=?", 0, 1);
        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv1), row(1, 1, 1, 0));
        assertRowCount(execute("SELECT * FROM " + mv2), 0);
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv3), row(1, 1, 1, 0));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv4), row(1, 1, 1));
        assertRowCount(execute("SELECT * FROM " + mv5), 0);
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv6), row(1, 0, 1));

        updateView("UPDATE %s using timestamp 4 set c = ? WHERE a=?", 0, 1);
        if (flush)
            Util.flush(ks);

        assertRowCount(execute("SELECT * FROM " + mv1), 0);
        assertRowCount(execute("SELECT * FROM " + mv2), 0);
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv3), row(1, 1, 0, 0));
        assertRowCount(execute("SELECT * FROM " + mv4), 0);
        assertRowCount(execute("SELECT * FROM " + mv5), 0);
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv6), row(1, 0, 0));

        updateView("UPDATE %s using timestamp 5 set d = ? WHERE a=?", 1, 1);
        if (flush)
            Util.flush(ks);

        // should not update as c=0
        assertRowCount(execute("SELECT * FROM " + mv1), 0);
        assertRowCount(execute("SELECT * FROM " + mv2), 0);
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv3), row(1, 1, 0, 1));
        assertRowCount(execute("SELECT * FROM " + mv4), 0);
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv5), row(1, 1, 0));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv6), row(1, 1, 0));

        updateView("UPDATE %s using timestamp 6 set c = ? WHERE a=?", 1, 1);

        // row should be back in views.
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv1), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv2), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv3), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv4), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv5), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv6), row(1, 1, 1));

        updateView("UPDATE %s using timestamp 7 set b = ? WHERE a=?", 2, 1);
        if (flush)
        {
            Util.flush(ks);
            for (String view : getViews())
                ks.getColumnFamilyStore(view).forceMajorCompaction();
        }
        // row should be back in views.
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv1), row(1, 2, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv2), row(1, 2, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv3), row(1, 2, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv4), row(1, 2, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv5), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv6), row(1, 1, 1));

        updateView("DELETE b, c FROM %s using timestamp 6 WHERE a=?", 1);
        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(execute("SELECT * FROM %s"), row(1, 2, null, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv2));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv3), row(1, 2, null, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv4));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv5), row(1, 1, null));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv6), row(1, 1, null));

        updateView("DELETE FROM %s using timestamp 8 where a=?", 1);
        if (flush)
            Util.flush(ks);

        assertRowCount(execute("SELECT * FROM " + mv1), 0);
        assertRowCount(execute("SELECT * FROM " + mv2), 0);
        assertRowCount(execute("SELECT * FROM " + mv3), 0);
        assertRowCount(execute("SELECT * FROM " + mv4), 0);
        assertRowCount(execute("SELECT * FROM " + mv5), 0);
        assertRowCount(execute("SELECT * FROM " + mv6), 0);

        updateView("UPDATE %s using timestamp 9 set b = ?,c = ? where a=?", 1, 1, 1); // upsert
        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv1), row(1, 1, 1, null));
        assertRows(execute("SELECT * FROM " + mv2));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv3), row(1, 1, 1, null));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv4), row(1, 1, 1));
        assertRows(execute("SELECT * FROM " + mv5));
        assertRows(execute("SELECT * FROM " + mv6));

        updateView("DELETE FROM %s using timestamp 10 where a=?", 1);
        if (flush)
            Util.flush(ks);

        assertRowCount(execute("SELECT * FROM " + mv1), 0);
        assertRowCount(execute("SELECT * FROM " + mv2), 0);
        assertRowCount(execute("SELECT * FROM " + mv3), 0);
        assertRowCount(execute("SELECT * FROM " + mv4), 0);
        assertRowCount(execute("SELECT * FROM " + mv5), 0);
        assertRowCount(execute("SELECT * FROM " + mv6), 0);

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) using timestamp 11", 1, 1, 1, 1);
        if (flush)
            Util.flush(ks);

        // row should be back in views.
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv1), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv2), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv3), row(1, 1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv4), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv5), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv6), row(1, 1, 1));

        updateView("DELETE FROM %s using timestamp 12 where a=?", 1);
        if (flush)
            Util.flush(ks);

        assertRowCount(execute("SELECT * FROM " + mv1), 0);
        assertRowCount(execute("SELECT * FROM " + mv2), 0);
        assertRowCount(execute("SELECT * FROM " + mv3), 0);
        assertRowCount(execute("SELECT * FROM " + mv4), 0);
        assertRowCount(execute("SELECT * FROM " + mv5), 0);
        assertRowCount(execute("SELECT * FROM " + mv6), 0);

        dropView(mv1);
        dropView(mv2);
        dropView(mv3);
        dropView(mv4);
        dropView(mv5);
        dropView(mv6);
        dropTable("DROP TABLE %s");
    }

    // TODO will revise the non-pk filter condition in MV, see CASSANDRA-11500
    @Ignore
    @Test
    public void testMVFilteringWithComplexColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, l list<int>, s set<int>, m map<int,int>, PRIMARY KEY (a, b))");

        String mv1 = createView("CREATE MATERIALIZED VIEW %s AS SELECT a,b,c FROM %%s " +
                                "WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL AND l contains (1) " +
                                "AND s contains (1) AND m contains key (1) " +
                                "PRIMARY KEY (a, b, c)");
        String mv2 = createView("CREATE MATERIALIZED VIEW %s AS SELECT a,b FROM %%s " +
                                "WHERE a IS NOT NULL and b IS NOT NULL AND l contains (1) " +
                                "PRIMARY KEY (a, b)");
        String mv3 = createView("CREATE MATERIALIZED VIEW %s AS SELECT a,b FROM %%s " +
                                "WHERE a IS NOT NULL AND b IS NOT NULL AND s contains (1) " +
                                "PRIMARY KEY (a, b)");
        String mv4 = createView("CREATE MATERIALIZED VIEW %s AS SELECT a,b FROM %%s " +
                                "WHERE a IS NOT NULL AND b IS NOT NULL AND m contains key (1) " +
                                "PRIMARY KEY (a, b)");

        // not able to drop base column filtered in view
        assertInvalidMessage("Cannot drop column l, depended on by materialized views", "ALTER TABLE %s DROP l");
        assertInvalidMessage("Cannot drop column s, depended on by materialized views", "ALTER TABLE %S DROP s");
        assertInvalidMessage("Cannot drop column m, depended on by materialized views", "ALTER TABLE %s DROP m");

        Keyspace ks = Keyspace.open(keyspace());
        ks.getColumnFamilyStore(mv1).disableAutoCompaction();
        ks.getColumnFamilyStore(mv2).disableAutoCompaction();
        ks.getColumnFamilyStore(mv3).disableAutoCompaction();
        ks.getColumnFamilyStore(mv4).disableAutoCompaction();

        execute("INSERT INTO %s (a, b, c, l, s, m) VALUES (?, ?, ?, ?, ?, ?) ",
                1,
                1,
                1,
                list(1, 1, 2),
                set(1, 2),
                map(1, 1, 2, 2));
        Util.flush(ks);

        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv1), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv2), row(1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv3), row(1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv4), row(1, 1));

        execute("UPDATE %s SET l=l-[1] WHERE a = 1 AND b = 1");
        Util.flush(ks);

        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv2));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv3), row(1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv4), row(1, 1));

        execute("UPDATE %s SET s=s-{2}, m=m-{2} WHERE a = 1 AND b = 1");
        Util.flush(ks);

        assertRowsIgnoringOrder(execute("SELECT a,b,c FROM %s"), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv2));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv3), row(1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv4), row(1, 1));

        execute("UPDATE %s SET  m=m-{1} WHERE a = 1 AND b = 1");
        Util.flush(ks);

        assertRowsIgnoringOrder(execute("SELECT a,b,c FROM %s"), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv2));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv3), row(1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv4));

        // filter conditions result not changed
        execute("UPDATE %s SET  l=l+[2], s=s-{0}, m=m+{3:3} WHERE a = 1 AND b = 1");
        Util.flush(ks);

        assertRowsIgnoringOrder(execute("SELECT a,b,c FROM %s"), row(1, 1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv2));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv3), row(1, 1));
        assertRowsIgnoringOrder(execute("SELECT * FROM " + mv4));
    }

    @Test
    public void testMVCreationSelectRestrictions() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY((a, b), c, d))");

        // IS NOT NULL is required on all PK statements that are not otherwise restricted
        List<String> badStatements = Arrays.asList(
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE b IS NOT NULL AND c IS NOT NULL AND d is NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND c IS NOT NULL AND d is NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND d is NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND c is NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = ? AND b IS NOT NULL AND c is NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = blob_as_int(?) AND b IS NOT NULL AND c is NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s PRIMARY KEY (a, b, c, d)"
        );

        for (String badStatement : badStatements)
        {
            try
            {
                createView(badStatement);
                Assert.fail("Create MV statement should have failed due to missing IS NOT NULL restriction: " + badStatement);
            }
            catch (RuntimeException e)
            {
                Assert.assertSame(InvalidRequestException.class, e.getCause().getClass());
            }
        }

        List<String> goodStatements = Arrays.asList(
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = 1 AND b = 1 AND c IS NOT NULL AND d is NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND c = 1 AND d IS NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND c = 1 AND d = 1 PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = 1 AND b = 1 AND c = 1 AND d = 1 PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = 1 AND b = 1 AND c > 1 AND d IS NOT NULL PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = 1 AND b = 1 AND c = 1 AND d IN (1, 2, 3) PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = 1 AND b = 1 AND (c, d) = (1, 1) PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = 1 AND b = 1 AND (c, d) > (1, 1) PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = 1 AND b = 1 AND (c, d) IN ((1, 1), (2, 2)) PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = (int) 1 AND b = 1 AND c = 1 AND d = 1 PRIMARY KEY ((a, b), c, d)",
        "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a = blob_as_int(int_as_blob(1)) AND b = 1 AND c = 1 AND d = 1 PRIMARY KEY ((a, b), c, d)"
        );

        for (int i = 0; i < goodStatements.size(); i++)
        {
            String mv;
            try
            {
                mv = createView(goodStatements.get(i));
            }
            catch (Exception e)
            {
                throw new RuntimeException("MV creation failed: " + goodStatements.get(i), e);
            }

            try
            {
                executeNet("ALTER MATERIALIZED VIEW " + mv + " WITH compaction = { 'class' : 'LeveledCompactionStrategy' }");
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

        execute("INSERT INTO %s (\"theKey\", \"theClustering\", \"the\"\"Value\") VALUES (?, ?, ?)", 0, 0, 0);
        execute("INSERT INTO %s (\"theKey\", \"theClustering\", \"the\"\"Value\") VALUES (?, ?, ?)", 0, 1, 0);
        execute("INSERT INTO %s (\"theKey\", \"theClustering\", \"the\"\"Value\") VALUES (?, ?, ?)", 1, 0, 0);
        execute("INSERT INTO %s (\"theKey\", \"theClustering\", \"the\"\"Value\") VALUES (?, ?, ?)", 1, 1, 0);

        String mv1 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                "WHERE \"theKey\" = 1 AND \"theClustering\" = 1 AND \"the\"\"Value\" IS NOT NULL " +
                                "PRIMARY KEY (\"theKey\", \"theClustering\")");

        String mv2 = createView("CREATE MATERIALIZED VIEW %s AS SELECT \"theKey\", \"theClustering\", \"the\"\"Value\" FROM %s " +
                                "WHERE \"theKey\" = 1 AND \"theClustering\" = 1 AND \"the\"\"Value\" IS NOT NULL " +
                                "PRIMARY KEY (\"theKey\", \"theClustering\")");

        for (String mvname : Arrays.asList(mv1, mv2))
        {
            assertRowsIgnoringOrder(execute("SELECT \"theKey\", \"theClustering\", \"the\"\"Value\" FROM " + mvname),
                                    row(1, 1, 0));
        }

        executeNet("ALTER TABLE %s RENAME \"theClustering\" TO \"Col\"");

        for (String mvname : Arrays.asList(mv1, mv2))
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

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 0, 2);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, 3);

        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                   "WHERE a = blob_as_int(int_as_blob(1)) AND b IS NOT NULL " +
                   "PRIMARY KEY (a, b)");

        assertRows(executeView("SELECT a, b, c FROM %s"),
                   row(1, 0, 2),
                   row(1, 1, 3)
        );

        executeNet("ALTER TABLE %s RENAME a TO foo");

        assertRows(executeView("SELECT foo, b, c FROM %s"),
                   row(1, 0, 2),
                   row(1, 1, 3)
        );
    }

    @Test
    public void testFilterWithTypecast() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");

        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 0, 2);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, 3);

        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                   "WHERE a = (int) 1 AND b IS NOT NULL " +
                   "PRIMARY KEY (a, b)");

        assertRows(executeView("SELECT a, b, c FROM %s"),
                   row(1, 0, 2),
                   row(1, 1, 3)
        );

        executeNet("ALTER TABLE %s RENAME a TO foo");

        assertRows(executeView("SELECT foo, b, c FROM %s"),
                   row(1, 0, 2),
                   row(1, 1, 3)
        );
    }
}
