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

import java.util.HashSet;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.assertj.core.api.Assertions;

import static org.junit.Assert.assertTrue;

/*
 * This test class was too large and used to timeout CASSANDRA-16777. We're splitting it into:
 * - ViewTest
 * - ViewPKTest
 * - ViewRangesTest
 * - ViewTimesTest
 */
public class ViewPKTest extends ViewAbstractTest
{
    @Test
    public void testPartitionTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, c1 int , val int, PRIMARY KEY (k1, c1))");

        createView("CREATE MATERIALIZED VIEW %s AS SELECT k1, c1, val FROM %s " +
                   "WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND val IS NOT NULL " +
                   "PRIMARY KEY (val, k1, c1)");

        updateView("INSERT INTO %s (k1, c1, val) VALUES (1, 2, 200)");
        updateView("INSERT INTO %s (k1, c1, val) VALUES (1, 3, 300)");

        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, executeView("select * from %s").size());

        updateView("DELETE FROM %s WHERE k1 = 1");

        Assert.assertEquals(0, execute("select * from %s").size());
        Assert.assertEquals(0, executeView("select * from %s").size());
    }

    @Test
    public void createMvWithUnrestrictedPKParts()
    {
        createTable("CREATE TABLE %s (k1 int, c1 int , val int, PRIMARY KEY (k1, c1))");

        createView("CREATE MATERIALIZED VIEW %s AS SELECT val, k1, c1 FROM %s " +
                   "WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND val IS NOT NULL " +
                   "PRIMARY KEY (val, k1, c1)");
    }

    @Test
    public void testClusteringKeyTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, c1 int , val int, PRIMARY KEY (k1, c1))");

        createView("CREATE MATERIALIZED VIEW %s AS SELECT k1, c1, val FROM %s " +
                   "WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND val IS NOT NULL " +
                   "PRIMARY KEY (val, k1, c1)");

        updateView("INSERT INTO %s (k1, c1, val) VALUES (1, 2, 200)");
        updateView("INSERT INTO %s (k1, c1, val) VALUES (1, 3, 300)");

        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, executeView("select * from %s").size());

        updateView("DELETE FROM %s WHERE k1 = 1 and c1 = 3");

        Assert.assertEquals(1, execute("select * from %s").size());
        Assert.assertEquals(1, executeView("select * from %s").size());
    }

    @Test
    public void testPrimaryKeyIsNotNull()
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "PRIMARY KEY((k, asciival)))");

        // Must include "IS NOT NULL" for primary keys
        try
        {
            createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s");
            Assert.fail("Should fail if no primary key is filtered as NOT NULL");
        }
        catch (Exception e)
        {
            Throwable cause = e.getCause();
            Assertions.assertThat(cause).isInstanceOf(SyntaxException.class);
            Assertions.assertThat(cause.getMessage()).contains("mismatched input");
        }

        // Must include both when the partition key is composite
        try
        {
            createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                       "WHERE bigintval IS NOT NULL AND asciival IS NOT NULL " +
                       "PRIMARY KEY (bigintval, k, asciival)");
            Assert.fail("Should fail if compound primary is not completely filtered as NOT NULL");
        }
        catch (Exception e)
        {
            Throwable cause = e.getCause();
            Assertions.assertThat(cause).isInstanceOf(InvalidRequestException.class);
            Assertions.assertThat(cause.getMessage()).contains("Primary key columns k must be restricted");
        }

        dropTable("DROP TABLE %s");

        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "PRIMARY KEY(k, asciival))");
        try
        {
            createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s");
            Assert.fail("Should fail if no primary key is filtered as NOT NULL");
        }
        catch (Exception e)
        {
            Throwable cause = e.getCause();
            Assertions.assertThat(cause).isInstanceOf(SyntaxException.class);
            Assertions.assertThat(cause.getMessage()).contains("mismatched input");
        }

        // Must still include both even when the partition key is composite
        try
        {
            createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                       "WHERE bigintval IS NOT NULL AND asciival IS NOT NULL " +
                       "PRIMARY KEY (bigintval, k, asciival)");
            Assert.fail("Should fail if compound primary is not completely filtered as NOT NULL");
        }
        catch (Exception e)
        {
            Throwable cause = e.getCause();
            Assertions.assertThat(cause).isInstanceOf(InvalidRequestException.class);
            Assertions.assertThat(cause.getMessage()).contains("Primary key columns k must be restricted");
        }
    }

    @Test
    public void testCompoundPartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "PRIMARY KEY((k, asciival)))");

        TableMetadata metadata = currentTableMetadata();

        for (ColumnMetadata def : new HashSet<>(metadata.columns()))
        {
            String asciival = def.name.toString().equals("asciival") ? "" : "AND asciival IS NOT NULL ";
            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + asciival + "PRIMARY KEY ("
                               + def.name + ", k" + (def.name.toString().equals("asciival") ? "" : ", asciival") + ")";
                createView("mv1_" + def.name, query);

                if (def.type.isMultiCell())
                    Assert.fail("MV on a multicell should fail " + def);
            }
            catch (Exception e)
            {
                if (!def.type.isMultiCell() && !def.isPartitionKey())
                    Assert.fail("MV creation failed on " + def);
            }

            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + asciival + " PRIMARY KEY ("
                               + def.name + ", asciival" + (def.name.toString().equals("k") ? "" : ", k") + ")";
                createView("mv2_" + def.name, query);

                if (def.type.isMultiCell())
                    Assert.fail("MV on a multicell should fail " + def);
            }
            catch (Exception e)
            {
                if (!def.type.isMultiCell() && !def.isPartitionKey())
                    Assert.fail("MV creation failed on " + def);
            }

            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + asciival + "PRIMARY KEY ((" + def.name + ", k), asciival)";
                createView("mv3_" + def.name, query);

                if (def.type.isMultiCell())
                    Assert.fail("MV on a multicell should fail " + def);
            }
            catch (Exception e)
            {
                if (!def.type.isMultiCell() && !def.isPartitionKey())
                    Assert.fail("MV creation failed on " + def);
            }

            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + asciival + "PRIMARY KEY ((" + def.name + ", k), asciival)";
                createView("mv3_" + def.name, query);

                Assert.fail("Should fail on duplicate name");
            }
            catch (Exception e)
            {
                Assertions.assertThat(e.getCause()).isInstanceOf(RequestValidationException.class);
            }

            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + asciival + "PRIMARY KEY ((" + def.name + ", k), nonexistentcolumn)";
                createView("mv4_" + def.name, query);
                Assert.fail("Should fail with unknown base column");
            }
            catch (Exception e)
            {
                Assertions.assertThat(e.getCause()).isInstanceOf(RequestValidationException.class);
            }
        }

        updateView("INSERT INTO %s (k, asciival, bigintval) VALUES (?, ?, from_json(?))", 0, "ascii text", "123123123123");
        updateView("INSERT INTO %s (k, asciival) VALUES (?, from_json(?))", 0, "\"ascii text\"");
        assertRows(execute("SELECT bigintval FROM %s WHERE k = ? and asciival = ?", 0, "ascii text"), row(123123123123L));

        //Check the MV
        assertRows(execute("SELECT k, bigintval from mv1_asciival WHERE asciival = ?", "ascii text"), row(0, 123123123123L));
        assertRows(execute("SELECT k, bigintval from mv2_k WHERE asciival = ? and k = ?", "ascii text", 0), row(0, 123123123123L));
        assertRows(execute("SELECT k from mv1_bigintval WHERE bigintval = ?", 123123123123L), row(0));
        assertRows(execute("SELECT asciival from mv3_bigintval where bigintval = ? AND k = ?", 123123123123L, 0), row("ascii text"));


        //UPDATE BASE
        updateView("INSERT INTO %s (k, asciival, bigintval) VALUES (?, ?, from_json(?))", 0, "ascii text", "1");
        assertRows(execute("SELECT bigintval FROM %s WHERE k = ? and asciival = ?", 0, "ascii text"), row(1L));

        //Check the MV
        assertRows(execute("SELECT k, bigintval from mv1_asciival WHERE asciival = ?", "ascii text"), row(0, 1L));
        assertRows(execute("SELECT k, bigintval from mv2_k WHERE asciival = ? and k = ?", "ascii text", 0), row(0, 1L));
        assertRows(execute("SELECT k from mv1_bigintval WHERE bigintval = ?", 123123123123L));
        assertRows(execute("SELECT asciival from mv3_bigintval where bigintval = ? AND k = ?", 123123123123L, 0));
        assertRows(execute("SELECT asciival from mv3_bigintval where bigintval = ? AND k = ?", 1L, 0), row("ascii text"));


        //test truncate also truncates all MV
        updateView("TRUNCATE %s");

        assertRows(execute("SELECT bigintval FROM %s WHERE k = ? and asciival = ?", 0, "ascii text"));
        assertRows(execute("SELECT k, bigintval from mv1_asciival WHERE asciival = ?", "ascii text"));
        assertRows(execute("SELECT k, bigintval from mv2_k WHERE asciival = ? and k = ?", "ascii text", 0));
        assertRows(execute("SELECT asciival from mv3_bigintval where bigintval = ? AND k = ?", 1L, 0));
    }

    @Test
    public void testClusteringOrder() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b, c))" +
                    "WITH CLUSTERING ORDER BY (b ASC, c DESC)");

        executeNet("USE " + keyspace());

        String mv1 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, b, c) WITH CLUSTERING ORDER BY (b DESC, c ASC)");
        String mv2 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, c, b) WITH CLUSTERING ORDER BY (c ASC, b ASC)");
        String mv3 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, b, c)");
        String mv4 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, c, b) WITH CLUSTERING ORDER BY (c DESC, b ASC)");

        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 1);
        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 2, 2, 2);

        ResultSet mvRows = executeNet("SELECT b FROM " + mv1);
        assertRowsNet(mvRows, row(2), row(1));

        mvRows = executeNet("SELECT c FROM " + mv2);
        assertRowsNet(mvRows, row(1), row(2));

        mvRows = executeNet("SELECT b FROM " + mv3);
        assertRowsNet(mvRows, row(1), row(2));

        mvRows = executeNet("SELECT c FROM " + mv4);
        assertRowsNet(mvRows, row(2), row(1));
    }

    @Test
    public void testPrimaryKeyOnlyTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "PRIMARY KEY (a, b))");

        executeNet("USE " + keyspace());

        // Cannot use SELECT *, as those are always handled by the includeAll shortcut in View.updateAffectsView
        createView("CREATE MATERIALIZED VIEW %s AS SELECT a, b FROM %s " +
                   "WHERE a IS NOT NULL AND b IS NOT NULL " +
                   "PRIMARY KEY (b, a)");

        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1);

        ResultSet mvRows = executeViewNet("SELECT a, b FROM %s");
        assertRowsNet(mvRows, row(1, 1));
    }

    @Test
    public void testPartitionKeyOnlyTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "PRIMARY KEY ((a, b)))");

        executeNet("USE " + keyspace());

        // Cannot use SELECT *, as those are always handled by the includeAll shortcut in View.updateAffectsView
        createView("CREATE MATERIALIZED VIEW %s AS SELECT a, b FROM %s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b, a)");

        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1);

        ResultSet mvRows = executeViewNet("SELECT a, b FROM %s");
        assertRowsNet(mvRows, row(1, 1));
    }

    @Test
    public void testDeleteSingleColumnInViewClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b))");

        executeNet("USE " + keyspace());
        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                   "WHERE a IS NOT NULL AND b IS NOT NULL AND d IS NOT NULL " +
                   "PRIMARY KEY (a, d, b)");

        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
        ResultSet mvRows = executeViewNet("SELECT a, d, b, c FROM %s");
        assertRowsNet(mvRows, row(0, 0, 0, 0));

        updateView("DELETE c FROM %s WHERE a = ? AND b = ?", 0, 0);
        mvRows = executeViewNet("SELECT a, d, b, c FROM %s");
        assertRowsNet(mvRows, row(0, 0, 0, null));

        updateView("DELETE d FROM %s WHERE a = ? AND b = ?", 0, 0);
        mvRows = executeViewNet("SELECT a, d, b FROM %s");
        assertTrue(mvRows.isExhausted());
    }

    @Test
    public void testDeleteSingleColumnInViewPartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b))");

        executeNet("USE " + keyspace());
        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                   "WHERE a IS NOT NULL AND b IS NOT NULL AND d IS NOT NULL " +
                   "PRIMARY KEY (d, a, b)");

        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
        ResultSet mvRows = executeViewNet("SELECT a, d, b, c FROM %s");
        assertRowsNet(mvRows, row(0, 0, 0, 0));

        updateView("DELETE c FROM %s WHERE a = ? AND b = ?", 0, 0);
        mvRows = executeViewNet("SELECT a, d, b, c FROM %s");
        assertRowsNet(mvRows, row(0, 0, 0, null));

        updateView("DELETE d FROM %s WHERE a = ? AND b = ?", 0, 0);
        mvRows = executeViewNet("SELECT a, d, b FROM %s");
        assertTrue(mvRows.isExhausted());
    }

    @Test
    public void testMultipleNonPrimaryKeysInView()
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "e int," +
                    "PRIMARY KEY ((a, b), c))");

        try
        {
            createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL AND d IS NOT NULL AND e IS NOT NULL PRIMARY KEY ((d, a), b, e, c)");
            Assert.fail("Should have rejected a query including multiple non-primary key base columns");
        }
        catch (Exception e)
        {
            Throwable cause = e.getCause();
            Assertions.assertThat(cause).isInstanceOf(InvalidRequestException.class);
            Assertions.assertThat(cause.getMessage()).contains("Cannot include more than one non-primary key column");
        }

        try
        {
            createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL AND d IS NOT NULL AND e IS NOT NULL PRIMARY KEY ((a, b), c, d, e)");
            Assert.fail("Should have rejected a query including multiple non-primary key base columns");
        }
        catch (Exception e)
        {
            Throwable cause = e.getCause();
            Assertions.assertThat(cause).isInstanceOf(InvalidRequestException.class);
            Assertions.assertThat(cause.getMessage()).contains("Cannot include more than one non-primary key column");
        }
    }

    @Test
    public void testNullInClusteringColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (id1 int, id2 int, v1 text, v2 text, PRIMARY KEY (id1, id2))");

        executeNet("USE " + keyspace());

        createView("CREATE MATERIALIZED VIEW %s AS" +
                   "  SELECT id1, v1, id2, v2" +
                   "  FROM %s" +
                   "  WHERE id1 IS NOT NULL AND v1 IS NOT NULL AND id2 IS NOT NULL" +
                   "  PRIMARY KEY (id1, v1, id2)" +
                   "  WITH CLUSTERING ORDER BY (v1 DESC, id2 ASC)");

        execute("INSERT INTO %s (id1, id2, v1, v2) VALUES (?, ?, ?, ?)", 0, 1, "foo", "bar");

        assertRowsNet(executeNet("SELECT * FROM %s"), row(0, 1, "foo", "bar"));
        assertRowsNet(executeViewNet("SELECT * FROM %s"), row(0, "foo", 1, "bar"));

        executeNet("UPDATE %s SET v1=? WHERE id1=? AND id2=?", null, 0, 1);
        assertRowsNet(executeNet("SELECT * FROM %s"), row(0, 1, null, "bar"));
        assertRowsNet(executeViewNet("SELECT * FROM %s"));

        executeNet("UPDATE %s SET v2=? WHERE id1=? AND id2=?", "rab", 0, 1);
        assertRowsNet(executeNet("SELECT * FROM %s"), row(0, 1, null, "rab"));
        assertRowsNet(executeViewNet("SELECT * FROM %s"));
    }
}
