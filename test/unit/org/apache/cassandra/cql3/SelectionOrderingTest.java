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

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.ClientState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import static org.apache.cassandra.cql3.QueryProcessor.process;
import static org.apache.cassandra.cql3.QueryProcessor.processInternal;
import static org.junit.Assert.assertEquals;

public class SelectionOrderingTest
{
    private static final Logger logger = LoggerFactory.getLogger(SelectWithTokenFunctionTest.class);
    static ClientState clientState;
    static String keyspace = "select_with_ordering_test";

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        executeSchemaChange("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.single_clustering (a int, b int, c int, PRIMARY KEY (a, b))");
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.single_clustering_desc (a int, b int, c int, PRIMARY KEY (a, b)) WITH CLUSTERING ORDER BY (b DESC)");
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.multiple_clustering (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");
        clientState = ClientState.forInternalCalls();
    }

    @AfterClass
    public static void stopGossiper()
    {
        Gossiper.instance.stop();
    }

    private static void executeSchemaChange(String query) throws Throwable
    {
        try
        {
            process(String.format(query, keyspace), ConsistencyLevel.ONE);
        }
        catch (RuntimeException exc)
        {
            throw exc.getCause();
        }
    }

    private static UntypedResultSet execute(String query) throws Throwable
    {
        try
        {
            return processInternal(String.format(query, keyspace));
        }
        catch (RuntimeException exc)
        {
            if (exc.getCause() != null)
                throw exc.getCause();
            throw exc;
        }
    }

    @Test
    public void testNormalSelectionOrderSingleClustering() throws Throwable
    {
        for (String descOption : new String[]{"", "_desc"})
        {
            execute("INSERT INTO %s.single_clustering" + descOption + " (a, b, c) VALUES (0, 0, 0)");
            execute("INSERT INTO %s.single_clustering" + descOption + " (a, b, c) VALUES (0, 1, 1)");
            execute("INSERT INTO %s.single_clustering" + descOption + " (a, b, c) VALUES (0, 2, 2)");

            try
            {
                UntypedResultSet results = execute("SELECT * FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b ASC");
                assertEquals(3, results.size());
                Iterator<UntypedResultSet.Row> rows = results.iterator();
                for (int i = 0; i < 3; i++)
                    assertEquals(i, rows.next().getInt("b"));

                results = execute("SELECT * FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b DESC");
                assertEquals(3, results.size());
                rows = results.iterator();
                for (int i = 2; i >= 0; i--)
                    assertEquals(i, rows.next().getInt("b"));

                // order by the only column in the selection
                results = execute("SELECT b FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b ASC");
                assertEquals(3, results.size());
                rows = results.iterator();
                for (int i = 0; i < 3; i++)
                    assertEquals(i, rows.next().getInt("b"));

                results = execute("SELECT b FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b DESC");
                assertEquals(3, results.size());
                rows = results.iterator();
                for (int i = 2; i >= 0; i--)
                    assertEquals(i, rows.next().getInt("b"));

                // order by a column not in the selection
                results = execute("SELECT c FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b ASC");
                assertEquals(3, results.size());
                rows = results.iterator();
                for (int i = 0; i < 3; i++)
                    assertEquals(i, rows.next().getInt("c"));

                results = execute("SELECT c FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b DESC");
                assertEquals(3, results.size());
                rows = results.iterator();
                for (int i = 2; i >= 0; i--)
                    assertEquals(i, rows.next().getInt("c"));
            }
            finally
            {
                execute("DELETE FROM %s.single_clustering" + descOption + " WHERE a = 0");
            }
        }
    }

    @Test
    public void testFunctionSelectionOrderSingleClustering() throws Throwable
    {
        for (String descOption : new String[]{"", "_desc"})
        {
            execute("INSERT INTO %s.single_clustering" + descOption + " (a, b, c) VALUES (0, 0, 0)");
            execute("INSERT INTO %s.single_clustering" + descOption + " (a, b, c) VALUES (0, 1, 1)");
            execute("INSERT INTO %s.single_clustering" + descOption + " (a, b, c) VALUES (0, 2, 2)");

            try
            {
                // order by a column in the selection (wrapped in a function)
                UntypedResultSet results = execute("SELECT blobAsInt(intAsBlob(b)) as col FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b ASC");
                assertEquals(3, results.size());
                Iterator<UntypedResultSet.Row> rows = results.iterator();
                for (int i = 0; i < 3; i++)
                    assertEquals(i, rows.next().getInt("col"));

                results = execute("SELECT blobAsInt(intAsBlob(b)) as col FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b DESC");
                assertEquals(3, results.size());
                rows = results.iterator();
                for (int i = 2; i >= 0; i--)
                    assertEquals(i, rows.next().getInt("col"));

                // order by a column in the selection, plus the column wrapped in a function
                results = execute("SELECT b, blobAsInt(intAsBlob(b)) as col FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b ASC");
                assertEquals(3, results.size());
                rows = results.iterator();
                for (int i = 0; i < 3; i++)
                {
                    UntypedResultSet.Row row = rows.next();
                    assertEquals(i, row.getInt("b"));
                    assertEquals(i, row.getInt("col"));
                }

                results = execute("SELECT b, blobAsInt(intAsBlob(b)) as col FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b DESC");
                assertEquals(3, results.size());
                rows = results.iterator();
                for (int i = 2; i >= 0; i--)
                {
                    UntypedResultSet.Row row = rows.next();
                    assertEquals(i, row.getInt("b"));
                    assertEquals(i, row.getInt("col"));
                }

                // order by a column not in the selection (wrapped in a function)
                results = execute("SELECT blobAsInt(intAsBlob(c)) as col FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b ASC");
                assertEquals(3, results.size());
                rows = results.iterator();
                for (int i = 0; i < 3; i++)
                    assertEquals(i, rows.next().getInt("col"));

                results = execute("SELECT blobAsInt(intAsBlob(c)) as col FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b DESC");
                assertEquals(3, results.size());
                rows = results.iterator();
                for (int i = 2; i >= 0; i--)
                    assertEquals(i, rows.next().getInt("col"));
            }
            finally
            {
                execute("DELETE FROM %s.single_clustering" + descOption + " WHERE a = 0");
            }
        }
    }

    @Test
    public void testNormalSelectionOrderMultipleClustering() throws Throwable
    {
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 0, 1, 1)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 0, 2, 2)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 1, 0, 3)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 1, 1, 4)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 1, 2, 5)");
        try
        {
            UntypedResultSet results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 ORDER BY b ASC");
            assertEquals(6, results.size());
            Iterator<UntypedResultSet.Row> rows = results.iterator();
            assertEquals(0, rows.next().getInt("d"));
            assertEquals(1, rows.next().getInt("d"));
            assertEquals(2, rows.next().getInt("d"));
            assertEquals(3, rows.next().getInt("d"));
            assertEquals(4, rows.next().getInt("d"));
            assertEquals(5, rows.next().getInt("d"));

            results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(5, rows.next().getInt("d"));
            assertEquals(4, rows.next().getInt("d"));
            assertEquals(3, rows.next().getInt("d"));
            assertEquals(2, rows.next().getInt("d"));
            assertEquals(1, rows.next().getInt("d"));
            assertEquals(0, rows.next().getInt("d"));

            results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC, c DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(5, rows.next().getInt("d"));
            assertEquals(4, rows.next().getInt("d"));
            assertEquals(3, rows.next().getInt("d"));
            assertEquals(2, rows.next().getInt("d"));
            assertEquals(1, rows.next().getInt("d"));
            assertEquals(0, rows.next().getInt("d"));

            // select and order by b
            results = execute("SELECT b FROM %s.multiple_clustering WHERE a=0 ORDER BY b ASC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(0, rows.next().getInt("b"));
            assertEquals(0, rows.next().getInt("b"));
            assertEquals(0, rows.next().getInt("b"));
            assertEquals(1, rows.next().getInt("b"));
            assertEquals(1, rows.next().getInt("b"));
            assertEquals(1, rows.next().getInt("b"));

            results = execute("SELECT b FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(1, rows.next().getInt("b"));
            assertEquals(1, rows.next().getInt("b"));
            assertEquals(1, rows.next().getInt("b"));
            assertEquals(0, rows.next().getInt("b"));
            assertEquals(0, rows.next().getInt("b"));
            assertEquals(0, rows.next().getInt("b"));

            // select c, order by b
            results = execute("SELECT c FROM %s.multiple_clustering WHERE a=0 ORDER BY b ASC");
            rows = results.iterator();
            assertEquals(0, rows.next().getInt("c"));
            assertEquals(1, rows.next().getInt("c"));
            assertEquals(2, rows.next().getInt("c"));
            assertEquals(0, rows.next().getInt("c"));
            assertEquals(1, rows.next().getInt("c"));
            assertEquals(2, rows.next().getInt("c"));

            results = execute("SELECT c FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(2, rows.next().getInt("c"));
            assertEquals(1, rows.next().getInt("c"));
            assertEquals(0, rows.next().getInt("c"));
            assertEquals(2, rows.next().getInt("c"));
            assertEquals(1, rows.next().getInt("c"));
            assertEquals(0, rows.next().getInt("c"));

            // select c, order by b, c
            results = execute("SELECT c FROM %s.multiple_clustering WHERE a=0 ORDER BY b ASC, c ASC");
            rows = results.iterator();
            assertEquals(0, rows.next().getInt("c"));
            assertEquals(1, rows.next().getInt("c"));
            assertEquals(2, rows.next().getInt("c"));
            assertEquals(0, rows.next().getInt("c"));
            assertEquals(1, rows.next().getInt("c"));
            assertEquals(2, rows.next().getInt("c"));

            results = execute("SELECT c FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC, c DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(2, rows.next().getInt("c"));
            assertEquals(1, rows.next().getInt("c"));
            assertEquals(0, rows.next().getInt("c"));
            assertEquals(2, rows.next().getInt("c"));
            assertEquals(1, rows.next().getInt("c"));
            assertEquals(0, rows.next().getInt("c"));

            // select d, order by b, c
            results = execute("SELECT d FROM %s.multiple_clustering WHERE a=0 ORDER BY b ASC, c ASC");
            rows = results.iterator();
            assertEquals(0, rows.next().getInt("d"));
            assertEquals(1, rows.next().getInt("d"));
            assertEquals(2, rows.next().getInt("d"));
            assertEquals(3, rows.next().getInt("d"));
            assertEquals(4, rows.next().getInt("d"));
            assertEquals(5, rows.next().getInt("d"));

            results = execute("SELECT d FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC, c DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(5, rows.next().getInt("d"));
            assertEquals(4, rows.next().getInt("d"));
            assertEquals(3, rows.next().getInt("d"));
            assertEquals(2, rows.next().getInt("d"));
            assertEquals(1, rows.next().getInt("d"));
            assertEquals(0, rows.next().getInt("d"));
        }
        finally
        {
            execute("DELETE FROM %s.multiple_clustering WHERE a = 0");
        }
    }

    @Test
    public void testFunctionSelectionOrderMultipleClustering() throws Throwable
    {
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 0, 1, 1)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 0, 2, 2)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 1, 0, 3)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 1, 1, 4)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 1, 2, 5)");
        try
        {
            // select function of b, order by b
            UntypedResultSet results = execute("SELECT blobAsInt(intAsBlob(b)) as col FROM %s.multiple_clustering WHERE a=0 ORDER BY b ASC");
            assertEquals(6, results.size());
            Iterator<UntypedResultSet.Row> rows = results.iterator();
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));

            results = execute("SELECT blobAsInt(intAsBlob(b)) as col FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));

            // select b and function of b, order by b
            results = execute("SELECT b, blobAsInt(intAsBlob(b)) as col FROM %s.multiple_clustering WHERE a=0 ORDER BY b ASC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));

            results = execute("SELECT b, blobAsInt(intAsBlob(b)) as col FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));

            // select c, order by b
            results = execute("SELECT blobAsInt(intAsBlob(c)) as col FROM %s.multiple_clustering WHERE a=0 ORDER BY b ASC");
            rows = results.iterator();
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(2, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(2, rows.next().getInt("col"));

            results = execute("SELECT blobAsInt(intAsBlob(c)) as col FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(2, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(2, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));

            // select c, order by b, c
            results = execute("SELECT blobAsInt(intAsBlob(c)) as col FROM %s.multiple_clustering WHERE a=0 ORDER BY b ASC, c ASC");
            rows = results.iterator();
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(2, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(2, rows.next().getInt("col"));

            results = execute("SELECT blobAsInt(intAsBlob(c)) as col FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC, c DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(2, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(2, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));

            // select d, order by b, c
            results = execute("SELECT blobAsInt(intAsBlob(d)) as col FROM %s.multiple_clustering WHERE a=0 ORDER BY b ASC, c ASC");
            rows = results.iterator();
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(2, rows.next().getInt("col"));
            assertEquals(3, rows.next().getInt("col"));
            assertEquals(4, rows.next().getInt("col"));
            assertEquals(5, rows.next().getInt("col"));

            results = execute("SELECT blobAsInt(intAsBlob(d)) as col FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC, c DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(5, rows.next().getInt("col"));
            assertEquals(4, rows.next().getInt("col"));
            assertEquals(3, rows.next().getInt("col"));
            assertEquals(2, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
        }
        finally
        {
            execute("DELETE FROM %s.multiple_clustering WHERE a = 0");
        }
    }
}
