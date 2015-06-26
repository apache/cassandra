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

import java.util.Iterator;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.google.common.collect.Lists.newArrayList;

import static org.apache.cassandra.cql3.QueryProcessor.processInternal;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.ClientState;

import static org.apache.cassandra.cql3.QueryProcessor.process;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CreateIndexTest
{
    private static final String KEYSPACE = "create_index_test";
    static ClientState clientState;

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        executeSchemaChange("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
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
            process(String.format(query, KEYSPACE), ConsistencyLevel.ONE);
        } catch (RuntimeException exc)
        {
            throw exc.getCause();
        }
    }

    @Test
    public void testCreateIndexOnCompactTableWithClusteringColumns() throws Throwable
    {
        executeSchemaChange("CREATE TABLE %s.compact_table_with_clustering_columns "
                            + "(a int, b int , c int, PRIMARY KEY (a, b))"
                            + " WITH COMPACT STORAGE;");

        assertInvalid("Secondary indexes are not supported on PRIMARY KEY columns in COMPACT STORAGE tables",
                      "CREATE INDEX ON %s.compact_table_with_clustering_columns (a);");

        assertInvalid("Secondary indexes are not supported on PRIMARY KEY columns in COMPACT STORAGE tables",
                      "CREATE INDEX ON %s.compact_table_with_clustering_columns (b);");

        assertInvalid("Secondary indexes are not supported on COMPACT STORAGE tables that have clustering columns",
                      "CREATE INDEX ON %s.compact_table_with_clustering_columns (c);");
    }

    @Test
    public void testCreateIndexOnCompactTableWithoutClusteringColumns() throws Throwable
    {
        executeSchemaChange("CREATE TABLE %s.compact_table (a int PRIMARY KEY, b int)"
                            + " WITH COMPACT STORAGE;");

        assertInvalid("Secondary indexes are not supported on PRIMARY KEY columns in COMPACT STORAGE tables",
                      "CREATE INDEX ON %s.compact_table (a);");

        executeSchemaChange("CREATE INDEX ON %s.compact_table (b);");

        execute("INSERT INTO %s.compact_table (a, b) VALUES (1, 1);");
        execute("INSERT INTO %s.compact_table (a, b) VALUES (2, 4);");
        execute("INSERT INTO %s.compact_table (a, b) VALUES (3, 6);");

        UntypedResultSet results = execute("SELECT * FROM %s.compact_table WHERE b = 4;");
        assertEquals(1, results.size());
        checkRow(0, results, 2, 4);
    }

    private static UntypedResultSet execute(String query) throws Throwable
    {
        try
        {
            return processInternal(String.format(query, KEYSPACE));
        } catch (RuntimeException exc)
        {
            if (exc.getCause() != null)
                throw exc.getCause();
            throw exc;
        }
    }

    private static void checkRow(int rowIndex, UntypedResultSet results, Integer... expectedValues)
    {
        List<UntypedResultSet.Row> rows = newArrayList(results.iterator());
        UntypedResultSet.Row row = rows.get(rowIndex);
        Iterator<ColumnSpecification> columns = row.getColumns().iterator();
        for (Integer expected : expectedValues)
        {
            String columnName = columns.next().name.toString();
            int actual = row.getInt(columnName);
            assertEquals(String.format("Expected value %d for column %s in row %d, but got %s", actual, columnName, rowIndex, expected),
                         (long) expected, actual);
        }
    }

    private static void assertInvalid(String msg, String stmt) throws Throwable {
        try
        {
            executeSchemaChange(stmt);
            fail();
        }
        catch (InvalidRequestException e)
        {
            assertEquals(msg, e.getMessage());
        }
    }}
