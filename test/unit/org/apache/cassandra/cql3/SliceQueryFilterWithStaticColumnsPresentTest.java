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
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MD5Digest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.cassandra.cql3.QueryProcessor.process;
import static org.apache.cassandra.cql3.QueryProcessor.processInternal;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.fail;

/**
 * Test column ranges and ordering with static column in table
 */
public class SliceQueryFilterWithStaticColumnsPresentTest
{
    static ClientState clientState;
    static String keyspace = "static_column_slice_test";

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        executeSchemaChange("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.single_clustering (p text, c text, v text, s text static, PRIMARY KEY (p, c));");
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.single_clustering_reversed (p text, c text, v text, s text static, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY (c DESC);");
        execute("INSERT INTO %s.single_clustering (p, c, v, s) values ('p1', 'k1', 'v1', 'sv1')");
        execute("INSERT INTO %s.single_clustering (p, c, v) values ('p1', 'k2', 'v2')");
        execute("INSERT INTO %s.single_clustering (p, s) values ('p2', 'sv2')");
        execute("INSERT INTO %s.single_clustering_reversed (p, c, v, s) values ('p1', 'k1', 'v1', 'sv1')");
        execute("INSERT INTO %s.single_clustering_reversed (p, c, v) values ('p1', 'k2', 'v2')");
        execute("INSERT INTO %s.single_clustering_reversed (p, s) values ('p2', 'sv2')");
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
        } catch (RuntimeException exc)
        {
            throw exc.getCause();
        }
    }

    private static UntypedResultSet execute(String query) throws Throwable
    {
        try
        {
            return processInternal(String.format(query, keyspace));
        } catch (RuntimeException exc)
        {
            if (exc.getCause() != null)
                throw exc.getCause();
            throw exc;
        }
    }

    @Test
    public void testNoClusteringColumnDefaultOrdering() throws Throwable
    {
        UntypedResultSet results = execute("SELECT * FROM %s.single_clustering WHERE p='p1'");
        assertEquals(2, results.size());
        checkRow(0, results, "p1", "k1", "sv1", "v1");
        checkRow(1, results, "p1", "k2", "sv1", "v2");

        results = execute("SELECT * FROM %s.single_clustering WHERE p='p2'");
        assertEquals(1, results.size());
        checkRow(0, results, "p2", null, "sv2", null);

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1'");
        assertEquals(2, results.size());
        checkRow(0, results, "p1", "k2", "sv1", "v2");
        checkRow(1, results, "p1", "k1", "sv1", "v1");

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p2'");
        assertEquals(1, results.size());
        checkRow(0, results, "p2", null, "sv2", null);
    }

    @Test
    public void testNoClusteringColumnAscending() throws Throwable
    {
        UntypedResultSet results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' ORDER BY c ASC");
        assertEquals(2, results.size());
        checkRow(0, results, "p1", "k1", "sv1", "v1");
        checkRow(1, results, "p1", "k2", "sv1", "v2");

        results = execute("SELECT * FROM %s.single_clustering WHERE p='p2' ORDER BY c ASC");
        assertEquals(1, results.size());
        checkRow(0, results, "p2", null, "sv2", null);

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' ORDER BY c ASC");
        assertEquals(2, results.size());
        checkRow(0, results, "p1", "k1", "sv1", "v1");
        checkRow(1, results, "p1", "k2", "sv1", "v2");

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p2' ORDER BY c ASC");
        assertEquals(1, results.size());
        checkRow(0, results, "p2", null, "sv2", null);
    }

    @Test
    public void testNoClusteringColumnDescending() throws Throwable
    {
        UntypedResultSet results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' ORDER BY c DESC");
        assertEquals(2, results.size());
        checkRow(0, results, "p1", "k2", "sv1", "v2");
        checkRow(1, results, "p1", "k1", "sv1", "v1");

        results = execute("SELECT * FROM %s.single_clustering WHERE p='p2' ORDER BY c DESC");
        assertEquals(1, results.size());
        checkRow(0, results, "p2", null, "sv2", null);

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' ORDER BY c DESC");
        assertEquals(2, results.size());
        checkRow(0, results, "p1", "k2", "sv1", "v2");
        checkRow(1, results, "p1", "k1", "sv1", "v1");

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p2' ORDER BY c DESC");
        assertEquals(1, results.size());
        checkRow(0, results, "p2", null, "sv2", null);
    }

    @Test
    public void testSingleRelationDefaultOrdering() throws Throwable
    {
        UntypedResultSet results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' AND c>='k1'");
        assertEquals(2, results.size());
        checkRow(0, results, "p1", "k1", "sv1", "v1");
        checkRow(1, results, "p1", "k2", "sv1", "v2");

        results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' AND c>='k2'");
        assertEquals(1, results.size());
        checkRow(0, results, "p1", "k2", "sv1", "v2");

        results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' AND c>='k3'");
        assertEquals(0, results.size());

        results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' AND c ='k1'");
        assertEquals(1, results.size());
        checkRow(0, results, "p1", "k1", "sv1", "v1");

        results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' AND c<='k1'");
        assertEquals(1, results.size());
        checkRow(0, results, "p1", "k1", "sv1", "v1");

        results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' AND c<='k0'");
        assertEquals(0, results.size());

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' AND c>='k1'");
        assertEquals(2, results.size());
        checkRow(0, results, "p1", "k2", "sv1", "v2");
        checkRow(1, results, "p1", "k1", "sv1", "v1");

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' AND c>='k2'");
        assertEquals(1, results.size());
        checkRow(0, results, "p1", "k2", "sv1", "v2");

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' AND c>='k3'");
        assertEquals(0, results.size());

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' AND c='k1'");
        assertEquals(1, results.size());
        checkRow(0, results, "p1", "k1", "sv1", "v1");

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' AND c<='k1'");
        assertEquals(1, results.size());
        checkRow(0, results, "p1", "k1", "sv1", "v1");

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' AND c<='k0'");
        assertEquals(0, results.size());
    }

    @Test
    public void testSingleRelationAscending() throws Throwable
    {
        UntypedResultSet results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' AND c>='k1' ORDER BY c ASC");
        assertEquals(2, results.size());
        checkRow(0, results, "p1", "k1", "sv1", "v1");
        checkRow(1, results, "p1", "k2", "sv1", "v2");

        results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' AND c>='k2' ORDER BY c ASC");
        assertEquals(1, results.size());
        checkRow(0, results, "p1", "k2", "sv1", "v2");

        results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' AND c>='k3' ORDER BY c ASC");
        assertEquals(0, results.size());

        results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' AND c ='k1' ORDER BY c ASC");
        assertEquals(1, results.size());
        checkRow(0, results, "p1", "k1", "sv1", "v1");

        results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' AND c<='k1' ORDER BY c ASC");
        assertEquals(1, results.size());
        checkRow(0, results, "p1", "k1", "sv1", "v1");

        results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' AND c<='k0' ORDER BY c ASC");
        assertEquals(0, results.size());

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' AND c>='k1' ORDER BY c ASC");
        assertEquals(2, results.size());
        checkRow(0, results, "p1", "k1", "sv1", "v1");
        checkRow(1, results, "p1", "k2", "sv1", "v2");

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' AND c>='k2' ORDER BY c ASC");
        assertEquals(1, results.size());
        checkRow(0, results, "p1", "k2", "sv1", "v2");

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' AND c>='k3' ORDER BY c ASC");
        assertEquals(0, results.size());

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' AND c='k1' ORDER BY c ASC");
        assertEquals(1, results.size());
        checkRow(0, results, "p1", "k1", "sv1", "v1");

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' AND c<='k1' ORDER BY c ASC");
        assertEquals(1, results.size());
        checkRow(0, results, "p1", "k1", "sv1", "v1");

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' AND c<='k0' ORDER BY c ASC");
        assertEquals(0, results.size());
    }

    @Test
    public void testSingleRelationDescending() throws Throwable
    {
        UntypedResultSet results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' AND c>='k1' ORDER BY c DESC");
        assertEquals(2, results.size());
        checkRow(0, results, "p1", "k2", "sv1", "v2");
        checkRow(1, results, "p1", "k1", "sv1", "v1");

        results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' AND c>='k2' ORDER BY c DESC");
        assertEquals(1, results.size());
        checkRow(0, results, "p1", "k2", "sv1", "v2");

        results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' AND c>='k3' ORDER BY c DESC");
        assertEquals(0, results.size());

        results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' AND c ='k1' ORDER BY c DESC");
        assertEquals(1, results.size());
        checkRow(0, results, "p1", "k1", "sv1", "v1");

        results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' AND c<='k1' ORDER BY c DESC");
        assertEquals(1, results.size());
        checkRow(0, results, "p1", "k1", "sv1", "v1");

        results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' AND c<='k0' ORDER BY c DESC");
        assertEquals(0, results.size());

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' AND c>='k1' ORDER BY c DESC");
        assertEquals(2, results.size());
        checkRow(0, results, "p1", "k2", "sv1", "v2");
        checkRow(1, results, "p1", "k1", "sv1", "v1");

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' AND c>='k2' ORDER BY c DESC");
        assertEquals(1, results.size());
        checkRow(0, results, "p1", "k2", "sv1", "v2");

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' AND c>='k3' ORDER BY c DESC");
        assertEquals(0, results.size());

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' AND c='k1' ORDER BY c DESC");
        assertEquals(1, results.size());
        checkRow(0, results, "p1", "k1", "sv1", "v1");

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' AND c<='k1' ORDER BY c DESC");
        assertEquals(1, results.size());
        checkRow(0, results, "p1", "k1", "sv1", "v1");

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' AND c<='k0' ORDER BY c DESC");
        assertEquals(0, results.size());
    }

    @Test
    public void testInDefaultOrdering() throws Throwable
    {
        UntypedResultSet results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' AND c IN ('k1', 'k2')");
        assertEquals(2, results.size());
        checkRow(0, results, "p1", "k1", "sv1", "v1");
        checkRow(1, results, "p1", "k2", "sv1", "v2");

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' AND c IN ('k1', 'k2')");
        assertEquals(2, results.size());
        checkRow(0, results, "p1", "k2", "sv1", "v2");
        checkRow(1, results, "p1", "k1", "sv1", "v1");
    }

    @Test
    public void testInAscending() throws Throwable
    {
        UntypedResultSet results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' AND c IN ('k1', 'k2') ORDER BY c ASC");
        assertEquals(2, results.size());
        checkRow(0, results, "p1", "k1", "sv1", "v1");
        checkRow(1, results, "p1", "k2", "sv1", "v2");

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' AND c IN ('k1', 'k2') ORDER BY c ASC");
        assertEquals(2, results.size());
        checkRow(0, results, "p1", "k1", "sv1", "v1");
        checkRow(1, results, "p1", "k2", "sv1", "v2");
    }

    @Test
    public void testInDescending() throws Throwable
    {
        UntypedResultSet results = execute("SELECT * FROM %s.single_clustering WHERE p='p1' AND c IN ('k1', 'k2') ORDER BY c DESC");
        assertEquals(2, results.size());
        checkRow(0, results, "p1", "k2", "sv1", "v2");
        checkRow(1, results, "p1", "k1", "sv1", "v1");

        results = execute("SELECT * FROM %s.single_clustering_reversed WHERE p='p1' AND c IN ('k1', 'k2') ORDER BY c DESC");
        assertEquals(2, results.size());
        checkRow(0, results, "p1", "k2", "sv1", "v2");
        checkRow(1, results, "p1", "k1", "sv1", "v1");
    }

    private static void checkRow(int rowIndex, UntypedResultSet results, String... expectedValues)
    {
        List<UntypedResultSet.Row> rows = newArrayList(results.iterator());
        UntypedResultSet.Row row = rows.get(rowIndex);
        Iterator<ColumnSpecification> columns = row.getColumns().iterator();
        for (String expected : expectedValues)
        {
            String columnName = columns.next().name.toString();
            String actual = row.has(columnName) ? row.getString(columnName) : null;
            assertEquals(String.format("Expected value %s for column %s in row %d, but got %s", actual, columnName, rowIndex, expected),
                    expected, actual);
        }
    }
}
