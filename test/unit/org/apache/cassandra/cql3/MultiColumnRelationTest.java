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

public class MultiColumnRelationTest
{
    private static final Logger logger = LoggerFactory.getLogger(MultiColumnRelationTest.class);
    static ClientState clientState;
    static String keyspace = "multi_column_relation_test";

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        executeSchemaChange("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        for (boolean isCompact : new boolean[]{false, true})
        {
            String tableSuffix = isCompact ? "_compact" : "";
            String compactOption = isCompact ? " WITH COMPACT STORAGE" : "";

            executeSchemaChange(
                    "CREATE TABLE IF NOT EXISTS %s.single_partition" + tableSuffix + "(a int PRIMARY KEY, b int)" + compactOption);
            executeSchemaChange(
                    "CREATE TABLE IF NOT EXISTS %s.compound_partition" + tableSuffix + "(a int, b int, c int, PRIMARY KEY ((a, b)))" + compactOption);
            executeSchemaChange(
                    "CREATE TABLE IF NOT EXISTS %s.single_clustering" + tableSuffix + "(a int, b int, c int, PRIMARY KEY (a, b))" + compactOption);
            executeSchemaChange(
                    "CREATE TABLE IF NOT EXISTS %s.multiple_clustering" + tableSuffix + "(a int, b int, c int, d int, PRIMARY KEY (a, b, c, d))" + compactOption);

            compactOption = isCompact ? " COMPACT STORAGE AND " : "";
            executeSchemaChange(
                    "CREATE TABLE IF NOT EXISTS %s.multiple_clustering_reversed" + tableSuffix +
                        "(a int, b int, c int, d int, PRIMARY KEY (a, b, c, d)) WITH " + compactOption + " CLUSTERING ORDER BY (b DESC, c ASC, d DESC)");
        }

        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.multiple_clustering_with_indices (a int, b int, c int, d int, e int, PRIMARY KEY (a, b, c, d))");
        executeSchemaChange("CREATE INDEX ON %s.multiple_clustering_with_indices (b)");
        executeSchemaChange("CREATE INDEX ON %s.multiple_clustering_with_indices (e)");

        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.partition_with_indices (a int, b int, c int, d int, e int, f int, PRIMARY KEY ((a, b), c, d, e))");
        executeSchemaChange("CREATE INDEX ON %s.partition_with_indices (c)");
        executeSchemaChange("CREATE INDEX ON %s.partition_with_indices (f)");

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

    private MD5Digest prepare(String query) throws RequestValidationException
    {
        ResultMessage.Prepared prepared = QueryProcessor.prepare(String.format(query, keyspace), clientState, false);
        return prepared.statementId;
    }

    private UntypedResultSet executePrepared(MD5Digest statementId, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        CQLStatement statement = QueryProcessor.instance.getPrepared(statementId);
        ResultMessage message = statement.executeInternal(QueryState.forInternalCalls(), options);

        if (message instanceof ResultMessage.Rows)
            return new UntypedResultSet(((ResultMessage.Rows)message).result);
        else
            return null;
    }

    @Test(expected=SyntaxException.class)
    public void testEmptyIdentifierTuple() throws Throwable
    {
        execute("SELECT * FROM %s.single_clustering WHERE () = (1, 2)");
    }

    @Test(expected=SyntaxException.class)
    public void testEmptyValueTuple() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE (b, c) > ()");
    }

    @Test(expected=InvalidRequestException.class)
    public void testDifferentTupleLengths() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE (b, c) > (1, 2, 3)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testNullInTuple() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE (b, c) > (1, null)");
    }

    @Test
    public void testEmptyIN() throws Throwable
    {
        UntypedResultSet results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) IN ()");
        assertTrue(results.isEmpty());
    }

    @Test(expected=InvalidRequestException.class)
    public void testNullInINValues() throws Throwable
    {
        UntypedResultSet results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) IN ((1, 2, null))");
        assertTrue(results.isEmpty());
    }

    @Test(expected=InvalidRequestException.class)
    public void testPartitionKeyInequality() throws Throwable
    {
        execute("SELECT * FROM %s.single_partition WHERE (a) > (1)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testPartitionKeyEquality() throws Throwable
    {
        execute("SELECT * FROM %s.single_partition WHERE (a) = (0)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testRestrictNonPrimaryKey() throws Throwable
    {
        execute("SELECT * FROM %s.single_partition WHERE (b) = (0)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testMixEqualityAndInequality() throws Throwable
    {
        execute("SELECT * FROM %s.single_clustering WHERE a=0 AND (b) = (0) AND (b) > (0)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testMixMultipleInequalitiesOnSameBound() throws Throwable
    {
        execute("SELECT * FROM %s.single_clustering WHERE a=0 AND (b) > (0) AND (b) > (1)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testClusteringColumnsOutOfOrderInInequality() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (d, c, b) > (0, 0, 0)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testSkipClusteringColumnInEquality() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (c, d) = (0, 0)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testSkipClusteringColumnInInequality() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (c, d) > (0, 0)");
    }

    @Test
    public void testSingleClusteringColumnEquality() throws Throwable
    {
        for (String tableSuffix : new String[]{"", "_compact"})
        {
            execute("INSERT INTO %s.single_clustering" + tableSuffix + "(a, b, c) VALUES (0, 0, 0)");
            execute("INSERT INTO %s.single_clustering" + tableSuffix + " (a, b, c) VALUES (0, 1, 0)");
            execute("INSERT INTO %s.single_clustering" + tableSuffix + " (a, b, c) VALUES (0, 2, 0)");
            UntypedResultSet results = execute("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND (b) = (1)");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0);

            results = execute("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND (b) = (3)");
            assertEquals(0, results.size());
        }
    }

    @Test
    public void testMultipleClusteringColumnEquality() throws Throwable
    {
        for (String tableSuffix : new String[]{"", "_compact"})
        {
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 1, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 1, 1)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 2, 0, 0)");
            UntypedResultSet results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b) = (1)");
            assertEquals(3, results.size());
            checkRow(0, results, 0, 1, 0, 0);
            checkRow(1, results, 0, 1, 1, 0);
            checkRow(2, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c) = (1, 1)");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 1, 1, 0);
            checkRow(1, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) = (1, 1, 1)");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 1, 1);
            execute("DELETE FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND b=2 and c=0 and d=0");
        }
    }

    @Test(expected=InvalidRequestException.class)
    public void testPartitionAndClusteringColumnEquality() throws Throwable
    {
        execute("SELECT * FROM %s.single_clustering WHERE (a, b) = (0, 0)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testClusteringColumnsOutOfOrderInEquality() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (d, c, b) = (3, 2, 1)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testBadType() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) = (1, 2, 'foobar')");
    }

    @Test(expected=SyntaxException.class)
    public void testSingleColumnTupleRelation() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND b = (1, 2, 3)");
    }

    @Test
    public void testInvalidMultiAndSingleColumnRelationMix() throws Throwable
    {
        for (String tableSuffix : new String[]{"", "_compact"})
        {
            String[] queries = new String[]{
                "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a = 0 AND (b, c, d) > (0, 1, 0) AND c < 1",
                "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a = 0 AND c > 1 AND (b, c, d) < (1, 1, 0)",
                "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE (a, b, c, d) IN ((0, 1, 2, 3))",
                "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE (c, d) IN ((0, 1))",
                "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a = 0  AND b > 0  AND (c, d) IN ((0, 0))",
                "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a = 0 AND b > 0  AND (c, d) > (0, 0)",
                "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a = 0 AND (c, d) > (0, 0) AND b > 0  ",
                "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a = 0 AND (b, c) > (0, 0) AND (b) < (0) AND (c) < (0)",
                "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a = 0 AND (c) < (0) AND (b, c) > (0, 0) AND (b) < (0)",
                "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a = 0 AND (b) < (0) AND (c) < (0) AND (b, c) > (0, 0)",
                "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a = 0 AND (b, c) > (0, 0) AND (c) < (0)",
                "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a = 0 AND (b, c) in ((0, 0), (0, 0)) AND d > 0"
            };

            for (String query : queries)
            {
                try
                {
                    execute(query);
                    fail(String.format("Expected query \"%s\" to throw an InvalidRequestException", query));
                }
                catch (InvalidRequestException e)
                {
                }
            }
        }
    }

    @Test
    public void testMultiAndSingleColumnRelationMix() throws Throwable
    {
        for (String tableSuffix : new String[]{"", "_compact"})
        {
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 1)");

            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 1, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 1, 1)");

            UntypedResultSet results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and b = 1 and (c, d) = (0, 0)");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0, 0);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and b = 1 and (c) IN ((0))");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0, 0);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and b = 1 and (c) IN ((0), (1))");
            assertEquals(3, results.size());
            checkRow(0, results, 0, 1, 0, 0);
            checkRow(1, results, 0, 1, 1, 0);
            checkRow(2, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and b = 1 and (c, d) IN ((0, 0))");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0, 0);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and b = 1 and (c, d) IN ((0, 0), (1, 1))");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 1, 0, 0);
            checkRow(1, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and b = 1 and (c, d) > (0, 0)");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 1, 1, 0);
            checkRow(1, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and b = 1 and (c, d) > (0, 0) and (c) <= (1)");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 1, 1, 0);
            checkRow(1, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and b = 1 and (c, d) > (0, 0) and c <= 1");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 1, 1, 0);
            checkRow(1, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and b = 1 and (c, d) >= (0, 0) and (c, d) < (1, 1)");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 1, 0, 0);
            checkRow(1, results, 0, 1, 1, 0);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and (b, c) = (0, 1) and d = 0");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 0, 1, 0);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and b = 0 and (c) = (1) and d = 0");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 0, 1, 0);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and (b, c) = (0, 1) and d IN (0, 2)");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 0, 1, 0);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and b = 0 and (c) = (1) and d IN (0, 2)");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 0, 1, 0);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and (b, c) = (0, 1) and d >= 0");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 1, 0);
            checkRow(1, results, 0, 0, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and d < 1 and (b, c) = (0, 1) and d >= 0");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 0, 1, 0);
        }
    }

    @Test
    public void testMultipleMultiColumnRelation() throws Throwable
    {
        for (String tableSuffix : new String[]{"", "_compact"})
        {
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 1)");

            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 1, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 1, 1)");

            UntypedResultSet results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and (b) = (1) and (c, d) = (0, 0)");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0, 0);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and (b) = (1) and (c) = (0) and (d) = (0)");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0, 0);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and (b) = (1) and (c) IN ((0))");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0, 0);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and (b) = (1) and (c) IN ((0), (1))");
            assertEquals(3, results.size());
            checkRow(0, results, 0, 1, 0, 0);
            checkRow(1, results, 0, 1, 1, 0);
            checkRow(2, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and (b) = (1) and (c, d) IN ((0, 0))");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0, 0);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and (b) = (1) and (c, d) IN ((0, 0), (1, 1))");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 1, 0, 0);
            checkRow(1, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and (b) = (1) and (c, d) > (0, 0)");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 1, 1, 0);
            checkRow(1, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and (b) = (1) and (c, d) > (0, 0) and (c) <= (1)");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 1, 1, 0);
            checkRow(1, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and (b) = (1) and (c, d) > (0, 0) and c <= 1");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 1, 1, 0);
            checkRow(1, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a = 0 and (b) = (1) and (c, d) >= (0, 0) and (c, d) < (1, 1)");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 1, 0, 0);
            checkRow(1, results, 0, 1, 1, 0);
        }
    }

    @Test
    public void testSingleClusteringColumnInequality() throws Throwable
    {
        for (String tableSuffix : new String[]{"", "_compact"})
        {
            execute("INSERT INTO %s.single_clustering" + tableSuffix + " (a, b, c) VALUES (0, 0, 0)");
            execute("INSERT INTO %s.single_clustering" + tableSuffix + " (a, b, c) VALUES (0, 1, 0)");
            execute("INSERT INTO %s.single_clustering" + tableSuffix + " (a, b, c) VALUES (0, 2, 0)");

            UntypedResultSet results = execute("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND (b) > (0)");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 1, 0);
            checkRow(1, results, 0, 2, 0);

            results = execute("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND (b) >= (1)");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 1, 0);
            checkRow(1, results, 0, 2, 0);

            results = execute("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND (b) < (2)");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 0);
            checkRow(1, results, 0, 1, 0);

            results = execute("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND (b) <= (1)");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 0);
            checkRow(1, results, 0, 1, 0);

            results = execute("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND (b) > (0) AND (b) < (2)");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0);

            results = execute("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND b > 0 AND (b) < (2)");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0);

            results = execute("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND (b) > (0) AND b < 2");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0);
        }
    }

    @Test
    public void testMultipleClusteringColumnInequality() throws Throwable
    {
        for (String tableSuffix : new String[]{"", "_compact"})
        {
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 1)");

            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 1, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 1, 1)");

            UntypedResultSet results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b) > (0)");
            assertEquals(3, results.size());
            checkRow(0, results, 0, 1, 0, 0);
            checkRow(1, results, 0, 1, 1, 0);
            checkRow(2, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b) >= (0)");
            assertEquals(6, results.size());
            checkRow(0, results, 0, 0, 0, 0);
            checkRow(1, results, 0, 0, 1, 0);
            checkRow(2, results, 0, 0, 1, 1);
            checkRow(3, results, 0, 1, 0, 0);
            checkRow(4, results, 0, 1, 1, 0);
            checkRow(5, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c) > (1, 0)");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 1, 1, 0);
            checkRow(1, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c) >= (1, 0)");
            assertEquals(3, results.size());
            checkRow(0, results, 0, 1, 0, 0);
            checkRow(1, results, 0, 1, 1, 0);
            checkRow(2, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) > (1, 1, 0)");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) >= (1, 1, 0)");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 1, 1, 0);
            checkRow(1, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b) < (1)");
            assertEquals(3, results.size());
            checkRow(0, results, 0, 0, 0, 0);
            checkRow(1, results, 0, 0, 1, 0);
            checkRow(2, results, 0, 0, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b) <= (1)");
            assertEquals(6, results.size());
            checkRow(0, results, 0, 0, 0, 0);
            checkRow(1, results, 0, 0, 1, 0);
            checkRow(2, results, 0, 0, 1, 1);
            checkRow(3, results, 0, 1, 0, 0);
            checkRow(4, results, 0, 1, 1, 0);
            checkRow(5, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c) < (0, 1)");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 0, 0, 0);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c) <= (0, 1)");
            assertEquals(3, results.size());
            checkRow(0, results, 0, 0, 0, 0);
            checkRow(1, results, 0, 0, 1, 0);
            checkRow(2, results, 0, 0, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) < (0, 1, 1)");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 0, 0);
            checkRow(1, results, 0, 0, 1, 0);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) <= (0, 1, 1)");
            checkRow(0, results, 0, 0, 0, 0);
            checkRow(1, results, 0, 0, 1, 0);
            checkRow(2, results, 0, 0, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) > (0, 1, 0) AND (b) < (1)");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 0, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) > (0, 1, 0) AND b < 1");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 0, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) > (0, 1, 1) AND (b, c) < (1, 1)");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0, 0);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) > (0, 1, 1) AND (b, c, d) < (1, 1, 0)");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0, 0);

            // reversed
            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b) > (0) ORDER BY b DESC, c DESC, d DESC");
            assertEquals(3, results.size());
            checkRow(2, results, 0, 1, 0, 0);
            checkRow(1, results, 0, 1, 1, 0);
            checkRow(0, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b) >= (0) ORDER BY b DESC, c DESC, d DESC");
            assertEquals(6, results.size());
            checkRow(5, results, 0, 0, 0, 0);
            checkRow(4, results, 0, 0, 1, 0);
            checkRow(3, results, 0, 0, 1, 1);
            checkRow(2, results, 0, 1, 0, 0);
            checkRow(1, results, 0, 1, 1, 0);
            checkRow(0, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c) > (1, 0) ORDER BY b DESC, c DESC, d DESC");
            assertEquals(2, results.size());
            checkRow(1, results, 0, 1, 1, 0);
            checkRow(0, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c) >= (1, 0) ORDER BY b DESC, c DESC, d DESC");
            assertEquals(3, results.size());
            checkRow(2, results, 0, 1, 0, 0);
            checkRow(1, results, 0, 1, 1, 0);
            checkRow(0, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) > (1, 1, 0) ORDER BY b DESC, c DESC, d DESC");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) >= (1, 1, 0) ORDER BY b DESC, c DESC, d DESC");
            assertEquals(2, results.size());
            checkRow(1, results, 0, 1, 1, 0);
            checkRow(0, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b) < (1) ORDER BY b DESC, c DESC, d DESC");
            assertEquals(3, results.size());
            checkRow(2, results, 0, 0, 0, 0);
            checkRow(1, results, 0, 0, 1, 0);
            checkRow(0, results, 0, 0, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b) <= (1) ORDER BY b DESC, c DESC, d DESC");
            assertEquals(6, results.size());
            checkRow(5, results, 0, 0, 0, 0);
            checkRow(4, results, 0, 0, 1, 0);
            checkRow(3, results, 0, 0, 1, 1);
            checkRow(2, results, 0, 1, 0, 0);
            checkRow(1, results, 0, 1, 1, 0);
            checkRow(0, results, 0, 1, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c) < (0, 1) ORDER BY b DESC, c DESC, d DESC");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 0, 0, 0);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c) <= (0, 1) ORDER BY b DESC, c DESC, d DESC");
            assertEquals(3, results.size());
            checkRow(2, results, 0, 0, 0, 0);
            checkRow(1, results, 0, 0, 1, 0);
            checkRow(0, results, 0, 0, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) < (0, 1, 1) ORDER BY b DESC, c DESC, d DESC");
            assertEquals(2, results.size());
            checkRow(1, results, 0, 0, 0, 0);
            checkRow(0, results, 0, 0, 1, 0);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) <= (0, 1, 1) ORDER BY b DESC, c DESC, d DESC");
            checkRow(2, results, 0, 0, 0, 0);
            checkRow(1, results, 0, 0, 1, 0);
            checkRow(0, results, 0, 0, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) > (0, 1, 0) AND (b) < (1) ORDER BY b DESC, c DESC, d DESC");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 0, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) > (0, 1, 1) AND (b, c) < (1, 1) ORDER BY b DESC, c DESC, d DESC");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0, 0);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) > (0, 1, 1) AND (b, c, d) < (1, 1, 0) ORDER BY b DESC, c DESC, d DESC");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0, 0);
        }
    }

    @Test
    public void testMultipleClusteringColumnInequalityReversedComponents() throws Throwable
    {
        for (String tableSuffix : new String[]{"", "_compact"})
        {
            execute("DELETE FROM %s.multiple_clustering_reversed" + tableSuffix + " WHERE a=0");

            // b and d are reversed in the clustering order
            execute("INSERT INTO %s.multiple_clustering_reversed" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering_reversed" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 1, 1)");
            execute("INSERT INTO %s.multiple_clustering_reversed" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 1, 0)");

            execute("INSERT INTO %s.multiple_clustering_reversed" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering_reversed" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 1)");
            execute("INSERT INTO %s.multiple_clustering_reversed" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 0)");


            Thread.sleep(5000); // There is a race here: if the SELECT is read before all of the first 3 INSERTS propagate, the test fails.
            UntypedResultSet results = execute("SELECT * FROM %s.multiple_clustering_reversed" + tableSuffix + " WHERE a=0 AND (b) > (0)");
            assertEquals(3, results.size());
            checkRow(0, results, 0, 1, 0, 0);
            checkRow(1, results, 0, 1, 1, 1);
            checkRow(2, results, 0, 1, 1, 0);

            results = execute("SELECT * FROM %s.multiple_clustering_reversed" + tableSuffix + " WHERE a=0 AND (b) >= (0)");
            assertEquals(6, results.size());
            checkRow(0, results, 0, 1, 0, 0);
            checkRow(1, results, 0, 1, 1, 1);
            checkRow(2, results, 0, 1, 1, 0);
            checkRow(3, results, 0, 0, 0, 0);
            checkRow(4, results, 0, 0, 1, 1);
            checkRow(5, results, 0, 0, 1, 0);

            results = execute("SELECT * FROM %s.multiple_clustering_reversed" + tableSuffix + " WHERE a=0 AND (b) < (1)");
            assertEquals(3, results.size());
            checkRow(0, results, 0, 0, 0, 0);
            checkRow(1, results, 0, 0, 1, 1);
            checkRow(2, results, 0, 0, 1, 0);

            results = execute("SELECT * FROM %s.multiple_clustering_reversed" + tableSuffix + " WHERE a=0 AND (b) <= (1)");
            assertEquals(6, results.size());
            checkRow(0, results, 0, 1, 0, 0);
            checkRow(1, results, 0, 1, 1, 1);
            checkRow(2, results, 0, 1, 1, 0);
            checkRow(3, results, 0, 0, 0, 0);
            checkRow(4, results, 0, 0, 1, 1);
            checkRow(5, results, 0, 0, 1, 0);

            // preserve pre-6875 behavior (even though the query result is technically incorrect)
            results = execute("SELECT * FROM %s.multiple_clustering_reversed" + tableSuffix + " WHERE a=0 AND (b, c) > (1, 0)");
            assertEquals(0, results.size());
        }
    }

    @Test
    public void testLiteralIn() throws Throwable
    {
        for (String tableSuffix : new String[]{"", "_compact"})
        {
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 1)");

            UntypedResultSet results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) IN ((0, 1, 0), (0, 1, 1))");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 1, 0);
            checkRow(1, results, 0, 0, 1, 1);

            // same query, but reversed order for the IN values
            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) IN ((0, 1, 1), (0, 1, 0))");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 1, 0);
            checkRow(1, results, 0, 0, 1, 1);


            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 and (b, c) IN ((0, 1))");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 1, 0);
            checkRow(1, results, 0, 0, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 and (b) IN ((0))");
            assertEquals(3, results.size());
            checkRow(0, results, 0, 0, 0, 0);
            checkRow(1, results, 0, 0, 1, 0);
            checkRow(2, results, 0, 0, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c) IN ((0, 1)) ORDER BY b DESC, c DESC, d DESC");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 1, 1);
            checkRow(1, results, 0, 0, 1, 0);
        }
    }


    @Test
    public void testLiteralInReversed() throws Throwable
    {
        for (String tableSuffix : new String[]{"", "_compact"})
        {
            execute("INSERT INTO %s.multiple_clustering_reversed" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering_reversed" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering_reversed" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 1)");
            execute("INSERT INTO %s.multiple_clustering_reversed" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 0)");
            execute("INSERT INTO %s.multiple_clustering_reversed" + tableSuffix + " (a, b, c, d) VALUES (0, -1, 0, 0)");

            UntypedResultSet results = execute("SELECT * FROM %s.multiple_clustering_reversed" + tableSuffix + " WHERE a=0 AND (b, c, d) IN ((0, 1, 0), (0, 1, 1))");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 1, 1);
            checkRow(1, results, 0, 0, 1, 0);

            // same query, but reversed order for the IN values
            results = execute("SELECT * FROM %s.multiple_clustering_reversed" + tableSuffix + " WHERE a=0 AND (b, c, d) IN ((0, 1, 1), (0, 1, 0))");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 1, 1);
            checkRow(1, results, 0, 0, 1, 0);

            results = execute("SELECT * FROM %s.multiple_clustering_reversed" + tableSuffix + " WHERE a=0 AND (b, c, d) IN ((1, 0, 0), (0, 0, 0), (0, 1, 1), (0, 1, 0), (-1, 0, 0))");
            assertEquals(5, results.size());
            checkRow(0, results, 0, 1, 0, 0);
            checkRow(1, results, 0, 0, 0, 0);
            checkRow(2, results, 0, 0, 1, 1);
            checkRow(3, results, 0, 0, 1, 0);
            checkRow(4, results, 0, -1, 0, 0);

            results = execute("SELECT * FROM %s.multiple_clustering_reversed" + tableSuffix + " WHERE a=0 AND (b, c, d) IN ((0, 0, 0))");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 0, 0, 0);

            results = execute("SELECT * FROM %s.multiple_clustering_reversed" + tableSuffix + " WHERE a=0 AND (b, c, d) IN ((0, 1, 1))");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 0, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering_reversed" + tableSuffix + " WHERE a=0 AND (b, c, d) IN ((0, 1, 0))");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 0, 1, 0);

            results = execute("SELECT * FROM %s.multiple_clustering_reversed" + tableSuffix + " WHERE a=0 and (b, c) IN ((0, 1))");
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 1, 1);
            checkRow(1, results, 0, 0, 1, 0);

            results = execute("SELECT * FROM %s.multiple_clustering_reversed" + tableSuffix + " WHERE a=0 and (b, c) IN ((0, 0))");
            assertEquals(1, results.size());
            checkRow(0, results, 0, 0, 0, 0);

            results = execute("SELECT * FROM %s.multiple_clustering_reversed" + tableSuffix + " WHERE a=0 and (b) IN ((0))");
            assertEquals(3, results.size());
            checkRow(0, results, 0, 0, 0, 0);
            checkRow(1, results, 0, 0, 1, 1);
            checkRow(2, results, 0, 0, 1, 0);
        }
    }

    @Test(expected=InvalidRequestException.class)
    public void testLiteralInWithShortTuple() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) IN ((0, 1))");
    }

    @Test(expected=InvalidRequestException.class)
    public void testLiteralInWithLongTuple() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) IN ((0, 1, 2, 3, 4))");
    }

    @Test(expected=InvalidRequestException.class)
    public void testLiteralInWithPartitionKey() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE (a, b, c, d) IN ((0, 1, 2, 3))");
    }

    @Test(expected=InvalidRequestException.class)
    public void testLiteralInSkipsClusteringColumn() throws Throwable
    {
        execute("SELECT * FROM %s.multiple_clustering WHERE (c, d) IN ((0, 1))");
    }
    @Test
    public void testPartitionAndClusteringInClauses() throws Throwable
    {
        for (String tableSuffix : new String[]{"", "_compact"})
        {
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 1)");

            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (1, 0, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (1, 0, 1, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (1, 0, 1, 1)");

            UntypedResultSet results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a IN (0, 1) AND (b, c, d) IN ((0, 1, 0), (0, 1, 1))");
            assertEquals(4, results.size());
            checkRow(0, results, 0, 0, 1, 0);
            checkRow(1, results, 0, 0, 1, 1);
            checkRow(2, results, 1, 0, 1, 0);
            checkRow(3, results, 1, 0, 1, 1);

            // same query, but reversed order for the IN values
            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a IN (1, 0) AND (b, c, d) IN ((0, 1, 1), (0, 1, 0))");
            assertEquals(4, results.size());
            checkRow(0, results, 1, 0, 1, 0);
            checkRow(1, results, 1, 0, 1, 1);
            checkRow(2, results, 0, 0, 1, 0);
            checkRow(3, results, 0, 0, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a IN (0, 1) and (b, c) IN ((0, 1))");
            assertEquals(4, results.size());
            checkRow(0, results, 0, 0, 1, 0);
            checkRow(1, results, 0, 0, 1, 1);
            checkRow(2, results, 1, 0, 1, 0);
            checkRow(3, results, 1, 0, 1, 1);

            results = execute("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a IN (0, 1) and (b) IN ((0))");
            assertEquals(6, results.size());
            checkRow(0, results, 0, 0, 0, 0);
            checkRow(1, results, 0, 0, 1, 0);
            checkRow(2, results, 0, 0, 1, 1);
            checkRow(3, results, 1, 0, 0, 0);
            checkRow(4, results, 1, 0, 1, 0);
            checkRow(5, results, 1, 0, 1, 1);
        }
    }

    // prepare statement tests

    @Test(expected=InvalidRequestException.class)
    public void testPreparePartitionAndClusteringColumnEquality() throws Throwable
    {
        prepare("SELECT * FROM %s.single_clustering WHERE (a, b) = (?, ?)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testPrepareDifferentTupleLengths() throws Throwable
    {
        prepare("SELECT * FROM %s.multiple_clustering WHERE (b, c) > (?, ?, ?)");
    }

    @Test
    public void testPrepareEmptyIN() throws Throwable
    {
        MD5Digest id = prepare("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) IN ()");
        UntypedResultSet results = executePrepared(id, makeIntOptions());
        assertTrue(results.isEmpty());
    }

    @Test(expected=InvalidRequestException.class)
    public void testPreparePartitionKeyInequality() throws Throwable
    {
        prepare("SELECT * FROM %s.single_partition WHERE (a) > (?)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testPreparePartitionKeyEquality() throws Throwable
    {
        prepare("SELECT * FROM %s.single_partition WHERE (a) = (?)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testPrepareRestrictNonPrimaryKey() throws Throwable
    {
        prepare("SELECT * FROM %s.single_partition WHERE (b) = (?)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testPrepareMixEqualityAndInequality() throws Throwable
    {
        prepare("SELECT * FROM %s.single_clustering WHERE a=0 AND (b) = (?) AND (b) > (?)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testPrepareMixMultipleInequalitiesOnSameBound() throws Throwable
    {
        prepare("SELECT * FROM %s.single_clustering WHERE a=0 AND (b) > (?) AND (b) > (?)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testPrepareMixMultipleInequalitiesOnSameBoundWithSingleColumnRestriction() throws Throwable
    {
        prepare("SELECT * FROM %s.single_clustering WHERE a=0 AND (b) > (?) AND b > ?");
    }

    @Test(expected=InvalidRequestException.class)
    public void testPrepareClusteringColumnsOutOfOrderInInequality() throws Throwable
    {
        prepare("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (d, c, b) > (?, ?, ?)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testPrepareSkipClusteringColumnInEquality() throws Throwable
    {
        prepare("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (c, d) = (?, ?)");
    }

    @Test(expected=InvalidRequestException.class)
    public void testPrepareSkipClusteringColumnInInequality() throws Throwable
    {
        prepare("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (c, d) > (?, ?)");
    }

    @Test
    public void testPreparedClusteringColumnEquality() throws Throwable
    {
        for (String tableSuffix : new String[]{"", "_compact"})
        {
            execute("INSERT INTO %s.single_clustering" + tableSuffix + " (a, b, c) VALUES (0, 0, 0)");
            execute("INSERT INTO %s.single_clustering" + tableSuffix + " (a, b, c) VALUES (0, 1, 0)");
            MD5Digest id = prepare("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND (b) = (?)");
            UntypedResultSet results = executePrepared(id, makeIntOptions(0));
            assertEquals(1, results.size());
            checkRow(0, results, 0, 0, 0);
        }
    }

    @Test
    public void testPreparedClusteringColumnEqualitySingleMarker() throws Throwable
    {
        for (String tableSuffix : new String[]{"", "_compact"})
        {
            execute("INSERT INTO %s.single_clustering" + tableSuffix + " (a, b, c) VALUES (0, 0, 0)");
            execute("INSERT INTO %s.single_clustering" + tableSuffix + " (a, b, c) VALUES (0, 1, 0)");
            MD5Digest id = prepare("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND (b) = ?");
            UntypedResultSet results = executePrepared(id, options(tuple(0)));
            assertEquals(1, results.size());
            checkRow(0, results, 0, 0, 0);
        }
    }

    @Test
    public void testPreparedSingleClusteringColumnInequality() throws Throwable
    {
        for (String tableSuffix : new String[]{"", "_compact"})
        {
            execute("INSERT INTO %s.single_clustering" + tableSuffix + " (a, b, c) VALUES (0, 0, 0)");
            execute("INSERT INTO %s.single_clustering" + tableSuffix + " (a, b, c) VALUES (0, 1, 0)");
            execute("INSERT INTO %s.single_clustering" + tableSuffix + " (a, b, c) VALUES (0, 2, 0)");

            MD5Digest id = prepare("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND (b) > (?)");
            UntypedResultSet results = executePrepared(id, makeIntOptions(0));
            assertEquals(2, results.size());
            checkRow(0, results, 0, 1, 0);
            checkRow(1, results, 0, 2, 0);

            results = executePrepared(prepare("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND (b) >= (?)"), makeIntOptions(1));
            assertEquals(2, results.size());
            checkRow(0, results, 0, 1, 0);
            checkRow(1, results, 0, 2, 0);

            results = executePrepared(prepare("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND (b) < (?)"), makeIntOptions(2));
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 0);
            checkRow(1, results, 0, 1, 0);

            results = executePrepared(prepare("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND (b) <= (?)"), makeIntOptions(1));
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 0);
            checkRow(1, results, 0, 1, 0);

            results = executePrepared(prepare("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND (b) > (?) AND (b) < (?)"), makeIntOptions(0, 2));
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0);

            results = executePrepared(prepare("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND (b) > (?) AND b < ?"), makeIntOptions(0, 2));
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0);

            results = executePrepared(prepare("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND b > ? AND (b) < (?)"), makeIntOptions(0, 2));
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0);
        }
    }

    @Test
    public void testPreparedSingleClusteringColumnInequalitySingleMarker() throws Throwable
    {
        for (String tableSuffix : new String[]{"", "_compact"})
        {
            execute("INSERT INTO %s.single_clustering" + tableSuffix + " (a, b, c) VALUES (0, 0, 0)");
            execute("INSERT INTO %s.single_clustering" + tableSuffix + " (a, b, c) VALUES (0, 1, 0)");
            execute("INSERT INTO %s.single_clustering" + tableSuffix + " (a, b, c) VALUES (0, 2, 0)");

            MD5Digest id = prepare("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND (b) > ?");
            UntypedResultSet results = executePrepared(id, options(tuple(0)));
            assertEquals(2, results.size());
            checkRow(0, results, 0, 1, 0);
            checkRow(1, results, 0, 2, 0);

            results = executePrepared(prepare("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND (b) >= ?"), options(tuple(1)));
            assertEquals(2, results.size());
            checkRow(0, results, 0, 1, 0);
            checkRow(1, results, 0, 2, 0);

            results = executePrepared(prepare("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND (b) < ?"), options(tuple(2)));
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 0);
            checkRow(1, results, 0, 1, 0);

            results = executePrepared(prepare("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND (b) <= ?"), options(tuple(1)));
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 0);
            checkRow(1, results, 0, 1, 0);


            results = executePrepared(prepare("SELECT * FROM %s.single_clustering" + tableSuffix + " WHERE a=0 AND (b) > ? AND (b) < ?"),
                    options(tuple(0), tuple(2)));
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0);
        }
    }

    @Test
    public void testPrepareMultipleClusteringColumnInequality() throws Throwable
    {
        for (String tableSuffix : new String[]{"", "_compact"})
        {
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 1)");

            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 1, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 1, 1)");

            UntypedResultSet results = executePrepared(prepare(
                    "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b) > (?)"), makeIntOptions(0));
            assertEquals(3, results.size());
            checkRow(0, results, 0, 1, 0, 0);
            checkRow(1, results, 0, 1, 1, 0);
            checkRow(2, results, 0, 1, 1, 1);

            results = executePrepared(prepare(
                    "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c) > (?, ?)"), makeIntOptions(1, 0));
            assertEquals(2, results.size());
            checkRow(0, results, 0, 1, 1, 0);
            checkRow(1, results, 0, 1, 1, 1);

            results = executePrepared(prepare
                    ("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) > (?, ?, ?)"), makeIntOptions(1, 1, 0));
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 1, 1);

            results = executePrepared(prepare(
                            "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) > (?, ?, ?) AND (b) < (?)"),
                    makeIntOptions(0, 1, 0, 1));
            assertEquals(1, results.size());
            checkRow(0, results, 0, 0, 1, 1);

            results = executePrepared(prepare("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) > (?, ?, ?) AND b < ?"), makeIntOptions(0, 1, 0, 1));
            assertEquals(1, results.size());
            checkRow(0, results, 0, 0, 1, 1);

            results = executePrepared(prepare
                            ("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) > (?, ?, ?) AND (b, c) < (?, ?)"),
                    makeIntOptions(0, 1, 1, 1, 1));
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0, 0);

            results = executePrepared(prepare(
                            "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) > (?, ?, ?) AND (b, c, d) < (?, ?, ?)"),
                    makeIntOptions(0, 1, 1, 1, 1, 0));
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0, 0);

            // reversed
            results = executePrepared(prepare(
                            "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b) > (?) ORDER BY b DESC, c DESC, d DESC"),
                    makeIntOptions(0));
            assertEquals(3, results.size());
            checkRow(2, results, 0, 1, 0, 0);
            checkRow(1, results, 0, 1, 1, 0);
            checkRow(0, results, 0, 1, 1, 1);

            results = executePrepared(prepare(
                            "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) > (?, ?, ?) AND (b, c) < (?, ?) ORDER BY b DESC, c DESC, d DESC"),
                    makeIntOptions(0, 1, 1, 1, 1));
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0, 0);
        }
    }

    @Test
    public void testPrepareMultipleClusteringColumnInequalitySingleMarker() throws Throwable
    {
        for (String tableSuffix : new String[]{"", "_compact"})
        {
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 1)");

            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 1, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 1, 1, 1)");

            UntypedResultSet results = executePrepared(prepare(
                    "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b) > ?"), options(tuple(0)));
            assertEquals(3, results.size());
            checkRow(0, results, 0, 1, 0, 0);
            checkRow(1, results, 0, 1, 1, 0);
            checkRow(2, results, 0, 1, 1, 1);

            results = executePrepared(prepare(
                    "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c) > ?"), options(tuple(1, 0)));
            assertEquals(2, results.size());
            checkRow(0, results, 0, 1, 1, 0);
            checkRow(1, results, 0, 1, 1, 1);

            results = executePrepared(prepare
                    ("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) > ?"), options(tuple(1, 1, 0)));
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 1, 1);

            results = executePrepared(prepare(
                            "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) > ? AND (b) < ?"),
                    options(tuple(0, 1, 0), tuple(1)));
            assertEquals(1, results.size());
            checkRow(0, results, 0, 0, 1, 1);

            results = executePrepared(prepare("SELECT * FROM %s.multiple_clustering" + tableSuffix
                    + " WHERE a=0 AND (b, c, d) > ? AND b < ?"), options(tuple(0, 1, 0), ByteBufferUtil.bytes(1)));
            assertEquals(1, results.size());
            checkRow(0, results, 0, 0, 1, 1);

            results = executePrepared(prepare
                            ("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) > ? AND (b, c) < ?"),
                    options(tuple(0, 1, 1), tuple(1, 1)));
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0, 0);

            results = executePrepared(prepare(
                            "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) > ? AND (b, c, d) < ?"),
                    options(tuple(0, 1, 1), tuple(1, 1, 0)));
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0, 0);

            // reversed
            results = executePrepared(prepare(
                            "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b) > ? ORDER BY b DESC, c DESC, d DESC"),
                    options(tuple(0)));
            assertEquals(3, results.size());
            checkRow(2, results, 0, 1, 0, 0);
            checkRow(1, results, 0, 1, 1, 0);
            checkRow(0, results, 0, 1, 1, 1);

            results = executePrepared(prepare(
                            "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) > ? AND (b, c) < ? ORDER BY b DESC, c DESC, d DESC"),
                    options(tuple(0, 1, 1), tuple(1, 1)));
            assertEquals(1, results.size());
            checkRow(0, results, 0, 1, 0, 0);
        }
    }

    @Test
    public void testPrepareLiteralIn() throws Throwable
    {
        for (String tableSuffix : new String[]{"", "_compact"})
        {
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 1)");

            UntypedResultSet results = executePrepared(prepare(
                            "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) IN ((?, ?, ?), (?, ?, ?))"),
                    makeIntOptions(0, 1, 0, 0, 1, 1));
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 1, 0);
            checkRow(1, results, 0, 0, 1, 1);

            // same query, but reversed order for the IN values
            results = executePrepared(prepare(
                            "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) IN ((?, ?, ?), (?, ?, ?))"),
                    makeIntOptions(0, 1, 1, 0, 1, 0));
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 1, 0);
            checkRow(1, results, 0, 0, 1, 1);

            results = executePrepared(prepare("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 and (b, c) IN ((?, ?))"),
                    makeIntOptions(0, 1));
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 1, 0);
            checkRow(1, results, 0, 0, 1, 1);

            results = executePrepared(prepare("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 and (b) IN ((?))"),
                    makeIntOptions(0));
            assertEquals(3, results.size());
            checkRow(0, results, 0, 0, 0, 0);
            checkRow(1, results, 0, 0, 1, 0);
            checkRow(2, results, 0, 0, 1, 1);
        }
    }

    @Test
    public void testPrepareInOneMarkerPerTuple() throws Throwable
    {
        for (String tableSuffix : new String[]{"", "_compact"})
        {
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 1)");

            UntypedResultSet results = executePrepared(prepare(
                            "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) IN (?, ?)"),
                    options(tuple(0, 1, 0), tuple(0, 1, 1)));
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 1, 0);
            checkRow(1, results, 0, 0, 1, 1);

            // same query, but reversed order for the IN values
            results = executePrepared(prepare(
                            "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) IN (?, ?)"),
                    options(tuple(0, 1, 1), tuple(0, 1, 0)));
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 1, 0);
            checkRow(1, results, 0, 0, 1, 1);


            results = executePrepared(prepare("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 and (b, c) IN (?)"),
                    options(tuple(0, 1)));
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 1, 0);
            checkRow(1, results, 0, 0, 1, 1);

            results = executePrepared(prepare("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 and (b) IN (?)"),
                    options(tuple(0)));
            assertEquals(3, results.size());
            checkRow(0, results, 0, 0, 0, 0);
            checkRow(1, results, 0, 0, 1, 0);
            checkRow(2, results, 0, 0, 1, 1);
        }
    }

    @Test
    public void testPrepareInOneMarker() throws Throwable
    {
        for (String tableSuffix : new String[]{"", "_compact"})
        {
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 0, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 0)");
            execute("INSERT INTO %s.multiple_clustering" + tableSuffix + " (a, b, c, d) VALUES (0, 0, 1, 1)");

            UntypedResultSet results = executePrepared(prepare(
                            "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) IN ?"),
                    options(list(tuple(0, 1, 0), tuple(0, 1, 1))));
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 1, 0);
            checkRow(1, results, 0, 0, 1, 1);

            // same query, but reversed order for the IN values
            results = executePrepared(prepare(
                            "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) IN ?"),
                    options(list(tuple(0, 1, 1), tuple(0, 1, 0))));
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 1, 0);
            checkRow(1, results, 0, 0, 1, 1);

            results = executePrepared(prepare(
                            "SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 AND (b, c, d) IN ?"),
                    options(list()));
            assertTrue(results.isEmpty());

            results = executePrepared(prepare("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 and (b, c) IN ?"),
                    options(list(tuple(0, 1))));
            assertEquals(2, results.size());
            checkRow(0, results, 0, 0, 1, 0);
            checkRow(1, results, 0, 0, 1, 1);

            results = executePrepared(prepare("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 and (b) IN ?"),
                    options(list(tuple(0))));
            assertEquals(3, results.size());
            checkRow(0, results, 0, 0, 0, 0);
            checkRow(1, results, 0, 0, 1, 0);
            checkRow(2, results, 0, 0, 1, 1);

            results = executePrepared(prepare("SELECT * FROM %s.multiple_clustering" + tableSuffix + " WHERE a=0 and (b) IN ?"),
                    options(list()));
            assertTrue(results.isEmpty());
        }
    }

    @Test
    public void testMultipleClusteringWithIndex() throws Throwable
    {
        execute("INSERT INTO %s.multiple_clustering_with_indices (a, b, c, d, e) VALUES (0, 0, 0, 0, 0)");
        execute("INSERT INTO %s.multiple_clustering_with_indices (a, b, c, d, e) VALUES (0, 0, 1, 0, 1)");
        execute("INSERT INTO %s.multiple_clustering_with_indices (a, b, c, d, e) VALUES (0, 0, 1, 1, 2)");
        execute("INSERT INTO %s.multiple_clustering_with_indices (a, b, c, d, e) VALUES (0, 1, 0, 0, 0)");
        execute("INSERT INTO %s.multiple_clustering_with_indices (a, b, c, d, e) VALUES (0, 1, 1, 0, 1)");
        execute("INSERT INTO %s.multiple_clustering_with_indices (a, b, c, d, e) VALUES (0, 1, 1, 1, 2)");
        execute("INSERT INTO %s.multiple_clustering_with_indices (a, b, c, d, e) VALUES (0, 2, 0, 0, 0)");

        UntypedResultSet results = execute("SELECT * FROM %s.multiple_clustering_with_indices WHERE (b) = (1)");
        assertEquals(3, results.size());
        checkRow(0, results, 0, 1, 0, 0, 0);
        checkRow(1, results, 0, 1, 1, 0, 1);
        checkRow(2, results, 0, 1, 1, 1, 2);

        results = execute("SELECT * FROM %s.multiple_clustering_with_indices WHERE (b, c) = (1, 1) ALLOW FILTERING");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 1, 1, 0, 1);
        checkRow(1, results, 0, 1, 1, 1, 2);

        results = execute("SELECT * FROM %s.multiple_clustering_with_indices WHERE a = 0 AND (b, c) = (1, 1) AND e = 2");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 1, 1, 1, 2);

        results = execute("SELECT * FROM %s.multiple_clustering_with_indices WHERE (b, c) = (1, 1) AND e = 2 ALLOW FILTERING");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 1, 1, 1, 2);

        results = execute("SELECT * FROM %s.multiple_clustering_with_indices WHERE a = 0 AND (b) IN ((1)) AND e = 2");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 1, 1, 1, 2);

        results = execute("SELECT * FROM %s.multiple_clustering_with_indices WHERE (b) IN ((1)) AND e = 2 ALLOW FILTERING");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 1, 1, 1, 2);

        results = execute("SELECT * FROM %s.multiple_clustering_with_indices WHERE a = 0 AND (b) IN ((0), (1)) AND e = 2");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 0, 1, 1, 2);
        checkRow(1, results, 0, 1, 1, 1, 2);

        results = execute("SELECT * FROM %s.multiple_clustering_with_indices WHERE (b) IN ((0), (1)) AND e = 2 ALLOW FILTERING");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 0, 1, 1, 2);
        checkRow(1, results, 0, 1, 1, 1, 2);

        results = execute("SELECT * FROM %s.multiple_clustering_with_indices WHERE a = 0 AND (b, c) IN ((0, 1)) AND e = 2");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 0, 1, 1, 2);

        results = execute("SELECT * FROM %s.multiple_clustering_with_indices WHERE (b, c) IN ((0, 1)) AND e = 2 ALLOW FILTERING");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 0, 1, 1, 2);

        results = execute("SELECT * FROM %s.multiple_clustering_with_indices WHERE a = 0 AND (b, c) IN ((0, 1), (1, 1)) AND e = 2");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 0, 1, 1, 2);
        checkRow(1, results, 0, 1, 1, 1, 2);

        results = execute("SELECT * FROM %s.multiple_clustering_with_indices WHERE (b, c) IN ((0, 1), (1, 1)) AND e = 2 ALLOW FILTERING");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 0, 1, 1, 2);
        checkRow(1, results, 0, 1, 1, 1, 2);

        results = execute("SELECT * FROM %s.multiple_clustering_with_indices WHERE a = 0 AND (b) >= (1) AND e = 2");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 1, 1, 1, 2);

        results = execute("SELECT * FROM %s.multiple_clustering_with_indices WHERE (b) >= (1) AND e = 2 ALLOW FILTERING");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 1, 1, 1, 2);

        results = execute("SELECT * FROM %s.multiple_clustering_with_indices WHERE a = 0 AND (b, c) >= (1, 1) AND e = 2");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 1, 1, 1, 2);

        results = execute("SELECT * FROM %s.multiple_clustering_with_indices WHERE (b, c) >= (1, 1) AND e = 2 ALLOW FILTERING");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 1, 1, 1, 2);
    }

    @Test
    public void testPartitionWithIndex() throws Throwable
    {
        execute("INSERT INTO %s.partition_with_indices (a, b, c, d, e, f) VALUES (0, 0, 0, 0, 0, 0)");
        execute("INSERT INTO %s.partition_with_indices (a, b, c, d, e, f) VALUES (0, 0, 0, 1, 0, 1)");
        execute("INSERT INTO %s.partition_with_indices (a, b, c, d, e, f) VALUES (0, 0, 0, 1, 1, 2)");

        execute("INSERT INTO %s.partition_with_indices (a, b, c, d, e, f) VALUES (0, 0, 1, 0, 0, 3)");
        execute("INSERT INTO %s.partition_with_indices (a, b, c, d, e, f) VALUES (0, 0, 1, 1, 0, 4)");
        execute("INSERT INTO %s.partition_with_indices (a, b, c, d, e, f) VALUES (0, 0, 1, 1, 1, 5)");

        execute("INSERT INTO %s.partition_with_indices (a, b, c, d, e, f) VALUES (0, 0, 2, 0, 0, 5)");

        UntypedResultSet results = execute("SELECT * FROM %s.partition_with_indices WHERE a = 0 AND (c) = (1) ALLOW FILTERING");
        assertEquals(3, results.size());
        checkRow(0, results, 0, 0, 1, 0, 0, 3);
        checkRow(1, results, 0, 0, 1, 1, 0, 4);
        checkRow(2, results, 0, 0, 1, 1, 1, 5);

        results = execute("SELECT * FROM %s.partition_with_indices WHERE a = 0 AND (c, d) = (1, 1) ALLOW FILTERING");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 0, 1, 1, 0, 4);
        checkRow(1, results, 0, 0, 1, 1, 1, 5);

        results = execute("SELECT * FROM %s.partition_with_indices WHERE a = 0 AND b = 0 AND (c) IN ((1)) AND f = 5");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 0, 1, 1, 1, 5);

        results = execute("SELECT * FROM %s.partition_with_indices WHERE a = 0  AND (c) IN ((1)) AND f = 5 ALLOW FILTERING");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 0, 1, 1, 1, 5);

        results = execute("SELECT * FROM %s.partition_with_indices WHERE a = 0 AND b = 0 AND (c) IN ((1), (2)) AND f = 5");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 0, 1, 1, 1, 5);
        checkRow(1, results, 0, 0, 2, 0, 0, 5);

        results = execute("SELECT * FROM %s.partition_with_indices WHERE a = 0 AND (c) IN ((1), (2)) AND f = 5 ALLOW FILTERING");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 0, 1, 1, 1, 5);
        checkRow(1, results, 0, 0, 2, 0, 0, 5);

        results = execute("SELECT * FROM %s.partition_with_indices WHERE a = 0 AND b = 0 AND (c, d) IN ((1, 0)) AND f = 3");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 0, 1, 0, 0, 3);

        results = execute("SELECT * FROM %s.partition_with_indices WHERE a = 0  AND (c, d) IN ((1, 0)) AND f = 3 ALLOW FILTERING");
        assertEquals(1, results.size());
        checkRow(0, results, 0, 0, 1, 0, 0, 3);

        results = execute("SELECT * FROM %s.partition_with_indices WHERE a = 0 AND b = 0 AND (c) >= (1) AND f = 5");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 0, 1, 1, 1, 5);
        checkRow(1, results, 0, 0, 2, 0, 0, 5);

        results = execute("SELECT * FROM %s.partition_with_indices WHERE a = 0 AND (c) >= (1) AND f = 5 ALLOW FILTERING");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 0, 1, 1, 1, 5);
        checkRow(1, results, 0, 0, 2, 0, 0, 5);

        results = execute("SELECT * FROM %s.partition_with_indices WHERE a = 0 AND b = 0 AND (c, d) >= (1, 1) AND f = 5");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 0, 1, 1, 1, 5);
        checkRow(1, results, 0, 0, 2, 0, 0, 5);

        results = execute("SELECT * FROM %s.partition_with_indices WHERE a = 0 AND (c, d) >= (1, 1) AND f = 5 ALLOW FILTERING");
        assertEquals(2, results.size());
        checkRow(0, results, 0, 0, 1, 1, 1, 5);
        checkRow(1, results, 0, 0, 2, 0, 0, 5);
    }

    @Test(expected=InvalidRequestException.class)
    public void testMissingPartitionComponentWithInRestrictionOnIndexedColumn() throws Throwable
    {
        execute("SELECT * FROM %s.partition_with_indices WHERE a = 0 AND (c, d) IN ((1, 1)) ALLOW FILTERING");
    }

    @Test(expected=InvalidRequestException.class)
    public void testMissingPartitionComponentWithSliceRestrictionOnIndexedColumn() throws Throwable
    {
        execute("SELECT * FROM %s.partition_with_indices WHERE a = 0 AND (c, d) >= (1, 1) ALLOW FILTERING");
    }

    @Test(expected=InvalidRequestException.class)
    public void testPrepareLiteralInWithShortTuple() throws Throwable
    {
        prepare("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) IN ((?, ?))");
    }

    @Test(expected=InvalidRequestException.class)
    public void testPrepareLiteralInWithLongTuple() throws Throwable
    {
        prepare("SELECT * FROM %s.multiple_clustering WHERE a=0 AND (b, c, d) IN ((?, ?, ?, ?, ?))");
    }

    @Test(expected=InvalidRequestException.class)
    public void testPrepareLiteralInWithPartitionKey() throws Throwable
    {
        prepare("SELECT * FROM %s.multiple_clustering WHERE (a, b, c, d) IN ((?, ?, ?, ?))");
    }

    @Test(expected=InvalidRequestException.class)
    public void testPrepareLiteralInSkipsClusteringColumn() throws Throwable
    {
        prepare("SELECT * FROM %s.multiple_clustering WHERE (c, d) IN ((?, ?))");
    }

    private static QueryOptions makeIntOptions(Integer... values)
    {
        List<ByteBuffer> buffers = new ArrayList<>(values.length);
        for (int value : values)
            buffers.add(ByteBufferUtil.bytes(value));
        return new QueryOptions(ConsistencyLevel.ONE, buffers);
    }

    private static ByteBuffer tuple(Integer... values)
    {
        List<AbstractType<?>> types = new ArrayList<>(values.length);
        ByteBuffer[] buffers = new ByteBuffer[values.length];
        for (int i = 0; i < values.length; i++)
        {
            types.add(Int32Type.instance);
            buffers[i] = ByteBufferUtil.bytes(values[i]);
        }

        TupleType type = new TupleType(types);
        return type.buildValue(buffers);
    }

    private static ByteBuffer list(ByteBuffer... values)
    {
        return CollectionType.pack(Arrays.asList(values), values.length);
    }

    private static QueryOptions options(ByteBuffer... buffers)
    {
        return new QueryOptions(ConsistencyLevel.ONE, Arrays.asList(buffers));
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
}
