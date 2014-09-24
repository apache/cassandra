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
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.ClientState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.cql3.QueryProcessor.process;
import static org.apache.cassandra.cql3.QueryProcessor.processInternal;
import static org.junit.Assert.assertEquals;

public class SelectWithTokenFunctionTest
{
    private static final Logger logger = LoggerFactory.getLogger(SelectWithTokenFunctionTest.class);
    static ClientState clientState;
    static String keyspace = "token_function_test";

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        executeSchemaChange("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.single_partition (a int PRIMARY KEY, b text)");
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.compound_partition (a int, b text, PRIMARY KEY ((a, b)))");
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.single_clustering (a int, b text, PRIMARY KEY (a, b))");
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
    public void testTokenFunctionWithSinglePartitionArgument() throws Throwable
    {
        execute("INSERT INTO %s.single_partition (a, b) VALUES (0, 'a')");

        try
        {
            UntypedResultSet results = execute("SELECT * FROM %s.single_partition WHERE token(a) >= token(0)");
            assertEquals(1, results.size());
            results = execute("SELECT * FROM %s.single_partition WHERE token(a) >= token(0) and token(a) < token(1)");
            assertEquals(1, results.size());
        }
        finally
        {
            execute("DELETE FROM %s.single_partition WHERE a = 0");
        }
    }

    @Test(expected = InvalidRequestException.class)
    public void testTokenFunctionWithWrongLiteralArgument() throws Throwable
    {
        execute("SELECT * FROM %s.single_partition WHERE token(a) > token('a')");
    }

    @Test(expected = InvalidRequestException.class)
    public void testTokenFunctionWithTwoGreaterThan() throws Throwable
    {
        execute("SELECT * FROM %s.single_clustering WHERE token(a) >= token(0) and token(a) >= token(1)");
    }

    @Test(expected = InvalidRequestException.class)
    public void testTokenFunctionWithGreaterThanAndEquals() throws Throwable
    {
        execute("SELECT * FROM %s.single_clustering WHERE token(a) >= token(0) and token(a) = token(1)");
    }

    @Test(expected = SyntaxException.class)
    public void testTokenFunctionWithGreaterThanAndIn() throws Throwable
    {
        execute("SELECT * FROM %s.single_clustering WHERE token(a) >= token(0) and token(a) in (token(1))");
    }

    @Test(expected = InvalidRequestException.class)
    public void testTokenFunctionWithPartitionKeyAndClusteringKeyArguments() throws Throwable
    {
        execute("SELECT * FROM %s.single_clustering WHERE token(a, b) > token(0, 'c')");
    }

    @Test(expected = InvalidRequestException.class)
    public void testTokenFunctionWithCompoundPartitionKeyAndWrongLiteralArgument() throws Throwable
    {
        execute("SELECT * FROM %s.single_partition WHERE token(a, b) >= token('c', 0)");
    }

    @Test
    public void testTokenFunctionWithCompoundPartition() throws Throwable
    {
        execute("INSERT INTO %s.compound_partition (a, b) VALUES (0, 'a')");
        execute("INSERT INTO %s.compound_partition (a, b) VALUES (0, 'b')");
        execute("INSERT INTO %s.compound_partition (a, b) VALUES (0, 'c')");

        try
        {
            UntypedResultSet results = execute("SELECT * FROM %s.compound_partition WHERE token(a, b) > token(0, 'a')");
            assertEquals(2, results.size());
            results = execute("SELECT * FROM %s.compound_partition WHERE token(a, b) > token(0, 'a') "
                    + "and token(a, b) < token(0, 'd')");
            assertEquals(2, results.size());
        }
        finally
        {
            execute("DELETE FROM %s.compound_partition WHERE a = 0 and b in ('a', 'b', 'c')");
        }
    }

    @Test(expected = InvalidRequestException.class)
    public void testTokenFunctionWithCompoundPartitionKeyAndColumnIdentifierInWrongOrder() throws Throwable
    {
        execute("SELECT * FROM %s.compound_partition WHERE token(b, a) > token(0, 'c')");
    }

    @Test(expected = InvalidRequestException.class)
    public void testTokenFunctionOnEachPartitionKeyColumns() throws Throwable
    {
        execute("SELECT * FROM %s.compound_partition WHERE token(a) > token(0) and token(b) > token('c')");
    }
}
