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
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.cql3.QueryProcessor.process;
import static org.apache.cassandra.cql3.QueryProcessor.processInternal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TypeTest
{
    private static final Logger logger = LoggerFactory.getLogger(TypeTest.class);
    static ClientState clientState;
    static String keyspace = "cql3_type_test";

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

    @Test
    public void testNowToUUIDCompatibility() throws Throwable
    {
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.uuid_now (a int, b uuid, PRIMARY KEY (a, b))");
        String insert = "INSERT INTO %s.uuid_now (a, b) VALUES (0, now())";
        String select = "SELECT * FROM %s.uuid_now WHERE a=0 AND b < now()";
        execute(insert);
        UntypedResultSet results = execute(select);
        assertEquals(1, results.size());

        executePrepared(prepare(insert), QueryOptions.DEFAULT);
        results = executePrepared(prepare(select), QueryOptions.DEFAULT);
        assertEquals(2, results.size());
    }

    @Test
    public void testDateCompatibility() throws Throwable
    {
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.date_compatibility (a int, b timestamp, c bigint, d varint, PRIMARY KEY (a, b, c, d))");
        String insert = "INSERT INTO %s.date_compatibility (a, b, c, d) VALUES (0, unixTimestampOf(now()), dateOf(now()), dateOf(now()))";
        String select = "SELECT * FROM %s.date_compatibility WHERE a=0 AND b < unixTimestampOf(now())";
        execute(insert);
        UntypedResultSet results = execute(select);
        assertEquals(1, results.size());

        executePrepared(prepare(insert), QueryOptions.DEFAULT);
        results = executePrepared(prepare(select), QueryOptions.DEFAULT);
        assertEquals(2, results.size());
    }

    @Test
    public void testReversedTypeCompatibility() throws Throwable
    {
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.uuid_now_reversed (a int, b timeuuid, PRIMARY KEY (a, b)) WITH CLUSTERING ORDER BY (b DESC)");
        String insert = "INSERT INTO %s.uuid_now_reversed (a, b) VALUES (0, now())";
        String select = "SELECT * FROM %s.uuid_now_reversed WHERE a=0 AND b < now()";
        execute(insert);
        UntypedResultSet results = execute(select);
        assertEquals(1, results.size());

        executePrepared(prepare(insert), QueryOptions.DEFAULT);
        results = executePrepared(prepare(select), QueryOptions.DEFAULT);
        assertEquals(2, results.size());
    }

    @Test
    // tests CASSANDRA-7797
    public void testAlterReversedColumn() throws Throwable
    {
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.test_alter_reversed (a int, b 'org.apache.cassandra.db.marshal.DateType', PRIMARY KEY (a, b)) WITH CLUSTERING ORDER BY (b DESC)");
        executeSchemaChange("ALTER TABLE %s.test_alter_reversed ALTER b TYPE 'org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType)'");
    }

    @Test
    public void testIncompatibleReversedTypes() throws Throwable
    {
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.test_incompatible_reversed (a int, b 'org.apache.cassandra.db.marshal.DateType', PRIMARY KEY (a, b)) WITH CLUSTERING ORDER BY (b DESC)");
        try
        {
            executeSchemaChange("ALTER TABLE %s.test_incompatible_reversed ALTER b TYPE 'org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimeUUIDType)'");
            fail("Expected error for ALTER statement");
        }
        catch (ConfigurationException e) { }
    }

    @Test
    public void testReversedAndNonReversed() throws Throwable
    {
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.test_reversed_and_non_reversed (a int, b 'org.apache.cassandra.db.marshal.DateType', PRIMARY KEY (a, b))");
        try
        {
            executeSchemaChange("ALTER TABLE %s.test_reversed_and_non_reversed ALTER b TYPE 'org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.DateType)'");
            fail("Expected error for ALTER statement");
        }
        catch (ConfigurationException e) { }
    }
}