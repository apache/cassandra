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

package org.apache.cassandra.db.guardrails;

import org.junit.Before;

import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;

import static org.apache.cassandra.db.ConsistencyLevel.ALL;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_ONE;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_SERIAL;
import static org.apache.cassandra.db.ConsistencyLevel.ONE;
import static org.apache.cassandra.db.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.SERIAL;

public class GuardrailWhenSerialConsistencyNotProvidedTest extends GuardrailTester
{
    public GuardrailWhenSerialConsistencyNotProvidedTest()
    {
        super(Guardrails.serialConsistency);
    }

    @Before
    public void before()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
    }

    @Test
    public void testWarnsWhenNoSerialConsistencyLevelProvided() throws Throwable
    {
        guardrails().setWarnIfNoSerialConsistencyLevelProvidedForCASEnabled(true);
        testLWTQuery(String.format("INSERT INTO %s.%s (k, v) VALUES (1, 1) IF NOT EXISTS;",
                                   KEYSPACE,
                                   currentTable()), true);
        testLWTQuery(String.format("BEGIN BATCH \n" +
                                   "INSERT INTO %s.%s \n" +
                                   "(k, v) VALUES (1, 1) IF NOT EXISTS\n;" +
                                   "INSERT INTO cql_test_keyspace.%s \n" +
                                   "(k, v) VALUES (1, 2) \n;" +
                                   "APPLY BATCH;",
                                   KEYSPACE,
                                   currentTable(), currentTable()), true);
        testNotUsingLWT(String.format("INSERT INTO %s.%s (k, v) VALUES (1, 1);",
                                      KEYSPACE,
                                      currentTable()));
    }

    @Test
    public void testFailWhenNoSerialConsistencyLevelProvided() throws Throwable
    {
        guardrails().setFailIfNoSerialConsistencyLevelProvidedForCASEnabled(true);
        testLWTQuery(String.format("INSERT INTO %s.%s (k, v) VALUES (1, 1) IF NOT EXISTS;",
                                   KEYSPACE,
                                   currentTable()), false);
        testLWTQuery(String.format("BEGIN BATCH \n" +
                                   "INSERT INTO %s.%s \n" +
                                   "(k, v) VALUES (1, 1) IF NOT EXISTS\n;" +
                                   "INSERT INTO cql_test_keyspace.%s \n" +
                                   "(k, v) VALUES (1, 2) \n;" +
                                   "APPLY BATCH;",
                                   KEYSPACE,
                                   currentTable(), currentTable()), false);
        testNotUsingLWT(String.format("INSERT INTO %s.%s (k, v) VALUES (1, 1);",
                                      KEYSPACE,
                                      currentTable()));
    }

    private void testNotUsingLWT(String query) throws Throwable
    {
        testNotUsingLWT(query, ONE);
        testNotUsingLWT(query, ALL);
        testNotUsingLWT(query, QUORUM);
        testNotUsingLWT(query, LOCAL_ONE);
        testNotUsingLWT(query, LOCAL_QUORUM);
    }

    private void testNotUsingLWT(String query, ConsistencyLevel cl) throws Throwable
    {
        assertValid(query, cl, SERIAL);
        assertValid(query, cl, LOCAL_SERIAL);
        assertValid(query, cl, null);
    }

    private void testLWTQuery(String query, boolean shouldWarn) throws Throwable
    {
        testLWTQuery(query, ONE, shouldWarn);
        testLWTQuery(query, ALL, shouldWarn);
        testLWTQuery(query, QUORUM, shouldWarn);
        testLWTQuery(query, LOCAL_ONE, shouldWarn);
        testLWTQuery(query, LOCAL_QUORUM, shouldWarn);
    }

    private void testLWTQuery(String query, ConsistencyLevel cl, boolean shouldWarn) throws Throwable
    {
        assertValid(query, cl, SERIAL);
        assertValid(query, cl, LOCAL_SERIAL);
        if (shouldWarn) {
            assertWarns(query, cl, null);
        } else {
            assertFails(query, cl, null);
        }
    }

    private void assertValid(String query, ConsistencyLevel cl, ConsistencyLevel serialCl) throws Throwable
    {
        assertValid(() -> execute(userClientState, query, cl, serialCl));
    }

    private void assertWarns(String query, ConsistencyLevel cl, ConsistencyLevel serialCl) throws Throwable
    {
        assertWarns(() -> execute(userClientState, query, cl, serialCl), "Query did not provide a serial consistency level.");
    }

    private void assertFails(String query, ConsistencyLevel cl, ConsistencyLevel serialCl) throws Throwable
    {
        assertFails(() -> execute(userClientState, query, cl, serialCl), "Query did not provide a serial consistency level.");
    }
}

