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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.gms.Gossiper;

import static org.apache.cassandra.cql3.QueryProcessor.process;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CreateAndAlterKeyspaceTest
{
    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
    }

    @AfterClass
    public static void stopGossiper()
    {
        Gossiper.instance.stop();
    }

    @Test
    // tests CASSANDRA-9565
    public void testCreateAndAlterWithDoubleWith() throws Throwable
    {
        String[] stmts = new String[] {"ALTER KEYSPACE WITH WITH DURABLE_WRITES = true",
                                       "ALTER KEYSPACE ks WITH WITH DURABLE_WRITES = true",
                                       "CREATE KEYSPACE WITH WITH DURABLE_WRITES = true",
                                       "CREATE KEYSPACE ks WITH WITH DURABLE_WRITES = true"};

        for (String stmt : stmts) {
            assertInvalidSyntax(stmt, "no viable alternative at input 'WITH'");
        }
    }

    /**
     * Checks that the specified statement result in a <code>SyntaxException</code> containing the specified message.
     *
     * @param stmt the statement to check
     */
    private static void assertInvalidSyntax(String stmt, String msg) throws Throwable {
        try {
            process(stmt, ConsistencyLevel.ONE);
            fail();
        } catch (RuntimeException e) {
            assertSyntaxException(e.getCause(), msg);
        }
    }

    /**
     * Asserts that the specified exception is a <code>SyntaxException</code> for which the error message contains
     * the specified text.
     *
     * @param exception the exception to test
     * @param expectedContent the expected content of the error message
     */
    private static void assertSyntaxException(Throwable exception, String expectedContent) {
        assertTrue("The exception should be a SyntaxException but is not", exception instanceof SyntaxException);

        String msg = exception.getMessage();
        assertTrue(String.format("The error message was expected to contains: %s but was %s", expectedContent, msg),
                   msg.contains(expectedContent));
    }
}
