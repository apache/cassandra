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
package org.apache.cassandra.management;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CqlExecuteCommandTest extends CQLTester
{
    @Test
    public void testExecuteCommandForcecompact() throws Throwable
    {
        String keyspaceName = createKeyspace("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        String tableName = createTable(keyspaceName, "CREATE TABLE %s (k text PRIMARY KEY, v int)");

        execute(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?)", keyspaceName, tableName), "k1", 1);
        execute(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?)", keyspaceName, tableName), "k2", 2);
        execute(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?)", keyspaceName, tableName), "k4", 4);
        execute(String.format("INSERT INTO %s.%s (k, v) VALUES (?, ?)", keyspaceName, tableName), "k7", 7);

        flush(keyspaceName, tableName);

        // Execute the INVOKE COMMAND statement using CQL-style syntax.
        String command = String.format("INVOKE COMMAND forcecompact WITH \"keyspace\" = '%s' AND \"table\" = '%s' AND keys = ['k4', 'k2', 'k7'];",
                                       keyspaceName, tableName);

        UntypedResultSet result = execute(command);

        assertNotNull("Result should not be null", result);
        assertEquals("Result should have one row", 1, result.size());
        
        UntypedResultSet.Row row = result.one();
        assertNotNull("Row should not be null", row);
        
        assertTrue("Result should have 'output' column", row.has("output"));
        String output = row.getString("output");
        assertNotNull("Output should not be null", output);
        assertFalse("Output should not be empty", output.trim().isEmpty());
    }

    @Test
    public void testExecuteCommandWithInvalidCommand() throws Throwable
    {
        String command = "INVOKE COMMAND nonexistentcommand WITH key = 'value';";
        assertInvalidThrow(InvalidRequestException.class, command);
    }

    @Test
    public void testExecuteCommandWithoutParameters() throws Throwable
    {
        // Test that INVOKE COMMAND statements without WITH clause parse correctly
        String command = "INVOKE COMMAND status;";
        
        UntypedResultSet result = execute(command);
        
        assertNotNull("Result should not be null", result);
        assertEquals("Result should have one row", 1, result.size());
        
        UntypedResultSet.Row row = result.one();
        assertNotNull("Row should not be null", row);
        assertTrue("Result should have 'output' column", row.has("output"));
        String output = row.getString("output");
        assertNotNull("Output should not be null", output);
    }
}
