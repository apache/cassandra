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
package org.apache.cassandra.cql3.statements;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientState;

import static org.junit.Assert.*;

public class DropDataSinkStatementTest
{
    private static final String TEST_SINK_NAME = "test_sink";
    private static final String DROP_QUERY = String.format("DROP DATA_SINK %s", TEST_SINK_NAME);
    private static final String DROP_IF_EXISTS_QUERY = String.format("DROP DATA_SINK IF EXISTS %s", TEST_SINK_NAME);

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testDropDataSinkStatementParsing()
    {
        CQLStatement.Raw statement = QueryProcessor.parseStatement(DROP_QUERY);
        assertTrue(statement instanceof DropDataSinkStatement.Raw);

        DropDataSinkStatement.Raw rawDropStatement = (DropDataSinkStatement.Raw) statement;
        ClientState clientState = ClientState.forInternalCalls();
        DropDataSinkStatement preparedStatement = rawDropStatement.prepare(clientState);

        assertTrue(preparedStatement.toString().contains(TEST_SINK_NAME));
        assertFalse(preparedStatement.toString().contains("IF EXISTS"));
    }

    @Test
    public void testDropDataSinkStatementParsingWithIfExists()
    {
        CQLStatement.Raw statement = QueryProcessor.parseStatement(DROP_IF_EXISTS_QUERY);
        assertTrue(statement instanceof DropDataSinkStatement.Raw);

        DropDataSinkStatement.Raw rawDropStatement = (DropDataSinkStatement.Raw) statement;
        ClientState clientState = ClientState.forInternalCalls();
        DropDataSinkStatement preparedStatement = rawDropStatement.prepare(clientState);

        assertTrue(preparedStatement.toString().contains(TEST_SINK_NAME));
        // Note: toString() doesn't include ifExists flag, just verifying successful parsing
    }

    @Test
    public void testValidConstruction()
    {
        DropDataSinkStatement statement = new DropDataSinkStatement(TEST_SINK_NAME, false);
        assertNotNull(statement);
        assertTrue(statement.toString().contains(TEST_SINK_NAME));
    }

    @Test
    public void testConstructionWithIfExists()
    {
        DropDataSinkStatement statement = new DropDataSinkStatement(TEST_SINK_NAME, true);
        assertNotNull(statement);
        assertTrue(statement.toString().contains(TEST_SINK_NAME));
    }

    @Test
    public void testValidateBasic()
    {
        DropDataSinkStatement statement = new DropDataSinkStatement(TEST_SINK_NAME, false);
        ClientState clientState = ClientState.forInternalCalls();

        // Basic validation should not throw exception for valid sink name
        statement.validate(clientState);
    }

    @Test(expected = InvalidRequestException.class)
    public void testValidateWithNullSinkName()
    {
        DropDataSinkStatement statement = new DropDataSinkStatement(null, false);
        ClientState clientState = ClientState.forInternalCalls();

        statement.validate(clientState);
    }

    @Test(expected = InvalidRequestException.class)
    public void testValidateWithEmptySinkName()
    {
        DropDataSinkStatement statement = new DropDataSinkStatement("", false);
        ClientState clientState = ClientState.forInternalCalls();

        statement.validate(clientState);
    }

    @Test
    public void testAuditLogContext()
    {
        DropDataSinkStatement statement = new DropDataSinkStatement(TEST_SINK_NAME, false);
        assertNotNull(statement.getAuditLogContext());
    }

    @Test
    public void testToString()
    {
        DropDataSinkStatement statement = new DropDataSinkStatement("my_sink", false);
        assertTrue(statement.toString().contains("my_sink"));

        DropDataSinkStatement statementWithIfExists = new DropDataSinkStatement("my_sink", true);
        assertTrue(statementWithIfExists.toString().contains("my_sink"));
    }
}