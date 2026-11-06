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
package org.apache.cassandra.transport;

import java.util.Arrays;
import java.util.Collections;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MD5Digest;

public class LwtDispatcherPrioritizationTest extends CQLTester
{
    private static boolean originalSetting;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        originalSetting = DatabaseDescriptor.getEnableLwtPartitionPrioritization();
        DatabaseDescriptor.setEnableLwtPartitionPrioritization(true);
    }

    @AfterClass
    public static void tearDownClass()
    {
        DatabaseDescriptor.setEnableLwtPartitionPrioritization(originalSetting);
        CQLTester.tearDownClass();
    }

    @Test
    public void testRequestClassifierExtraction()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val int)");

        String lwtQuery = "UPDATE " + KEYSPACE + "." + currentTable() + " SET val = ? WHERE id = ? IF val = ?";
        ResultMessage.Prepared lwtPrepared = QueryProcessor.instance.prepare(lwtQuery, ClientState.forInternalCalls());
        MD5Digest lwtId = lwtPrepared.statementId;

        String selectQuery = "SELECT * FROM " + KEYSPACE + "." + currentTable() + " WHERE id = ?";
        ResultMessage.Prepared selectPrepared = QueryProcessor.instance.prepare(selectQuery, ClientState.forInternalCalls());
        MD5Digest selectId = selectPrepared.statementId;

        CQLStatement lwtStmt = QueryProcessor.instance.getPrepared(lwtId).statement;
        CQLStatement selectStmt = QueryProcessor.instance.getPrepared(selectId).statement;
        Assert.assertTrue(lwtStmt instanceof ModificationStatement);
        Assert.assertTrue(lwtStmt.hasConditions());
        Assert.assertTrue(selectStmt instanceof SelectStatement);
        QueryOptions lwtOptions = QueryOptions.forInternalCalls(
        Arrays.asList(ByteBufferUtil.bytes(10), ByteBufferUtil.bytes(42), ByteBufferUtil.bytes(5)));
        ExecuteMessage lwtMsg = new ExecuteMessage(lwtId, lwtPrepared.resultMetadataId, lwtOptions);

        RequestClassifier.RequestMetadata lwtMeta = RequestClassifier.classify(lwtMsg);
        Assert.assertEquals(RequestClassifier.RequestKind.LWT, lwtMeta.kind);
        Assert.assertNotNull(lwtMeta.partitionKey);
        Assert.assertEquals(42, ByteBufferUtil.toInt(lwtMeta.partitionKey));
        QueryOptions selectOptions = QueryOptions.forInternalCalls(
        Collections.singletonList(ByteBufferUtil.bytes(42)));
        ExecuteMessage selectMsg = new ExecuteMessage(selectId, selectPrepared.resultMetadataId, selectOptions);

        RequestClassifier.RequestMetadata selectMeta = RequestClassifier.classify(selectMsg);
        Assert.assertEquals(RequestClassifier.RequestKind.READ, selectMeta.kind);
    }
}
