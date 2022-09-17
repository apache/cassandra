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
package org.apache.cassandra.cql3.validation.miscellaneous;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;

public class PreparedStatementCollisionTest extends CQLTester
{
    private void helpTestEnsureNonFullyQualifiedPreparedDoNotCollide(boolean newPreparedStatementBehaviour) throws Throwable
    {
        execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".test_nonfullyqualified(a int primary key, b int)");
        execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE_PER_TEST + ".test_nonfullyqualified(a int primary key, b int)");

        String queryString = "SELECT b FROM test_nonfullyqualified where a = 10";
        MD5Digest hashWithoutKeyspace = QueryProcessor.computeId(queryString, null);
        MD5Digest hashWithKeyspace1 = QueryProcessor.computeId(queryString, KEYSPACE);
        MD5Digest hashWithKeyspace2 = QueryProcessor.computeId(queryString, KEYSPACE_PER_TEST);

        ClientState state = ClientState.forInternalCalls();
        state.setKeyspace(KEYSPACE);
        ResultMessage.Prepared preparedSelect1 = QueryProcessor.instance.prepare(queryString, state, false, newPreparedStatementBehaviour);
        Assert.assertEquals(hashWithKeyspace1, preparedSelect1.statementId);
        Assert.assertNull(QueryProcessor.instance.getPrepared(hashWithoutKeyspace));


        state.setKeyspace(KEYSPACE_PER_TEST);
        ResultMessage.Prepared preparedSelect2 = QueryProcessor.instance.prepare(queryString, state, false, newPreparedStatementBehaviour);
        Assert.assertEquals(hashWithKeyspace2, preparedSelect2.statementId);
        Assert.assertNull(QueryProcessor.instance.getPrepared(hashWithoutKeyspace));
        Assert.assertNotEquals(preparedSelect1.statementId, preparedSelect2.statementId);

        state.setKeyspace(KEYSPACE_PER_TEST);
        ResultMessage.Prepared preparedSelect3 = QueryProcessor.instance.prepare(queryString, state, false, newPreparedStatementBehaviour);
        Assert.assertEquals(hashWithKeyspace2, preparedSelect2.statementId);
        Assert.assertEquals(preparedSelect2.statementId, preparedSelect3.statementId);
        Assert.assertNull(QueryProcessor.instance.getPrepared(hashWithoutKeyspace));
        Assert.assertEquals(preparedSelect2.statementId, preparedSelect3.statementId);
        Assert.assertEquals(0, QueryProcessor.metrics.preparedStatementCollisionFound.getCount());
    }

    public void helpTestEnsureFullyQualifiedPreparedNoKeyspaceDoNotCollide(boolean newPreparedStatementBehaviour) throws Throwable
    {
        execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".test_fullyqualified_noks(a int primary key, b int)");
        execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE_PER_TEST + ".test_fullyqualified_noks(a int primary key, b int)");

        String queryString1 = String.format("SELECT b FROM %s.test_fullyqualified_noks where a = 10", KEYSPACE);
        String queryString2 = String.format("SELECT b FROM %s.test_fullyqualified_noks where a = 10", KEYSPACE_PER_TEST);
        MD5Digest hashWithoutKeyspace1 = QueryProcessor.computeId(queryString1, null);
        MD5Digest hashWithKeyspace1 = QueryProcessor.computeId(queryString1, KEYSPACE);
        MD5Digest hashWithoutKeyspace2 = QueryProcessor.computeId(queryString2, null);
        MD5Digest hashWithKeyspace2 = QueryProcessor.computeId(queryString2, KEYSPACE_PER_TEST);


        ClientState state = ClientState.forInternalCalls();
        ResultMessage.Prepared preparedSelect1 = QueryProcessor.instance.prepare(queryString1, state, false, newPreparedStatementBehaviour);
        Assert.assertEquals(hashWithoutKeyspace1, preparedSelect1.statementId);
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(hashWithoutKeyspace1));
        Assert.assertNull(QueryProcessor.instance.getPrepared(hashWithKeyspace1));


        ResultMessage.Prepared preparedSelect2 = QueryProcessor.instance.prepare(queryString2, state, false, newPreparedStatementBehaviour);
        Assert.assertEquals(hashWithoutKeyspace2, preparedSelect2.statementId);
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(hashWithoutKeyspace2));
        Assert.assertNull(QueryProcessor.instance.getPrepared(hashWithKeyspace2));
        Assert.assertNotEquals(preparedSelect1.statementId, preparedSelect2.statementId);

        ResultMessage.Prepared preparedSelect3 = QueryProcessor.instance.prepare(queryString2, state, false, newPreparedStatementBehaviour);
        Assert.assertEquals(hashWithoutKeyspace2, preparedSelect3.statementId);
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(hashWithoutKeyspace2));
        Assert.assertNull(QueryProcessor.instance.getPrepared(hashWithKeyspace2));
        Assert.assertNotEquals(preparedSelect1.statementId, preparedSelect2.statementId);
        Assert.assertEquals(preparedSelect2.statementId, preparedSelect3.statementId);

        Assert.assertEquals(0, QueryProcessor.metrics.preparedStatementCollisionFound.getCount());
    }

    public void helpTestEnsureFullyQualifiedPreparedWithKeyspaceDoNotCollide(boolean newPreparedStatementBehaviour) throws Throwable
    {
        execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".test_fullyqualified_withks(a int primary key, b int)");
        execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE_PER_TEST + ".test_fullyqualified_withks(a int primary key, b int)");

        String queryString1 = String.format("SELECT b FROM %s.test_fullyqualified_withks where a = 10", KEYSPACE);
        String queryString2 = String.format("SELECT b FROM %s.test_fullyqualified_withks where a = 10", KEYSPACE_PER_TEST);

        MD5Digest hashWithoutKeyspace1 = QueryProcessor.computeId(queryString1, null);
        MD5Digest hashWithKeyspace1 = QueryProcessor.computeId(queryString1, KEYSPACE);
        MD5Digest hashWithoutKeyspace2 = QueryProcessor.computeId(queryString2, null);
        MD5Digest hashWithKeyspace2 = QueryProcessor.computeId(queryString2, KEYSPACE_PER_TEST);


        ClientState state = ClientState.forInternalCalls();
        state.setKeyspace(KEYSPACE);
        ResultMessage.Prepared preparedSelect1 = QueryProcessor.instance.prepare(queryString1, state, false, newPreparedStatementBehaviour);
        if (newPreparedStatementBehaviour)
        {
            Assert.assertEquals(hashWithoutKeyspace1, preparedSelect1.statementId);
        }
        else
        {
            Assert.assertEquals(hashWithKeyspace1, preparedSelect1.statementId);
        }
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(hashWithoutKeyspace1));
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(hashWithKeyspace1));


        state.setKeyspace(KEYSPACE_PER_TEST);
        ResultMessage.Prepared preparedSelect2 = QueryProcessor.instance.prepare(queryString2, state, false, newPreparedStatementBehaviour);
        if (newPreparedStatementBehaviour)
        {
            Assert.assertEquals(hashWithoutKeyspace2, preparedSelect2.statementId);
        }
        else
        {
            Assert.assertEquals(hashWithKeyspace2, preparedSelect2.statementId);
        }
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(hashWithoutKeyspace2));
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(hashWithKeyspace2));
        Assert.assertNotEquals(preparedSelect1.statementId, preparedSelect2.statementId);

        state.setKeyspace(KEYSPACE_PER_TEST);
        ResultMessage.Prepared preparedSelect3 = QueryProcessor.instance.prepare(queryString2, state, false, newPreparedStatementBehaviour);
        if (newPreparedStatementBehaviour)
        {
            Assert.assertEquals(hashWithoutKeyspace2, preparedSelect3.statementId);
        }
        else
        {
            Assert.assertEquals(hashWithKeyspace2, preparedSelect3.statementId);
        }
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(hashWithoutKeyspace2));
        Assert.assertNotNull(QueryProcessor.instance.getPrepared(hashWithKeyspace2));
        Assert.assertNotEquals(preparedSelect1.statementId, preparedSelect2.statementId);
        Assert.assertEquals(preparedSelect2.statementId, preparedSelect3.statementId);

        Assert.assertEquals(0, QueryProcessor.metrics.preparedStatementCollisionFound.getCount());
    }

    @Test
    public void testEnsureNonFullyQualifiedPreparedDoNotCollideOldBehavior() throws Throwable
    {
        helpTestEnsureNonFullyQualifiedPreparedDoNotCollide(false);
    }

    @Test
    public void testEnsureNonFullyQualifiedPreparedDoNotCollideNewBehavior() throws Throwable
    {
        helpTestEnsureNonFullyQualifiedPreparedDoNotCollide(true);
    }

    @Test
    public void testEnsureFullyQualifiedPreparedNoKeyspaceDoNotCollideOldBehavior() throws Throwable
    {
        helpTestEnsureFullyQualifiedPreparedNoKeyspaceDoNotCollide(false);
    }

    @Test
    public void testEnsureFullyQualifiedPreparedNoKeyspaceDoNotCollideNewBehavior() throws Throwable
    {
        helpTestEnsureFullyQualifiedPreparedNoKeyspaceDoNotCollide(true);
    }

    @Test
    public void testEnsureFullyQualifiedPreparedWithKeyspaceDoNotCollideOldBehavior() throws Throwable
    {
        helpTestEnsureFullyQualifiedPreparedWithKeyspaceDoNotCollide(false);
    }

    @Test
    public void testEnsureFullyQualifiedPreparedWithKeyspaceDoNotCollideNewBehavior() throws Throwable
    {
        helpTestEnsureFullyQualifiedPreparedWithKeyspaceDoNotCollide(true);
    }
}
