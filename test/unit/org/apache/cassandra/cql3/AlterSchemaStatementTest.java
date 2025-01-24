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

import java.net.InetSocketAddress;

import org.junit.Test;

import org.apache.cassandra.schema.SchemaTransformation;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.MD5Digest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AlterSchemaStatementTest extends CQLTester
{
    // For the purposes of these tests, the specific DDL statements aren't especially relevant.
    // When acting as a SchemaTransformation, all AlterSchemaStatements must be able to supply a
    // CQL string for serialization in the cluster metadata log.
    private static final String[] stmts = new String[]
                     {
                     "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
                     "CREATE TABLE ks.t1 (k int PRIMARY KEY)",
                     "ALTER MATERIALIZED VIEW ks.v1 WITH compaction = { 'class' : 'LeveledCompactionStrategy' }",
                     "ALTER TABLE ks.t1 ADD v int",
                     "CREATE TABLE ks.tb like ks1.tb",
                     "CREATE TABLE ks.tb like ks1.tb WITH indexes"
                     };
    private final ClientState clientState = ClientState.forExternalCalls(InetSocketAddress.createUnresolved("127.0.0.1", 1234));

    @Test
    public void testParsingSetsCQLString() throws Throwable
    {
        for (String cql : stmts)
        {
            CQLStatement stmt = QueryProcessor.getStatement(cql, clientState);
            assertTrue(stmt instanceof SchemaTransformation);
            assertEquals(cql, ((SchemaTransformation) stmt).cql());
        }
    }

    @Test
    public void testPreparingSetsCQLString() throws Throwable
    {
        for (String cql : stmts)
        {
            MD5Digest stmtId = QueryProcessor.instance.prepare(cql, clientState).statementId;
            QueryHandler.Prepared prepared = QueryProcessor.instance.getPrepared(stmtId);
            assertEquals(prepared.rawCQLStatement, ((SchemaTransformation)prepared.statement).cql());
        }
    }
}
