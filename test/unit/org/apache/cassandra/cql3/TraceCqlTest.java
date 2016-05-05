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

import java.util.List;

import org.junit.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.Session;

import static org.junit.Assert.assertEquals;

public class TraceCqlTest extends CQLTester
{
    @Test
    public void testCqlStatementTracing() throws Throwable
    {
        requireNetwork();

        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 1, "Apache", "Cassandra");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 2, "trace", "test");

        try (Session session = sessionNet())
        {
            String cql = "SELECT id, v1, v2 FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = ?";
            PreparedStatement pstmt = session.prepare(cql)
                                             .enableTracing();
            QueryTrace trace = session.execute(pstmt.bind(1)).getExecutionInfo().getQueryTrace();
            assertEquals(cql, trace.getParameters().get("query"));
        }
    }
}
