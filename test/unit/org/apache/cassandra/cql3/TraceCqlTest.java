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

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import org.apache.cassandra.tracing.TraceStateImpl;

import static org.junit.Assert.assertEquals;

public class TraceCqlTest extends CQLTester
{
    static int DEFAULT_WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS;

    @BeforeClass
    public static void setUp()
    {
        // make sure we wait for trace events to complete, see CASSANDRA-12754
        DEFAULT_WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS = TraceStateImpl.WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS;
        TraceStateImpl.WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS = 5;
    }

    @AfterClass
    public static void tearDown()
    {
        TraceStateImpl.WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS = DEFAULT_WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS;
    }

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

            assertEquals("1", trace.getParameters().get("bound_var_0_id"));

            String cql2 = "SELECT id, v1, v2 FROM " + KEYSPACE + '.' + currentTable() + " WHERE id IN (?, ?, ?)";
            pstmt = session.prepare(cql2).enableTracing();
            trace = session.execute(pstmt.bind(19, 15, 16)).getExecutionInfo().getQueryTrace();
            assertEquals(cql2, trace.getParameters().get("query"));
            assertEquals("19", trace.getParameters().get("bound_var_0_id"));
            assertEquals("15", trace.getParameters().get("bound_var_1_id"));
            assertEquals("16", trace.getParameters().get("bound_var_2_id"));

            //some more complex tests for tables with map and tuple data types and long bound values
            createTable("CREATE TABLE %s (id int primary key, v1 text, v2 tuple<int, text, float>, v3 map<int, text>)");
            execute("INSERT INTO %s (id, v1, v2, v3) values (?, ?, ?, ?)", 12, "mahdix", tuple(3, "bar", 2.1f),
                    map(1290, "birthday", 39, "anniversary"));
            execute("INSERT INTO %s (id, v1, v2, v3) values (?, ?, ?, ?)", 274, "CassandraRocks", tuple(9, "foo", 3.14f),
                    map(9181, "statement", 716, "public speech"));

            cql = "SELECT id, v1, v2, v3 FROM " + KEYSPACE + '.' + currentTable() + " WHERE v2 = ? ALLOW FILTERING";
            pstmt = session.prepare(cql)
                           .enableTracing();
            TupleType tt = TupleType.of(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE, DataType.cint(),
                                        DataType.text(), DataType.cfloat());
            TupleValue value = tt.newValue();
            value.setInt(0, 3);
            value.setString(1, "bar");
            value.setFloat(2, 2.1f);

            trace = session.execute(pstmt.bind(value)).getExecutionInfo().getQueryTrace();
            assertEquals(cql, trace.getParameters().get("query"));
            assertEquals("(3, 'bar', 2.1)", trace.getParameters().get("bound_var_0_v2"));

            cql2 = "SELECT id, v1, v2, v3 FROM " + KEYSPACE + '.' + currentTable() + " WHERE v3 CONTAINS KEY ? ALLOW FILTERING";
            pstmt = session.prepare(cql2).enableTracing();
            trace = session.execute(pstmt.bind(9181)).getExecutionInfo().getQueryTrace();

            assertEquals(cql2, trace.getParameters().get("query"));
            assertEquals("9181", trace.getParameters().get("bound_var_0_key(v3)"));

            String boundValue = "Indulgence announcing uncommonly met she continuing two unpleasing terminated. Now " +
                                "busy say down the shed eyes roof paid her. Of shameless collected suspicion existence " +
                                "in. Share walls stuff think but the arise guest. Course suffer to do he sussex it " +
                                "window advice. Yet matter enable misery end extent common men should. Her indulgence " +
                                "but assistance favourable cultivated everything collecting." +
                                "On projection apartments unsatiable so if he entreaties appearance. Rose you wife " +
                                "how set lady half wish. Hard sing an in true felt. Welcomed stronger if steepest " +
                                "ecstatic an suitable finished of oh. Entered at excited at forming between so " +
                                "produce. Chicken unknown besides attacks gay compact out you. Continuing no " +
                                "simplicity no favourable on reasonably melancholy estimating. Own hence views two " +
                                "ask right whole ten seems. What near kept met call old west dine. Our announcing " +
                                "sufficient why pianoforte. Full age foo set feel her told. Tastes giving in passed" +
                                "direct me valley as supply. End great stood boy noisy often way taken short. Rent the " +
                                "size our more door. Years no place abode in \uFEFFno child my. Man pianoforte too " +
                                "solicitude friendship devonshire ten ask. Course sooner its silent but formal she " +
                                "led. Extensive he assurance extremity at breakfast. Dear sure ye sold fine sell on. " +
                                "Projection at up connection literature insensible motionless projecting." +
                                "Nor hence hoped her after other known defer his. For county now sister engage had " +
                                "season better had waited. Occasional mrs interested far expression acceptance. Day " +
                                "either mrs talent pulled men rather regret admire but. Life ye sake it shed. Five " +
                                "lady he cold in meet up. Service get met adapted matters offence for. Principles man " +
                                "any insipidity age you simplicity understood. Do offering pleasure no ecstatic " +
                                "whatever on mr directly. ";

            String cql3 = "SELECT id, v1, v2, v3 FROM " + KEYSPACE + '.' + currentTable() + " WHERE v3 CONTAINS ? ALLOW FILTERING";
            pstmt = session.prepare(cql3).enableTracing();
            trace = session.execute(pstmt.bind(boundValue)).getExecutionInfo().getQueryTrace();

            assertEquals(cql3, trace.getParameters().get("query"));

            //when tracing is done, this boundValue will be surrounded by single quote, and first 1000 characters
            //will be filtered. Here we take into account single quotes by adding them to the expected output
            assertEquals("'" + boundValue.substring(0, 999) + "...'", trace.getParameters().get("bound_var_0_value(v3)"));
        }
    }
}
