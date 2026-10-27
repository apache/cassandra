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

import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.ExecuteCommandStatement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests that INVOKE COMMAND CQL statements are parsed correctly into {@link ExecuteCommandStatement.Raw},
 * using real nodetool command names and realistic argument patterns.
 */
public class ExecuteCommandStatementParseTest
{
    @Test
    public void testParseVersionWithoutArgs()
    {
        ExecuteCommandStatement.Raw stmt = parse("INVOKE COMMAND version;");
        assertEquals("version", stmt.commandName());
        assertTrue(stmt.args().isEmpty());
    }

    @Test
    public void testParseStatusWithKeyspace()
    {
        ExecuteCommandStatement.Raw stmt = parse("INVOKE COMMAND status WITH param0 = 'system';");
        assertEquals("status", stmt.commandName());
        assertEquals(Map.of("param0", "system"), stmt.args());
    }

    @Test
    public void testParseSnapshotWithEscapedQuoteInTag()
    {
        ExecuteCommandStatement.Raw stmt = parse("INVOKE COMMAND snapshot WITH tag = 'pre-upgrade''s_backup';");
        assertEquals("snapshot", stmt.commandName());
        assertEquals(Map.of("tag", "pre-upgrade's_backup"), stmt.args());
    }

    @Test
    public void testParseCleanupWithJobs()
    {
        ExecuteCommandStatement.Raw stmt = parse("INVOKE COMMAND cleanup WITH jobs = 2;");
        assertEquals("cleanup", stmt.commandName());
        assertEquals(Map.of("jobs", "2"), stmt.args());
    }

    @Test
    public void testParseDecommissionWithForce()
    {
        ExecuteCommandStatement.Raw stmt = parse("INVOKE COMMAND decommission WITH force = true;");
        assertEquals("decommission", stmt.commandName());
        assertEquals(Map.of("force", "true"), stmt.args());
    }

    @Test
    public void testParseSetTraceProbabilityWithFloat()
    {
        ExecuteCommandStatement.Raw stmt = parse("INVOKE COMMAND settraceprobability WITH param0 = 0.2;");
        assertEquals("settraceprobability", stmt.commandName());
        assertEquals(Map.of("param0", "0.2"), stmt.args());
    }

    @Test
    public void testParseSetTraceProbabilityWithExponentAndNegativeFloats()
    {
        ExecuteCommandStatement.Raw stmt = parse(
            "INVOKE COMMAND foo WITH a = 1.5e3 AND b = -0.75 AND c = 100.0;"
        );
        assertEquals("foo", stmt.commandName());
        Map<String, Object> args = stmt.args();
        assertEquals(3, args.size());
        assertEquals("1.5e3", args.get("a"));
        assertEquals("-0.75", args.get("b"));
        assertEquals("100.0", args.get("c"));
    }

    @Test
    public void testParseRepairWithDatacenters()
    {
        ExecuteCommandStatement.Raw stmt = parse("INVOKE COMMAND repair WITH specific_dc = ['dc1', 'dc2', 'dc3'];");
        assertEquals("repair", stmt.commandName());
        Object dcs = stmt.args().get("specific_dc");
        assertTrue("Expected List but got " + dcs.getClass().getName(), dcs instanceof List);
        assertEquals(List.of("dc1", "dc2", "dc3"), dcs);
    }

    @Test
    public void testParseRepairWithEmptyHostList()
    {
        ExecuteCommandStatement.Raw stmt = parse("INVOKE COMMAND repair WITH specific_host = [];");
        assertEquals("repair", stmt.commandName());
        Object hosts = stmt.args().get("specific_host");
        assertTrue("Expected List but got " + hosts.getClass().getName(), hosts instanceof List);
        assertEquals(List.of(), hosts);
    }

    @Test
    public void testParseRepairWithHostsContainingEscapedQuotes()
    {
        ExecuteCommandStatement.Raw stmt = parse(
            "INVOKE COMMAND repair WITH specific_host = ['node-1''s.example.com', 'node-2''s.example.com'];"
        );
        assertEquals(List.of("node-1's.example.com", "node-2's.example.com"),
                     stmt.args().get("specific_host"));
    }

    @Test
    public void testParseRepairWithSingleDatacenter()
    {
        ExecuteCommandStatement.Raw stmt = parse("INVOKE COMMAND repair WITH specific_dc = ['us-east-1'];");
        assertEquals(List.of("us-east-1"), stmt.args().get("specific_dc"));
    }

    @Test
    public void testParseForcecompactWithMixedArgs()
    {
        ExecuteCommandStatement.Raw stmt = parse(
            "INVOKE COMMAND forcecompact WITH \"keyspace\" = 'my_ks' AND \"table\" = 'my_table' AND keys = ['k1', 'k2'];"
        );

        assertEquals("forcecompact", stmt.commandName());
        Map<String, Object> args = stmt.args();
        assertEquals(3, args.size());
        assertEquals("my_ks", args.get("keyspace"));
        assertEquals("my_table", args.get("table"));
        assertEquals(List.of("k1", "k2"), args.get("keys"));
    }

    @Test
    public void testParseSetLoggingLevelWithNamedParams()
    {
        ExecuteCommandStatement.Raw stmt = parse(
            "INVOKE COMMAND setlogginglevel WITH component = 'org.apache.cassandra.db' AND level = 'DEBUG';"
        );

        assertEquals("setlogginglevel", stmt.commandName());
        Map<String, Object> args = stmt.args();
        assertEquals(2, args.size());
        assertEquals("org.apache.cassandra.db", args.get("component"));
        assertEquals("DEBUG", args.get("level"));
    }

    private static ExecuteCommandStatement.Raw parse(String cql)
    {
        CQLStatement.Raw stmt = QueryProcessor.parseStatement(cql);
        assertTrue("Expected ExecuteCommandStatement.Raw but got: " + stmt.getClass().getName(),
                   stmt instanceof ExecuteCommandStatement.Raw);
        return (ExecuteCommandStatement.Raw) stmt;
    }
}
