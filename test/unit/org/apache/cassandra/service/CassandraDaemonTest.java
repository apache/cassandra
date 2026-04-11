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

package org.apache.cassandra.service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Test;

import static org.apache.cassandra.config.CassandraRelevantProperties.OVERRIDE_COMPACTION_ENTITIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CassandraDaemonTest
{
    @After
    public void tearDown()
    {
        System.clearProperty(OVERRIDE_COMPACTION_ENTITIES.getKey());
    }

    @Test
    public void testParseEntitiesBlankReturnsEmptyMap()
    {
        for (String blank : Arrays.asList(null, "", "  "))
        {
            if (blank == null)
                System.clearProperty(OVERRIDE_COMPACTION_ENTITIES.getKey());
            else
                OVERRIDE_COMPACTION_ENTITIES.setString(blank);

            Map<String, List<String>> result = CassandraDaemon.parseEntititesToOverrideCompaction();
            assertTrue(result.isEmpty());
        }
    }

    @Test
    public void testParseEntitiesSingleKeyspace()
    {
        OVERRIDE_COMPACTION_ENTITIES.setString("ks1");
        Map<String, List<String>> result = CassandraDaemon.parseEntititesToOverrideCompaction();
        assertEquals(1, result.size());
        assertTrue(result.get("ks1").isEmpty());
    }

    @Test
    public void testParseEntitiesMultipleKeyspaces()
    {
        OVERRIDE_COMPACTION_ENTITIES.setString("ks1,ks2,ks3");
        Map<String, List<String>> result = CassandraDaemon.parseEntititesToOverrideCompaction();
        assertEquals(3, result.size());
        assertTrue(result.get("ks1").isEmpty());
        assertTrue(result.get("ks2").isEmpty());
        assertTrue(result.get("ks3").isEmpty());
    }

    @Test
    public void testParseEntitiesSpecificTables()
    {
        OVERRIDE_COMPACTION_ENTITIES.setString("ks1.tbl1,ks1.tbl2");
        Map<String, List<String>> result = CassandraDaemon.parseEntititesToOverrideCompaction();
        assertEquals(1, result.size());
        assertEquals(List.of("tbl1", "tbl2"), result.get("ks1"));
    }

    @Test
    public void testParseEntitiesMixedKeyspacesAndTables()
    {
        OVERRIDE_COMPACTION_ENTITIES.setString("ks1,ks2.tbl1,ks2.tbl2");
        Map<String, List<String>> result = CassandraDaemon.parseEntititesToOverrideCompaction();
        assertEquals(2, result.size());
        assertTrue(result.get("ks1").isEmpty());
        assertEquals(List.of("tbl1", "tbl2"), result.get("ks2"));
    }

    @Test
    public void testParseEntitiesKeyspaceAfterTableOverrides()
    {
        // keyspace-only entry after table-specific entries overrides to all tables
        OVERRIDE_COMPACTION_ENTITIES.setString("ks1.tbl1,ks1.tbl2,ks1");
        Map<String, List<String>> result = CassandraDaemon.parseEntititesToOverrideCompaction();
        assertEquals(1, result.size());
        assertTrue(result.get("ks1").isEmpty());
    }

    @Test
    public void testParseEntitiesTableAfterKeyspaceIsIgnored()
    {
        // keyspace-only entry selects all tables, subsequent table entries are ignored
        OVERRIDE_COMPACTION_ENTITIES.setString("ks1,ks1.tbl1");
        Map<String, List<String>> result = CassandraDaemon.parseEntititesToOverrideCompaction();
        assertEquals(1, result.size());
        assertTrue(result.get("ks1").isEmpty());
    }

    @Test
    public void testParseEntitiesTableAfterKeyspaceOverrideIsIgnored()
    {
        OVERRIDE_COMPACTION_ENTITIES.setString("ks1.tbl1,ks1,ks1.tbl2");
        Map<String, List<String>> result = CassandraDaemon.parseEntititesToOverrideCompaction();
        assertEquals(1, result.size());
        assertTrue(result.get("ks1").isEmpty());
    }

    @Test
    public void testParseEntitiesWhitespaceTrimmed()
    {
        OVERRIDE_COMPACTION_ENTITIES.setString(" ks1 , ks2 . tbl1 ");
        Map<String, List<String>> result = CassandraDaemon.parseEntititesToOverrideCompaction();
        assertEquals(2, result.size());
        assertTrue(result.get("ks1").isEmpty());
        assertEquals(List.of("tbl1"), result.get("ks2"));
    }
}
