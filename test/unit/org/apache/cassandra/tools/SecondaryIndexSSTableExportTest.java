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

package org.apache.cassandra.tools;


import java.util.List;
import java.util.Map;

import org.apache.cassandra.utils.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SecondaryIndexSSTableExportTest extends CQLTester
{
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final TypeReference<List<Map<String, Object>>> jacksonListOfMapsType = new TypeReference<List<Map<String, Object>>>() {};

    @BeforeClass
    public static void beforeClass()
    {
        System.setProperty(org.apache.cassandra.tools.Util.ALLOW_TOOL_REINIT_FOR_TEST, "true"); // Necessary for testing
    }

    @AfterClass
    public static void afterClass()
    {
        System.clearProperty(org.apache.cassandra.tools.Util.ALLOW_TOOL_REINIT_FOR_TEST);   
    }

    @Test
    public void testRegularColumnIndex() throws Throwable
    {
        String createTable = "CREATE TABLE %s (k int PRIMARY KEY, v int)";
        String createIndex = "CREATE INDEX ON %s (v)";
        String insert = "INSERT INTO %s (k, v) VALUES (0, 0)";
        indexSstableValidation(createTable, createIndex, insert);
    }

    @Test
    public void testPartitionKeyIndex() throws Throwable
    {
        String createTable = "CREATE TABLE %s (k int, v int, c text, primary key((k, v)))";
        String createIndex = "CREATE INDEX ON %s (k)";
        String insert = "INSERT INTO %s (k, v) VALUES (0, 0)";
        indexSstableValidation(createTable, createIndex, insert);
    }

    @Test
    public void testKeysWithStaticIndex() throws Throwable
    {
        String createTable = "CREATE TABLE %s (k int , v int, s  text static, primary key(k, v))";
        String createIndex = "CREATE INDEX ON %s (v)";
        String insert = "INSERT INTO %s (k, v, s) VALUES (0, 0, 's')";
        indexSstableValidation(createTable, createIndex, insert);
    }

    @Test
    public void testClusteringIndex() throws Throwable
    {
        String createTable = "CREATE TABLE %s (k int , v int, s  text static, c bigint, primary key((k, v), c))";
        String createIndex = "CREATE INDEX ON %s (c)";
        String insert = "INSERT INTO %s (k, v, s, c) VALUES (0, 0, 's', 10)";
        indexSstableValidation(createTable, createIndex, insert);
    }

    @Test
    public void testCollectionMapKeyIndex() throws Throwable
    {
        String createTable = "CREATE TABLE %s (k int , v int, s  text static, c bigint, m map<bigint, text>, l list<text>, st set<int>, primary key((k, v), c))";
        String createIndex = "CREATE INDEX ON %s (KEYS(m))";
        String insert = "INSERT INTO %s (k, v, s, c, m, l, st) VALUES (0, 0, 's', 10, {100:'v'}, ['l1', 'l2'], {1, 2, 3})";
        indexSstableValidation(createTable, createIndex, insert);
    }

    @Test
    public void testCollectionMapValueIndex() throws Throwable
    {
        String createTable = "CREATE TABLE %s (k int , v int, s  text static, c bigint, m map<bigint, text>, l list<text>, st set<int>, primary key((k, v), c))";
        String createIndex = "CREATE INDEX ON %s (VALUES(m))";
        String insert = "INSERT INTO %s (k, v, s, c, m, l, st) VALUES (0, 0, 's', 10, {100:'v'}, ['l1', 'l2'], {1, 2, 3})";
        indexSstableValidation(createTable, createIndex, insert);
    }

    @Test
    public void testCollectionListIndex() throws Throwable
    {
        String createTable = "CREATE TABLE %s (k int , v int, s  text static, c bigint, m map<bigint, text>, l list<text>, st set<int>, primary key((k, v), c))";
        String createIndex = "CREATE INDEX ON %s (l)";
        String insert = "INSERT INTO %s (k, v, s, c, m, l, st) VALUES (0, 0, 's', 10, {100:'v'}, ['l1', 'l2'], {1, 2, 3})";
        indexSstableValidation(createTable, createIndex, insert);
    }

    @Test
    public void testCollectionSetIndex() throws Throwable
    {
        String createTable = "CREATE TABLE %s (k int , v int, s  text static, c bigint, m map<bigint, text>, l list<text>, st set<int>, primary key((k, v), c))";
        String createIndex = "CREATE INDEX ON %s (st)";
        String insert = "INSERT INTO %s (k, v, s, c, m, l, st) VALUES (0, 0, 's', 10, {100:'v'}, ['l1', 'l2'], {1, 2, 3})";
        indexSstableValidation(createTable, createIndex, insert);
    }

    private void indexSstableValidation(String createTableCql, String createIndexCql, String insertCql) throws Throwable
    {
        Pair<String, String> tableIndex = generateSstable(createTableCql, createIndexCql, insertCql);
        ColumnFamilyStore cfs = getColumnFamilyStore(KEYSPACE, tableIndex.left);
        assertTrue(cfs.indexManager.hasIndexes());
        assertNotNull(cfs.indexManager.getIndexByName(tableIndex.right));
        for (ColumnFamilyStore columnFamilyStore : cfs.indexManager.getAllIndexColumnFamilyStores())
        {
            assertTrue(columnFamilyStore.isIndex());
            for (SSTableReader sst : columnFamilyStore.getLiveSSTables())
            {
                String file = sst.getFilename();
                ToolRunner.ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, file);
                List<Map<String, Object>> parsed = mapper.readValue(tool.getStdout(), jacksonListOfMapsType);
                assertNotNull(tool.getStdout(), parsed.get(0).get("partition"));
                assertNotNull(tool.getStdout(), parsed.get(0).get("rows"));
            }
        }
    }

    private Pair<String, String> generateSstable(String createTableCql, String createIndexCql, String insertCql) throws Throwable
    {
        String table = createTable(createTableCql);
        String index = createIndex(createIndexCql);
        execute(insertCql);
        flush();
        return Pair.create(table, index);
    }
}