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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.junit.Test;


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SecondaryIndexSSTableExportTest extends CQLTester
{

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final TypeReference<List<Map<String, Object>>> jacksonListOfMapsType = new TypeReference<List<Map<String, Object>>>() {};
     

    @Test
    public void testRegularColumnIndex() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String index  = createIndex("CREATE INDEX ON %s (v)");
        execute("INSERT INTO %s (k, v) VALUES (0, 0) ");
        flush();
        ColumnFamilyStore cfs = getColumnFamilyStore(KEYSPACE, table);
        assertTrue(cfs.indexManager.hasIndexes());
        assertTrue(cfs.indexManager.getIndexByName(index) != null);
        for(ColumnFamilyStore columnFamilyStore : cfs.indexManager.getAllIndexColumnFamilyStores())
        {
            assertTrue(columnFamilyStore.isIndex());
            for(SSTableReader sst : columnFamilyStore.getLiveSSTables())
            {
                String file = sst.getFilename();
                System.out.println(file);
                ToolRunner.ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, file);
                List<Map<String, Object>> parsed = mapper.readValue(tool.getStdout(), jacksonListOfMapsType);
                System.out.println(parsed);
                assertNotNull(tool.getStdout(), parsed.get(0).get("partition"));
                assertNotNull(tool.getStdout(), parsed.get(0).get("rows"));
            }
        }
    }

    @Test
    public void testPartitionKeyIndex() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (k int, v int, c text, primary key((k, v)))");
        String index  = createIndex("CREATE INDEX ON %s (k)");
        execute("INSERT INTO %s (k, v) VALUES (0, 0) ");
        flush();
        ColumnFamilyStore cfs = getColumnFamilyStore(KEYSPACE, table);
        assertTrue(cfs.indexManager.hasIndexes());
        assertTrue(cfs.indexManager.getIndexByName(index) != null);
        for(ColumnFamilyStore columnFamilyStore : cfs.indexManager.getAllIndexColumnFamilyStores())
        {
            assertTrue(columnFamilyStore.isIndex());
            for(SSTableReader sst : columnFamilyStore.getLiveSSTables())
            {
                String file = sst.getFilename();
                System.out.println(file);
                ToolRunner.ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, file);
                List<Map<String, Object>> parsed = mapper.readValue(tool.getStdout(), jacksonListOfMapsType);
                System.out.println(parsed);
                assertNotNull(tool.getStdout(), parsed.get(0).get("partition"));
                assertNotNull(tool.getStdout(), parsed.get(0).get("rows"));
            }
        }
    }
    
    @Test
    public void testKeysWithStaticIndex() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (k int , v int, s  text static, primary key(k, v))");
        String index  = createIndex("CREATE INDEX ON %s (v)");
        execute("INSERT INTO %s (k, v, s) VALUES (0, 0, 's') ");
        flush();
        ColumnFamilyStore cfs = getColumnFamilyStore(KEYSPACE, table);
        assertTrue(cfs.indexManager.hasIndexes());
        assertTrue(cfs.indexManager.getIndexByName(index) != null);
        for(ColumnFamilyStore columnFamilyStore : cfs.indexManager.getAllIndexColumnFamilyStores())
        {
            assertTrue(columnFamilyStore.isIndex());
            for(SSTableReader sst : columnFamilyStore.getLiveSSTables())
            {
                String file = sst.getFilename();
                System.out.println(file);
                ToolRunner.ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, file);
                List<Map<String, Object>> parsed = mapper.readValue(tool.getStdout(), jacksonListOfMapsType);
                System.out.println(parsed);
                assertNotNull(tool.getStdout(), parsed.get(0).get("partition"));
                assertNotNull(tool.getStdout(), parsed.get(0).get("rows"));
            }
        }
    }

    @Test
    public void testClusteringIndex() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (k int , v int, s  text static, c bigint, primary key((k, v), c))");
        String index  = createIndex("CREATE INDEX ON %s (c)");
        execute("INSERT INTO %s (k, v, s, c) VALUES (0, 0, 's', 10) ");
        flush();
        ColumnFamilyStore cfs = getColumnFamilyStore(KEYSPACE, table);
        assertTrue(cfs.indexManager.hasIndexes());
        assertTrue(cfs.indexManager.getIndexByName(index) != null);
        for(ColumnFamilyStore columnFamilyStore : cfs.indexManager.getAllIndexColumnFamilyStores())
        {
            assertTrue(columnFamilyStore.isIndex());
            for(SSTableReader sst : columnFamilyStore.getLiveSSTables())
            {
                String file = sst.getFilename();
                System.out.println(file);
                ToolRunner.ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, file);
                List<Map<String, Object>> parsed = mapper.readValue(tool.getStdout(), jacksonListOfMapsType);
                System.out.println(parsed);
                assertNotNull(tool.getStdout(), parsed.get(0).get("partition"));
                assertNotNull(tool.getStdout(), parsed.get(0).get("rows"));
            }
        }
    }


    @Test
    public void testCollectionMapKeyIndex() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (k int , v int, s  text static, c bigint, m map<bigint, text>, l list<text>, st set<int>, primary key((k, v), c))");
        String index  = createIndex("CREATE INDEX ON %s ( KEYS (m))");
        execute("INSERT INTO %s (k, v, s, c, m, l, st) VALUES (0, 0, 's', 10, {100:'v'}, ['l1', 'l2'], {1, 2, 3}) ");
        flush();
        ColumnFamilyStore cfs = getColumnFamilyStore(KEYSPACE, table);
        assertTrue(cfs.indexManager.hasIndexes());
        assertTrue(cfs.indexManager.getIndexByName(index) != null);
        for(ColumnFamilyStore columnFamilyStore : cfs.indexManager.getAllIndexColumnFamilyStores())
        {
            assertTrue(columnFamilyStore.isIndex());
            for(SSTableReader sst : columnFamilyStore.getLiveSSTables())
            {
                String file = sst.getFilename();
                System.out.println(file);
                ToolRunner.ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, file);
                List<Map<String, Object>> parsed = mapper.readValue(tool.getStdout(), jacksonListOfMapsType);
                System.out.println(parsed);
                assertNotNull(tool.getStdout(), parsed.get(0).get("partition"));
                assertNotNull(tool.getStdout(), parsed.get(0).get("rows"));
            }
        }
    }

    @Test
    public void testCollectionMapValueIndex() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (k int , v int, s  text static, c bigint, m map<bigint, text>, l list<text>, st set<int>, primary key((k, v), c))");
        String index  = createIndex("CREATE INDEX ON %s ( VALUES (m))");
        execute("INSERT INTO %s (k, v, s, c, m, l, st) VALUES (0, 0, 's', 10, {100:'v'}, ['l1', 'l2'], {1, 2, 3}) ");
        flush();
        ColumnFamilyStore cfs = getColumnFamilyStore(KEYSPACE, table);
        assertTrue(cfs.indexManager.hasIndexes());
        assertTrue(cfs.indexManager.getIndexByName(index) != null);
        for(ColumnFamilyStore columnFamilyStore : cfs.indexManager.getAllIndexColumnFamilyStores())
        {
            assertTrue(columnFamilyStore.isIndex());
            for(SSTableReader sst : columnFamilyStore.getLiveSSTables())
            {
                String file = sst.getFilename();
                System.out.println(file);
                ToolRunner.ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, file);
                List<Map<String, Object>> parsed = mapper.readValue(tool.getStdout(), jacksonListOfMapsType);
                System.out.println(parsed);
                assertNotNull(tool.getStdout(), parsed.get(0).get("partition"));
                assertNotNull(tool.getStdout(), parsed.get(0).get("rows"));
            }
        }
    }

    @Test
    public void testCollectionListIndex() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (k int , v int, s  text static, c bigint, m map<bigint, text>, l list<text>, st set<int>, primary key((k, v), c))");
        String index  = createIndex("CREATE INDEX ON %s (l)");
        execute("INSERT INTO %s (k, v, s, c, m, l, st) VALUES (0, 0, 's', 10, {100:'v'}, ['l1', 'l2'], {1, 2, 3}) ");
        flush();
        ColumnFamilyStore cfs = getColumnFamilyStore(KEYSPACE, table);
        assertTrue(cfs.indexManager.hasIndexes());
        assertTrue(cfs.indexManager.getIndexByName(index) != null);
        for(ColumnFamilyStore columnFamilyStore : cfs.indexManager.getAllIndexColumnFamilyStores())
        {
            assertTrue(columnFamilyStore.isIndex());
            for(SSTableReader sst : columnFamilyStore.getLiveSSTables())
            {
                String file = sst.getFilename();
                System.out.println(file);
                ToolRunner.ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, file);
                List<Map<String, Object>> parsed = mapper.readValue(tool.getStdout(), jacksonListOfMapsType);
                System.out.println(parsed);
                assertNotNull(tool.getStdout(), parsed.get(0).get("partition"));
                assertNotNull(tool.getStdout(), parsed.get(0).get("rows"));
            }
        }
    }

    @Test
    public void testCollectionSetIndex() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (k int , v int, s  text static, c bigint, m map<bigint, text>, l list<text>, st set<int>, primary key((k, v), c))");
        String index  = createIndex("CREATE INDEX ON %s (  st)");
        execute("INSERT INTO %s (k, v, s, c, m, l, st) VALUES (0, 0, 's', 10, {100:'v'}, ['l1', 'l2'], {1, 2, 3}) ");
        flush();
        ColumnFamilyStore cfs = getColumnFamilyStore(KEYSPACE, table);
        assertTrue(cfs.indexManager.hasIndexes());
        assertTrue(cfs.indexManager.getIndexByName(index) != null);
        for(ColumnFamilyStore columnFamilyStore : cfs.indexManager.getAllIndexColumnFamilyStores())
        {
            assertTrue(columnFamilyStore.isIndex());
            for(SSTableReader sst : columnFamilyStore.getLiveSSTables())
            {
                String file = sst.getFilename();
                System.out.println(file);
                ToolRunner.ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, file);
                List<Map<String, Object>> parsed = mapper.readValue(tool.getStdout(), jacksonListOfMapsType);
                System.out.println(parsed);
                assertNotNull(tool.getStdout(), parsed.get(0).get("partition"));
                assertNotNull(tool.getStdout(), parsed.get(0).get("rows"));
            }
        }
    }
}
