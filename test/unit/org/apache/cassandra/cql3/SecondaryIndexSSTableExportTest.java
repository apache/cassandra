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
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.apache.cassandra.utils.Pair;
import org.assertj.core.api.Assertions;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * sstabledump is forked as a separate process (rather than using
 * {@link ToolRunner#invokeClass}) because it runs in the same JVM as this test's
 * schema, and {@link org.apache.cassandra.tools.SSTableExport}'s tool initialization
 * cannot run twice in one JVM.
 */
public class SecondaryIndexSSTableExportTest extends CQLTester
{
    private static final String SSTABLEDUMP_TOOL = "tools/bin/sstabledump";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final TypeReference<List<Map<String, Object>>> jacksonListOfMapsType = new TypeReference<List<Map<String, Object>>>() {};

    @Test
    public void testRegularColumnIndex() throws Throwable
    {
        String createTable = "CREATE TABLE %s (k int PRIMARY KEY, v int)";
        String createIndex = "CREATE INDEX ON %s (v)";
        String insert = "INSERT INTO %s (k, v) VALUES (0, 0)";
        indexSstableValidation(createTable, createIndex, insert);
    }

    @Test
    public void testClusteringColumnIndex() throws Throwable
    {
        String createTable = "CREATE TABLE %s (k int, v int, c bigint, PRIMARY KEY ((k, v), c))";
        String createIndex = "CREATE INDEX ON %s (c)";
        String insert = "INSERT INTO %s (k, v, c) VALUES (0, 0, 10)";
        indexSstableValidation(createTable, createIndex, insert);
    }

    private void indexSstableValidation(String createTableCql, String createIndexCql, String insertCql) throws Throwable
    {
        Pair<String, String> tableIndex = generateSstable(createTableCql, createIndexCql, insertCql);
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableIndex.left);
        assertTrue(cfs.indexManager.hasIndexes());
        assertNotNull(cfs.indexManager.getIndexByName(tableIndex.right));

        for (ColumnFamilyStore indexCfs : cfs.indexManager.getAllIndexColumnFamilyStores())
        {
            assertTrue(indexCfs.isIndex());
            assertFalse(indexCfs.getLiveSSTables().isEmpty());
            for (SSTableReader sstable : indexCfs.getLiveSSTables())
                assertDumpProducesHexClustering(sstable.getFilename());
        }
    }

    @SuppressWarnings("unchecked")
    private void assertDumpProducesHexClustering(String sstableFile) throws Exception
    {
        ToolResult tool = ToolRunner.invoke(SSTABLEDUMP_TOOL, sstableFile);
        tool.assertOnCleanExit();

        List<Map<String, Object>> parsed = mapper.readValue(tool.getStdout(), jacksonListOfMapsType);
        assertNotNull(tool.getStdout(), parsed.get(0).get("partition"));

        List<Map<String, Object>> rows = (List<Map<String, Object>>) parsed.get(0).get("rows");
        assertNotNull(tool.getStdout(), rows);

        List<Object> clustering = (List<Object>) rows.get(0).get("clustering");
        assertFalse(tool.getStdout(), clustering.isEmpty());
        for (Object value : clustering)
            Assertions.assertThat((String) value).startsWith("0x");
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
