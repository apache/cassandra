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

package org.apache.cassandra.db.compression;

import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compression.CompressionDictionary.LightweightCompressionDictionary;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.tools.ToolRunner.ToolResult;

import static java.lang.String.format;
import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CompressionDictionaryOrphanedTest extends CQLTester
{
    private static final String tableName = "mytable";

    @BeforeClass
    public static void setup() throws Throwable
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testOrphanedCompressionDictionaries()
    {
        String firstTableId = createDictTable();
        trainDictionary();
        trainDictionary();

        assertDicts(firstTableId);

        // drop that table, so we will have two orphaned
        dropDictTable();
        // this will produce orphaned dictionaries - ones without existing table
        assertOrphaned(firstTableId);

        // create new table but with same name and train, we will have still two orphaned, from last table of same name
        String secondTableId = createDictTable();
        trainDictionary();
        trainDictionary();

        // still two from the first run
        assertOrphaned(firstTableId);
        // call nodetool, clear orphaned
        cleanupOrphaned();
        // verify that orphaned were cleared
        assertNoOrphaned();

        // now we have the second table only, the first one is dropped, and we have no orphaned dicts
        // drop the second table of the same name, that will also produce two orphaned dics
        assertDicts(secondTableId);

        dropDictTable();
        assertOrphaned(secondTableId);
        cleanupOrphaned();
        assertNoOrphaned();
    }

    private String getTableId()
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace(), tableName);
        Assert.assertNotNull(cfs);
        return cfs.metadata.id.toLongString();
    }

    private String createDictTable()
    {
        schemaChange(format("CREATE TABLE %s.%s (id int PRIMARY KEY, data text)" +
                            " WITH compression = {'class': 'ZstdDictionaryCompressor'}",
                            keyspace(), tableName));

        return getTableId();
    }

    private void dropDictTable()
    {
        schemaChange(format("DROP TABLE %s.%s", keyspace(), tableName));
    }

    private void assertDicts(String tableId)
    {
        List<LightweightCompressionDictionary> dicts = SystemDistributedKeyspace.retrieveLightweightCompressionDictionaries();
        assertNotNull(dicts);
        assertEquals(2, dicts.size());
        assertEquals(tableId, dicts.get(0).tableId);
        assertEquals(tableId, dicts.get(1).tableId);
    }

    private void assertOrphaned(String tableId)
    {
        List<LightweightCompressionDictionary> orphaned = SystemDistributedKeyspace.retrieveOrphanedLightweightCompressionDictionaries();
        assertNotNull(orphaned);
        assertEquals(2, orphaned.size());
        assertEquals(tableId, orphaned.get(0).tableId);
        assertEquals(tableId, orphaned.get(1).tableId);

        ToolResult toolResult = invokeNodetool("compressiondictionary", "cleanup", "--dry");
        toolResult.asserts().success();
        String[] split = toolResult.getStdout().split(System.lineSeparator());
        // split[0] is the header
        assertEquals(3, split.length);
        assertTrue(split[1].contains(tableId));
        assertTrue(split[2].contains(tableId));
    }

    private void cleanupOrphaned()
    {
        invokeNodetool("compressiondictionary", "cleanup").asserts().success();
    }

    private void assertNoOrphaned()
    {
        ToolResult toolResult = invokeNodetool("compressiondictionary", "cleanup", "--dry");
        toolResult.asserts().success();
        assertTrue(toolResult.getStdout().isBlank());
    }

    private void trainDictionary()
    {
        createSSTables();

        // Test training command with --force since we have limited test data
        ToolResult result = invokeNodetool("compressiondictionary", "train", "--force", keyspace(), tableName);
        result.assertOnCleanExit();

        assertThat(result.getStdout())
        .as("Should indicate training completed")
        .contains("Training completed successfully")
        .contains(keyspace())
        .contains(tableName);
    }

    private static int batch = 1;

    private void createSSTables()
    {
        for (int file = 0; file < 10; file++)
        {
            int batchSize = 1000;
            for (int i = 0; i < batchSize; i++)
            {
                int index = batch + (i + file * batchSize);
                executeFormattedQuery(format("INSERT INTO %s.%s (id, data) VALUES (?, ?)", keyspace(), tableName),
                                      index, "test data " + index);
            }

            flush();
        }

        batch++;
    }
}
