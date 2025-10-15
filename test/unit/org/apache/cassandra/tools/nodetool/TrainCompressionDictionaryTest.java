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

package org.apache.cassandra.tools.nodetool;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;
import static org.assertj.core.api.Assertions.assertThat;

public class TrainCompressionDictionaryTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Throwable
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testTrainCommandSuccess()
    {
        // Create a table with dictionary compression enabled
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");

        createSSTables(true);

        // Test training command
        ToolRunner.ToolResult result = invokeNodetool("traincompressiondictionary", keyspace(), table);
        result.assertOnCleanExit();

        assertThat(result.getStdout())
        .as("Should indicate training completed")
        .contains("Training completed successfully")
        .contains(keyspace())
        .contains(table);
    }

    @Test
    public void testTrainCommandWithDataButNoSSTables()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");

        // Add test data but don't flush - memtable should be flushed automatically
        createSSTables(false);

        // Test training, the command should run flush before sampling
        ToolRunner.ToolResult result = invokeNodetool("traincompressiondictionary",
                                                      keyspace(),
                                                      table);
        result.assertOnCleanExit();

        assertThat(result.getStdout())
        .as("Should flush automatically when no SSTables available")
        .contains("Training completed successfully");
    }

    @Test
    public void testTrainCommandWithNoSSTables()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");
        ToolRunner.ToolResult result = invokeNodetool("traincompressiondictionary",
                                                      keyspace(),
                                                      table);
        assertThat(result.getStderr())
        .contains("Failed to trigger training: No SSTables available for training", "after flush");
    }

    @Test
    public void testInvalidKeyspace()
    {
        ToolRunner.ToolResult result = invokeNodetool("traincompressiondictionary",
                                                      "nonexistent_keyspace",
                                                      "nonexistent_table");
        result.asserts()
              .failure()
              .errorContains("Failed to trigger training");
    }

    @Test
    public void testInvalidTable()
    {
        ToolRunner.ToolResult result = invokeNodetool("traincompressiondictionary",
                                                      keyspace(),
                                                      "nonexistent_table");
        result.asserts()
              .failure()
              .errorContains("Failed to trigger training");
    }

    @Test
    public void testTrainingOnNonDictionaryTable()
    {
        // Create table without dictionary compression
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'LZ4Compressor'}");

        ToolRunner.ToolResult result = invokeNodetool("traincompressiondictionary",
                                                      keyspace(),
                                                      table);
        result.asserts()
              .failure()
              .errorContains("does not support dictionary compression");
    }

    @Test
    public void testTrainingWithoutDictionaryCompressionEnabled()
    {
        // Create table with Zstd but without dictionary compression
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdCompressor'}");

        ToolRunner.ToolResult result = invokeNodetool("traincompressiondictionary",
                                                      keyspace(),
                                                      table);
        result.asserts()
              .failure()
              .errorContains("does not support dictionary compression");
    }


    @Test
    public void testHelpOutput()
    {
        ToolRunner.ToolResult result = invokeNodetool("help", "traincompressiondictionary");
        result.assertOnCleanExit();

        assertThat(result.getStdout())
        .as("Should show command help")
        .contains("nodetool traincompressiondictionary - Manually trigger compression")
        .contains("dictionary training for a table")
        .contains("keyspace name")
        .contains("table name");
    }

    @Test
    public void testCommandLineArgumentParsing()
    {
        // Test missing required arguments
        ToolRunner.ToolResult result = invokeNodetool("traincompressiondictionary");
        result.asserts()
              .failure()
              .stdoutContains("Missing required parameter");

        // Test missing table argument
        result = invokeNodetool("traincompressiondictionary", keyspace());
        result.asserts()
              .failure()
              .stdoutContains("Missing required parameter");
    }

    private void createSSTables(boolean flush)
    {
        for (int file = 0; file < 10; file++)
        {
            int batchSize = 1000;
            for (int i = 0; i < batchSize; i++)
            {
                int index = i + file * batchSize;
                execute("INSERT INTO %s (id, data) VALUES (?, ?)", index, "test data " + index);
            }
            if (flush)
            {
                flush();
            }
        }
    }
}
