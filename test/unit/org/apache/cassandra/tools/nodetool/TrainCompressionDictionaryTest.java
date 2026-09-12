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

import org.apache.cassandra.cql3.CQLNodetoolProtocolTester;
import org.apache.cassandra.io.compress.IDictionaryCompressor;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class TrainCompressionDictionaryTest extends CQLNodetoolProtocolTester
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

        // Test training command with --force since we have limited test data
        ToolRunner.ToolResult result = invokeNodetool("compressiondictionary", "train", "--force", keyspace(), table);
        result.assertOnCleanExit();

        assertThat(result.getStdout())
        .as("Should indicate training completed")
        .contains("Training completed successfully")
        .contains(keyspace())
        .contains(table);
    }

    @Test
    public void testTrainingParameterOverride()
    {
        // Create a table with dictionary compression enabled
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");

        disableCompaction(keyspace(), table);

        createSSTables(true);

        // Test training command without --force since we have limited test data will fail
        ToolRunner.ToolResult result = invokeNodetool("compressiondictionary", "train", keyspace(), table);
        result.asserts().failure();
        assertThat(result.getStderr())
        .as("Should indicate training not completed")
        .contains("Trainer is not ready: insufficient sample size")
        .contains("/8 MiB") // 10MiB / 10 * 8
        .contains(keyspace())
        .contains(table);

        ToolRunner.ToolResult resultWithOverrides = invokeNodetool("compressiondictionary",
                                                                   "train",
                                                                   "--max-total-sample-size", "5MiB",
                                                                   keyspace(), table);

        assertThat(resultWithOverrides.getStderr())
        .as("Should indicate training not completed")
        .contains("Trainer is not ready: insufficient sample size")
        .contains("/4 MiB") // 5MiB / 10 * 8
        .contains(keyspace())
        .contains(table);

        execute(String.format("ALTER TABLE %s.%s WITH " +
                              "compression = {'class': 'ZstdDictionaryCompressor', '%s': '6MiB'}",
                              keyspace(),
                              table,
                              IDictionaryCompressor.TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME));

        // we are not overriding, but we have changed training_max_total_sample_size to 6MiB via CQL, so it sticks

        ToolRunner.ToolResult resultWithoutOverrides = invokeNodetool("compressiondictionary", "train", keyspace(), table);

        assertThat(resultWithoutOverrides.getStderr())
        .as("Should indicate training not completed")
        .contains("Trainer is not ready: insufficient sample size")
        .contains("/4.8 MiB") // 6MiB / 10 * 8
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
        // Use --force since we have limited test data
        ToolRunner.ToolResult result = invokeNodetool("compressiondictionary",
                                                      "train",
                                                      "--force",
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
        ToolRunner.ToolResult result = invokeNodetool("compressiondictionary",
                                                      "train",
                                                      keyspace(),
                                                      table);
        assertThat(result.getStdout()).contains("No SSTables available for training", "after flush");
    }

    @Test
    public void testInvalidKeyspace()
    {
        ToolRunner.ToolResult result = invokeNodetool("compressiondictionary",
                                                      "train",
                                                      "nonexistent_keyspace",
                                                      "nonexistent_table");
        assertThat(result.getStdout()).contains("Failed to trigger training");
    }

    @Test
    public void testInvalidTable()
    {
        ToolRunner.ToolResult result = invokeNodetool("compressiondictionary",
                                                      "train",
                                                      keyspace(),
                                                      "nonexistent_table");
        assertThat(result.getStdout()).contains("Failed to trigger training");
    }

    @Test
    public void testTrainingOnNonDictionaryTable()
    {
        // Create table without dictionary compression
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'LZ4Compressor'}");

        ToolRunner.ToolResult result = invokeNodetool("compressiondictionary",
                                                      "train",
                                                      keyspace(),
                                                      table);
        result.asserts().failure();
        assertThat(result.getStdout())
              .contains("is not enabled or SSTable compressor is not a dictionary compressor");
    }

    @Test
    public void testTrainingWithoutDictionaryCompressionEnabled()
    {
        // Create table with Zstd but without dictionary compression
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdCompressor'}");

        ToolRunner.ToolResult result = invokeNodetool("compressiondictionary",
                                                      "train",
                                                      keyspace(),
                                                      table);
        result.asserts().failure();
        assertThat(result.getStdout())
              .contains("is not enabled or SSTable compressor is not a dictionary compressor");
    }


    @Test
    public void testAlterCompressionToZstdDictionary()
    {
        // Create table with LZ4 compression
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'LZ4Compressor'}");

        // Training should fail on LZ4 table
        ToolRunner.ToolResult result = invokeNodetool("compressiondictionary", "train", keyspace(), table);
        result.asserts().failure();
        assertThat(result.getStdout())
              .contains("is not enabled or SSTable compressor is not a dictionary compressor");

        // Alter table to use ZstdDictionaryCompressor
        execute("ALTER TABLE %s WITH compression = {'class': 'ZstdDictionaryCompressor'}");

        // Training should fail with no sstables
        result = invokeNodetool("compressiondictionary", "train", keyspace(), table);
        assertThat(result.getStdout())
        .contains("No SSTables available for training", "after flush");

        // Write sstables
        createSSTables(true);

        // Training should now succeed (use --force since we have limited test data)
        result = invokeNodetool("compressiondictionary", "train", "--force", keyspace(), table);
        result.assertOnCleanExit();

        assertThat(result.getStdout())
        .as("Should indicate training completed with new dictionary")
        .contains("Training completed successfully")
        .contains(keyspace())
        .contains(table);
    }

    @Test
    public void testHelpOutput()
    {
        ToolRunner.ToolResult result = invokeNodetool("help", "compressiondictionary", "train");
        result.assertOnCleanExit();

        assertThat(result.getStdout())
        .as("Should show command help")
        .contains("nodetool compressiondictionary train - Manually trigger compression")
        .contains("dictionary training for a table")
        .contains("keyspace name")
        .contains("table name")
        .contains("-f", "--force");
    }

    @Test
    public void testForceOptionShortForm()
    {
        // Create a table with dictionary compression enabled
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");

        createSSTables(true);

        // Test training command with -f flag
        ToolRunner.ToolResult result = invokeNodetool("compressiondictionary", "train", "-f", keyspace(), table);
        result.assertOnCleanExit();

        assertThat(result.getStdout())
        .as("Should indicate training completed with force option")
        .contains("Training completed successfully")
        .contains(keyspace())
        .contains(table);
    }

    @Test
    public void testForceOptionLongForm()
    {
        // Create a table with dictionary compression enabled
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");

        createSSTables(true);

        // Test training command with --force flag
        ToolRunner.ToolResult result = invokeNodetool("compressiondictionary", "train", "--force", keyspace(), table);
        result.assertOnCleanExit();

        assertThat(result.getStdout())
        .as("Should indicate training completed with force option")
        .contains("Training completed successfully")
        .contains(keyspace())
        .contains(table);
    }

    @Test
    public void testCommandLineArgumentParsing()
    {
        // Test missing required arguments
        ToolRunner.ToolResult result = invokeNodetool("compressiondictionary", "train");
        result.asserts()
              .failure()
              .stdoutContains("Missing required parameter");

        // Test missing table argument
        result = invokeNodetool("compressiondictionary", "train", keyspace());
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
