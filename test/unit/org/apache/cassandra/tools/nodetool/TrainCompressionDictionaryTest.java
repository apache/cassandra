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

        // Add some data to make training meaningful
        for (int i = 0; i < 100; i++)
        {
            execute("INSERT INTO %s (id, data) VALUES (?, ?)", i, "This is sample data for compression dictionary training " + i);
        }
        flush(keyspace());

        // Test async training command
        ToolRunner.ToolResult result = invokeNodetool("traincompressiondictionary", "--async", keyspace(), table);
        result.assertOnCleanExit();

        assertThat(result.getStdout())
        .as("Should indicate training started")
        .contains("Training started asynchronously")
        .contains(keyspace())
        .contains(table);
    }

    @Test
    public void testTrainCommandWithCustomDuration()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");

        // Add test data
        for (int i = 0; i < 50; i++)
        {
            execute("INSERT INTO %s (id, data) VALUES (?, ?)", i, "Sample text for dictionary training " + i);
        }
        flush(keyspace());

        // Test with custom sampling duration
        ToolRunner.ToolResult result = invokeNodetool("traincompressiondictionary",
                                                      "--async",
                                                      "--max-sampling-duration", "30",
                                                      keyspace(),
                                                      table);
        result.assertOnCleanExit();

        assertThat(result.getStdout())
        .as("Should use custom sampling duration")
        .contains("Will collect samples for up to 30 seconds");
    }

    @Test
    public void testStatusCommand()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");

        // Check status before any training
        ToolRunner.ToolResult result = invokeNodetool("traincompressiondictionary",
                                                      "--status",
                                                      keyspace(),
                                                      table);
        result.assertOnCleanExit();

        assertThat(result.getStdout())
        .as("Should show initial status")
        .containsAnyOf("Trainer is not running", "Trainer is collecting sample data");
    }

    @Test
    public void testStatusAfterTrainingStart()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");

        // Add data
        for (int i = 0; i < 20; i++)
        {
            execute("INSERT INTO %s (id, data) VALUES (?, ?)", i, "Data for training " + i);
        }
        flush(keyspace());

        // Start training asynchronously
        invokeNodetool("traincompressiondictionary", "--async", keyspace(), table)
        .assertOnCleanExit();

        // Check status - should show SAMPLING or TRAINING
        ToolRunner.ToolResult statusResult = invokeNodetool("traincompressiondictionary",
                                                            "--status",
                                                            keyspace(),
                                                            table);
        statusResult.assertOnCleanExit();

        assertThat(statusResult.getStdout())
        .as("Should show training in progress")
        .containsAnyOf("collecting sample data", "Training is in progress");
    }

    @Test
    public void testInvalidKeyspace()
    {
        ToolRunner.ToolResult result = invokeNodetool("traincompressiondictionary",
                                                      "--status",
                                                      "nonexistent_keyspace",
                                                      "nonexistent_table");
        result.asserts()
              .failure()
              .errorContains("Failed to get training status");
    }

    @Test
    public void testInvalidTable()
    {
        ToolRunner.ToolResult result = invokeNodetool("traincompressiondictionary",
                                                      "--status",
                                                      keyspace(),
                                                      "nonexistent_table");
        result.asserts()
              .failure()
              .errorContains("Failed to get training status");
    }

    @Test
    public void testTrainingOnNonDictionaryTable()
    {
        // Create table without dictionary compression
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'LZ4Compressor'}");

        ToolRunner.ToolResult result = invokeNodetool("traincompressiondictionary",
                                                      "--async",
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
                                                      "--async",
                                                      keyspace(),
                                                      table);
        result.asserts()
              .failure()
              .errorContains("does not support dictionary compression");
    }

    @Test
    public void testInvalidSamplingDuration()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");

        // Test with invalid (negative) duration
        ToolRunner.ToolResult result = invokeNodetool("traincompressiondictionary",
                                                      "--async",
                                                      "--max-sampling-duration", "-10",
                                                      keyspace(),
                                                      table);

        // Command line parser should handle this validation
        result.asserts()
              .failure();
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
        .contains("table name")
        .contains("-a, --async")
        .contains("-d <maxSamplingDurationSeconds>, --max-sampling-duration")
        .contains("-r <samplingRate>, --sampling-rate")
        .contains("-s, --status");
    }

    @Test
    public void testAllStatusValues()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");

        // Test NOT_STARTED status
        ToolRunner.ToolResult result = invokeNodetool("traincompressiondictionary",
                                                      "--status",
                                                      keyspace(),
                                                      table);
        result.assertOnCleanExit();

        String output = result.getStdout();
        assertThat(output)
        .as("Should handle NOT_STARTED status appropriately")
        .satisfiesAnyOf(out -> assertThat(out).contains("not running"),
                        out -> assertThat(out).contains("NOT_STARTED"));
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

    @Test
    public void testMutuallyExclusiveOptions()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");

        // Both --async and --status should work independently
        invokeNodetool("traincompressiondictionary", "--async", keyspace(), table)
        .assertOnCleanExit();

        invokeNodetool("traincompressiondictionary", "--status", keyspace(), table)
        .assertOnCleanExit();
    }

    @Test
    public void testStatusOutputFormatting()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");

        ToolRunner.ToolResult result = invokeNodetool("traincompressiondictionary",
                                                      "--status",
                                                      keyspace(),
                                                      table);
        result.assertOnCleanExit();

        assertThat(result.getStdout())
        .as("Status output should include keyspace and table names")
        .contains(keyspace())
        .contains(table);
    }

    @Test
    public void testSamplingRateOption()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");

        // Add some data
        for (int i = 0; i < 20; i++)
        {
            execute("INSERT INTO %s (id, data) VALUES (?, ?)", i, "Data for sampling rate test " + i);
        }
        flush(keyspace());

        // Test with valid sampling rates
        ToolRunner.ToolResult result = invokeNodetool("traincompressiondictionary",
                                                      "--async",
                                                      "--sampling-rate", "0.5",
                                                      keyspace(),
                                                      table);
        result.assertOnCleanExit();

        assertThat(result.getStdout())
        .as("Should show sampling rate was used")
        .contains("Using sampling rate: 0.50 (50.0%)");
    }

    @Test
    public void testInvalidSamplingRate()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");

        // Test with sampling rate too high
        ToolRunner.ToolResult result = invokeNodetool("traincompressiondictionary",
                                                      "--async",
                                                      "--sampling-rate", "1.5",
                                                      keyspace(),
                                                      table);
        result.asserts()
              .failure()
              .errorContains("Invalid value for sampling-rate: 1.5. Must be in range (0, 1]");

        // Test with sampling rate zero
        result = invokeNodetool("traincompressiondictionary",
                                "--async",
                                "--sampling-rate", "0.0",
                                keyspace(),
                                table);
        result.asserts()
              .failure()
              .errorContains("Invalid value for sampling-rate: 0.0. Must be in range (0, 1]");

        // Test with negative sampling rate
        result = invokeNodetool("traincompressiondictionary",
                                "--async",
                                "--sampling-rate", "-0.5",
                                keyspace(),
                                table);
        result.asserts()
              .failure()
              .errorContains("Invalid value for sampling-rate: -0.5. Must be in range (0, 1]");
    }
}
