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

/**
 * Tests for the {@code nodetool mvbackfill} command
 */
public class MVBackfillTest extends CQLTester
{
    private static final String KEYSPACE = "test_keyspace";
    private static final String VIEW = "test_view";
    private static final String BASE_TABLE = "test_table";

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    /**
     * Test with dot notation: keyspace.view
     */
    @Test
    public void testDotNotationFormat() throws Throwable
    {
        // Create keyspace, table and view for testing
        createKeyspace(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
        
        execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id int PRIMARY KEY, value text)", KEYSPACE, BASE_TABLE));
        execute(String.format("CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s AS SELECT * FROM %s.%s WHERE id IS NOT NULL AND value IS NOT NULL PRIMARY KEY (value, id)", 
                             KEYSPACE, VIEW, KEYSPACE, BASE_TABLE));
        
        // Run mvbackfill with dot notation
        ToolRunner.ToolResult tool = invokeNodetool("mvbackfill", KEYSPACE + "." + VIEW);
        
        // Should complete successfully
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Starting MV backfill for " + KEYSPACE + "." + VIEW);
        assertThat(tool.getStdout()).contains("completed successfully");
    }

    /**
     * Test with force restart flag (-fr)
     */
    @Test
    public void testForceRestartFlag() throws Throwable
    {
        // Create keyspace, table and view for testing
        createKeyspace(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
        
        execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id int PRIMARY KEY, value text)", KEYSPACE, BASE_TABLE));
        execute(String.format("CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s AS SELECT * FROM %s.%s WHERE id IS NOT NULL AND value IS NOT NULL PRIMARY KEY (value, id)", 
                             KEYSPACE, VIEW, KEYSPACE, BASE_TABLE));
        
        // Run mvbackfill with -fr flag
        ToolRunner.ToolResult tool = invokeNodetool("mvbackfill", "-fr", KEYSPACE + "." + VIEW);
        
        // Should complete successfully
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Starting MV backfill for " + KEYSPACE + "." + VIEW);
        assertThat(tool.getStdout()).contains("Force restart enabled - backfill will start from the beginning");
        assertThat(tool.getStdout()).contains("completed successfully");
    }

    /**
     * Test with force restart and token range
     */
    @Test
    public void testForceRestartWithTokenRange() throws Throwable
    {
        // Create keyspace, table and view for testing
        createKeyspace(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
        
        execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id int PRIMARY KEY, value text)", KEYSPACE, BASE_TABLE));
        execute(String.format("CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s AS SELECT * FROM %s.%s WHERE id IS NOT NULL AND value IS NOT NULL PRIMARY KEY (value, id)", 
                             KEYSPACE, VIEW, KEYSPACE, BASE_TABLE));
        
        // Get a valid token range for the node
        String startToken = Long.toString(Long.MIN_VALUE);
        String endToken = "0";
        
        // Run mvbackfill with -fr and token range
        ToolRunner.ToolResult tool = invokeNodetool("mvbackfill", "-fr", "-st", startToken, "-et", endToken, KEYSPACE + "." + VIEW);
        
        // Should complete successfully if range is local
        assertThat(tool.getStdout()).contains("Starting MV backfill for " + KEYSPACE + "." + VIEW);
        assertThat(tool.getStdout()).contains("Force restart enabled - backfill will start from the beginning");
        assertThat(tool.getStdout()).contains("Backfilling token range");
        assertThat(tool.getStdout()).contains("completed successfully");
    }

    /**
     * Test with no arguments - should fail
     */
    @Test
    public void testNoArguments()
    {
        ToolRunner.ToolResult tool = invokeNodetool("mvbackfill");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("mvbackfill requires keyspace.view argument");
    }

    /**
     * Test with too many arguments - should fail
     */
    @Test
    public void testTooManyArguments()
    {
        ToolRunner.ToolResult tool = invokeNodetool("mvbackfill", KEYSPACE + "." + VIEW, "extra_arg");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("mvbackfill requires keyspace.view argument");
    }

    /**
     * Test with invalid dot notation (more than one dot) - should fail
     */
    @Test
    public void testInvalidDotNotation()
    {
        ToolRunner.ToolResult tool = invokeNodetool("mvbackfill", KEYSPACE + "." + VIEW + ".extra");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("mvbackfill requires keyspace.view argument in format: keyspace_name.view_name");
    }

    /**
     * Test with only keyspace (no dot, single argument) - should fail
     */
    @Test
    public void testSingleArgumentNoDot()
    {
        ToolRunner.ToolResult tool = invokeNodetool("mvbackfill", KEYSPACE);
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("mvbackfill requires keyspace.view argument in format: keyspace_name.view_name");
    }

    /**
     * Test with start token but no end token - should fail
     */
    @Test
    public void testStartTokenWithoutEndToken()
    {
        ToolRunner.ToolResult tool = invokeNodetool("mvbackfill", "-st", "0", KEYSPACE + "." + VIEW);
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("Both start token (-st) and end token (-et) must be specified");
    }

    /**
     * Test with end token but no start token - should fail
     */
    @Test
    public void testEndTokenWithoutStartToken()
    {
        ToolRunner.ToolResult tool = invokeNodetool("mvbackfill", "-et", "1000", KEYSPACE + "." + VIEW);
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("Both start token (-st) and end token (-et) must be specified");
    }

    /**
     * Test with valid token range
     */
    @Test
    public void testWithTokenRange() throws Throwable
    {
        // Create keyspace, table and view for testing
        createKeyspace(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
        
        execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id int PRIMARY KEY, value text)", KEYSPACE, BASE_TABLE));
        execute(String.format("CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s AS SELECT * FROM %s.%s WHERE id IS NOT NULL AND value IS NOT NULL PRIMARY KEY (value, id)", 
                             KEYSPACE, VIEW, KEYSPACE, BASE_TABLE));
        
        // Get a valid token range for the node
        // Using a range that should be valid for testing (may need adjustment based on partitioner)
        String startToken = Long.toString(Long.MIN_VALUE);
        String endToken = "0";
        
        // Run mvbackfill with token range
        ToolRunner.ToolResult tool = invokeNodetool("mvbackfill", "-st", startToken, "-et", endToken, KEYSPACE + "." + VIEW);
        
        // Should complete successfully if the range is local, or fail with appropriate error
        assertThat(tool.getStdout()).contains("Starting MV backfill for " + KEYSPACE + "." + VIEW);
        assertThat(tool.getStdout()).contains("Backfilling token range");
        assertThat(tool.getStdout()).contains("completed successfully");
    }

    /**
     * Test with non-existent view - should fail
     */
    @Test
    public void testNonExistentView() throws Throwable
    {
        createKeyspace(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
        
        ToolRunner.ToolResult tool = invokeNodetool("mvbackfill", KEYSPACE + ".nonexistent_view");
        assertThat(tool.getExitCode()).isEqualTo(2);
    }

    /**
     * Test with non-existent keyspace - should fail
     */
    @Test
    public void testNonExistentKeyspace()
    {
        ToolRunner.ToolResult tool = invokeNodetool("mvbackfill", "nonexistent_keyspace." + VIEW);
        assertThat(tool.getExitCode()).isEqualTo(2);
        assertThat(tool.getStderr()).contains("Unknown keyspace");
    }

    /**
     * Test help output
     */
    @Test
    public void testHelp()
    {
        ToolRunner.ToolResult tool = invokeNodetool("help", "mvbackfill");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("mvbackfill");
        assertThat(tool.getStdout()).contains("Perform materialized view backfill");
        assertThat(tool.getStdout()).contains("-st");
        assertThat(tool.getStdout()).contains("--start-token");
        assertThat(tool.getStdout()).contains("-et");
        assertThat(tool.getStdout()).contains("--end-token");
        assertThat(tool.getStdout()).contains("-fr");
        assertThat(tool.getStdout()).contains("--force-restart");
    }
}


