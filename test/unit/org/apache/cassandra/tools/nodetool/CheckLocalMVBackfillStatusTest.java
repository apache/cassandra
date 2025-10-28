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
 * Tests for the {@code nodetool checklocalmvbackfillstatus} command
 */
public class CheckLocalMVBackfillStatusTest extends CQLTester
{
    private static final String KEYSPACE = "test_keyspace";
    private static final String VIEW = "test_view";

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
    public void testDotNotationFormat()
    {
        // Test command execution with non-existent view (no backfill data in system keyspace)
        ToolRunner.ToolResult tool = invokeNodetool("checklocalmvbackfillstatus", KEYSPACE + "." + VIEW);
        
        // Should complete successfully
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Local MV Backfill Status for " + KEYSPACE + "." + VIEW);
        assertThat(tool.getStdout()).contains("Primary Ranges Finished:");
    }

    /**
     * Test with no backfill status
     */
    @Test
    public void testNoBackfillStatus()
    {
        // Test with non-existent view status
        ToolRunner.ToolResult tool = invokeNodetool("checklocalmvbackfillstatus", "nonexistent_ks.nonexistent_view");
        
        // Should complete successfully but show no ranges
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Local MV Backfill Status");
        assertThat(tool.getStdout()).contains("Primary Ranges Finished: false");
        assertThat(tool.getStdout()).contains("No backfill operations found for this view");
    }

    /**
     * Test with no arguments - should fail
     */
    @Test
    public void testNoArguments()
    {
        ToolRunner.ToolResult tool = invokeNodetool("checklocalmvbackfillstatus");
        assertThat(tool.getStdout()).contains("checklocalmvbackfillstatus requires keyspace.view argument");
    }

    /**
     * Test with too many arguments - should fail
     */
    @Test
    public void testTooManyArguments()
    {
        ToolRunner.ToolResult tool = invokeNodetool("checklocalmvbackfillstatus", KEYSPACE + "." + VIEW, "extra_arg");
        assertThat(tool.getStdout()).contains("checklocalmvbackfillstatus requires keyspace.view argument");
    }

    /**
     * Test with invalid dot notation (more than one dot) - should fail
     */
    @Test
    public void testInvalidDotNotation()
    {
        ToolRunner.ToolResult tool = invokeNodetool("checklocalmvbackfillstatus", KEYSPACE + "." + VIEW + ".extra");
        assertThat(tool.getStdout()).contains("checklocalmvbackfillstatus requires keyspace.view argument in format: keyspace_name.view_name");
    }

    /**
     * Test with only keyspace (no dot, single argument) - should fail
     */
    @Test
    public void testSingleArgumentNoDot()
    {
        ToolRunner.ToolResult tool = invokeNodetool("checklocalmvbackfillstatus", KEYSPACE);
        assertThat(tool.getStdout()).contains("checklocalmvbackfillstatus requires keyspace.view argument in format: keyspace_name.view_name");
    }
}

