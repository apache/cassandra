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

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.fql.FullQueryLoggerOptions;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class GetFullQueryLogTest extends CQLTester
{
    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @After
    public void afterTest() throws InterruptedException
    {
        disableFullQueryLog();
    }

    @Test
    public void getFullQueryLogTest()
    {
        testDefaultOutput(getFullQueryLog());
    }

    @Test
    public void enableFullQueryLogTest()
    {
        enableFullQueryLog();
        testChangedOutput(getFullQueryLog());
    }

    @Test
    public void resetFullQueryLogTest()
    {
        enableFullQueryLog();
        testChangedOutput(getFullQueryLog());

        // reset and get and test that it reset configuration into defaults
        resetFullQueryLog();

        testDefaultOutput(getFullQueryLog());
    }

    private String getFullQueryLog()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("getfullquerylog");
        tool.assertOnCleanExit();
        return tool.getStdout();
    }

    private void resetFullQueryLog()
    {
        ToolRunner.invokeNodetool("resetfullquerylog").assertOnCleanExit();
    }

    private void disableFullQueryLog()
    {
        ToolRunner.invokeNodetool("disablefullquerylog").assertOnCleanExit();
    }

    private void enableFullQueryLog()
    {
        ToolRunner.invokeNodetool("enablefullquerylog",
                                  "--path",
                                  temporaryFolder.getRoot().toString(),
                                  "--blocking",
                                  "false",
                                  "--max-archive-retries",
                                  "5",
                                  "--archive-command",
                                  "/path/to/script.sh %path",
                                  "--max-log-size",
                                  "100000",
                                  "--max-queue-weight",
                                  "10000",
                                  "--roll-cycle",
                                  "DAILY")
                  .assertOnCleanExit();
    }

    @SuppressWarnings("DynamicRegexReplaceableByCompiledPattern")
    private void testChangedOutput(final String getFullQueryLogOutput)
    {
        final String output = getFullQueryLogOutput.replaceAll("( )+", " ").trim();
        assertThat(output).contains("enabled true");
        assertThat(output).contains("log_dir " + temporaryFolder.getRoot().toString());
        assertThat(output).contains("archive_command /path/to/script.sh %path");
        assertThat(output).contains("roll_cycle DAILY");
        assertThat(output).contains("max_log_size 100000");
        assertThat(output).contains("max_queue_weight 10000");
        assertThat(output).contains("max_archive_retries 5");
        assertThat(output).contains("block false");
    }

    @SuppressWarnings("DynamicRegexReplaceableByCompiledPattern")
    private void testDefaultOutput(final String getFullQueryLogOutput)
    {
        final FullQueryLoggerOptions options = new FullQueryLoggerOptions();
        final String output = getFullQueryLogOutput.replaceAll("( )+", " ").trim();
        assertThat(output).contains("enabled false");
        assertThat(output).doesNotContain("log_dir " + temporaryFolder.getRoot().toString());
        assertThat(output).doesNotContain("archive_command /path/to/script.sh %path");
        assertThat(output).contains("roll_cycle " + options.roll_cycle);
        assertThat(output).contains("max_log_size " + options.max_log_size);
        assertThat(output).contains("max_queue_weight " + options.max_queue_weight);
        assertThat(output).contains("max_archive_retries " + options.max_archive_retries);
        assertThat(output).contains("block " + options.block);
    }
}
