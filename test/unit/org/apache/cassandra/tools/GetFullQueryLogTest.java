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

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.fql.FullQueryLoggerOptions;
import org.apache.cassandra.tools.ToolRunner.ToolResult;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GetFullQueryLogTest extends CQLTester
{
    private static NodeProbe probe;

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @BeforeClass
    public static void setup() throws Exception
    {
        startJMXServer();
        probe = new NodeProbe(jmxHost, jmxPort);
    }

    @AfterClass
    public static void teardown() throws IOException
    {
        probe.close();
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
        ToolResult tool = ToolRunner.invokeNodetool("getfullquerylog");
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

    private void testChangedOutput(final String getFullQueryLogOutput)
    {
        final String output = getFullQueryLogOutput.replaceAll("( )+", " ").trim();
        assertTrue(output.contains("enabled true"));
        assertTrue(output.contains("log_dir " + temporaryFolder.getRoot().toString()));
        assertTrue(output.contains("archive_command /path/to/script.sh %path"));
        assertTrue(output.contains("roll_cycle DAILY"));
        assertTrue(output.contains("max_log_size 100000"));
        assertTrue(output.contains("max_queue_weight 10000"));
        assertTrue(output.contains("max_archive_retries 5"));
        assertTrue(output.contains("block false"));
    }

    private void testDefaultOutput(final String getFullQueryLogOutput)
    {
        final FullQueryLoggerOptions options = new FullQueryLoggerOptions();
        final String output = getFullQueryLogOutput.replaceAll("( )+", " ").trim();
        assertTrue(output.contains("enabled false"));
        assertFalse(output.contains("log_dir " + temporaryFolder.getRoot().toString()));
        assertFalse(output.contains("archive_command /path/to/script.sh %path"));
        assertTrue(output.contains("roll_cycle " + options.roll_cycle));
        assertTrue(output.contains("max_log_size " + options.max_log_size));
        assertTrue(output.contains("max_queue_weight " + options.max_queue_weight));
        assertTrue(output.contains("max_archive_retries " + options.max_archive_retries));
        assertTrue(output.contains("block " + options.block));
    }
}
