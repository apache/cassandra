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

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class NodetoolConnectFailureTest extends CQLTester
{
    private static final String CLOSED_HOST = "127.0.0.1";
    // A port that is essentially always closed; connecting to it fails fast.
    private static final String CLOSED_PORT = "2";

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
    }

    @Test
    public void cqlConnectFailurePrintsFriendlyMessageAndExits()
    {
        assertConnectFailure("cql", "nodetool: Failed to connect to '" + CLOSED_HOST + ':' + CLOSED_PORT + "' via CQL");
    }

    @Test
    public void staticMBeanConnectFailurePrintsFriendlyMessageAndExits()
    {
        assertConnectFailure("static_mbean", "nodetool: Failed to connect to '" + CLOSED_HOST + ':' + CLOSED_PORT + '\'');
    }

    @Test
    public void commandMBeanConnectFailurePrintsFriendlyMessageAndExits()
    {
        assertConnectFailure("command_mbean", "nodetool: Failed to connect to '" + CLOSED_HOST + ':' + CLOSED_PORT + '\'');
    }

    private static void assertConnectFailure(String protocol, String expectedMessage)
    {
        try (WithProperties ignored = new WithProperties()
                                      .set(CassandraRelevantProperties.CASSANDRA_CLI_EXECUTION_PROTOCOL, protocol))
        {
            ToolRunner.ToolResult result =
                ToolRunner.invokeNodetoolInJvm(NodeTool::new, NodetoolConnectFailureTest::closedTargetArgs, "status");

            assertThat(result.getExitCode()).as("connect failure should exit 1 (clean), not 2 (stack trace)")
                                            .isEqualTo(1);
            assertThat(result.getCleanedStderr()).contains(expectedMessage)
                                                 .doesNotContain("-- StackTrace --");
        }
    }

    /** Builds nodetool args with the closed host/port placed before the subcommand. */
    private static List<String> closedTargetArgs(List<String> commandArgs)
    {
        List<String> all = new ArrayList<>();
        all.add("bin/nodetool");
        all.add("-h");
        all.add(CLOSED_HOST);
        all.add("-p");
        all.add(CLOSED_PORT);
        all.addAll(commandArgs);
        return all;
    }
}
