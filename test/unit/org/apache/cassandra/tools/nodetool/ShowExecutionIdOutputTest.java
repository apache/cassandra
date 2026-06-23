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

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLNodetoolProtocolTester;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tools.nodetool.strategy.CommandExecutionStrategy;

import static org.assertj.core.api.Assertions.assertThat;

public class ShowExecutionIdOutputTest extends CQLNodetoolProtocolTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Override
    public ToolRunner.ToolResult invokeNodetool(String... args)
    {
        try (WithProperties with = new WithProperties()
                                   .set(CassandraRelevantProperties.CASSANDRA_CLI_EXECUTION_PROTOCOL, strategy.name().toLowerCase())
                                   .set(CassandraRelevantProperties.CASSANDRA_CLI_EXECUTION_SHOW_EXECUTION_ID, true))
        {
            return ToolRunner.invokeNodetoolInJvm(NodeTool::new,
                                                  strategy == CommandExecutionStrategy.Type.CQL ?
                                                  CQLTester::buildNodetoolCqlArgs :
                                                  CQLTester::buildNodetoolArgs,
                                                  args);
        }
    }

    @Test
    public void testExecutionIdInOutputWhenShowExecutionIdEnabled()
    {
        Assume.assumeFalse("STATIC_MBEAN does not print execution ID",
                           strategy == CommandExecutionStrategy.Type.STATIC_MBEAN);

        ToolRunner.ToolResult tool = invokeNodetool("status");
        tool.assertOnCleanExit();
        assertThat(EXECUTION_ID_UUID_PATTERN.matcher(tool.getStdout()).find())
        .as("When CASSANDRA_CLI_EXECUTION_SHOW_EXECUTION_ID is true, output should contain 'Command execution id: <uuid>'")
        .isTrue();
    }
}