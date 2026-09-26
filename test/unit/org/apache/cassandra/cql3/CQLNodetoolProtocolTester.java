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

package org.apache.cassandra.cql3;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tools.nodetool.strategy.CommandExecutionStrategy;

@RunWith(Parameterized.class)
public abstract class CQLNodetoolProtocolTester extends CQLTester
{
    protected static final Pattern EXECUTION_ID_UUID_PATTERN =
        Pattern.compile("Command execution id: [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\s*");

    @Parameterized.Parameter
    public CommandExecutionStrategy.Type strategy;

    @Parameterized.Parameters(name = "strategy={0}")
    public static Collection<Object[]> data()
    {
        List<Object[]> params = new ArrayList<>();
        for (CommandExecutionStrategy.Type type : CommandExecutionStrategy.Type.values())
            params.add(new Object[]{type});
        return params;
    }

    public ToolRunner.ToolResult invokeNodetool(List<String> args)
    {
        return invokeNodetool(args.toArray(new String[0]));
    }

    public ToolRunner.ToolResult invokeNodetool(String... args)
    {
        // Use invokeNodetoolInJvm for faster execution of the command operations and
        // enabling easier debugging when running in debug mode from IDE. There is also
        // no need to run 'ant jars' to run nodetool commands in this test after code changes.
        try (WithProperties with = new WithProperties().set(CassandraRelevantProperties.CASSANDRA_CLI_EXECUTION_PROTOCOL,
                                                            strategy.name().toLowerCase()))
        {
            return ToolRunner.invokeNodetoolInJvm(NodeTool::new,
                                                  strategy == CommandExecutionStrategy.Type.CQL ?
                                                  CQLTester::buildNodetoolCqlArgs :
                                                  CQLTester::buildNodetoolArgs,
                                                  args);
        }
    }
}
