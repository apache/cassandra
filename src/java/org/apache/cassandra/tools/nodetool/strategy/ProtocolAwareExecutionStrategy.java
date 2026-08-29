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

package org.apache.cassandra.tools.nodetool.strategy;

import org.apache.cassandra.config.CassandraRelevantEnv;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.tools.nodetool.CqlConnect;
import org.apache.cassandra.tools.nodetool.JmxConnect;

import picocli.CommandLine;

import static org.apache.cassandra.utils.LocalizeString.toUpperCaseLocalized;

public class ProtocolAwareExecutionStrategy implements CommandExecutionStrategy
{
    public static int executionStrategy(CommandLine.ParseResult parseResult)
    {
        try (ProtocolAwareExecutionStrategy strategy = new ProtocolAwareExecutionStrategy())
        {
            return strategy.execute(parseResult);
        }
    }

    /**
     * Picks the strategy named by {@link #getExecutionStrategyTypeFromEnvAndSys()}, finds its connection
     * mixin on the top-level command and runs the parsed command through it.
     * @param parseResult The parsed command line.
     * @return The exit code.
     */
    @Override
    public int execute(CommandLine.ParseResult parseResult)
    {
        CommandLine.Model.CommandSpec connectCmd = parseResult.commandSpec()
                                                       .mixins()
                                                       .get(getExecutionStrategyTypeFromEnvAndSys().toString());
        if (connectCmd == null)
            throw new CommandLine.InitializationException("No 'connection' command found in the top-level hierarchy");

        try (CommandExecutionStrategy invoker = createInvoker(connectCmd.userObject()))
        {
            return invoker.execute(parseResult);
        }
        catch (NodetoolConnectionException e)
        {
            throw new CommandLine.ExecutionException(parseResult.commandSpec().commandLine(), e.getMessage(), e);
        }
        catch (ExecutionStrategyCloseException e)
        {
            CommandLine commandLine = parseResult.commandSpec().commandLine();
            commandLine.getErr().println("Failed to close connection: " + e.getMessage());
            CommandLine.IExitCodeExceptionMapper mapper = commandLine.getExitCodeExceptionMapper();
            return mapper != null ? mapper.getExitCode(e) : commandLine.getCommandSpec().exitCodeOnExecutionException();
        }
    }

    public static Type getExecutionStrategyTypeFromEnvAndSys()
    {
        Type defaultStrategy = Type.valueOf(toUpperCaseLocalized(CassandraRelevantProperties.CASSANDRA_CLI_EXECUTION_PROTOCOL.getDefaultValue()));
        Type strategyEnv = CassandraRelevantEnv.CASSANDRA_CLI_EXECUTION_PROTOCOL.getEnum(true, Type.class, defaultStrategy.name());
        Type strategySys = CassandraRelevantProperties.CASSANDRA_CLI_EXECUTION_PROTOCOL.getEnum(true, Type.class);
        return strategyEnv != defaultStrategy ? strategyEnv : strategySys;
    }

    protected CommandExecutionStrategy createInvoker(Object connectCmd)
    {
        Type strategy = getExecutionStrategyTypeFromEnvAndSys();
        switch (strategy)
        {
            case CQL:
                return new CqlCommandExecutionStrategy((CqlConnect) connectCmd);
            case COMMAND_MBEAN:
                return new CommandMBeanExecutionStrategy((JmxConnect) connectCmd);
            case STATIC_MBEAN:
                return new StaticMBeanExecutionStrategy((JmxConnect) connectCmd);
            default:
                throw new IllegalStateException("Unknown execution strategy: " + strategy);
        }
    }
}
