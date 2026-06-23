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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.RuntimeMBeanException;

import com.google.common.base.Strings;

import org.apache.cassandra.config.CassandraRelevantEnv;
import org.apache.cassandra.cql3.statements.ExecuteCommandStatement;
import org.apache.cassandra.management.CommandExecutionArgsSerde;
import org.apache.cassandra.management.CommandMBeanAdapter;
import org.apache.cassandra.management.api.CommandExecutionArgs;
import org.apache.cassandra.management.picocli.PicocliCommandArgsConverter;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.RemoteJmxMBeanAccessor;
import org.apache.cassandra.tools.nodetool.AbstractCommand;
import org.apache.cassandra.tools.nodetool.JmxConnect;
import org.apache.cassandra.tools.nodetool.StopDaemon;

import picocli.CommandLine;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CLI_EXECUTION_SHOW_EXECUTION_ID;
import static org.apache.cassandra.management.ManagementUtils.fullCommandName;
import static org.apache.cassandra.utils.JsonUtils.fromJsonMap;

public class CommandMBeanExecutionStrategy implements CommandExecutionStrategy
{
    private static final String MBEAN_DOMAIN = "org.apache.cassandra.management";
    private static final String MBEAN_TYPE_COMMAND = "Command";

    private final JmxConnect connect;

    public CommandMBeanExecutionStrategy(JmxConnect connect)
    {
        this.connect = connect;
    }

    @Override
    public int execute(CommandLine.ParseResult parseResult) throws CommandLine.ExecutionException, CommandLine.ParameterException
    {
        CommandLine.Model.CommandSpec lastParent = StaticMBeanExecutionStrategy.lastExecutableSubcommandWithSameParent(parseResult.asCommandLineList());

        NodeProbe probe = null;
        Object userObject = lastParent.userObject();

        if (userObject instanceof AbstractCommand)
        {
            AbstractCommand command = (AbstractCommand) userObject;
            if (command.shouldConnect())
                connect.run();
            probe = connect.probe();
        }

        // Local command execution with no JMX connection.
        if (probe == null || parseResult.isUsageHelpRequested() || parseResult.isVersionHelpRequested())
            return new CommandLine.RunLast().execute(parseResult);

        String commandName = extractCommandName(parseResult);
        if (commandName == null || commandName.isEmpty())
            return new CommandLine.RunLast().execute(parseResult);

        // Command is already populated with args from CommandLine.parseArgs(), convert to CommandExecutionArgs
        CommandExecutionArgs args = PicocliCommandArgsConverter.fromCommand(userObject);
        String jsonParams = CommandExecutionArgsSerde.toJson(args);

        try
        {
            MBeanServerConnection mbs = ((RemoteJmxMBeanAccessor) probe.getMBeanAccessor()).getMBeanServerConnection();

            if (!mbs.isRegistered(constructCommandMBeanName(commandName)))
                return new CommandLine.RunLast().execute(parseResult);

            String rawResult = executeViaRemoteMBean(mbs, commandName, jsonParams);

            Map<String, String> commandResult = fromJsonMap(rawResult);

            String executionId = commandResult.get(ExecuteCommandStatement.COMMAND_RESULT_SCHEMA_EXECUTION_ID);
            String output = commandResult.get(ExecuteCommandStatement.COMMAND_RESULT_SCHEMA_OUTPUT);

            if (Strings.isNullOrEmpty(executionId) || output == null)
                throw new RuntimeException("Invalid command result schema received from CommandMBeanAdapter: " + rawResult);

            probe.output().printInfo(output);
            // The output needs to be consistent for the tests we already have and for thouse which heavily rely on
            // asserting command output, so we use this property to control whether the execution id is printed or not.
            if (CASSANDRA_CLI_EXECUTION_SHOW_EXECUTION_ID.getBoolean() ||
                CassandraRelevantEnv.CASSANDRA_CLI_EXECUTION_SHOW_EXECUTION_ID.getBoolean())
                probe.output().printInfo("Command execution id: " + executionId);
            return 0;
        }
        catch (NodetoolConnectionException e)
        {
            throw e;
        }
        catch (RuntimeMBeanException e)
        {
            Throwable cause = e.getCause();
            if (cause instanceof IllegalArgumentException || cause instanceof IllegalStateException)
            {
                throw new CommandLine.ParameterException(parseResult.commandSpec().commandLine(),
                                                        "Invalid command parameters: " + cause.getMessage(), cause);
            }
            else
            {
                // Include security exception, instance not found, invocation target exceptions.
                throw new CommandLine.ExecutionException(parseResult.commandSpec().commandLine(),
                                                         "Failed to execute command via MBean: " + cause.getMessage(), cause);
            }
        }
        catch (IOException e)
        {
            if (userObject instanceof StopDaemon)
            {
                probe.output().printInfo("Cassandra has shutdown.");
                return 0;
            }
            throw new CommandLine.ExecutionException(parseResult.commandSpec().commandLine(),
                                                     String.format("JMX connection to the node was lost while executing command '%s'. " +
                                                                   "The command may still be running on the server.",
                                                                   commandName),
                                                     e);
        }
        catch (Exception e)
        {
            throw new CommandLine.ExecutionException(parseResult.commandSpec().commandLine(),
                                                     "Unexpected error during command execution: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws ExecutionStrategyCloseException
    {
        if (connect.probe() == null)
            return;
        try
        {
            connect.probe().close();
        }
        catch (Exception e)
        {
            throw new ExecutionStrategyCloseException("Failed to close JMX connection", e);
        }
    }

    /** Handles nested commands (e.g. compressiondictionary.train). */
    public static String extractCommandName(CommandLine.ParseResult parseResult)
    {
        List<CommandLine> commandLineList = parseResult.asCommandLineList();
        if (commandLineList.size() <= 1) // Only root "nodetool"
            return null;

        // Skip the root command ("nodetool") and concatenate the rest to form the full command name.
        return fullCommandName(commandLineList.subList(1, commandLineList.size()), CommandLine::getCommandName);
    }

    private String executeViaRemoteMBean(MBeanServerConnection mbs, String commandName, String jsonParams) throws Exception
    {
        ObjectName mbeanName = constructCommandMBeanName(commandName);

        try
        {
            Object result = mbs.invoke(mbeanName, CommandMBeanAdapter.INVOKE_METHOD,
                                       new Object[]{ jsonParams },
                                       new String[]{ String.class.getName() });

            return result == null ? "" : result.toString();
        }
        catch (MBeanException | ReflectionException e)
        {
            throw new RuntimeMBeanException(new RuntimeException(e), "Failed to invoke CommandMBeanAdapter: " + commandName);
        }
    }

    private ObjectName constructCommandMBeanName(String commandName) throws Exception
    {
        String escapedName = ObjectName.quote(commandName);
        String objectNameStr = String.format("%s:type=%s,name=%s", MBEAN_DOMAIN, MBEAN_TYPE_COMMAND, escapedName);
        return new ObjectName(objectNameStr);
    }
}
