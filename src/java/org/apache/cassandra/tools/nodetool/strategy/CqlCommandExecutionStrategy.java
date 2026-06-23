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

import java.lang.reflect.Array;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.CassandraRelevantEnv;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.exceptions.CommandRequestExecutionException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.management.api.CommandExecutionArgs;
import org.apache.cassandra.management.api.OptionMetadata;
import org.apache.cassandra.management.api.ParameterMetadata;
import org.apache.cassandra.management.picocli.PicocliCommandArgsConverter;
import org.apache.cassandra.tools.nodetool.AbstractCommand;
import org.apache.cassandra.tools.nodetool.CqlConnect;
import org.apache.cassandra.tools.nodetool.StopDaemon;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.ResultMessage;

import picocli.CommandLine;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CLI_EXECUTION_SHOW_EXECUTION_ID;
import static org.apache.cassandra.management.ManagementUtils.normalizeOptionName;
import static org.apache.cassandra.management.api.ParameterMetadata.COMMAND_POSITIONAL_PARAM_PREFIX;
import static org.apache.cassandra.tools.nodetool.strategy.CommandMBeanExecutionStrategy.extractCommandName;

public class CqlCommandExecutionStrategy implements CommandExecutionStrategy
{
    private final CqlConnect connect;

    public CqlCommandExecutionStrategy(CqlConnect connect)
    {
        this.connect = connect;
    }

    @Override
    public int execute(CommandLine.ParseResult parseResult) throws CommandLine.ExecutionException, CommandLine.ParameterException
    {
        CommandLine.Model.CommandSpec lastParent = StaticMBeanExecutionStrategy.lastExecutableSubcommandWithSameParent(parseResult.asCommandLineList());

        Object userObject = lastParent.userObject();

        if (userObject instanceof AbstractCommand)
        {
            AbstractCommand command = (AbstractCommand) userObject;
            if (command.shouldConnect())
                connect.run();
        }

        // Local command execution with no CQL connection.
        if (connect.client() == null || parseResult.isUsageHelpRequested() || parseResult.isVersionHelpRequested())
            return new CommandLine.RunLast().execute(parseResult);

        String commandName = extractCommandName(parseResult);
        if (commandName == null || commandName.isEmpty())
            return new CommandLine.RunLast().execute(parseResult);

        // Command is already populated with args from CommandLine.parseArgs(), converting it to CommandExecutionArgs
        CommandExecutionArgs args = PicocliCommandArgsConverter.fromCommand(userObject);

        try
        {
            String cqlCommand = buildCqlCommandString(commandName, args);
            ResultMessage result = connect.client().execute(cqlCommand, ConsistencyLevel.ONE);

            if (result instanceof ResultMessage.Rows)
            {
                ResultMessage.Rows rows = (ResultMessage.Rows) result;
                assert rows.result.size() == 1 : "Command execution result should have exactly 1 row, got " + rows.result.size();
                assert rows.result.metadata.getColumnCount() == 2 : "Command execution result schema has been changed. " +
                                                               "Expected 2 columns in result, got " + rows.result.metadata.getColumnCount();

                if (!rows.result.isEmpty() && rows.result.metadata.names.size() >= 2)
                {
                    List<byte[]> firstRow = rows.result.rows.get(0);
                    if (firstRow.size() >= 2)
                    {
                        UUID executionId = UUIDType.instance.getSerializer().deserialize(firstRow.get(0), ByteArrayAccessor.instance);
                        String output = UTF8Type.instance.getSerializer().deserialize(firstRow.get(1), ByteArrayAccessor.instance);
                        // NodeProbe instance is not available here, so print directly to the command output.
                        parseResult.commandSpec().commandLine().getOut().println(output);
                        if (CASSANDRA_CLI_EXECUTION_SHOW_EXECUTION_ID.getBoolean()
                            || CassandraRelevantEnv.CASSANDRA_CLI_EXECUTION_SHOW_EXECUTION_ID.getBoolean())
                            parseResult.commandSpec().commandLine().getOut().println("Command execution id: " + executionId.toString());
                    }
                }
            }

            return 0;
        }
        catch (SimpleClient.ConnectionClosedException e)
        {
            if (userObject instanceof StopDaemon)
            {
                parseResult.commandSpec().commandLine().getOut().println("Cassandra has shutdown.");
                return 0;
            }
            throw new CommandLine.ExecutionException(parseResult.commandSpec().commandLine(),
                                                     String.format("Connection to the node was closed before a response " +
                                                                   "was received for command '%s'. " +
                                                                   "The command may still be running on the server.",
                                                                   commandName),
                                                     e);
        }
        catch (SimpleClient.TimeoutException e)
        {
            throw new CommandLine.ExecutionException(parseResult.commandSpec().commandLine(),
                                                     String.format("No response received for command '%s' within the client timeout. " +
                                                                   "The command may still be running on the server. " +
                                                                   "Increase the timeout via the %s environment variable " +
                                                                   "or the %s system property (0 waits indefinitely). %s",
                                                                   commandName,
                                                                   CassandraRelevantEnv.CASSANDRA_CLI_EXECUTION_TIMEOUT_SECONDS.getKey(),
                                                                   CassandraRelevantProperties.CASSANDRA_CLI_EXECUTION_TIMEOUT_SECONDS.getKey(),
                                                                   e.getMessage()),
                                                     e);
        }
        catch (RuntimeException e)
        {
            Throwable cause = e.getCause();
            if (cause instanceof UnauthorizedException)
            {
                throw new CommandLine.ExecutionException(parseResult.commandSpec().commandLine(),
                                                         "Unauthorized: " + cause.getMessage());
            }
            else if (cause instanceof InvalidRequestException)
            {
                // CQL invalid request (e.g. validating params and/or param values) translates into picocli param exection.
                throw new CommandLine.ParameterException(parseResult.commandSpec().commandLine(),
                                                         "Invalid request: " + cause.getMessage());
            }
            else if (cause instanceof CommandRequestExecutionException)
            {
                CommandRequestExecutionException cree = (CommandRequestExecutionException) cause;
                String msg = String.format("Command execution failed (executionId: %s): %s",
                                           cree.getCommandExecutionId(), cree.getMessage());
                throw new CommandLine.ExecutionException(parseResult.commandSpec().commandLine(), msg);
            }
            throw new CommandLine.ExecutionException(parseResult.commandSpec().commandLine(),
                                                     "Unknown command execution exception via CQL: " + e.getMessage(), e);
        }
        catch (Exception e) {
            // Catch-all for checked exceptions thrown during command execution.
            throw new CommandLine.ExecutionException(parseResult.commandSpec().commandLine(),
                                                     "Unknown command execution exception via CQL: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws ExecutionStrategyCloseException
    {
        if (connect == null)
            return;
        try
        {
            connect.close();
        }
        catch (Exception e)
        {
            throw new ExecutionStrategyCloseException("Failed to close CQL connection", e);
        }
    }

    /**
     * Build a CQL INVOKE COMMAND statement string from command name and CommandExecutionArgs.
     * The format is: INVOKE COMMAND commandName WITH "key1" = 'value1' AND "key2" = 'value2';
     */
    @VisibleForTesting
    static String buildCqlCommandString(String commandName, CommandExecutionArgs args)
    {
        CqlBuilder builder = new CqlBuilder();
        builder.append("INVOKE COMMAND ");
        builder.appendQuotingIfNeeded(commandName);

        Map<String, Object> paramsMap = new LinkedHashMap<>();
        for (Map.Entry<OptionMetadata, Object> entry : args.options().entrySet())
        {
            OptionMetadata optionMetadata = entry.getKey();
            Object value = entry.getValue();

            if (value == null)
                continue;

            String key = normalizeOptionName(optionMetadata.paramLabel());
            paramsMap.put(key, value);
        }

        for (Map.Entry<ParameterMetadata, Object> entry : args.parameters().entrySet())
        {
            ParameterMetadata paramMetadata = entry.getKey();
            Object value = entry.getValue();

            if (value == null)
                continue;

            String key = COMMAND_POSITIONAL_PARAM_PREFIX + paramMetadata.index();
            paramsMap.put(key, value);
        }

        if (!paramsMap.isEmpty())
        {
            builder.append(" WITH");
            boolean first = true;
            for (Map.Entry<String, Object> entry : paramsMap.entrySet())
            {
                if (first)
                    builder.append(" ");
                else
                    builder.append(" AND ");

                // Using ColumnIdentifier.maybeQuote as some of the keys
                // may be reserved words in CQL (e.g., "keyspace", "table").
                builder.append(ColumnIdentifier.maybeQuote(entry.getKey()));
                builder.append(" = ");
                appendCqlValue(builder, entry.getValue());

                first = false;
            }
        }

        builder.append(";");
        return builder.toString();
    }

    /** Append a value to CqlBuilder with proper type handling and escaping. */
    private static void appendCqlValue(CqlBuilder builder, Object value)
    {
        if (value == null)
        {
            builder.append("NULL");
            return;
        }

        if (value instanceof String)
            builder.appendWithSingleQuotes((String) value);
        else if (value instanceof Enum)
            builder.appendWithSingleQuotes(((Enum<?>) value).name());
        else if (value instanceof Number)
            builder.append(value);
        else if (value instanceof Boolean)
            builder.append(value);
        else if (value instanceof List)
        {
            List<?> list = (List<?>) value;
            builder.append("[");
            for (int i = 0; i < list.size(); i++)
            {
                if (i > 0)
                    builder.append(", ");
                appendCqlValue(builder, list.get(i));
            }
            builder.append("]");
        }
        else if (value instanceof Map)
        {
            Map<String, Object> map = (Map<String, Object>) value;
            builder.append("{");
            boolean first = true;
            for (Map.Entry<String, Object> entry : map.entrySet())
            {
                if (!first)
                    builder.append(", ");
                builder.appendWithSingleQuotes(entry.getKey());
                builder.append(": ");
                appendCqlValue(builder, entry.getValue());
                first = false;
            }
            builder.append("}");
        }
        else if (value instanceof Set)
        {
            Set<?> set = (Set<?>) value;
            builder.append("{");
            boolean first = true;
            for (Object item : set)
            {
                if (!first)
                    builder.append(", ");
                appendCqlValue(builder, item);
                first = false;
            }
            builder.append("}");
        }
        else if (value.getClass().isArray())
        {
            int length = Array.getLength(value);
            builder.append("[");
            for (int i = 0; i < length; i++)
            {
                if (i > 0)
                    builder.append(", ");
                appendCqlValue(builder, Array.get(value, i));
            }
            builder.append("]");
        }
        else
            builder.appendWithSingleQuotes(value.toString());
    }
}
