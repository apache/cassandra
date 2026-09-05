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
package org.apache.cassandra.cql3.statements;

import java.util.List;
import java.util.Map;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.exceptions.CommandRequestExecutionException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.management.CommandAuthorizationException;
import org.apache.cassandra.management.CommandExecutionArgsSerde;
import org.apache.cassandra.management.CommandExecutionException;
import org.apache.cassandra.management.CommandInvokerService;
import org.apache.cassandra.management.CommandValidationException;
import org.apache.cassandra.management.api.Command;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.management.ManagementUtils.causeMessages;
import static org.apache.cassandra.management.ManagementUtils.findRegistryCommand;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class ExecuteCommandStatement
{
    public static final String COMMAND_RESULT_SCHEMA_EXECUTION_ID = "execution_id";
    public static final String COMMAND_RESULT_SCHEMA_OUTPUT = "output";

    public static class Raw extends CQLStatement.Raw implements CQLStatement
    {
        private final String commandName;
        private final Map<String, Object> args;

        public Raw(String commandName, Map<String, Object> args)
        {
            this.commandName = commandName;
            this.args = args;
        }

        public String commandName()
        {
            return commandName;
        }

        public Map<String, Object> args()
        {
            return args;
        }

        public CQLStatement prepare(ClientState state)
        {
            return this;
        }

        public void authorize(ClientState state) throws UnauthorizedException
        {
            // TODO: CASSANDRA-XXXXX Restrict command execution to deployments without authentication
            //  This is a temporary limitation until full authentication support is implemented.
            if (DatabaseDescriptor.getAuthenticator().requireAuthentication())
            {
                throw new UnauthorizedException("Command execution via management port is currently only supported " +
                                                "when authentication is disabled (AllowAllAuthenticator). " +
                                                "Full authentication and authorization support will be added in a " +
                                                "future release.");
            }

            // Validate login (will succeed with AllowAllAuthenticator)
            state.validateLogin();
        }

        @Override
        public void validate(ClientState state) throws InvalidRequestException
        {
            Command<?> command = findRegistryCommand(commandName, CommandInvokerService.instance.getRegistry());
            if (command == null)
                throw new InvalidRequestException("Command not found: " + commandName);
        }

        @Override
        public ResultMessage execute(QueryState state, QueryOptions options, Dispatcher.RequestTime requestTime)
        {
            try
            {
                ClientState clientState = state.getClientState();
                if (!clientState.isInternal && !clientState.isManagement())
                    throw  new InvalidRequestException("Command execution is only allowed via native management interface");

                Command<?> command = findRegistryCommand(commandName, CommandInvokerService.instance.getRegistry());
                if (command == null)
                    throw new InvalidRequestException("Command not found: " + commandName);

                CommandInvokerService.CommandResult result = CommandInvokerService.instance
                                                             .invokeCommand(commandName,
                                                                            () -> CommandExecutionArgsSerde.fromMap(args, command.metadata()));

                ResultSet resultSet = getCommandResultSet();
                resultSet.addColumnValue(bytes(result.getExecutionId()));
                resultSet.addColumnValue(bytes(result.getOutput()));
                return new ResultMessage.Rows(resultSet);
            }
            catch (CommandAuthorizationException e)
            {
                throw new UnauthorizedException(e.getMessage());
            }
            catch (CommandValidationException e)
            {
                throw new InvalidRequestException(causeMessages(e), e.getCause());
            }
            catch (CommandExecutionException e)
            {
                throw new CommandRequestExecutionException(e.getExecutionId(),
                                                           causeMessages(e),
                                                           e.getCause());
            }
        }

        private static ResultSet getCommandResultSet()
        {
            ColumnSpecification executionIdColumn = new ColumnSpecification("system",
                                                                            "command_output",
                                                                            new ColumnIdentifier(COMMAND_RESULT_SCHEMA_EXECUTION_ID, true),
                                                                            UUIDType.instance);
            ColumnSpecification outputColumn = new ColumnSpecification("system",
                                                                       "command_output",
                                                                       new ColumnIdentifier(COMMAND_RESULT_SCHEMA_OUTPUT, true),
                                                                       UTF8Type.instance);
            return new ResultSet(new ResultSet.ResultMetadata(List.of(executionIdColumn, outputColumn)));
        }

        public ResultMessage executeLocally(QueryState state, QueryOptions options) throws InvalidRequestException
        {
            return execute(state, options, Dispatcher.RequestTime.forImmediateExecution());
        }

        @Override
        public AuditLogContext getAuditLogContext()
        {
            return new AuditLogContext(AuditLogEntryType.EXECUTE_COMMAND, commandName);
        }
    }
}
