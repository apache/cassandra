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

import org.junit.Test;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.ExecuteCommandStatement;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.management.CommandExecutionArgsSerde;
import org.apache.cassandra.management.api.CommandExecutionArgs;
import org.apache.cassandra.management.picocli.PicocliCommandArgsConverter;
import org.apache.cassandra.management.picocli.PicocliCommandMetadata;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.AbstractCommand;
import org.apache.cassandra.tools.nodetool.Stop;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import static org.assertj.core.api.Assertions.assertThat;

public class CqlCommandExecutionStrategyTest
{
    @Test
    public void testStopDefaultOperationTypeSurvivesCqlRoundTrip()
    {
        String cql = CqlCommandExecutionStrategy.buildCqlCommandString("stop", PicocliCommandArgsConverter.fromCommand(new Stop()));

        assertThat(cql).contains("'UNKNOWN'")
                       .doesNotContain(OperationType.UNKNOWN.toString());

        CommandExecutionArgs serverArgs = serverSideArgs(cql, new Stop());
        assertThat(serverArgs.parameters().values()).contains(OperationType.UNKNOWN);
    }

    @Test
    public void testAllOperationTypesSurviveCqlRoundTrip()
    {
        for (OperationType type : OperationType.values())
        {
            EnumParamCommand client = new EnumParamCommand();
            client.compactionType = type;
            String cql = CqlCommandExecutionStrategy.buildCqlCommandString("stop", PicocliCommandArgsConverter.fromCommand(client));

            EnumParamCommand server = new EnumParamCommand();
            PicocliCommandArgsConverter.toCommand(serverSideArgs(cql, server), server);
            assertThat(server.compactionType).as("round trip of %s via: %s", type, cql).isEqualTo(type);
        }
    }

    /**
     * Parse the statement and convert its raw args the same way {@link ExecuteCommandStatement} does.
     */
    private static CommandExecutionArgs serverSideArgs(String cql, AbstractCommand command)
    {
        CQLStatement.Raw stmt = QueryProcessor.parseStatement(cql);
        assertThat(stmt).isInstanceOf(ExecuteCommandStatement.Raw.class);
        ExecuteCommandStatement.Raw raw = (ExecuteCommandStatement.Raw) stmt;
        return CommandExecutionArgsSerde.fromMap(raw.args(), PicocliCommandMetadata.from(command));
    }

    @Command(name = "stop")
    static class EnumParamCommand extends AbstractCommand
    {
        @Parameters(paramLabel = "compaction_type", arity = "0..1")
        OperationType compactionType = OperationType.UNKNOWN;

        @Override
        protected void execute(NodeProbe probe)
        {
        }
    }
}
