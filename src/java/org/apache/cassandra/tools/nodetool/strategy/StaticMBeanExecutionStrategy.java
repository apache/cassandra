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

import java.util.List;

import org.apache.cassandra.tools.nodetool.AbstractCommand;
import org.apache.cassandra.tools.nodetool.JmxConnect;

import picocli.CommandLine;

public class StaticMBeanExecutionStrategy implements CommandExecutionStrategy
{
    private final JmxConnect connect;

    public StaticMBeanExecutionStrategy(JmxConnect connect)
    {
        this.connect = connect;
    }

    @Override
    public int execute(CommandLine.ParseResult parseResult) throws CommandLine.ExecutionException, CommandLine.ParameterException
    {
        CommandLine.Model.CommandSpec lastParent = lastExecutableSubcommandWithSameParent(parseResult.asCommandLineList());
        if (lastParent.userObject() instanceof AbstractCommand)
        {
            AbstractCommand command = (AbstractCommand) lastParent.userObject();
            if (command.shouldConnect())
                connect.run();
            command.probe(connect.probe());
        }
        return new CommandLine.RunLast().execute(parseResult);
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

    static CommandLine.Model.CommandSpec lastExecutableSubcommandWithSameParent(List<CommandLine> parsedCommands)
    {
        int start = parsedCommands.size() - 1;
        for (int i = parsedCommands.size() - 2; i >= 0; i--)
        {
            if (parsedCommands.get(i).getParent() != parsedCommands.get(i + 1).getParent())
                break;
            start = i;
        }
        return parsedCommands.get(start).getCommandSpec();
    }
}
