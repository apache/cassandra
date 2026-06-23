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

package org.apache.cassandra.management.picocli;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.management.api.Command;
import org.apache.cassandra.management.api.CommandsProvider;
import org.apache.cassandra.tools.nodetool.AbstractCommand;
import org.apache.cassandra.tools.nodetool.NodetoolCommand;

import picocli.CommandLine;

public class PicocliCommandsProvider implements CommandsProvider
{
    @Override
    public Collection<Command<?>> commands()
    {
        CommandLine commandLine = new CommandLine(NodetoolCommand.class);
        List<Command<?>> commands = new ArrayList<>();
        
        commandLine.getSubcommands().forEach((name, subcommandLine) -> {
            if (!subcommandLine.getCommandSpec().subcommands().isEmpty())
            {
                @SuppressWarnings("unchecked")
                Class<? extends AbstractCommand> abstractCommandClass =
                    (Class<? extends AbstractCommand>) subcommandLine.getCommand().getClass();
                commands.add(new PicocliCommandRegistryAdapter(abstractCommandClass));
            }
            else
            {
                Class<?> commandClass = subcommandLine.getCommand().getClass();
                if (AbstractCommand.class.isAssignableFrom(commandClass))
                {
                    @SuppressWarnings("unchecked")
                    Class<? extends AbstractCommand> abstractCommandClass =
                        (Class<? extends AbstractCommand>) commandClass;
                    commands.add(new PicocliCommandAdapter(abstractCommandClass));
                }
            }
        });
        
        return commands;
    }
}
