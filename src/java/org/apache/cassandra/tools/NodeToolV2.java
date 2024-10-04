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
package org.apache.cassandra.tools;

import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;
import javax.inject.Inject;

import com.google.common.base.Throwables;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.tools.nodetool.CassandraHelpLayout;
import org.apache.cassandra.tools.nodetool.JmxConnect;
import org.apache.cassandra.tools.nodetool.TopLevelCommand;
import org.apache.cassandra.utils.FBUtilities;
import picocli.CommandLine;

import static org.apache.cassandra.tools.NodeTool.badUse;
import static org.apache.cassandra.tools.NodeTool.err;
import static org.apache.cassandra.tools.NodeTool.printHistory;

/**
 * The command line utility for managing a Cassandra cluster via JMX, using the PicoCLI framework.
 * This class is the entry point for the nodetool command line utility.
 */
public class NodeToolV2
{
    static
    {
        FBUtilities.preventIllegalAccessWarnings();
    }

    private final INodeProbeFactory nodeProbeFactory;
    private final Output output;
    private CommandLine.IParameterExceptionHandler parameterExceptionHandler = (ex, arg) -> {
        badUse(ex.getCommandLine().getOut()::println, Throwables.getRootCause(ex));
        return 1;
    };

    private CommandLine.IExecutionExceptionHandler executionExceptionHandler = (ex, cmdLine, parseResult) -> {
        err(cmdLine.getErr()::println, Throwables.getRootCause(ex));
        return 2;
    };

    private CommandLine.IExecutionStrategy strategy;

    public static void main(String... args)
    {
        System.exit(new NodeToolV2(new NodeProbeFactory(), Output.CONSOLE).execute(args));
    }

    public NodeToolV2(INodeProbeFactory nodeProbeFactory, Output output)
    {
        this.nodeProbeFactory = nodeProbeFactory;
        this.output = output;
    }

    /**
     * Execute the command line utility with the given arguments via the JMX connection.
     * @param args command line arguments
     * @return 0 on success, 1 on bad use, 2 on execution error
     */
    public int execute(String... args)
    {
        return execute(createCommandLine(new CassandraCliFactory(nodeProbeFactory, output)), args);
    }

    protected int execute(CommandLine commandLine, String... args)
    {
        try
        {
            configureCliLayout(commandLine);
            commandLine.setExecutionStrategy(strategy == null ? JmxConnect::executionStrategy : strategy)
                       .setExecutionExceptionHandler(executionExceptionHandler)
                       .setParameterExceptionHandler(parameterExceptionHandler)
                       // Some of the Cassandra commands don't comply with the POSIX standard, so we need to disable such options.
                       // Example: ./nodetool -h localhost -p 7100 repair mykeyspayce -hosts 127.0.0.1,127.0.0.2
                       //
                       // This also means that option parameters must be separated from the option name by whitespace
                       // or the = separator character, so -D key=value and -D=key=value will be recognized but
                       // -Dkey=value will not.
                       .setPosixClusteredShortOptionsAllowed(false);

            printHistory(args);
            return commandLine.execute(args);
        }
        catch (Exception e)
        {
            err(output.err::println, e);
            return 2;
        }
    }

    /**
     * Filter the command by its name and return the given exit code when the filter is matched.
     * @param commandPredicate the predicate to filter the command name.
     * @param exitCodeWhenMatched the exit code to return when the filter is matched.
     * @return this instance.
     */
    public NodeToolV2 withCommandNameFilter(Predicate<String> commandPredicate, int exitCodeWhenMatched)
    {
        strategy = parsed -> {
            CommandLine.Model.CommandSpec spec = lastExecutableSubcommandWithSameParent(parsed.asCommandLineList());
            if (commandPredicate.test(spec.name()))
                return exitCodeWhenMatched;
            return JmxConnect.executionStrategy(parsed);
        };
        return this;
    }

    public NodeToolV2 withParameterExceptionHandler(CommandLine.IParameterExceptionHandler handler)
    {
        parameterExceptionHandler = handler;
        return this;
    }

    public NodeToolV2 withExecutionExceptionHandler(CommandLine.IExecutionExceptionHandler handler)
    {
        executionExceptionHandler = handler;
        return this;
    }

    public boolean isCommandPresent(String commandName)
    {
        CommandLine commandLine = createCommandLine(new CassandraCliFactory(nodeProbeFactory, output));
        return commandLine.getSubcommands().values().stream()
                          .anyMatch(sub -> sub.getCommandName().equals(commandName));
    }

    public Map<String, String> getCommandsDescription()
    {
        Map<String, String> commands = new TreeMap<>();
        CommandLine commandLine = createCommandLine(new CassandraCliFactory(nodeProbeFactory, output));
        commandLine.getSubcommands()
                   .values()
                   .forEach(sub -> commands.put(sub.getCommandName(),
                                                   CommandLine.Help.join(commandLine.getColorScheme().ansi(), 1000,
                                                                         sub.getCommandSpec().usageMessage().adjustLineBreaksForWideCJKCharacters(),
                                                                         sub.getCommandSpec().usageMessage().description(), new StringBuilder()).toString()));
        // Remove the help command from the list of commands, as it's not applicable for backward compatibility.
        return commands;
    }

    public static CommandLine.Model.CommandSpec lastExecutableSubcommandWithSameParent(List<CommandLine> parsedCommands)
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

    private static CommandLine createCommandLine(CassandraCliFactory factory)
    {
        return new CommandLine(new TopLevelCommand(), factory)
                   .addMixin(JmxConnect.MIXIN_KEY, factory.create(JmxConnect.class))
                   .setOut(new PrintWriter(factory.output.out, true))
                   .setErr(new PrintWriter(factory.output.err, true));
    }

    private static void configureCliLayout(CommandLine commandLine)
    {
        switch (CassandraRelevantProperties.CASSANDRA_CLI_LAYOUT.getEnum(true, CliLayout.class))
        {
            case CASSANDRA:
                commandLine.setHelpFactory(CassandraHelpLayout::new)
                           .setUsageHelpWidth(CassandraHelpLayout.DEFAULT_USAGE_HELP_WIDTH)
                           .setHelpSectionKeys(CassandraHelpLayout.cassandraHelpSectionKeys());
                break;
            case PICOCLI:
                break;
            default:
                throw new IllegalStateException("Unknown CLI layout: " +
                                                CassandraRelevantProperties.CASSANDRA_CLI_LAYOUT.getString());
        }
    }

    private enum CliLayout
    {
        CASSANDRA,
        PICOCLI
    }

    private static class CassandraCliFactory implements CommandLine.IFactory
    {
        private final CommandLine.IFactory fallback;
        private final INodeProbeFactory nodeProbeFactory;
        private final Output output;

        public CassandraCliFactory(INodeProbeFactory nodeProbeFactory, Output output)
        {
            this.fallback = CommandLine.defaultFactory();
            this.nodeProbeFactory = nodeProbeFactory;
            this.output = output;
        }

        public <K> K create(Class<K> cls)
        {
            try
            {
                K bean = this.fallback.create(cls);
                Class<?> beanClass = bean.getClass();
                do
                {
                    Field[] fields = beanClass.getDeclaredFields();
                    for (Field field : fields)
                    {
                        if (!field.isAnnotationPresent(Inject.class))
                            continue;
                        field.setAccessible(true);
                        if (field.getType().equals(INodeProbeFactory.class))
                            field.set(bean, nodeProbeFactory);
                        else if (field.getType().equals(Output.class))
                            field.set(bean, output);
                    }
                }
                while ((beanClass = beanClass.getSuperclass()) != null);
                return bean;
            }
            catch (Exception e)
            {
                throw new CommandLine.InitializationException("Failed to create instance of " + cls, e);
            }
        }
    }
}
