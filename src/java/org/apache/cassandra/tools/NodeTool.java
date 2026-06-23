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

import java.io.IOError;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;
import javax.management.InstanceNotFoundException;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;

import org.apache.cassandra.config.CassandraRelevantEnv;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileWriter;
import org.apache.cassandra.tools.nodetool.CqlConnect;
import org.apache.cassandra.tools.nodetool.JmxConnect;
import org.apache.cassandra.tools.nodetool.NodetoolCommand;
import org.apache.cassandra.tools.nodetool.layout.CassandraCliHelpLayout;
import org.apache.cassandra.tools.nodetool.strategy.CommandExecutionStrategy;
import org.apache.cassandra.tools.nodetool.strategy.NodetoolConnectionException;
import org.apache.cassandra.tools.nodetool.strategy.ProtocolAwareExecutionStrategy;
import org.apache.cassandra.utils.FBUtilities;

import picocli.CommandLine;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static org.apache.cassandra.io.util.File.WriteMode.APPEND;
import static org.apache.cassandra.tools.nodetool.PrintPortMixin.PRINT_PORT_LONG;
import static org.apache.cassandra.tools.nodetool.PrintPortMixin.PRINT_PORT_SHORT;
import static org.apache.cassandra.utils.LocalizeString.toUpperCaseLocalized;

public class NodeTool
{
    static
    {
        FBUtilities.preventIllegalAccessWarnings();
    }

    private static final String HISTORYFILE = "nodetool.history";

    /**
     * Set of subcommand names that accept the {@code -pp/--print-port} options.
     * These are the commands that declare the option via @Mixin and thus require
     * relocating the option for backward compatibility in order to be recognized
     * by picocli when specified before the subcommand name.
     * <p>
     * Both calls such as {@code ./nodetool --print-port status}, and
     * {@code ./nodetool status --print-port} should work as expected.
     */
    private static final Set<String> COMMANDS_SUPPORTING_PRINT_PORT_OPTION = Set.of("status", "ring", "netstats",
                                                                                    "gossipinfo", "getendpoints",
                                                                                    "failuredetector", "describering",
                                                                                    "describecluster");

    private final INodeProbeFactory nodeProbeFactory;
    private final Output output;

    public static void main(String... args)
    {
        System.exit(new NodeTool(new NodeProbeFactory(), Output.CONSOLE).execute(args));
    }

    public NodeTool(INodeProbeFactory nodeProbeFactory, Output output)
    {
        this.nodeProbeFactory = nodeProbeFactory;
        this.output = output;
    }

    /**
     * Execute the command line utility with the given arguments via the JMX connection.
     *
     * @param args command line arguments
     * @return 0 on success, 1 on bad use, 2 on execution error
     */
    public int execute(String... args)
    {
        try
        {
            CommandLine commandLine = createCommandLine(new CassandraCliFactory(nodeProbeFactory, output));
            commandLine.setOut(new PrintWriter(output.out, true));
            commandLine.setErr(new PrintWriter(output.err, true));

            configureCliLayout(commandLine);
            commandLine.setExecutionStrategy(ProtocolAwareExecutionStrategy::executionStrategy)
                       .setExecutionExceptionHandler((ex, c, arg) -> {
                           // Used for backward compatibility, some commands are validated when a command is run.
                           if (ex instanceof IllegalArgumentException |
                               ex instanceof IllegalStateException)
                           {
                               badUse(ex);
                               return 1;
                           }

                           NodetoolConnectionException connectFailure = Throwables.getCausalChain(ex).stream()
                                                                                  .filter(NodetoolConnectionException.class::isInstance)
                                                                                  .map(NodetoolConnectionException.class::cast)
                                                                                  .findFirst().orElse(null);
                           if (connectFailure != null)
                           {
                               output.err.println("nodetool: " + connectFailure.getMessage());
                               return 1;
                           }

                           // CASSANDRA-11537 friendly error message when server is not ready
                           Throwable root = Throwables.getRootCause(ex);
                           if (root instanceof InstanceNotFoundException)
                           {
                               badUse(new IllegalArgumentException("Server is not initialized yet, cannot run nodetool."));
                               return 1;
                           }

                           err(root);
                           return 2;
                       })
                       .setParameterExceptionHandler((ex, arg) -> {
                           badUse(ex);
                           return 1;
                       })
                       // Some of the Cassandra commands don't comply with the POSIX standard, so we need to disable such options.
                       // Example: ./nodetool -h localhost -p 7100 repair mykeyspayce -hosts 127.0.0.1,127.0.0.2
                       //
                       // This also means that option parameters must be separated from the option name by whitespace
                       // or the = separator character, so -D key=value and -D=key=value will be recognized but
                       // -Dkey=value will not.
                       .setPosixClusteredShortOptionsAllowed(false);

            printHistory(args);
            return commandLine.execute(relocatePrintPortOptionsForBackwardCompatibility(args));
        }
        catch (ConfigurationException e)
        {
            badUse(e);
            return 1;
        }
        catch (Throwable e)
        {
            err(Throwables.getRootCause(e));
            return 2;
        }
    }

    private static void printHistory(String... args)
    {
        //don't bother to print if no args passed (meaning, nodetool is just printing out the sub-commands list)
        if (args.length == 0)
            return;

        String cmdLine = Joiner.on(" ").skipNulls().join(args);
        cmdLine = cmdLine.replaceFirst("(?<=(-pw|--password))\\s+\\S+", " <hidden>");

        try (FileWriter writer = getHistoryFile().newWriter(APPEND))
        {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
            writer.append(sdf.format(new Date())).append(": ").append(cmdLine).append(System.lineSeparator());
        }
        catch (IOException | IOError ioe)
        {
            //quietly ignore any errors about not being able to write out history
        }
    }

    public static File getHistoryFile()
    {
        return new File(FBUtilities.getToolsOutputDirectory(), HISTORYFILE);
    }

    public static List<String> getCommandsWithoutRoot(String separator)
    {
        List<String> commands = new ArrayList<>();
        try
        {
            getCommandsWithoutRoot(createCommandLine(new CassandraCliFactory(new NodeProbeFactory(), Output.CONSOLE)), commands, separator);
            return commands;
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to initialize command line hierarchy", e);
        }
    }

    private static void getCommandsWithoutRoot(CommandLine cli, List<String> commands, String separator)
    {
        String name = cli.getCommandSpec().qualifiedName(separator);
        // Skip the root command as it's not a real command.
        if (cli.getCommandSpec().root() != cli.getCommandSpec())
            commands.add(name.replace(cli.getCommandSpec().root().qualifiedName() + separator, ""));
        for (CommandLine sub : cli.getSubcommands().values())
            getCommandsWithoutRoot(sub, commands, separator);
    }

    public static CommandLine createCommandLine(CommandLine.IFactory factory) throws Exception
    {
        CommandLine commandLine = new CommandLine(new NodetoolCommand(), factory);
        CommandExecutionStrategy.Type strategyType = ProtocolAwareExecutionStrategy.getExecutionStrategyTypeFromEnvAndSys();
        switch (strategyType)
        {
            case CQL:
                return commandLine.addMixin(strategyType.toString(), factory.create(CqlConnect.class));
            case STATIC_MBEAN:
            case COMMAND_MBEAN:
                return commandLine.addMixin(strategyType.toString(), factory.create(JmxConnect.class));
            default:
                throw new IllegalStateException("Unknown execution strategy: " + strategyType);
        }
    }

    private static void configureCliLayout(CommandLine commandLine)
    {
        CliLayout defaultLayout = CliLayout.valueOf(toUpperCaseLocalized(CassandraRelevantProperties.CASSANDRA_CLI_LAYOUT.getDefaultValue()));
        CliLayout layoutEnv = CassandraRelevantEnv.CASSANDRA_CLI_LAYOUT.getEnum(true, CliLayout.class,
                                                                                CassandraRelevantProperties.CASSANDRA_CLI_LAYOUT.getDefaultValue());
        CliLayout layoutSys = CassandraRelevantProperties.CASSANDRA_CLI_LAYOUT.getEnum(true, CliLayout.class);
        CliLayout layout = layoutEnv != defaultLayout ? layoutEnv : layoutSys;

        switch (layout)
        {
            case AIRLINE:
                commandLine.setHelpFactory(CassandraCliHelpLayout::new)
                           .setUsageHelpWidth(CassandraCliHelpLayout.DEFAULT_USAGE_HELP_WIDTH)
                           .setHelpSectionKeys(CassandraCliHelpLayout.cassandraHelpSectionKeys());
                break;
            case PICOCLI:
                break;
            default:
                throw new IllegalStateException("Unknown CLI layout: " + layout);
        }
    }

    protected void badUse(Exception e)
    {
        output.out.println("nodetool: " + e.getMessage());
        output.out.println("See 'nodetool help' or 'nodetool help <command>'.");
    }

    protected void err(Throwable e)
    {
        output.err.println("error: " + e.getMessage());
        output.err.println("-- StackTrace --");
        output.err.println(getStackTraceAsString(e));
    }

    /**
     * Rewrites global {@code -pp/--print-port} options that have been moved
     * to subcommands via @Mixin for backward compatibility. When a user types:
     * <pre>
     *   nodetool -pp status
     *   nodetool --print-port status -r
     * </pre>
     * this method rewrites them to:
     * <pre>
     *   nodetool status -pp
     *   nodetool status -r --print-port
     * </pre>
     * so that picocli assigns the option to the subcommand that declares it.
     * <p>
     * Options that appear after the subcommand name are left untouched:
     * <pre>
     *   nodetool status -pp -> unchanged
     *   nodetool status -pp -r -> unchanged
     * </pre>
     */
    static String[] relocatePrintPortOptionsForBackwardCompatibility(String[] args)
    {
        if (args == null || args.length < 2)
            return args;

        Set<String> relocatable = Set.of(PRINT_PORT_SHORT, PRINT_PORT_LONG);

        int subcommandIdx = -1;
        for (int i = 0; i < args.length; i++)
        {
            if (COMMANDS_SUPPORTING_PRINT_PORT_OPTION.contains(args[i]))
            {
                subcommandIdx = i;
                break;
            }
        }

        if (subcommandIdx < 0)
            return args;

        List<String> before = new ArrayList<>();
        List<String> toRelocate = new ArrayList<>();
        for (int i = 0; i < subcommandIdx; i++)
        {
            if (relocatable.contains(args[i]))
                toRelocate.add(args[i]);
            else
                before.add(args[i]);
        }

        if (toRelocate.isEmpty())
            return args;

        List<String> result = new ArrayList<>(before);
        result.addAll(Arrays.asList(args).subList(subcommandIdx, args.length));
        result.addAll(toRelocate);

        return result.toArray(new String[0]);
    }

    private enum CliLayout
    {
        AIRLINE,
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
                        if (field.getType().equals(INodeProbeFactory.class))
                        {
                            field.setAccessible(true);
                            field.set(bean, nodeProbeFactory);
                        }
                        else if (field.getType().equals(Output.class))
                        {
                            field.setAccessible(true);
                            field.set(bean, output);
                        }
                        else
                        {
                            throw new RuntimeException("Unsupported injectable field type: " + field.getType() +
                                    " in class " + beanClass.getName() + ". " +
                                    "Only INodeProbeFactory and Output are supported.");
                        }
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
