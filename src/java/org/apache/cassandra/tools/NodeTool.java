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
import java.util.Date;
import java.util.List;
import javax.inject.Inject;
import javax.management.InstanceNotFoundException;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;

import org.apache.cassandra.config.CassandraRelevantEnv;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileWriter;
import org.apache.cassandra.tools.nodetool.JmxConnect;
import org.apache.cassandra.tools.nodetool.NodetoolCommand;
import org.apache.cassandra.tools.nodetool.layout.CassandraCliHelpLayout;
import org.apache.cassandra.utils.FBUtilities;
import picocli.CommandLine;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static org.apache.cassandra.io.util.File.WriteMode.APPEND;
import static org.apache.cassandra.utils.LocalizeString.toUpperCaseLocalized;

public class NodeTool
{
    static
    {
        FBUtilities.preventIllegalAccessWarnings();
    }

    private static final String HISTORYFILE = "nodetool.history";

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
            commandLine.setExecutionStrategy(JmxConnect::executionStrategy)
                       .setExecutionExceptionHandler((ex, c, arg) -> {
                           // Used for backward compatibility, some commands are validated when a command is run.
                           if (ex instanceof IllegalArgumentException |
                               ex instanceof IllegalStateException)
                           {
                               badUse(ex);
                               return 1;
                           }

                           err(Throwables.getRootCause(ex));
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
            return commandLine.execute(args);
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

        try (FileWriter writer = new File(FBUtilities.getToolsOutputDirectory(), HISTORYFILE).newWriter(APPEND))
        {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
            writer.append(sdf.format(new Date())).append(": ").append(cmdLine).append(System.lineSeparator());
        }
        catch (IOException | IOError ioe)
        {
            //quietly ignore any errors about not being able to write out history
        }
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
        return new CommandLine(new NodetoolCommand(), factory)
                   .addMixin(JmxConnect.MIXIN_KEY, factory.create(JmxConnect.class));
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
        // CASSANDRA-11537: friendly error message when server is not ready
        if (e instanceof InstanceNotFoundException)
            throw new IllegalArgumentException("Server is not initialized yet, cannot run nodetool.");

        output.err.println("error: " + e.getMessage());
        output.err.println("-- StackTrace --");
        output.err.println(getStackTraceAsString(e));
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
