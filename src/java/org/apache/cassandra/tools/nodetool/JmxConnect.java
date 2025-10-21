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

package org.apache.cassandra.tools.nodetool;

import java.io.Console;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Scanner;

import javax.inject.Inject;

import com.google.common.base.Throwables;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tools.INodeProbeFactory;
import org.apache.cassandra.tools.NodeProbe;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.InitializationException;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.RunLast;
import picocli.CommandLine.Spec;

import static java.lang.Integer.parseInt;
import static org.apache.cassandra.tools.NodeProbe.defaultPort;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * Command options for NodeTool commands that are executed via JMX.
 */
@Command(name = "connect", description = "Connect to a Cassandra node via JMX")
public class JmxConnect extends AbstractCommand implements AutoCloseable
{
    public static final String MIXIN_KEY = "jmx";

    /** The command specification, used to access command-specific properties. */
    @Spec
    protected CommandSpec spec; // injected by picocli

    @Option(names = { "-h", "--host" }, description = "Node hostname or ip address", arity = "0..1")
    private String host = "127.0.0.1";

    @Option(names = { "-p", "--port" }, description = "Remote jmx agent port number", arity = "0..1")
    private String port = String.valueOf(defaultPort);

    @Option(names = { "-u", "--username" }, description = "Remote jmx agent username", arity = "0..1")
    private String username = EMPTY;

    @Option(names = { "-pw", "--password" }, description = "Remote jmx agent password", arity = "0..1")
    private String password = EMPTY;

    @Option(names = { "-pwf", "--password-file" }, description = "Path to the JMX password file", arity = "0..1")
    private String passwordFilePath = EMPTY;

    @Inject
    private INodeProbeFactory nodeProbeFactory;

    /**
     * This method is called by picocli and used depending on the execution strategy.
     * @param parseResult The parsed command line.
     * @return The exit code.
     */
    public static int executionStrategy(ParseResult parseResult)
    {
        CommandSpec jmx = parseResult.commandSpec().mixins().get(MIXIN_KEY);
        if (jmx == null)
            throw new InitializationException("No JmxConnect command found in the top-level hierarchy");

        try (JmxConnectionCommandInvoker invoker = new JmxConnectionCommandInvoker((JmxConnect) jmx.userObject()))
        {
            return invoker.execute(parseResult);
        }
        catch (JmxConnectionCommandInvoker.CloseException e)
        {
            jmx.commandLine()
               .getErr()
               .println("Failed to connect to JMX: " + e.getMessage());
            return jmx.commandLine().getExitCodeExceptionMapper().getExitCode(e);
        }
    }

    /**
     * Initialize the JMX connection to the Cassandra node using the provided options.
     */
    @Override
    protected void execute(NodeProbe probe)
    {
        assert probe == null;
        try
        {
            if (isNotEmpty(username))
            {
                if (isNotEmpty(passwordFilePath))
                    password = readUserPasswordFromFile(username, passwordFilePath);

                if (isEmpty(password))
                    password = promptAndReadPassword();
            }

            probe(username.isEmpty() ? nodeProbeFactory.create(host, parseInt(port))
                                     : nodeProbeFactory.create(host, parseInt(port), username, password));
        }
        catch (IOException | SecurityException e)
        {
            Throwable rootCause = Throwables.getRootCause(e);
            output.printError("nodetool: Failed to connect to '%s:%s' - %s: '%s'.%n", host, port,
                         rootCause.getClass().getSimpleName(), rootCause.getMessage());
            throw new InitializationException("Failed to connect to JMX", e);
        }
    }

    @Override
    public void close() throws Exception
    {
        if (probe() == null)
            return;
        ((AutoCloseable) probe()).close();
    }

    private static String readUserPasswordFromFile(String username, String passwordFilePath)
    {
        String password = EMPTY;

        File passwordFile = new File(passwordFilePath);
        try (Scanner scanner = new Scanner(passwordFile.toJavaIOFile()).useDelimiter("\\s+"))
        {
            while (scanner.hasNextLine())
            {
                if (scanner.hasNext())
                {
                    String jmxRole = scanner.next();
                    if (jmxRole.equals(username) && scanner.hasNext())
                    {
                        password = scanner.next();
                        break;
                    }
                }
                scanner.nextLine();
            }
        }
        catch (FileNotFoundException e)
        {
            throw new UncheckedIOException(e);
        }

        return password;
    }

    private static String promptAndReadPassword()
    {
        String password = EMPTY;

        Console console = System.console();
        if (console != null)
            password = String.valueOf(console.readPassword("Password:"));

        return password;
    }

    private static class JmxConnectionCommandInvoker implements IExecutionStrategy, AutoCloseable
    {
        private final JmxConnect connect;

        public JmxConnectionCommandInvoker(JmxConnect connect)
        {
            this.connect = connect;
        }

        @Override
        public int execute(ParseResult parseResult) throws ExecutionException, ParameterException
        {
            CommandSpec lastParent = lastExecutableSubcommandWithSameParent(parseResult.asCommandLineList());
            if (lastParent.userObject() instanceof AbstractCommand)
            {
                AbstractCommand command = (AbstractCommand) lastParent.userObject();
                if (command.shouldConnect())
                    connect.run();
                command.probe(connect.probe());
            }
            return new RunLast().execute(parseResult);
        }

        @Override
        public void close() throws CloseException
        {
            try
            {
                if (connect.probe() != null)
                    ((AutoCloseable) connect.probe()).close();
            }
            catch (Exception e)
            {
                throw new CloseException("Failed to close JMX connection", e);
            }
        }

        private static CommandLine.Model.CommandSpec lastExecutableSubcommandWithSameParent(List<CommandLine> parsedCommands)
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

        private static class CloseException extends RuntimeException
        {
            public CloseException(String message, Throwable cause)
            {
                super(message, cause);
            }
        }
    }
}
