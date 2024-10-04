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

import java.io.IOException;
import javax.inject.Inject;

import com.google.common.base.Throwables;

import org.apache.cassandra.tools.INodeProbeFactory;
import org.apache.cassandra.tools.NodeProbe;
import picocli.CommandLine;

import static java.lang.Integer.parseInt;
import static org.apache.cassandra.tools.NodeTool.NodeToolCmd.promptAndReadPassword;
import static org.apache.cassandra.tools.NodeTool.NodeToolCmd.readUserPasswordFromFile;
import static org.apache.cassandra.tools.NodeToolV2.lastExecutableSubcommandWithSameParent;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * Command options for NodeTool commands that are executed via JMX.
 */
@CommandLine.Command(name = "connect", description = "Connect to a Cassandra node via JMX")
public class JmxConnect extends AbstractCommand implements AutoCloseable
{
    public static final String MIXIN_KEY = "jmx";

    /** The command specification, used to access command-specific properties. */
    @CommandLine.Spec
    protected CommandLine.Model.CommandSpec spec; // injected by picocli

    @CommandLine.Option(names = { "-h", "--host" }, description = "Node hostname or ip address")
    public String host = "127.0.0.1";

    @CommandLine.Option(names = { "-p", "--port" }, description = "Remote jmx agent port number")
    public String port = "7199";

    @CommandLine.Option(names = { "-u", "--username" }, description = "Remote jmx agent username")
    public String username = EMPTY;

    @CommandLine.Option(names = { "-pw", "--password" }, description = "Remote jmx agent password")
    public String password = EMPTY;

    @CommandLine.Option(names = { "-pwf", "--password-file" }, description = "Path to the JMX password file")
    public String passwordFilePath = EMPTY;

    @CommandLine.Option(names = { "-pp", "--print-port" }, description = "Operate in 4.0 mode with hosts disambiguated by port number")
    public boolean printPort = false;

    @Inject
    private INodeProbeFactory nodeProbeFactory;

    /**
     * This method is called by picocli and used depending on the execution strategy.
     * @param parseResult The parsed command line.
     * @return The exit code.
     */
    public static int executionStrategy(CommandLine.ParseResult parseResult)
    {
        CommandLine.Model.CommandSpec jmx = parseResult.commandSpec().mixins().get(MIXIN_KEY);
        if (jmx == null)
            throw new CommandLine.InitializationException("No JmxConnect command found in the top-level hierarchy");

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
            if (isNotEmpty(username)) {
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
            logger.error("nodetool: Failed to connect to '%s:%s' - %s: '%s'.%n", host, port,
                              rootCause.getClass().getSimpleName(), rootCause.getMessage());
            throw new CommandLine.ExecutionException(spec.commandLine(), "Failed to connect to JMX", e);
        }
    }

    @Override
    public void close() throws Exception
    {
        if (probe != null)
            ((AutoCloseable) probe).close();
    }

    private static class JmxConnectionCommandInvoker implements CommandLine.IExecutionStrategy, AutoCloseable
    {
        private final JmxConnect connect;

        public JmxConnectionCommandInvoker(JmxConnect connect)
        {
            this.connect = connect;
        }

        @Override
        public int execute(CommandLine.ParseResult parseResult) throws CommandLine.ExecutionException, CommandLine.ParameterException
        {
            CommandLine.Model.CommandSpec lastParent = lastExecutableSubcommandWithSameParent(parseResult.asCommandLineList());
            if (lastParent.userObject() instanceof AbstractCommand)
            {
                connect.run();
                ((AbstractCommand) lastParent.userObject()).probe(connect.probe());
            }
            return new CommandLine.RunLast().execute(parseResult);
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

        private static class CloseException extends RuntimeException
        {
            public CloseException(String message, Throwable cause)
            {
                super(message, cause);
            }
        }
    }
}
