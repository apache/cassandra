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
import java.util.Scanner;

import javax.inject.Inject;

import com.google.common.base.Throwables;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tools.INodeProbeFactory;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.strategy.NodetoolConnectionException;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

import static java.lang.Integer.parseInt;
import static org.apache.cassandra.tools.RemoteJmxMBeanAccessor.defaultPort;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * Command options for NodeTool commands that are executed via JMX.
 */
@Command(name = "connect", description = "Connect to a Cassandra node via JMX")
public class JmxConnect extends AbstractCommand implements AutoCloseable
{
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
            throw new NodetoolConnectionException(String.format("Failed to connect to '%s:%s' - %s: '%s'.",
                                                                host, port,
                                                                rootCause.getClass().getSimpleName(),
                                                                rootCause.getMessage()),
                                                  e);
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

    public String getHost()
    {
        return host;
    }

    public String getPort()
    {
        return port;
    }
}
