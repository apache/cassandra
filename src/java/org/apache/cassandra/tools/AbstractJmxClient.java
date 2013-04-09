package org.apache.cassandra.tools;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public abstract class AbstractJmxClient implements Closeable
{
    private static final Options options = new Options();
    protected static final int DEFAULT_JMX_PORT = 7199;
    protected static final String DEFAULT_HOST = "localhost";

    protected final String host;
    protected final int port;
    protected final String username;
    protected final String password;
    protected JMXConnection jmxConn;
    protected PrintStream out = System.out;

    static
    {
        options.addOption("h", "host", true,  "JMX hostname or IP address (Default: localhost)");
        options.addOption("p", "port", true,  "JMX port number (Default: 7199)");
        options.addOption("u", "username", true,  "JMX username");
        options.addOption("pw", "password", true,  "JMX password");
        options.addOption("H", "help", false, "Print help information");
    }

    public AbstractJmxClient(String host, Integer port, String username, String password) throws IOException
    {
        this.host = (host != null) ? host : DEFAULT_HOST;
        this.port = (port != null) ? port : DEFAULT_JMX_PORT;
        this.username = username;
        this.password = password;
        jmxConn = new JMXConnection(this.host, this.port, username, password);
    }

    public void close() throws IOException
    {
        jmxConn.close();
    }

    public void writeln(Throwable err)
    {
        writeln(err.getMessage());
    }

    public void writeln(String msg)
    {
        out.println(msg);
    }

    public void write(String msg)
    {
        out.print(msg);
    }

    public void writeln(String format, Object...args)
    {
        write(format + "%n", args);
    }

    public void write(String format, Object...args)
    {
        out.printf(format, args);
    }

    public void setOutput(PrintStream out)
    {
        this.out = out;
    }

    public static CommandLine processArguments(String[] args) throws ParseException
    {
        CommandLineParser parser = new PosixParser();
        return parser.parse(options, args);
    }
    
    public static void addCmdOption(String shortOpt, String longOpt, boolean hasArg, String description)
    {
        options.addOption(shortOpt, longOpt, hasArg, description);
    }

    public static void printHelp(String synopsis, String header)
    {
        System.out.printf("Usage: %s%n%n", synopsis);
        System.out.print(header);
        System.out.println("Options:");
        for (Object opt : options.getOptions())
        {
            String shortOpt = String.format("%s,", ((Option)opt).getOpt());
            String longOpt = ((Option)opt).getLongOpt();
            String description = ((Option)opt).getDescription();
            System.out.printf(" -%-4s --%-17s %s%n", shortOpt, longOpt, description);
        }
    }
}

class JMXConnection
{
    private static final String FMT_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";
    private final String host, username, password;
    private final int port;
    private JMXConnector jmxc;
    private MBeanServerConnection mbeanServerConn;

    JMXConnection(String host, int port, String username, String password) throws IOException
    {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        connect();
    }

    private void connect() throws IOException
    {
        JMXServiceURL jmxUrl = new JMXServiceURL(String.format(FMT_URL, host, port));
        Map<String, Object> env = new HashMap<String, Object>();

        if (username != null)
            env.put(JMXConnector.CREDENTIALS, new String[]{ username, password });

        jmxc = JMXConnectorFactory.connect(jmxUrl, env);
        mbeanServerConn = jmxc.getMBeanServerConnection();
    }

    public void close() throws IOException
    {
        jmxc.close();
    }

    public MBeanServerConnection getMbeanServerConn()
    {
        return mbeanServerConn;
    }
}
