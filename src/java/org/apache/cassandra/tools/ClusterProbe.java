/**
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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.List;
import java.net.InetAddress;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.cassandra.db.CompactionManager;
import org.apache.cassandra.db.CompactionManagerMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 * JMX cluster wide operations for Cassandra.
 */
public class ClusterProbe {
    private static final String fmtUrl = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";
    private static final String ssObjName = "org.apache.cassandra.service:type=StorageService";
    private static final String HOST_OPTION = "host";
    private static final String PORT_OPTION = "port";
    private static final int defaultPort = 8080;
    private static Options options = null;
    private CommandLine cmd = null;
    private String host;
    private int port;

    private MBeanServerConnection mbeanServerConn;
    private StorageServiceMBean ssProxy;
    private MemoryMXBean memProxy;
    private RuntimeMXBean runtimeProxy;
    private CompactionManagerMBean mcmProxy;

    static
    {
        options = new Options();
        Option optHost = new Option(HOST_OPTION, true, "node hostname or ip address");
        optHost.setRequired(true);
        options.addOption(optHost);
        options.addOption(PORT_OPTION, true, "remote jmx agent port number");
    }

    /**
     * Creates a ClusterProbe using command-line arguments.
     *
     * @param cmdArgs list of arguments passed on the command line
     * @throws ParseException for missing required, or unrecognized options
     * @throws IOException on connection failures
     */
    private ClusterProbe(String[] cmdArgs) throws ParseException, IOException, InterruptedException
    {
        parseArgs(cmdArgs);
        this.host = cmd.getOptionValue(HOST_OPTION);

        String portNum = cmd.getOptionValue(PORT_OPTION);
        if (portNum != null)
        {
            try
            {
                this.port = Integer.parseInt(portNum);
            }
            catch (NumberFormatException e)
            {
                throw new ParseException("Port must be a number");
            }
        }
        else
        {
            this.port = defaultPort;
        }

        connect();
    }

    /**
     * Creates a ClusterProbe using the specified JMX host and port.
     *
     * @param host hostname or IP address of the JMX agent
     * @param port TCP port of the remote JMX agent
     * @throws IOException on connection failures
     */
    public ClusterProbe(String host, int port) throws IOException, InterruptedException
    {
        this.host = host;
        this.port = port;
        connect();
    }

    /**
     * Creates a ClusterProbe using the specified JMX host and default port.
     *
     * @param host hostname or IP address of the JMX agent
     * @throws IOException on connection failures
     */
    public ClusterProbe(String host) throws IOException, InterruptedException
    {
        this.host = host;
        this.port = defaultPort;
        connect();
    }

    /**
     * Create a connection to the JMX agent and setup the M[X]Bean proxies.
     *
     * @throws IOException on connection failures
     */
    private void connect() throws IOException
    {
        JMXServiceURL jmxUrl = new JMXServiceURL(String.format(fmtUrl, host, port));
        JMXConnector jmxc = JMXConnectorFactory.connect(jmxUrl, null);
        mbeanServerConn = jmxc.getMBeanServerConnection();

        try
        {
            ObjectName name = new ObjectName(ssObjName);
            ssProxy = JMX.newMBeanProxy(mbeanServerConn, name, StorageServiceMBean.class);
            name = new ObjectName(CompactionManager.MBEAN_OBJECT_NAME);
            mcmProxy = JMX.newMBeanProxy(mbeanServerConn, name, CompactionManagerMBean.class);
        } catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(
                    "Invalid ObjectName? Please report this as a bug.", e);
        }

        memProxy = ManagementFactory.newPlatformMXBeanProxy(mbeanServerConn,
                ManagementFactory.MEMORY_MXBEAN_NAME, MemoryMXBean.class);
        runtimeProxy = ManagementFactory.newPlatformMXBeanProxy(
                mbeanServerConn, ManagementFactory.RUNTIME_MXBEAN_NAME, RuntimeMXBean.class);
    }

    /**
     * Parse the supplied command line arguments.
     *
     * @param args arguments passed on the command line
     * @throws ParseException for missing required, or unrecognized options
     */
    private void parseArgs(String[] args) throws ParseException
    {
        CommandLineParser parser = new PosixParser();
        cmd = parser.parse(options, args);
    }

    /**
     * Retrieve any non-option arguments passed on the command line.
     *
     * @return non-option command args
     */
    private String[] getArgs()
    {
        return cmd.getArgs();
    }

    /**
     * Prints usage information to stdout.
     */
    private static void printUsage()
    {
        HelpFormatter hf = new HelpFormatter();
        String header = String.format(
                "%nAvailable commands: get_endpoints [key]");
        String usage = String.format("java %s -host <arg> <command>%n", ClusterProbe.class.getName());
        hf.printHelp(usage, "", options, header);
    }

    public void getEndPoints(String key)
    {
        List<InetAddress> endpoints = ssProxy.getLiveNaturalEndpoints(key);
        System.out.println(String.format("%-17s: %s", "Key", key));
        System.out.println(String.format("%-17s: %s", "Endpoints", endpoints));
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException, InterruptedException
    {
        ClusterProbe probe = null;
        try
        {
            probe = new ClusterProbe(args);
        }
        catch (ParseException pe)
        {
            System.err.println(pe.getMessage());
            ClusterProbe.printUsage();
            System.exit(1);
        }
        catch (IOException ioe)
        {
            System.err.println("Error connecting to remote JMX agent!");
            ioe.printStackTrace();
            System.exit(3);
        }

        if (probe.getArgs().length < 1)
        {
            System.err.println("Missing argument for command.");
            ClusterProbe.printUsage();
            System.exit(1);
        }

        // Execute the requested command.
        String[] arguments = probe.getArgs();
        String cmdName = arguments[0];
        if (cmdName.equals("get_endpoints"))
        {
            if (arguments.length <= 1)
            {
                System.err.println("missing key argument");
            }
            probe.getEndPoints(arguments[1]);
        }
        else
        {
            System.err.println("Unrecognized command: " + cmdName + ".");
            ClusterProbe.printUsage();
            System.exit(1);
        }

        System.exit(0);
    }
    
}
