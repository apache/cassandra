package org.apache.cassandra.stress.settings;
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


import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;

import com.google.common.net.HostAndPort;

import com.datastax.driver.core.Host;
import org.apache.cassandra.stress.util.ResultLogger;

public class SettingsNode implements Serializable
{
    public final List<String> nodes;
    public final boolean isWhiteList;
    public final String datacenter;

    public SettingsNode(Options options)
    {
        if (options.file.setByUser())
        {
            try
            {
                String node;
                List<String> tmpNodes = new ArrayList<>();
                try (BufferedReader in = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(options.file.value())))))
                {
                    while ((node = in.readLine()) != null)
                    {
                        if (node.length() > 0)
                            tmpNodes.add(node);
                    }
                    nodes = Arrays.asList(tmpNodes.toArray(new String[tmpNodes.size()]));
                }
            }
            catch(IOException ioe)
            {
                throw new RuntimeException(ioe);
            }

        }
        else
        {
            nodes = Arrays.asList(options.list.value().split(","));
        }

        isWhiteList = options.whitelist.setByUser();
        datacenter = options.datacenter.value();
    }

    public Set<String> resolveAllPermitted(StressSettings settings)
    {
        Set<String> r = new HashSet<>();
        switch (settings.mode.api)
        {
            case JAVA_DRIVER_NATIVE:
                if (!isWhiteList)
                {
                    for (Host host : settings.getJavaDriverClient().getCluster().getMetadata().getAllHosts())
                        r.add(host.getSocketAddress().getHostString() + ":" + host.getSocketAddress().getPort());
                    break;
                }
            case SIMPLE_NATIVE:
                for (InetAddress address : resolveAllSpecified())
                    r.add(address.getHostName());
        }
        return r;
    }

    public Set<InetAddress> resolveAllSpecified()
    {
        Set<InetAddress> r = new HashSet<>();
        for (String node : nodes)
        {
            try
            {
                HostAndPort hap = HostAndPort.fromString(node);
                r.add(InetAddress.getByName(hap.getHost()));
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }
        return r;
    }

    public Set<InetSocketAddress> resolveAll(int port)
    {
        Set<InetSocketAddress> r = new HashSet<>();
        for (String node : nodes)
        {
            try
            {
                HostAndPort hap = HostAndPort.fromString(node).withDefaultPort(port);
                r.add(new InetSocketAddress(InetAddress.getByName(hap.getHost()), hap.getPort()));
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }
        return r;
    }

    public String randomNode()
    {
        int index = (int) (Math.random() * nodes.size());
        if (index >= nodes.size())
            index = nodes.size() - 1;
        return nodes.get(index);
    }

    // Option Declarations

    public static final class Options extends GroupedOptions
    {
        final OptionSimple datacenter = new OptionSimple("datacenter=", ".*", null, "Datacenter used for DCAwareRoundRobinLoadPolicy", false);
        final OptionSimple whitelist = new OptionSimple("whitelist", "", null, "Limit communications to the provided nodes", false);
        final OptionSimple file = new OptionSimple("file=", ".*", null, "Node file (one per line)", false);
        final OptionSimple list = new OptionSimple("", "[^=,]+(,[^=,]+)*", "localhost", "comma delimited list of nodes", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(datacenter, whitelist, file, list);
        }
    }

    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        out.println("  Nodes: " + nodes);
        out.println("  Is White List: " + isWhiteList);
        out.println("  Datacenter: " + datacenter);
    }

    public static SettingsNode get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-node");
        if (params == null)
            return new SettingsNode(new Options());

        GroupedOptions options = GroupedOptions.select(params, new Options());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -node options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsNode((Options) options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-node", new Options());
    }

    public static Runnable helpPrinter()
    {
        return SettingsNode::printHelp;
    }
}
