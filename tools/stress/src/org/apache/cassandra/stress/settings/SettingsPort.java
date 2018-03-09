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


import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.stress.util.ResultLogger;

public class SettingsPort implements Serializable
{

    public final int nativePort;
    public final int jmxPort;

    public SettingsPort(PortOptions options)
    {
        nativePort = Integer.parseInt(options.nativePort.value());
        jmxPort = Integer.parseInt(options.jmxPort.value());
    }

    // Option Declarations

    private static final class PortOptions extends GroupedOptions
    {
        final OptionSimple nativePort = new OptionSimple("native=", "[0-9]+", "9042", "Use this port for the Cassandra native protocol", false);
        final OptionSimple jmxPort = new OptionSimple("jmx=", "[0-9]+", "7199", "Use this port for retrieving statistics over jmx", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(nativePort, jmxPort);
        }
    }

    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        out.printf("  Native Port: %d%n", nativePort);
        out.printf("  JMX Port: %d%n", jmxPort);
    }


    public static SettingsPort get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-port");
        if (params == null)
        {
            return new SettingsPort(new PortOptions());
        }
        PortOptions options = GroupedOptions.select(params, new PortOptions());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -port options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsPort(options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-port", new PortOptions());
    }

    public static Runnable helpPrinter()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                printHelp();
            }
        };
    }
}

