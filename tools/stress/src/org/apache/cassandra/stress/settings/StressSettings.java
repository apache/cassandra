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
import java.util.*;

import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.ResultLogger;
import org.apache.cassandra.transport.SimpleClient;

public class StressSettings implements Serializable
{
    public final SettingsCommand command;
    public final SettingsRate rate;
    public final SettingsPopulation generate;
    public final SettingsInsert insert;
    public final SettingsColumn columns;
    public final SettingsErrors errors;
    public final SettingsLog log;
    public final SettingsCredentials credentials;
    public final SettingsMode mode;
    public final SettingsNode node;
    public final SettingsSchema schema;
    public final SettingsTransport transport;
    public final SettingsPort port;
    public final SettingsJMX jmx;
    public final SettingsGraph graph;
    public final SettingsTokenRange tokenRange;
    public final SettingsReporting reporting;

    public StressSettings(SettingsCommand command,
                          SettingsRate rate,
                          SettingsPopulation generate,
                          SettingsInsert insert,
                          SettingsColumn columns,
                          SettingsErrors errors,
                          SettingsLog log,
                          SettingsCredentials credentials,
                          SettingsMode mode,
                          SettingsNode node,
                          SettingsSchema schema,
                          SettingsTransport transport,
                          SettingsPort port,
                          SettingsJMX jmx,
                          SettingsGraph graph,
                          SettingsTokenRange tokenRange,
                          SettingsReporting reporting)
    {
        this.command = command;
        this.rate = rate;
        this.insert = insert;
        this.generate = generate;
        this.columns = columns;
        this.errors = errors;
        this.log = log;
        this.credentials = credentials;
        this.mode = mode;
        this.node = node;
        this.schema = schema;
        this.transport = transport;
        this.port = port;
        this.jmx = jmx;
        this.graph = graph;
        this.tokenRange = tokenRange;
        this.reporting = reporting;
    }

    public SimpleClient getSimpleNativeClient()
    {
        try
        {
            String currentNode = node.randomNode();
            SimpleClient client = new SimpleClient(currentNode, port.nativePort);
            client.connect(false);
            client.execute("USE \"" + schema.keyspace + "\";", org.apache.cassandra.db.ConsistencyLevel.ONE);
            return client;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }

    private static volatile JavaDriverClient client;
    private static volatile int numFailures;
    private static int MAX_NUM_FAILURES = 10;

    public JavaDriverClient getJavaDriverClient()
    {
        return getJavaDriverClient(true);
    }

    public JavaDriverClient getJavaDriverClient(boolean setKeyspace)
    {
        if (setKeyspace)
        {
            return getJavaDriverClient(schema.keyspace);
        } else {
            return getJavaDriverClient(null);
        }
    }


    public JavaDriverClient getJavaDriverClient(String keyspace)
    {
        if (client != null)
            return client;

        synchronized (this)
        {
            if (numFailures >= MAX_NUM_FAILURES)
                throw new RuntimeException("Failed to create client too many times");

            try
            {
                if (client != null)
                    return client;

                EncryptionOptions encOptions = transport.getEncryptionOptions();
                JavaDriverClient c = new JavaDriverClient(this, node.nodes, port.nativePort, encOptions);
                c.connect(mode.compression());
                if (keyspace != null)
                    c.execute("USE \"" + keyspace + "\";", org.apache.cassandra.db.ConsistencyLevel.ONE);

                return client = c;
            }
            catch (Exception e)
            {
                numFailures +=1;
                throw new RuntimeException(e);
            }
        }
    }

    public void maybeCreateKeyspaces()
    {
        if (command.type == Command.WRITE || command.type == Command.COUNTER_WRITE)
            schema.createKeySpaces(this);
        else if (command.type == Command.USER)
            ((SettingsCommandUser) command).profiles.forEach((k,v) -> v.maybeCreateSchema(this));
    }

    public static StressSettings parse(String[] args)
    {
        args = repairParams(args);
        final Map<String, String[]> clArgs = parseMap(args);
        if (SettingsMisc.maybeDoSpecial(clArgs))
            return null;
        return get(clArgs);

    }

    private static String[] repairParams(String[] args)
    {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String arg : args)
        {
            if (!first)
                sb.append(" ");
            sb.append(arg);
            first = false;
        }
        return sb.toString()
                 .replaceAll("\\s+([,=()])", "$1")
                 .replaceAll("([,=(])\\s+", "$1")
                 .split(" +");
    }

    public static StressSettings get(Map<String, String[]> clArgs)
    {
        SettingsCommand command = SettingsCommand.get(clArgs);
        if (command == null)
            throw new IllegalArgumentException("No command specified");
        SettingsPort port = SettingsPort.get(clArgs);
        SettingsRate rate = SettingsRate.get(clArgs, command);
        SettingsPopulation generate = SettingsPopulation.get(clArgs, command);
        SettingsTokenRange tokenRange = SettingsTokenRange.get(clArgs);
        SettingsInsert insert = SettingsInsert.get(clArgs);
        SettingsColumn columns = SettingsColumn.get(clArgs);
        SettingsErrors errors = SettingsErrors.get(clArgs);
        SettingsLog log = SettingsLog.get(clArgs);
        SettingsCredentials credentials = SettingsCredentials.get(clArgs);
        SettingsMode mode = SettingsMode.get(clArgs, credentials);
        SettingsNode node = SettingsNode.get(clArgs);
        SettingsSchema schema = SettingsSchema.get(clArgs, command);
        SettingsTransport transport = SettingsTransport.get(clArgs, credentials);
        SettingsJMX jmx = SettingsJMX.get(clArgs, credentials);
        SettingsGraph graph = SettingsGraph.get(clArgs, command);
        SettingsReporting reporting = SettingsReporting.get(clArgs);
        if (!clArgs.isEmpty())
        {
            printHelp();
            System.out.println("Error processing command line arguments. The following were ignored:");
            for (Map.Entry<String, String[]> e : clArgs.entrySet())
            {
                System.out.print(e.getKey());
                for (String v : e.getValue())
                {
                    System.out.print(" ");
                    System.out.print(v);
                }
                System.out.println();
            }
            System.exit(1);
        }

        return new StressSettings(command, rate, generate, insert, columns, errors, log, credentials, mode, node, schema, transport, port, jmx, graph, tokenRange, reporting);
    }

    private static Map<String, String[]> parseMap(String[] args)
    {
        // first is the main command/operation, so specified without a -
        if (args.length == 0)
        {
            System.out.println("No command provided");
            printHelp();
            System.exit(1);
        }
        final LinkedHashMap<String, String[]> r = new LinkedHashMap<>();
        String key = null;
        List<String> params = new ArrayList<>();
        for (int i = 0 ; i < args.length ; i++)
        {
            if (i == 0 || args[i].startsWith("-"))
            {
                if (i > 0)
                    putParam(key, params.toArray(new String[0]), r);
                key = args[i].toLowerCase();
                params.clear();
            }
            else
                params.add(args[i]);
        }
        putParam(key, params.toArray(new String[0]), r);
        return r;
    }

    private static void putParam(String key, String[] args, Map<String, String[]> clArgs)
    {
        String[] prev = clArgs.put(key, args);
        if (prev != null)
            throw new IllegalArgumentException(key + " is defined multiple times. Each option/command can be specified at most once.");
    }

    public static void printHelp()
    {
        SettingsMisc.printHelp();
    }

    public void printSettings(ResultLogger out)
    {
        out.println("******************** Stress Settings ********************");
        // done
        out.println("Command:");
        command.printSettings(out);
        out.println("Rate:");
        rate.printSettings(out);
        out.println("Population:");
        generate.printSettings(out);
        out.println("Insert:");
        insert.printSettings(out);
        if (command.type != Command.USER)
        {
            out.println("Columns:");
            columns.printSettings(out);
        }
        out.println("Errors:");
        errors.printSettings(out);
        out.println("Log:");
        log.printSettings(out);
        out.println("Mode:");
        mode.printSettings(out);
        out.println("Node:");
        node.printSettings(out);
        out.println("Schema:");
        schema.printSettings(out);
        out.println("Transport:");
        transport.printSettings(out);
        out.println("Port:");
        port.printSettings(out);
        out.println("JMX:");
        jmx.printSettings(out);
        out.println("Graph:");
        graph.printSettings(out);
        out.println("TokenRange:");
        tokenRange.printSettings(out);
        out.println("Credentials file:");
        credentials.printSettings(out);
        out.println("Reporting:");
        reporting.printSettings(out);

        if (command.type == Command.USER)
        {
            out.println();
            out.println("******************** Profile(s) ********************");
            ((SettingsCommandUser) command).profiles.forEach((k,v) -> v.printSettings(out, this));
        }
        out.println();

    }

    public synchronized void disconnect()
    {
        if (client == null)
            return;

        client.disconnect();
        client = null;
    }
}
