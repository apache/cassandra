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


import java.util.HashMap;
import java.util.Map;

public enum CliOption
{
    POP("Population distribution and intra-partition visit order", SettingsPopulation.helpPrinter()),
    INSERT("Insert specific options relating to various methods for batching and splitting partition updates", SettingsInsert.helpPrinter()),
    COL("Column details such as size and count distribution, data generator, names, comparator and if super columns should be used", SettingsColumn.helpPrinter()),
    RATE("Thread count, rate limit or automatic mode (default is auto)", SettingsRate.helpPrinter()),
    MODE("Thrift or CQL with options", SettingsMode.helpPrinter()),
    ERRORS("How to handle errors when encountered during stress", SettingsErrors.helpPrinter()),
    SAMPLE("Specify the number of samples to collect for measuring latency", SettingsSamples.helpPrinter()),
    SCHEMA("Replication settings, compression, compaction, etc.", SettingsSchema.helpPrinter()),
    NODE("Nodes to connect to", SettingsNode.helpPrinter()),
    LOG("Where to log progress to, and the interval at which to do it", SettingsLog.helpPrinter()),
    TRANSPORT("Custom transport factories", SettingsTransport.helpPrinter()),
    PORT("The port to connect to cassandra nodes on", SettingsPort.helpPrinter()),
    SENDTO("-send-to", "Specify a stress server to send this command to", SettingsMisc.sendToDaemonHelpPrinter())
    ;

    private static final Map<String, CliOption> LOOKUP;
    static
    {
        final Map<String, CliOption> lookup = new HashMap<>();
        for (CliOption cmd : values())
        {
            lookup.put("-" + cmd.toString().toLowerCase(), cmd);
            if (cmd.extraName != null)
                lookup.put(cmd.extraName, cmd);
        }
        LOOKUP = lookup;
    }

    public static CliOption get(String command)
    {
        return LOOKUP.get(command.toLowerCase());
    }

    public final String extraName;
    public final String description;
    private final Runnable helpPrinter;

    private CliOption(String description, Runnable helpPrinter)
    {
        this(null, description, helpPrinter);
    }
    private CliOption(String extraName, String description, Runnable helpPrinter)
    {
        this.extraName = extraName;
        this.description = description;
        this.helpPrinter = helpPrinter;
    }

    public void printHelp()
    {
        helpPrinter.run();
    }

}
