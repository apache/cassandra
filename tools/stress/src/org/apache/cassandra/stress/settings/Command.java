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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

public enum Command
{

    READ(false, "standard1",
            "Multiple concurrent reads - the cluster must first be populated by a write test",
            CommandCategory.BASIC
    ),
    WRITE(true, "standard1",
            "insert",
            "Multiple concurrent writes against the cluster",
            CommandCategory.BASIC
    ),
    MIXED(true, null,
            "Interleaving of any basic commands, with configurable ratio and distribution - the cluster must first be populated by a write test",
            CommandCategory.MIXED
    ),
    COUNTER_WRITE(true, "counter1",
            "counter_add",
            "Multiple concurrent updates of counters.",
            CommandCategory.BASIC
    ),
    COUNTER_READ(false, "counter1",
            "counter_get",
            "Multiple concurrent reads of counters. The cluster must first be populated by a counterwrite test.",
            CommandCategory.BASIC
    ),
    USER(true, null,
          "Interleaving of user provided queries, with configurable ratio and distribution",
          CommandCategory.USER
    ),

    HELP(false, null, "-?", "Print help for a command or option", null),
    PRINT(false, null, "Inspect the output of a distribution definition", null),
    LEGACY(false, null, "Legacy support mode", null)
    ;

    private static final Map<String, Command> LOOKUP;
    static
    {
        final Map<String, Command> lookup = new HashMap<>();
        for (Command cmd : values())
        {
            for (String name : cmd.names)
                lookup.put(name, cmd);
        }
        LOOKUP = lookup;
    }

    public static Command get(String command)
    {
        return LOOKUP.get(command.toLowerCase());
    }

    public final boolean updates;
    public final CommandCategory category;
    public final List<String> names;
    public final String description;
    public final String table;

    Command(boolean updates, String table, String description, CommandCategory category)
    {
        this(updates, table, null, description, category);
    }

    Command(boolean updates, String table, String extra, String description, CommandCategory category)
    {
        this.table = table;
        this.updates = updates;
        this.category = category;
        List<String> names = new ArrayList<>();
        names.add(this.toString().toLowerCase());
        names.add(this.toString().replaceAll("_", "").toLowerCase());
        if (extra != null)
        {
            names.add(extra.toLowerCase());
            names.add(extra.replaceAll("_", "").toLowerCase());
        }
        this.names = ImmutableList.copyOf(names);
        this.description = description;
    }

    public void printHelp()
    {
        helpPrinter().run();
    }

    public final Runnable helpPrinter()
    {
        switch (this)
        {
            case PRINT:
                return SettingsMisc.printHelpPrinter();
            case HELP:
                return SettingsMisc.helpHelpPrinter();
            case LEGACY:
                return Legacy.helpPrinter();
        }
        switch (category)
        {
            case USER:
                return SettingsCommandUser.helpPrinter();
            case BASIC:
                return SettingsCommandPreDefined.helpPrinter(this);
            case MIXED:
                return SettingsCommandPreDefinedMixed.helpPrinter();
        }
        throw new AssertionError();
    }

}