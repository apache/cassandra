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
import java.util.List;

// Settings common to commands that operate over multiple keys at once
public class SettingsCommandMulti extends SettingsCommand
{

    public final int keysAtOnce;

    public SettingsCommandMulti(Command type, Options options)
    {
        super(type, options.parent);
        this.keysAtOnce = Integer.parseInt(options.maxKeys.value());
    }

    // Option Declarations

    static final class Options extends GroupedOptions
    {
        final GroupedOptions parent;
        Options(GroupedOptions parent)
        {
            this.parent = parent;
        }
        final OptionSimple maxKeys = new OptionSimple("at-once=", "[0-9]+", "1000", "Number of keys per operation", false);

        @Override
        public List<? extends Option> options()
        {
            final List<Option> options = new ArrayList<>();
            options.add(maxKeys);
            options.addAll(parent.options());
            return options;
        }
    }

    // CLI Utility Methods

    public static SettingsCommand build(Command type, String[] params)
    {
        GroupedOptions options = GroupedOptions.select(params, new Options(new Uncertainty()), new Options(new Count()));
        if (options == null)
        {
            printHelp(type);
            System.out.println("Invalid " + type + " options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsCommandMulti(type, (Options) options);
    }

    public static void printHelp(Command type)
    {
        GroupedOptions.printOptions(System.out, type.toString().toLowerCase(), new Options(new Uncertainty()), new Options(new Count()));
    }

    public static Runnable helpPrinter(final Command type)
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                printHelp(type);
            }
        };
    }
}
