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


import java.io.PrintStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;

public abstract class GroupedOptions implements Serializable
{

    int accepted = 0;

    public boolean accept(String param)
    {
        for (Option option : options())
        {
            if (option.accept(param))
            {
                accepted++;
                return true;
            }
        }
        return false;
    }

    public boolean happy()
    {
        for (Option option : options())
            if (!option.happy())
                return false;
        return true;
    }

    public abstract List<? extends Option> options();

    // hands the parameters to each of the option groups, and returns the first provided
    // option group that is happy() after this is done, that also accepted all the parameters
    public static <G extends GroupedOptions> G select(String[] params, G... groupings)
    {
        for (String param : params)
        {
            boolean accepted = false;
            for (GroupedOptions grouping : groupings)
                accepted |= grouping.accept(param);
            if (!accepted)
                throw new IllegalArgumentException("Invalid parameter " + param);
        }
        for (G grouping : groupings)
            if (grouping.happy() && grouping.accepted == params.length)
                return grouping;
        return null;
    }

    // pretty prints all of the option groupings
    public static void printOptions(PrintStream out, String command, GroupedOptions... groupings)
    {
        out.println();
        boolean firstRow = true;
        for (GroupedOptions grouping : groupings)
        {
            if (!firstRow)
            {
                out.println(" OR ");
            }
            firstRow = false;

            StringBuilder sb = new StringBuilder("Usage: ").append(command);
            for (Option option : grouping.options())
            {
                sb.append(" ");
                sb.append(option.shortDisplay());
            }
            out.println(sb.toString());
        }
        out.println();
        final Set<Option> printed = new HashSet<>();
        for (GroupedOptions grouping : groupings)
        {
            for (Option option : grouping.options())
            {
                if (printed.add(option))
                {
                    if (option.longDisplay() != null)
                    {
                        out.println("  " + option.longDisplay());
                        for (String row : option.multiLineDisplay())
                            out.println("      " + row);
                    }
                }
            }
        }
    }

    public String getOptionAsString()
    {
        StringBuilder sb = new StringBuilder();
        for (Option option : options())
        {
            sb.append(option.getOptionAsString());
            sb.append("; ");
        }
        return sb.toString();
    }


    public static List<? extends Option> merge(List<? extends Option> ... optionss)
    {
        ImmutableList.Builder<Option> builder = ImmutableList.builder();
        for (List<? extends Option> options : optionss)
            for (Option option : options)
                if (option instanceof OptionSimple && ((OptionSimple) option).isRequired())
                    builder.add(option);
        for (List<? extends Option> options : optionss)
            for (Option option : options)
                if (!(option instanceof OptionSimple && ((OptionSimple) option).isRequired()))
                    builder.add(option);
        return builder.build();
    }

    static String formatLong(String longDisplay, String description)
    {
        return String.format("%-40s %s", longDisplay, description);
    }

    static String formatMultiLine(String longDisplay, String description)
    {
        return String.format("%-36s %s", longDisplay, description);
    }

}
