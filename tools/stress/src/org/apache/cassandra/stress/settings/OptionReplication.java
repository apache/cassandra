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


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.locator.AbstractReplicationStrategy;

/**
 * For specifying replication options
 */
class OptionReplication extends Option
{

    private static final Pattern FULL = Pattern.compile("replication\\((.*)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern OPTION = Pattern.compile("([^,=]+)=([^,]+)", Pattern.CASE_INSENSITIVE);

    private String strategy = "org.apache.cassandra.locator.SimpleStrategy";
    private Map<String, String> options = new HashMap<>();

    public String getStrategy()
    {
        return strategy;
    }

    public Map<String, String> getOptions()
    {
        if (!options.containsKey("replication_factor") && strategy.endsWith("SimpleStrategy"))
            options.put("replication_factor", "1");
        return options;
    }


    @Override
    public boolean accept(String param)
    {
        Matcher m = FULL.matcher(param);
        if (!m.matches())
            return false;
        String args = m.group(1);
        m = OPTION.matcher(args);
        int last = -1;
        while (m.find())
        {
            if (m.start() != last + 1)
                throw new IllegalArgumentException("Invalid replication specification: " + param);
            last = m.end();
            String key = m.group(1).toLowerCase();
            sw: switch(key)
            {
                case "factor":
                    try
                    {
                        Integer.parseInt(m.group(2));
                    } catch (NumberFormatException e)
                    {
                        throw new IllegalArgumentException("Invalid replication factor: " + param);
                    }
                    options.put("replication_factor", m.group(2));
                    break;
                case "strategy":
                    for (String name : new String[] { m.group(2), "org.apache.cassandra.locator." + m.group(2) })
                    {
                        try
                        {
                            Class<?> clazz = Class.forName(name);
                            if (!AbstractReplicationStrategy.class.isAssignableFrom(clazz))
                                throw new RuntimeException();
                            strategy = name;
                            break sw;
                        } catch (Exception _)
                        {
                        }
                    }
                    throw new IllegalArgumentException("Invalid replication strategy: " + param);
                default:

            }
        }
        return true;
    }

    @Override
    public boolean happy()
    {
        return true;
    }

    @Override
    public String shortDisplay()
    {
        return "replication(?)";
    }

    @Override
    public String longDisplay()
    {
        return "replication(factor=?,strategy=?,<option1>=?,...)";
    }

    @Override
    public List<String> multiLineDisplay()
    {
        return Arrays.asList(
                GroupedOptions.formatMultiLine("factor=?","The replication factor to use (default 1)"),
                GroupedOptions.formatMultiLine("strategy=?","The replication strategy to use (default SimpleStrategy)"),
                GroupedOptions.formatMultiLine("option=?","Arbitrary replication strategy options")
        );
    }

}
