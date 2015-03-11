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


import java.util.*;


public final class OptionAnyProbabilities extends OptionMulti
{
    public OptionAnyProbabilities(String name, String description)
    {
        super(name, description, false);
    }

    final CollectRatios ratios = new CollectRatios();

    private static final class CollectRatios extends Option
    {
        Map<String, Double> options = new LinkedHashMap<>();

        boolean accept(String param)
        {
            String[] args = param.split("=");
            if (args.length == 2 && args[1].length() > 0 && args[0].length() > 0)
            {
                if (options.put(args[0], Double.parseDouble(args[1])) != null)
                    throw new IllegalArgumentException(args[0] + " set twice");
                return true;
            }
            return false;
        }

        boolean happy()
        {
            return !options.isEmpty();
        }

        String shortDisplay()
        {
            return null;
        }

        String longDisplay()
        {
            return null;
        }

        List<String> multiLineDisplay()
        {
            return Collections.emptyList();
        }

        boolean setByUser()
        {
            return !options.isEmpty();
        }

        boolean present()
        {
            return setByUser();
        }
    }


    @Override
    public List<? extends Option> options()
    {
        return Arrays.asList(ratios);
    }

    Map<String, Double> ratios()
    {
        return ratios.options;
    }
}

