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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * For specifying multiple grouped sub-options in the form: group(arg1=,arg2,arg3) etc.
 */
abstract class OptionMulti extends Option
{

    private static final Pattern ARGS = Pattern.compile("([^,]+)", Pattern.CASE_INSENSITIVE);

    private final class Delegate extends GroupedOptions
    {
        @Override
        public List<? extends Option> options()
        {
            if (collectAsMap == null)
                return OptionMulti.this.options();

            List<Option> options = new ArrayList<>(OptionMulti.this.options());
            options.add(collectAsMap);
            return options;
        }
    }

    protected abstract List<? extends Option> options();

    public Map<String, String> extraOptions()
    {
        return collectAsMap == null ? new HashMap<String, String>() : collectAsMap.options;
    }

    private final String name;
    private final Pattern pattern;
    private final String description;
    private final Delegate delegate = new Delegate();
    private final CollectAsMap collectAsMap;

    public OptionMulti(String name, String description, boolean collectExtraOptionsInMap)
    {
        this.name = name;
        pattern = Pattern.compile(name + "\\((.*)\\)", Pattern.CASE_INSENSITIVE);
        this.description = description;
        this.collectAsMap = collectExtraOptionsInMap ? new CollectAsMap() : null;
    }

    @Override
    public boolean accept(String param)
    {
        Matcher m = pattern.matcher(param);
        if (!m.matches())
            return false;
        m = ARGS.matcher(m.group(1));
        int last = -1;
        while (m.find())
        {
            if (m.start() != last + 1)
                throw new IllegalArgumentException("Invalid " + name + " specification: " + param);
            last = m.end();
            if (!delegate.accept(m.group()))
            {

                throw new IllegalArgumentException("Invalid " + name + " specification: " + m.group());
            }
        }
        return true;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append("(");
        for (Option option : delegate.options())
        {
            sb.append(option);
            sb.append(",");
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String shortDisplay()
    {
        return (happy() ? "[" : "") + name + "(?)" + (happy() ? "]" : "");
    }
    public String getOptionAsString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(name).append(": ");
        sb.append(delegate.getOptionAsString());
        sb.append(";");
        if (collectAsMap != null)
        {
            sb.append("[");
            sb.append(collectAsMap.getOptionAsString());
            sb.append("];");
        }
        return sb.toString();
    }


    @Override
    public String longDisplay()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append("(");
        for (Option opt : delegate.options())
        {
            sb.append(opt.shortDisplay());
        }
        sb.append("): ");
        sb.append(description);
        return sb.toString();
    }

    @Override
    public List<String> multiLineDisplay()
    {
        final List<String> r = new ArrayList<>();
        for (Option option : options())
            r.add(option.longDisplay());
        return r;
    }

    @Override
    boolean happy()
    {
        return delegate.happy();
    }

    private static final class CollectAsMap extends Option
    {

        static final String description = "Extra options";
        Map<String, String> options = new LinkedHashMap<>();

        boolean accept(String param)
        {
            String[] args = param.split("=");
            if (args.length == 2 && args[1].length() > 0 && args[0].length() > 0)
            {
                if (options.put(args[0], args[1]) != null)
                    throw new IllegalArgumentException(args[0] + " set twice");
                return true;
            }
            return false;
        }

        boolean happy()
        {
            return true;
        }

        String shortDisplay()
        {
            return "[<option 1..N>=?]";
        }

        public String getOptionAsString()
        {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> entry : options.entrySet())
            {
                sb.append(entry.getKey()).append("=").append(entry.getValue()).append(",");
            }
            return sb.toString();
        }


        String longDisplay()
        {
            return GroupedOptions.formatLong(shortDisplay(), description);
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
            return !options.isEmpty();
        }
    }

    List<Option> optionsSetByUser()
    {
        List<Option> r = new ArrayList<>();
        for (Option option : delegate.options())
            if (option.setByUser())
                r.add(option);
        return r;
    }

    List<Option> defaultOptions()
    {
        List<Option> r = new ArrayList<>();
        for (Option option : delegate.options())
            if (!option.setByUser() && option.present())
                r.add(option);
        return r;
    }

    boolean setByUser()
    {
        for (Option option : delegate.options())
            if (option.setByUser())
                return true;
        return false;
    }

    boolean present()
    {
        for (Option option : delegate.options())
            if (option.present())
                return true;
        return false;
    }

}
