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
            return OptionMulti.this.options();
        }
    }

    protected abstract List<? extends Option> options();

    private final String name;
    private final Pattern pattern;
    private final String description;
    private final Delegate delegate = new Delegate();
    public OptionMulti(String name, String description)
    {
        this.name = name;
        pattern = Pattern.compile(name + "\\((.*)\\)", Pattern.CASE_INSENSITIVE);
        this.description = description;
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
                throw new IllegalArgumentException("Invalid " + name + " specification: " + m.group());
        }
        return true;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append("(");
        for (Option option : options())
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
        return name + "(?)";
    }

    @Override
    public String longDisplay()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append("(");
        for (Option opt : options())
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

}
