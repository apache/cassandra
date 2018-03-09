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
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import com.google.common.base.Function;

/**
 * For parsing a simple (sub)option for a command/major option
 */
class OptionSimple extends Option implements Serializable
{

    final String displayPrefix;
    private final Pattern matchPrefix;
    private final String defaultValue;
    private final Function<String, String> valueAdapter;
    private final String description;
    private final boolean required;
    private String value;

    private static final class ValueMatcher implements Function<String, String>, Serializable
    {
        final Pattern pattern;
        private ValueMatcher(Pattern pattern)
        {
            this.pattern = pattern;
        }
        public String apply(String s)
        {
            if (!pattern.matcher(s).matches())
                throw new IllegalArgumentException("Invalid value " + s + "; must match pattern " + pattern);
            return s;
        }
    }

    public OptionSimple(String prefix, String valuePattern, String defaultValue, String description, boolean required)
    {
        this(prefix, Pattern.compile(Pattern.quote(prefix), Pattern.CASE_INSENSITIVE),
             Pattern.compile(valuePattern, Pattern.CASE_INSENSITIVE), defaultValue, description, required);
    }

    public OptionSimple(String prefix, Function<String, String> valueAdapter, String defaultValue, String description, boolean required)
    {
        this(prefix, Pattern.compile(Pattern.quote(prefix), Pattern.CASE_INSENSITIVE), valueAdapter, defaultValue, description, required);
    }

    public OptionSimple(String displayPrefix, Pattern matchPrefix, Pattern valuePattern, String defaultValue, String description, boolean required)
    {
        this(displayPrefix, matchPrefix, new ValueMatcher(valuePattern), defaultValue, description, required);
    }

    public OptionSimple(String displayPrefix, Pattern matchPrefix, Function<String, String> valueAdapter, String defaultValue, String description, boolean required)
    {
        this.displayPrefix = displayPrefix;
        this.matchPrefix = matchPrefix;
        this.valueAdapter = valueAdapter;
        this.defaultValue = defaultValue;
        this.description = description;
        this.required = required;
    }

    public boolean setByUser()
    {
        return value != null;
    }

    public boolean isRequired()
    {
        return required;
    }

    public boolean present()
    {
        return value != null || defaultValue != null;
    }

    public String value()
    {
        return value != null ? value : defaultValue;
    }

    public boolean accept(String param)
    {
        if (matchPrefix.matcher(param).lookingAt())
        {
            if (value != null)
                throw new IllegalArgumentException("Suboption " + displayPrefix + " has been specified more than once");
            String v = param.substring(displayPrefix.length());
            value = valueAdapter.apply(v);
            assert value != null;
            return true;
        }
        return false;
    }

    @Override
    public boolean happy()
    {
        return !required || value != null;
    }

    public String shortDisplay()
    {
        StringBuilder sb = new StringBuilder();
        if (!required)
            sb.append("[");
        sb.append(displayPrefix);
        if (displayPrefix.endsWith("="))
            sb.append("?");
        if (displayPrefix.endsWith("<"))
            sb.append("?");
        if (displayPrefix.endsWith(">"))
            sb.append("?");
        if (!required)
            sb.append("]");
        return sb.toString();
    }

    public String longDisplay()
    {
        if (description.equals("") && defaultValue == null
            && (valueAdapter instanceof ValueMatcher && ((ValueMatcher) valueAdapter).pattern.pattern().equals("")))
            return null;
        StringBuilder sb = new StringBuilder();
        sb.append(displayPrefix);
        if (displayPrefix.endsWith("="))
            sb.append("?");
        if (displayPrefix.endsWith("<"))
            sb.append("?");
        if (displayPrefix.endsWith(">"))
            sb.append("?");
        if (defaultValue != null)
        {
            sb.append(" (default=");
            sb.append(defaultValue);
            sb.append(")");
        }
        return GroupedOptions.formatLong(sb.toString(), description);
    }

    public String getOptionAsString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(displayPrefix);

        if (!(displayPrefix.endsWith("=") || displayPrefix.endsWith("<") || displayPrefix.endsWith(">")))
        {
            sb.append(setByUser() ? ":*set*" : ":*not set*");
        }else{
            sb.append(value == null ? defaultValue : value);
        }
        return sb.toString();
    }

    public List<String> multiLineDisplay()
    {
        return Collections.emptyList();
    }

    public int hashCode()
    {
        return displayPrefix.hashCode();
    }

    @Override
    public boolean equals(Object that)
    {
        return that instanceof OptionSimple && ((OptionSimple) that).displayPrefix.equals(this.displayPrefix);
    }

}
