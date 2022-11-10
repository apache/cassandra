/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.statements;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.SyntaxException;

import static java.lang.String.format;

public class PropertyDefinitions
{
    private static final Pattern POSITIVE_PATTERN = Pattern.compile("(1|true|yes)");
    private static final Pattern NEGATIVE_PATTERN = Pattern.compile("(0|false|no)");
    
    protected static final Logger logger = LoggerFactory.getLogger(PropertyDefinitions.class);

    protected final Map<String, Object> properties = new HashMap<>();

    public void addProperty(String name, String value) throws SyntaxException
    {
        if (properties.put(name, value) != null)
            throw new SyntaxException(format("Multiple definitions for property '%s'", name));
    }

    public void addProperty(String name, Map<String, String> value) throws SyntaxException
    {
        if (properties.put(name, value) != null)
            throw new SyntaxException(format("Multiple definitions for property '%s'", name));
    }

    public void validate(Set<String> keywords, Set<String> obsolete) throws SyntaxException
    {
        for (String name : properties.keySet())
        {
            if (keywords.contains(name))
                continue;

            if (obsolete.contains(name))
                logger.warn("Ignoring obsolete property {}", name);
            else
                throw new SyntaxException(format("Unknown property '%s'", name));
        }
    }

    /**
     * Returns the name of all the properties that are updated by this object.
     */
    public Set<String> updatedProperties()
    {
        return properties.keySet();
    }

    public void removeProperty(String name)
    {
        properties.remove(name);
    }

    public boolean hasProperty(String name)
    {
        return properties.containsKey(name);
    }

    protected String getString(String name) throws SyntaxException
    {
        Object val = properties.get(name);
        if (val == null)
            return null;
        if (!(val instanceof String))
            throw new SyntaxException(format("Invalid value for property '%s'. It should be a string", name));
        return (String)val;
    }

    protected Map<String, String> getMap(String name) throws SyntaxException
    {
        Object val = properties.get(name);
        if (val == null)
            return null;
        if (!(val instanceof Map))
            throw new SyntaxException(format("Invalid value for property '%s'. It should be a map.", name));
        return (Map<String, String>)val;
    }

    public boolean getBoolean(String key, boolean defaultValue) throws SyntaxException
    {
        String value = getString(key);
        return value != null ? parseBoolean(key, value) : defaultValue;
    }

    public static boolean parseBoolean(String key, String value) throws SyntaxException
    {
        if (null == value)
            throw new IllegalArgumentException("value argument can't be null");

        String lowerCasedValue = value.toLowerCase();

        if (POSITIVE_PATTERN.matcher(lowerCasedValue).matches())
            return true;
        else if (NEGATIVE_PATTERN.matcher(lowerCasedValue).matches())
            return false;

        throw new SyntaxException(format("Invalid boolean value %s for '%s'. " +
                                         "Positive values can be '1', 'true' or 'yes'. " +
                                         "Negative values can be '0', 'false' or 'no'.",
                                         value, key));
    }

    public int getInt(String key, int defaultValue) throws SyntaxException
    {
        String value = getString(key);
        return value != null ? parseInt(key, value) : defaultValue;
    }

    public static int parseInt(String key, String value) throws SyntaxException
    {
        if (null == value)
            throw new IllegalArgumentException("value argument can't be null");

        try
        {
            return Integer.parseInt(value);
        }
        catch (NumberFormatException e)
        {
            throw new SyntaxException(format("Invalid integer value %s for '%s'", value, key));
        }
    }

    public double getDouble(String key, double defaultValue) throws SyntaxException
    {
        String value = getString(key);
        return value != null ? parseDouble(key, value) : defaultValue;
    }

    public static double parseDouble(String key, String value) throws SyntaxException
    {
        if (null == value)
            throw new IllegalArgumentException("value argument can't be null");

        try
        {
            return Double.parseDouble(value);
        }
        catch (NumberFormatException e)
        {
            throw new SyntaxException(format("Invalid double value %s for '%s'", value, key));
        }
    }
}
