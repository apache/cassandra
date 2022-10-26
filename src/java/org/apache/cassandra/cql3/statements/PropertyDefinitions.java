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

import java.util.*;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.SyntaxException;
import org.mortbay.io.nio.SelectorManager;

public class PropertyDefinitions
{
    private static final Pattern PATTERN_POSITIVE = Pattern.compile("(1|true|yes)");
    
    protected static final Logger logger = LoggerFactory.getLogger(PropertyDefinitions.class);

    protected final Map<String, Object> setProperties = new HashMap<String, Object>();

    protected final Map<String, Object> updateProperties = new HashMap<String, Object>();

    public void addProperty(String name, String value) throws SyntaxException
    {
        if (hasProperty(name))
            throw new SyntaxException(String.format("Multiple definition for property '%s'", name));

        setProperties.put(name, value);
    }

    public void addProperty(String name, Map<String, String> value) throws SyntaxException
    {
        if (hasProperty(name))
            throw new SyntaxException(String.format("Multiple definition for property '%s'", name));

        setProperties.put(name, value);
    }

    public void updateProperty(String name, Map<String, String> value) throws SyntaxException
    {
        if (hasProperty(name))
            throw new SyntaxException(String.format("Multiple definition for property '%s'", name));

        updateProperties.put(name, value);
    }

    public void validate(Set<String> keywords, Set<String> obsolete) throws SyntaxException
    {
        for (String name : setProperties.keySet())
        {
            validateProperty(keywords, name, obsolete);
        }

        for (String name : updateProperties.keySet())
        {
            validateProperty(keywords, name, obsolete);
        }
    }

    private void validateProperty(Set<String> keywords, String name, Set<String> obsolete)
    {
        if (keywords.contains(name))
            return;

        if (obsolete.contains(name))
            logger.warn("Ignoring obsolete property {}", name);
        else
            throw new SyntaxException(String.format("Unknown property '%s'", name));
    }

    /**
     * Returns the name of all the properties that are updated by this object.
     */
    public Set<String> updatedProperties()
    {
        return setProperties.keySet();
    } // TODO merge update + props

    public void removeProperty(String name)
    {
        setProperties.remove(name);
    } // TODO update

    protected String getSimple(String name) throws SyntaxException
    {
        Object val = setProperties.get(name);
        if (val == null)
            return null;
        if (!(val instanceof String))
            throw new SyntaxException(String.format("Invalid value for property '%s'. It should be a string", name));
        return (String)val;
    }

    protected Map<String, String> getMap(String name) throws SyntaxException
    {
        if (!hasProperty(name))
            return null;

        Object val;
        if (setProperties.containsKey(name))
            val = setProperties.get(name);
        else
            val = updateProperties.get(name);

        if (!(val instanceof Map))
            throw new SyntaxException(String.format("Invalid value for property '%s'. It should be a map.", name));

        return (Map<String, String>) val;
    }

    public Boolean isEmpty()
    {
        return setProperties.isEmpty() && updateProperties.isEmpty();
    }

    public Boolean hasProperty(String name)
    {
        return setProperties.containsKey(name) ||  updateProperties.containsKey(name);
    }

    public String getString(String key, String defaultValue) throws SyntaxException
    {
        String value = getSimple(key);
        return value != null ? value : defaultValue;
    }

    // Return a property value, typed as a Boolean
    public Boolean getBoolean(String key, Boolean defaultValue) throws SyntaxException
    {
        String value = getSimple(key);
        return (value == null) ? defaultValue : PATTERN_POSITIVE.matcher(value.toLowerCase()).matches();
    }

    // Return a property value, typed as a double
    public double getDouble(String key, double defaultValue) throws SyntaxException
    {
        String value = getSimple(key);
        if (value == null)
        {
            return defaultValue;
        }
        else
        {
            try
            {
                return Double.parseDouble(value);
            }
            catch (NumberFormatException e)
            {
                throw new SyntaxException(String.format("Invalid double value %s for '%s'", value, key));
            }
        }
    }

    // Return a property value, typed as an Integer
    public Integer getInt(String key, Integer defaultValue) throws SyntaxException
    {
        String value = getSimple(key);
        return toInt(key, value, defaultValue);
    }

    public static Integer toInt(String key, String value, Integer defaultValue) throws SyntaxException
    {
        if (value == null)
        {
            return defaultValue;
        }
        else
        {
            try
            {
                return Integer.valueOf(value);
            }
            catch (NumberFormatException e)
            {
                throw new SyntaxException(String.format("Invalid integer value %s for '%s'", value, key));
            }
        }
    }
}
