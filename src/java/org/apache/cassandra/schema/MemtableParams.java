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
package org.apache.cassandra.schema;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InheritingClass;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.SkipListMemtableFactory;
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Memtable types and options are specified with these parameters. Memtable classes must either contain a static
 * {@code FACTORY} field (if they take no arguments other than class), or implement a
 * {@code factory(Map<String, String>)} method.
 *
 * The latter should consume any further options (using {@code map.remove}).
 *
 * See Memtable_API.md for further details on the configuration and usage of memtable implementations.
  */
public final class MemtableParams
{
    private final Memtable.Factory factory;
    private final String configurationKey;

    private MemtableParams(Memtable.Factory factory, String configurationKey)
    {
        this.configurationKey = configurationKey;
        this.factory = factory;
    }

    public String configurationKey()
    {
        return configurationKey;
    }

    public Memtable.Factory factory()
    {
        return factory;
    }

    @Override
    public String toString()
    {
        return configurationKey;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof MemtableParams))
            return false;

        MemtableParams c = (MemtableParams) o;

        return Objects.equal(configurationKey, c.configurationKey);
    }

    @Override
    public int hashCode()
    {
        return configurationKey.hashCode();
    }

    private static final String DEFAULT_CONFIGURATION_KEY = "default";
    private static final Memtable.Factory DEFAULT_MEMTABLE_FACTORY = SkipListMemtableFactory.INSTANCE;
    private static final ParameterizedClass DEFAULT_CONFIGURATION = SkipListMemtableFactory.CONFIGURATION;
    private static final Map<String, ParameterizedClass>
        CONFIGURATION_DEFINITIONS = expandDefinitions(DatabaseDescriptor.getMemtableConfigurations());
    private static final Map<String, MemtableParams> CONFIGURATIONS = new HashMap<>();
    public static final MemtableParams DEFAULT = get(null);

    public static Set<String> knownDefinitions()
    {
        return CONFIGURATION_DEFINITIONS.keySet();
    }

    public static MemtableParams get(String key)
    {
        if (key == null)
            key = DEFAULT_CONFIGURATION_KEY;

        synchronized (CONFIGURATIONS)
        {
            return CONFIGURATIONS.computeIfAbsent(key, MemtableParams::parseConfiguration);
        }
    }

    public static MemtableParams getWithFallback(String key)
    {
        try
        {
            return get(key);
        }
        catch (ConfigurationException e)
        {
            LoggerFactory.getLogger(MemtableParams.class).error("Invalid memtable configuration \"" + key + "\" in schema. " +
                                                                "Falling back to default to avoid schema mismatch.\n" +
                                                                "Please ensure the correct definition is given in cassandra.yaml.",
                                                                e);
            return new MemtableParams(DEFAULT.factory(), key);
        }
    }

    @VisibleForTesting
    static Map<String, ParameterizedClass> expandDefinitions(Map<String, InheritingClass> memtableConfigurations)
    {
        if (memtableConfigurations == null)
            return ImmutableMap.of(DEFAULT_CONFIGURATION_KEY, DEFAULT_CONFIGURATION);

        LinkedHashMap<String, ParameterizedClass> configs = new LinkedHashMap<>(memtableConfigurations.size() + 1);

        // If default is not overridden, add an entry first so that other configurations can inherit from it.
        // If it is, process it in its point of definition, so that the default can inherit from another configuration.
        if (!memtableConfigurations.containsKey(DEFAULT_CONFIGURATION_KEY))
            configs.put(DEFAULT_CONFIGURATION_KEY, DEFAULT_CONFIGURATION);

        for (Map.Entry<String, InheritingClass> en : memtableConfigurations.entrySet())
            configs.put(en.getKey(), en.getValue().resolve(configs));

        return ImmutableMap.copyOf(configs);
    }

    private static MemtableParams parseConfiguration(String configurationKey)
    {
        ParameterizedClass definition = CONFIGURATION_DEFINITIONS.get(configurationKey);

        if (definition == null)
            throw new ConfigurationException("Memtable configuration \"" + configurationKey + "\" not found.");
        return new MemtableParams(getMemtableFactory(definition), configurationKey);
    }


    private static Memtable.Factory getMemtableFactory(ParameterizedClass options)
    {
        // Special-case this so that we don't initialize memtable class for tests that need to delay that.
        if (options == DEFAULT_CONFIGURATION)
            return DEFAULT_MEMTABLE_FACTORY;

        String className = options.class_name;
        if (className == null || className.isEmpty())
            throw new ConfigurationException("The 'class_name' option must be specified.");

        className = className.contains(".") ? className : "org.apache.cassandra.db.memtable." + className;
        try
        {
            Memtable.Factory factory;
            Class<?> clazz = Class.forName(className);
            final Map<String, String> parametersCopy = options.parameters != null
                                                       ? new HashMap<>(options.parameters)
                                                       : new HashMap<>();
            try
            {
                Method factoryMethod = clazz.getDeclaredMethod("factory", Map.class);
                factory = (Memtable.Factory) factoryMethod.invoke(null, parametersCopy);
            }
            catch (NoSuchMethodException e)
            {
                // continue with FACTORY field
                Field factoryField = clazz.getDeclaredField("FACTORY");
                factory = (Memtable.Factory) factoryField.get(null);
            }
            if (!parametersCopy.isEmpty())
                throw new ConfigurationException("Memtable class " + className + " does not accept any futher parameters, but " +
                                                 parametersCopy + " were given.");
            return factory;
        }
        catch (NoSuchFieldException | ClassNotFoundException | IllegalAccessException | InvocationTargetException | ClassCastException e)
        {
            if (e.getCause() instanceof ConfigurationException)
                throw (ConfigurationException) e.getCause();
            throw new ConfigurationException("Could not create memtable factory for class " + options, e);
        }
    }
}
