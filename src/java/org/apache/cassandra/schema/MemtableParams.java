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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.SkipListMemtableFactory;
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Memtable types and options are specified with these parameters. Memtable classes must either contain a static FACTORY
 * field (if they take no arguments other than class), or implement a factory(Map<String, String>) method.
 *
 * The latter should consume any further options (using map.remove).
 *
 *
 * CQL: {'class' : 'SkipListMemtable'}
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

    private static final String CLASS_OPTION = "class";
    private static final String EXTENDS_OPTION = "extends";
    private static final String DEFAULT_CONFIGURATION_KEY = "default";
    private static final Memtable.Factory DEFAULT_MEMTABLE_FACTORY = SkipListMemtableFactory.INSTANCE;
    private static final Map<String, String> DEFAULT_CONFIGURATION = SkipListMemtableFactory.CONFIGURATION;
    private static final Map<String, Map<String, String>> CONFIGURATION_DEFINITIONS;
    private static final Map<String, MemtableParams> CONFIGURATIONS;
    public static final MemtableParams DEFAULT;

    static {
        CONFIGURATION_DEFINITIONS = expandDefinitions(DatabaseDescriptor.getMemtableConfigurations());
        CONFIGURATIONS = new HashMap<>();
        DEFAULT = get(null);
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

    private static Map<String, Map<String, String>> expandDefinitions(Map<String, Map<String, String>> memtableConfigurations)
    {
        if (memtableConfigurations == null)
            return ImmutableMap.of(DEFAULT_CONFIGURATION_KEY, DEFAULT_CONFIGURATION);

        LinkedHashMap<String, Map<String, String>> configs = new LinkedHashMap<>(memtableConfigurations.size() + 1);
        if (!memtableConfigurations.containsKey(DEFAULT_CONFIGURATION_KEY))
            configs.put(DEFAULT_CONFIGURATION_KEY, DEFAULT_CONFIGURATION);
        for (Map.Entry<String, Map<String, String>> config : memtableConfigurations.entrySet())
        {
            String key = config.getKey();
            Map<String, String> configuration = config.getValue();
            if (configuration.containsKey(EXTENDS_OPTION))
            {
                Map<String, String> parentConfig = configs.get(configuration.get(EXTENDS_OPTION));
                if (parentConfig == null)
                    throw new ConfigurationException("Memtable configuration " + key + " extends undefined " + configuration.get(EXTENDS_OPTION)
                                                     + ". A configuration can only extend one defined earlier or \"default\".");
                if (configuration.size() == 1)  // only extends, i.e. a new name for an existing config
                    configuration = parentConfig;
                else
                {
                    Map<String, String> childConfig = new HashMap<>(parentConfig);
                    childConfig.putAll(configuration);
                    childConfig.remove(EXTENDS_OPTION);
                    configuration = childConfig;
                }
            }
            configs.put(key, ImmutableMap.copyOf(configuration));
        }
        return ImmutableMap.copyOf(configs);
    }

    private static MemtableParams parseConfiguration(String configurationKey)
    {
        Map<String, String> definition = CONFIGURATION_DEFINITIONS.get(configurationKey);

        if (definition == null)
            throw new ConfigurationException("Memtable configuration \"" + configurationKey + "\" not found.");
        return new MemtableParams(getMemtableFactory(definition), configurationKey);
    }


    private static Memtable.Factory getMemtableFactory(Map<String, String> options)
    {
        // Special-case this so that we don't initialize memtable class for tests that need to delay that.
        if (options == DEFAULT_CONFIGURATION)
            return DEFAULT_MEMTABLE_FACTORY;

        Map<String, String> copy = new HashMap<>(options);
        String className = copy.remove(CLASS_OPTION);
        if (className.isEmpty() || className == null)
            throw new ConfigurationException(
            "The 'class' option must not be empty.");

        className = className.contains(".") ? className : "org.apache.cassandra.db.memtable." + className;
        try
        {
            Memtable.Factory factory;
            Class<?> clazz = Class.forName(className);
            try
            {
                Method factoryMethod = clazz.getDeclaredMethod("factory", Map.class);
                factory = (Memtable.Factory) factoryMethod.invoke(null, copy);
            }
            catch (NoSuchMethodException e)
            {
                // continue with FACTORY field
                Field factoryField = clazz.getDeclaredField("FACTORY");
                factory = (Memtable.Factory) factoryField.get(null);
            }
            if (!copy.isEmpty())
                throw new ConfigurationException("Memtable class " + className + " does not accept any futher parameters, but " +
                                                 copy + " were given.");
            return factory;
        }
        catch (NoSuchFieldException | ClassNotFoundException | IllegalAccessException | InvocationTargetException | ClassCastException e)
        {
            if (e.getCause() instanceof ConfigurationException)
                throw (ConfigurationException) e.getCause();
            throw new ConfigurationException("Could not create memtable factory for " + options, e);
        }
    }
}
