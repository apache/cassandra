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

package org.apache.cassandra.config.registry;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Mutable;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.PropertyNotFoundException;
import org.apache.cassandra.utils.Pair;
import org.yaml.snakeyaml.introspector.Property;

import static java.util.Optional.ofNullable;
import static org.apache.cassandra.config.Properties.defaultLoader;
import static org.apache.commons.lang3.ClassUtils.primitiveToWrapper;


/**
 * This is a simple configuration property registry that stores all the {@link Config} settings, it doesn't
 * take into account any configuration changes that might happen during properties replacement between releases.
 */
public class DatabaseConfigurationSource implements ConfigurationSource
{
    private static final Logger logger = LoggerFactory.getLogger(DatabaseConfigurationSource.class);
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final TypeConverterRegistry typeConverterRegistry;
    private final Config source;
    private final Map<String, Property> properties;
    private final List<ConfigurationSourceListener> propertyChangeListeners = new ArrayList<>();
    private final List<ConfigurationHandler> handlers = new ArrayList<>();

    public DatabaseConfigurationSource(Config source)
    {
        this.source = source;
        properties = ImmutableMap.copyOf(defaultLoader()
                                         .flatten(Config.class)
                                         .entrySet()
                                         .stream()
                                         .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        typeConverterRegistry = TypeConverterRegistry.instance;
        // Initialize the configuration handlers.
        addConfigurationHandler(DatabaseDescriptor::applySimpleConfig);
    }

    @Override
    public <T> void set(String name, T value)
    {
        Property property = ofNullable(properties.get(name)).orElseThrow(() -> notFound(name));
        rwLock.writeLock().lock();
        try
        {
            Class<?> originalType = property.getType();
            // Do conversion if the value is not null and the type is not the same as the property type.
            Object convertedValue = ofNullable(value)
                                            .map(Object::getClass)
                                            .map(from -> typeConverterRegistry.get(from, primitiveToWrapper(originalType))
                                                                              .convert(value))
                                            .orElse(null);
            // TODO: do validation first for converted new value
            Config config = SerializationUtils.clone(source);
            for (ConfigurationHandler handler : handlers)
                handler.validate(config, new DatabaseDescriptor.DynamicDatabaseDescriptor(), logger);
            // Do set the value only if the validation passes.
            Object oldValue = property.get(source);
            propertyChangeListeners.forEach(l -> l.listen(name, ConfigurationSourceListener.EventType.BEFORE_CHANGE, oldValue, convertedValue));
            property.set(source, convertedValue);
            propertyChangeListeners.forEach(l -> l.listen(name, ConfigurationSourceListener.EventType.AFTER_CHANGE, oldValue, convertedValue));
            // This potentially may expose the values that are not safe to see in logs on production.
            logger.info("Property '{}' updated from '{}' to '{}'.", property.getName(), oldValue, convertedValue);
        }
        catch (ConfigurationException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new ConfigurationException(String.format("Error updating property '%s'; cause: %s", property.getName(), e.getMessage()), e);
        }
        finally
        {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Configuration property is writable if it has {@link Mutable} annotation.
     * @param name the property name to check.
     * @return {@code true} if the property is writable, {@code false} otherwise.
     */
    public boolean isWritable(String name)
    {
        return ofNullable(properties.get(name))
               .map(p -> p.getAnnotation(Mutable.class))
               .isPresent();
    }

    @Override
    public Class<?> type(String name)
    {
        return ofNullable(properties.get(name)).orElseThrow(() -> notFound(name)).getType();
    }

    @Override
    public Object getRaw(String name)
    {
        rwLock.readLock().lock();
        try
        {
            return ofNullable(properties.get(name))
                   .orElseThrow(() -> notFound(name))
                   .get(source);
        }
        finally
        {
            rwLock.readLock().unlock();
        }
    }

    /**
     * @param cls Class to cast the property value to.
     * @param name the property name to get.
     * @return The value of the property with the given name.
     */
    public <T> T get(Class<T> cls, String name)
    {
        Object value = getRaw(name);
        return value == null ? null : typeConverterRegistry.get(primitiveToWrapper(properties.get(name).getType()), cls)
                                                           .convert(value);
    }

    @Override
    public Iterator<Pair<String, Object>> iterator()
    {
        return Iterators.transform(properties.entrySet().iterator(),
                                   e -> Pair.create(e.getKey(), getRaw(e.getKey())));
    }

    /**
     * Adds a listener for the property with the given name.
     * @param listener listener to add.
     */
    public void addConfigurationChangeListener(ConfigurationSourceListener listener)
    {
        propertyChangeListeners.add(listener);
    }

    public final void addConfigurationHandler(ConfigurationHandler handler)
    {
        this.handlers.add(handler);
    }

    private static PropertyNotFoundException notFound(String name)
    {
        return new PropertyNotFoundException(String.format("Property with name '%s' is not availabe.", name));
    }
}
