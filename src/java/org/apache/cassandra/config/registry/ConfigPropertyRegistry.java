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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Mutable;
import org.apache.cassandra.config.Properties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.yaml.snakeyaml.introspector.Property;

import static org.apache.commons.lang3.ClassUtils.primitiveToWrapper;


/**
 * This is a simple configuration property registry that stores all the {@link Config} settings.
 * It is a singleton and can be accessed via {@link #instance} field.
 */
public class ConfigPropertyRegistry implements PropertyRegistry
{
    public static final ConfigPropertyRegistry instance = new ConfigPropertyRegistry();
    private static final Logger logger = LoggerFactory.getLogger(ConfigPropertyRegistry.class);
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Map<String, ConfigurationProperty> properties;
    private final Map<PropertyChangeListener.ChangeType, PropertyChangeListenerList> propertyChangeListeners = new EnumMap<>(PropertyChangeListener.ChangeType.class);
    private final Map<String, List<PropertyChangeListenerWrapper<?>>> propertyConstraints = new HashMap<>();

    public ConfigPropertyRegistry()
    {
        this(DatabaseDescriptor::getRawConfig);
    }

    @VisibleForTesting
    public ConfigPropertyRegistry(Supplier<Config> config)
    {
        properties = ImmutableMap.copyOf(Properties.defaultLoader()
                                                   .flatten(Config.class)
                                                   .entrySet()
                                                   .stream()
                                                   .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), new ConfigurationProperty(config, e.getValue())))
                                                   .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        for (PropertyChangeListener.ChangeType type : PropertyChangeListener.ChangeType.values())
            propertyChangeListeners.put(type, new PropertyChangeListenerList());
    }

    /**
     * Setter for the property with the given name. Can accept {@code null} value.
     * @param name the name of the property.
     * @param value the value to set.
     */
    @Override public void set(String name, Object value)
    {
        ConfigurationProperty property = properties.get(name);
        validatePropertyExists(property, name);

        Object oldValue = get(name);
        List<PropertyChangeListenerWrapper<?>> constraints = propertyConstraints.get(name);
        if (constraints != null)
        {
            for (PropertyChangeListenerWrapper<?> constraint : constraints)
                constraint.onChange(name, oldValue, value);
        }

        rwLock.writeLock().lock();
        try
        {
            propertyChangeListeners.get(PropertyChangeListener.ChangeType.BEFORE).fire(name, oldValue, value);
            property.set(value);
            propertyChangeListeners.get(PropertyChangeListener.ChangeType.AFTER).fire(name, oldValue, value);
            // This potentially may expose the values that are not safe to see in logs on production.
            logger.info("Property '{}' updated from '{}' to '{}'.", name, oldValue, value);
        }
        catch (Exception e)
        {
            throw new ConfigurationException(String.format("Error updating property with name '%s', cause: %s", name, e.getMessage()), e);
        }
        finally
        {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * @param name the property name to get.
     * @return The value of the property with the given name.
     */
    @Override public <T> T get(String name)
    {
        rwLock.readLock().lock();
        try
        {
            validatePropertyExists(properties.get(name), name);
            return (T) properties.get(name).get();
        }
        finally
        {
            rwLock.readLock().unlock();
        }
    }

    /**
     * @param name the property name to check.
     * @return {@code true} if the property with the given name is available, {@code false} otherwise.
     */
    @Override public boolean contains(String name)
    {
        return properties.containsKey(name);
    }

    /**
     * Returns a set of all the property names.
     * @return set of all the property names.
     */
    @Override public Iterable<String> keys()
    {
        return properties.keySet();
    }

    /**
     * @param name The property name to get the type for.
     * @return Property type for the property with the given name.
     */
    @Override public Class<?> type(String name)
    {
        validatePropertyExists(properties.get(name), name);
        return properties.get(name).getType();
    }

    /**
     * @return The number of properties.
     */
    @Override public int size()
    {
        return properties.size();
    }

    /**
     * @param name The property name to get the type for.
     * @return Property type for the property with the given name.
     */
    public boolean isWritable(String name)
    {
        validatePropertyExists(properties.get(name), name);
        return properties.get(name).isWritable();
    }

    /**
     * Adds a listener for the property with the given name.
     * @param name property name to listen to.
     * @param listener listener to add.
     * @param <T> type of the property.
     */
    public <T> void addPropertyChangeListener(String name, PropertyChangeListener.ChangeType type, PropertyChangeListener<T> listener, Class<T> listenerType)
    {
        validatePropertyExists(properties.get(name), name);
        validatePropertyType(properties.get(name), name, listenerType);
        propertyChangeListeners.get(type).add(name, new PropertyChangeListenerWrapper<>(listener, listenerType));
    }

    public <T> void addPropertyConstraint(String name, Consumer<T> constraint, Class<T> constraintType)
    {
        validatePropertyExists(properties.get(name), name);
        validatePropertyType(properties.get(name), name, constraintType);
        propertyConstraints.computeIfAbsent(name, k -> new ArrayList<>())
                           .add(new PropertyChangeListenerWrapper<>(new PropertyConstraint<>(constraint), constraintType));
    }

    private static void validatePropertyExists(ConfigurationProperty property, String name)
    {
        if (property == null)
            throw new ConfigurationException(String.format("Property with name '%s' is not availabe.", name));
    }

    private static void validatePropertyType(ConfigurationProperty property, String name, Class<?> targetType)
    {
        if (!property.getType().equals(targetType))
            throw new ConfigurationException(String.format("Property with name '%s' expects type '%s', but got '%s'.", name, property.getType(), targetType));
    }

    private static class PropertyConstraint<T> implements PropertyChangeListener<T>
    {
        private final Consumer<T> constraint;

        public PropertyConstraint(Consumer<T> constraint)
        {
            this.constraint = constraint;
        }

        @Override
        public void onChange(String name, T oldValue, T newValue)
        {
            try
            {
                constraint.accept(newValue);
            }
            catch (Exception e)
            {
                throw new IllegalStateException(String.format("Failed constraint check. Unable to set property with name '%s' from '%s' to '%s'.",
                                                              name, oldValue, newValue), e);
            }
        }
    }

    private static class PropertyChangeListenerWrapper<T> implements PropertyChangeListener<T>
    {
        private final PropertyChangeListener<T> listener;
        private final Class<T> type;

        public PropertyChangeListenerWrapper(PropertyChangeListener<T> listener, Class<T> type)
        {
            this.listener = listener;
            this.type = type;
        }

        /** {@inheritDoc} */
        @Override
        public void onChange(String name, Object oldValue, Object newValue)
        {
            try
            {
                T castedOldValue = primitiveToWrapperType(type).cast(oldValue);
                T castedNewValue = primitiveToWrapperType(type).cast(newValue);
                listener.onChange(name, castedOldValue, castedNewValue);
            }
            catch (ClassCastException e)
            {
                throw new ConfigurationException(String.format("Property with name '%s' expects type '%s', but got '%s'.",
                                                               name, type, oldValue.getClass()), e);
            }
        }

        private static <T> Class<T> primitiveToWrapperType(Class<T> type)
        {
            return type.isPrimitive() ? (Class<T>) primitiveToWrapper(type) : type;
        }
    }

    /**
     * A simple holder of liseners of a configuration change for a particular type.
     */
    private static class PropertyChangeListenerList
    {
        private final Map<String, List<PropertyChangeListenerWrapper<?>>> listeners = new HashMap<>();

        public void add(String name, PropertyChangeListenerWrapper<?> listener)
        {
            listeners.computeIfAbsent(name, k -> new ArrayList<>()).add(listener);
        }

        public void fire(String name, Object oldValue, Object newValue)
        {
            if (listeners.get(name) == null)
                return;

            for (PropertyChangeListenerWrapper<?> listener : listeners.get(name))
                listener.onChange(name, oldValue, newValue);
        }
    }

    private static class ConfigurationProperty
    {
        private final Supplier<Config> configSupplier;
        private final Property accessor;

        public ConfigurationProperty(Supplier<Config> configSupplier, Property accessor)
        {
            this.configSupplier = configSupplier;
            this.accessor = accessor;
        }

        public Class<?> getType()
        {
            return accessor.getType();
        }

        public boolean isWritable()
        {
            return accessor.getAnnotation(Mutable.class) != null;
        }

        public void set(Object value)
        {
            if (!isWritable())
                throw new ConfigurationException(String.format("Property with name '%s' must be writable.", accessor.getName()));

            try
            {
                accessor.set(configSupplier.get(), value);
            }
            catch (Exception e)
            {
                throw new ConfigurationException(String.format("Error updating property with name '%s', cause: %s", accessor.getName(), e.getMessage()), e);
            }
        }

        public Object get()
        {
            try
            {
                return accessor.get(configSupplier.get());
            }
            catch (Exception e)
            {
                throw new ConfigurationException(String.format("Error getting with name '%s', cause: %s", accessor.getName(), e.getMessage()), e);
            }
        }
    }
}
