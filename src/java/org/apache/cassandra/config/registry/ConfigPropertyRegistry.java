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
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Mutable;
import org.apache.cassandra.config.Properties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.yaml.snakeyaml.introspector.Property;

import static org.apache.commons.lang3.ClassUtils.primitiveToWrapper;


/**
 * This is a simple configuration property registry that stores all the {@link Config} settings, it doesn't
 * take into account any configuration changes that might happen during properties replacement between releases.
 */
public class ConfigPropertyRegistry implements PropertyRegistry
{
    private static final Logger logger = LoggerFactory.getLogger(ConfigPropertyRegistry.class);
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Map<String, ConfigurationProperty> properties;
    private final TypeConverterRegistry typeConverterRegistry = new TypeConverterRegistry();
    private final Map<PropertyChangeListener.ChangeType, PropertyChangeListenerList> propertyChangeListeners = new EnumMap<>(PropertyChangeListener.ChangeType.class);
    private final Map<String, List<PropertyTypeSafeWrapper<?>>> validatorsMap = new HashMap<>();

    public ConfigPropertyRegistry(Supplier<Config> configProvider)
    {
        properties = ImmutableMap.copyOf(Properties.defaultLoader()
                                                   .flatten(Config.class)
                                                   .entrySet()
                                                   .stream()
                                                   .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), new ConfigurationProperty(configProvider, e.getValue())))
                                                   .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        // Initialize the property change listeners.
        for (PropertyChangeListener.ChangeType type : PropertyChangeListener.ChangeType.values())
            propertyChangeListeners.put(type, new PropertyChangeListenerList());
    }

    @Override public void set(String name, @Nullable Object value)
    {
        ConfigurationProperty property = properties.get(name);
        validatePropertyExists(property, name);
        setInternal(property, value);
    }

    /**
     * Setter for the property with the given name. Can accept {@code null} value.
     * @param property The property.
     * @param value The value to set.
     */
    private void setInternal(ConfigurationProperty property, @Nullable Object value)
    {
        rwLock.writeLock().lock();
        try
        {
            Class<?> originalType = property.getType();
            Class<?> sourceType = value == null ? null : value.getClass();
            Object convertedValue = value;
            // Do conversion if the value is not null and the type is not the same as the property type.
            if (value != null && !primitiveToWrapperType(originalType).equals(sourceType))
            {
                TypeConverter<?> converter = typeConverterRegistry.getConverter(sourceType, originalType);
                if (converter == null)
                    throw new ConfigurationException(String.format("No converter found for type '%s'", sourceType.getName()));

                convertedValue = converter.convert(value);
            }
            // Do validation first for converted new value.
            List<PropertyTypeSafeWrapper<?>> validators = validatorsMap.get(property.getName());
            if (validators != null)
            {
                Object oldValue = property.getValue();
                for (PropertyTypeSafeWrapper<?> wrapper : validators)
                    wrapper.fireSafe(property.getName(), oldValue, convertedValue);
            }
            // Do set the value only if the validation passes.
            Object oldValue = property.getValue();
            propertyChangeListeners.get(PropertyChangeListener.ChangeType.BEFORE).fire(property.getName(), oldValue, convertedValue);
            property.setValue(convertedValue);
            propertyChangeListeners.get(PropertyChangeListener.ChangeType.AFTER).fire(property.getName(), oldValue, convertedValue);
            // This potentially may expose the values that are not safe to see in logs on production.
            logger.info("Property '{}' updated from '{}' to '{}'.", property.getName(), oldValue, convertedValue);
        }
        catch (Exception e)
        {
            throw new ConfigurationException(String.format("Error updating property '%s', cause: %s", property, e.getMessage()), e);
        }
        finally
        {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * @param cls Class to cast the property value to.
     * @param name the property name to get.
     * @return The value of the property with the given name.
     */
    public <T> T get(Class<T> cls, String name)
    {
        rwLock.readLock().lock();
        try
        {
            validatePropertyExists(properties.get(name), name);
            Class<?> propertyType = type(name);
            if (cls.equals(propertyType))
                return primitiveToWrapperType(cls).cast(properties.get(name).getValue());
            else if (cls.equals(String.class))
                return cls.cast(typeConverterRegistry.getConverterOrDefault(propertyType, String.class, TypeConverter.DEFAULT).convert(properties.get(name).getValue()));
            else
                throw new ConfigurationException(String.format("Property '%s' is of type '%s' and cannot be cast to '%s'",
                                                               name, type(name).getName(), cls.getName()));
        }
        finally
        {
            rwLock.readLock().unlock();
        }
    }

    @Override public String getString(String name)
    {
        return get(String.class, name);
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
        propertyChangeListeners.get(type).add(name, new PropertyTypeSafeWrapper<>(listener, listenerType));
    }

    public <T> void addPropertyValidator(String name, BiConsumer<T, T> validator, Class<T> type)
    {
        validatePropertyExists(properties.get(name), name);
        validatePropertyType(properties.get(name), name, type);
        validatorsMap.computeIfAbsent(name, k -> new ArrayList<>())
                     .add(new PropertyTypeSafeWrapper<>((prop, oldValue, newValue) -> validator.accept(oldValue, newValue), type));
    }

    private static void validatePropertyExists(ConfigurationProperty property, String name)
    {
        if (property == null)
            throw new ConfigurationException(String.format("Property with name '%s' is not availabe.", name));
    }

    private static void validatePropertyType(ConfigurationProperty property, String name, @Nullable Class<?> targetType)
    {
        if (targetType != null && !property.getType().equals(targetType))
            throw new ConfigurationException(String.format("Property with name '%s' expects type '%s', but got '%s'.", name, property.getType(), targetType));
    }

    private static <T> Class<T> primitiveToWrapperType(Class<T> type)
    {
        return type.isPrimitive() ? (Class<T>) primitiveToWrapper(type) : type;
    }

    private static class PropertyTypeSafeWrapper<T>
    {
        private final PropertyChangeListener<T> listener;
        private final Class<T> type;

        public PropertyTypeSafeWrapper(PropertyChangeListener<T> listener, Class<T> type)
        {
            this.listener = listener;
            this.type = type;
        }

        public void fireSafe(String name, Object oldValue, Object newValue)
        {
            // Casting to the type of the listener is safe because we validate the type of the property on listener's registration.
            listener.onChange(name, primitiveToWrapperType(type).cast(oldValue), primitiveToWrapperType(type).cast(newValue));
        }
    }

    /**
     * A simple holder of liseners of a configuration change for a particular type.
     */
    private static class PropertyChangeListenerList
    {
        private final Map<String, List<PropertyTypeSafeWrapper<?>>> wrappers = new HashMap<>();

        public void add(String name, PropertyTypeSafeWrapper<?> listener)
        {
            wrappers.computeIfAbsent(name, k -> new ArrayList<>()).add(listener);
        }

        public void fire(String name, Object oldValue, Object newValue)
        {
            if (wrappers.get(name) == null)
                return;

            for (PropertyTypeSafeWrapper<?> wrapper : wrappers.get(name))
                wrapper.fireSafe(name, oldValue, newValue);
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

        public String getName()
        {
            return accessor.getName();
        }

        public Class<?> getType()
        {
            return accessor.getType();
        }

        public boolean isWritable()
        {
            return accessor.getAnnotation(Mutable.class) != null;
        }

        public void setValue(Object value)
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

        public Object getValue()
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

        @Override public String toString()
        {
            return "ConfigurationProperty[name=" + getName() + ", " +
                   "type=" + getType() +
                   ", isWritable=" + isWritable() + ']';
        }
    }
}
