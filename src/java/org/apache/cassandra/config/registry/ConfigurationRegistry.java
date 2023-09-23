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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Mutable;
import org.apache.cassandra.config.StringConverters;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.yaml.snakeyaml.introspector.Property;

import static org.apache.cassandra.config.Properties.defaultLoader;
import static org.apache.cassandra.config.registry.PrimitiveUnaryConverter.convertSafe;
import static org.apache.commons.lang3.ClassUtils.primitiveToWrapper;


/**
 * This is a simple configuration property registry that stores all the {@link Config} settings, it doesn't
 * take into account any configuration changes that might happen during properties replacement between releases.
 */
public class ConfigurationRegistry implements Registry
{
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationRegistry.class);
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Supplier<Config> configProvider;
    private final Map<ConfigurationListener.ChangeType, ConfigurationListenerList> propertyChangeListeners = new EnumMap<>(ConfigurationListener.ChangeType.class);
    private final Map<String, List<TypedConstraintAdapter<?>>> constraints = new HashMap<>();
    private volatile boolean initialized;
    private Map<String, PropertyAdapter> properties = Collections.emptyMap();

    public ConfigurationRegistry(Supplier<Config> configProvider)
    {
        this.configProvider = configProvider;
        // Initialize the property change listeners.
        for (ConfigurationListener.ChangeType type : ConfigurationListener.ChangeType.values())
            propertyChangeListeners.put(type, new ConfigurationListenerList());
    }

    private void lazyInit()
    {
        if (initialized)
            return;

        rwLock.writeLock().lock();
        try
        {
            if (initialized)
                return;
            properties = ImmutableMap.copyOf(defaultLoader()
                                             .flatten(Config.class)
                                             .entrySet()
                                             .stream()
                                             .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), new PropertyAdapter(configProvider, e.getValue())))
                                             .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
            Set<String> leftConstraints = new HashSet<>(constraints.keySet());
            leftConstraints.removeAll(properties.keySet());
            if (!leftConstraints.isEmpty())
                throw new ConfigurationException("Constraints are defined for non-existing properties:" + leftConstraints);
            Set<String> leftListeners = propertyChangeListeners.values().stream()
                                                               .map(l -> l.wrappers.keySet())
                                                               .flatMap(Collection::stream)
                                                               .collect(Collectors.toSet());
            leftListeners.removeAll(properties.keySet());
            if (!leftListeners.isEmpty())
                throw new ConfigurationException("Listeners are defined for non-existing properties:" + leftListeners);
            initialized = true;
        }
        finally
        {
            rwLock.writeLock().unlock();
        }
    }

    @Override public void set(String name, @Nullable Object value)
    {
        lazyInit();
        PropertyAdapter property = properties.get(name);
        validatePropertyExists(property, name);
        setInternal(property, value);
    }

    /**
     * Setter for the property with the given name. Can accept {@code null} value.
     * @param property The property.
     * @param value The value to set.
     */
    private void setInternal(PropertyAdapter property, @Nullable Object value)
    {
        rwLock.writeLock().lock();
        try
        {
            Class<?> originalType = property.getType();
            Class<?> sourceType = value == null ? null : value.getClass();
            Object convertedValue = value;
            // Do conversion if the value is not null and the type is not the same as the property type.
            if (sourceType != null && !primitiveToWrapper(originalType).equals(sourceType))
            {
                StringConverters converter;
                if (sourceType.equals(String.class) && (converter = StringConverters.fromType(originalType)) != null)
                    convertedValue = converter.fromString((String) value, originalType);
                else
                    throw new IllegalArgumentException(String.format("No converter found for type '%s'", originalType.getName()));
            }
            // Do validation first for converted new value.
            List<TypedConstraintAdapter<?>> constraintsList = constraints.getOrDefault(property.getName(), Collections.emptyList());
            for (TypedConstraintAdapter<?> typed : constraintsList)
                typed.validateTypeCast(convertedValue);
            // Do set the value only if the validation passes.
            Object oldValue = property.getValue();
            propertyChangeListeners.get(ConfigurationListener.ChangeType.BEFORE).fireTypeCast(property.getName(), oldValue, convertedValue);
            property.setValue(convertedValue);
            propertyChangeListeners.get(ConfigurationListener.ChangeType.AFTER).fireTypeCast(property.getName(), oldValue, convertedValue);
            // This potentially may expose the values that are not safe to see in logs on production.
            logger.info("Property '{}' updated from '{}' to '{}'.", property.getName(), oldValue, convertedValue);
        }
        catch (Exception e)
        {
            if (e instanceof ConfigurationException)
                throw (ConfigurationException) e;
            else
                throw new ConfigurationException(String.format("Error updating property '%s'; cause: %s", property.getName(), e.getMessage()), e);
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
        lazyInit();
        rwLock.readLock().lock();
        try
        {
            validatePropertyExists(properties.get(name), name);
            Class<?> propertyType = type(name);
            Object value = properties.get(name).getValue();
            if (cls.equals(propertyType))
                return convertSafe(cls, value);
            else if (cls.equals(String.class))
            {
                StringConverters converter = StringConverters.fromType(propertyType);
                return cls.cast(converter == null ? TypeConverter.DEFAULT.convertNullable(value) : converter.toString(value));
            }
            else
                throw new ConfigurationException(String.format("Property '%s' is of type '%s' and cannot be cast to '%s'",
                                                               name, propertyType.getCanonicalName(), cls.getCanonicalName()));
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
        lazyInit();
        return properties.containsKey(name);
    }

    /**
     * Returns a set of all the property names.
     * @return set of all the property names.
     */
    @Override public Iterable<String> keys()
    {
        lazyInit();
        return properties.keySet();
    }

    /**
     * @param name The property name to get the type for.
     * @return Property type for the property with the given name.
     */
    @Override public Class<?> type(String name)
    {
        lazyInit();
        validatePropertyExists(properties.get(name), name);
        return properties.get(name).getType();
    }

    /**
     * @return The number of properties.
     */
    @Override public int size()
    {
        lazyInit();
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
    public <T> void addPropertyChangeListener(String name, ConfigurationListener.ChangeType type, ConfigurationListener<T> listener, Class<T> listenerType)
    {
        PropertyAdapter property = properties.get(name);
        validatePropertyExists(property, name);
        validatePropertyType(property, name, listenerType);
        propertyChangeListeners.get(type).add(name, new TypedListenerAdapter<>(listener, listenerType));
    }

    @SafeVarargs
    public final <T> void addPropertyConstraint(String name, Class<T> type, ConfigurationConstraint<T>... constraints)
    {
        PropertyAdapter property = properties.get(name);
        validatePropertyExists(property, name);
        validatePropertyType(property, name, type);
        for (ConfigurationConstraint<T> constraint : constraints)
            this.constraints.computeIfAbsent(name, k -> new ArrayList<>()).add(new TypedConstraintAdapter<>(constraint, type));
    }

    private void validatePropertyExists(PropertyAdapter property, String name)
    {
        if (property == null && initialized)
            throw new ConfigurationException(String.format("Property with name '%s' is not availabe.", name));
    }

    private void validatePropertyType(PropertyAdapter property, String name, @Nullable Class<?> targetType)
    {
        if (initialized && targetType != null && !property.getType().equals(targetType))
            throw new ConfigurationException(String.format("Property with name '%s' expects type '%s', but got '%s'.", name, property.getType(), targetType));
    }

    private static class TypedListenerAdapter<T>
    {
        private final ConfigurationListener<T> listener;
        private final Class<T> type;

        public TypedListenerAdapter(ConfigurationListener<T> listener, Class<T> type)
        {
            this.listener = listener;
            this.type = type;
        }

        public void fireTypeCast(String name, Object oldValue, Object newValue)
        {
            // Casting to the type of the listener is safe because we validate the type of the property on listener's registration.
            listener.onUpdate(name, convertSafe(type, oldValue), convertSafe(type, newValue));
        }
    }

    private static class TypedConstraintAdapter<T>
    {
        private final ConfigurationConstraint<T> constraint;
        private final Class<T> type;

        public TypedConstraintAdapter(ConfigurationConstraint<T> constraint, Class<T> type)
        {
            this.constraint = constraint;
            this.type = type;
        }

        public void validateTypeCast(Object newValue)
        {
            constraint.validate(convertSafe(type, newValue));
        }
    }

    /**
     * A simple holder of liseners of a configuration change for a particular type.
     */
    private static class ConfigurationListenerList
    {
        private final Map<String, List<TypedListenerAdapter<?>>> wrappers = new HashMap<>();

        public void add(String name, TypedListenerAdapter<?> listener)
        {
            wrappers.computeIfAbsent(name, k -> new ArrayList<>()).add(listener);
        }

        public void fireTypeCast(String name, Object oldValue, Object newValue)
        {
            if (!wrappers.containsKey(name)) return;
            for (TypedListenerAdapter<?> wrapper : wrappers.get(name))
                wrapper.fireTypeCast(name, oldValue, newValue);
        }
    }

    private static class PropertyAdapter
    {
        private final Supplier<Config> configSupplier;
        private final Property accessor;

        public PropertyAdapter(Supplier<Config> configSupplier, Property accessor)
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
            return "PropertyAdapter[name=" + getName() + ", " +
                   "type=" + getType() +
                   ", isWritable=" + isWritable() + ']';
        }
    }
}
