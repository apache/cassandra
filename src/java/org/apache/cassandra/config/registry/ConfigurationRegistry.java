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
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DatabaseDescriptorWalker;
import org.apache.cassandra.config.GuardrailsOptionsWalker;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.apache.commons.lang3.ClassUtils.primitiveToWrapper;


/**
 * This is a simple configuration property registry that stores all the {@link Config} settings.
 * It is a singleton and can be accessed via {@link #instance} field.
 */
public class ConfigurationRegistry
{
    public static final List<PropertyAccessorsWalker> PROPERTY_SETTERS_LIST =
        ImmutableList.of(new DatabaseDescriptorWalker(null), new GuardrailsOptionsWalker(DatabaseDescriptor.getGuardrailsConfig()));
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationRegistry.class);
    public static final ConfigurationRegistry instance = new ConfigurationRegistry();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Map<String, ConfigurationProperty<?>> properties;
    private final Map<PropertyChangeListener.ChangeType, PropertyChangeListenerList> propertyChangeListeners = new EnumMap<>(PropertyChangeListener.ChangeType.class);
    private final Map<String, List<PropertyChangeListenerWrapper<?>>> propertyConstraints = new HashMap<>();

    public ConfigurationRegistry()
    {
        Map<String, ConfigurationProperty<?>> configProperties = new HashMap<>();
        ConfigurationEnricher enricher = new ConfigurationEnricher(configProperties);
        PROPERTY_SETTERS_LIST.forEach(w -> w.walk(enricher));
        properties = ImmutableMap.copyOf(configProperties);

        for (PropertyChangeListener.ChangeType type : PropertyChangeListener.ChangeType.values())
            propertyChangeListeners.put(type, new PropertyChangeListenerList());
    }

    /**
     * Setter for the property with the given name. Can accept {@code null} value.
     * @param name the name of the property.
     * @param value the value to set.
     */
    public void update(String name, Object value)
    {
        ConfigurationProperty<?> property = properties.get(name);
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
            property.setValue(value);
            propertyChangeListeners.get(PropertyChangeListener.ChangeType.AFTER).fire(name, oldValue, value);
            // This potentially may expose the values that are not safe to see in logs on production.
            logger.info("Property '{}' updated from '{}' to '{}'.", name, oldValue, value);
        }
        catch (Exception e)
        {
            logger.error(">>> ", e);
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
    public <T> T get(String name)
    {
        rwLock.readLock().lock();
        try
        {
            validatePropertyExists(properties.get(name), name);
            return (T) properties.get(name).getValue();
        }
        finally
        {
            rwLock.readLock().unlock();
        }
    }

    /**
     * @param key the property name to check.
     * @return {@code true} if the property with the given name is available, {@code false} otherwise.
     */
    public boolean contains(String key)
    {
        return properties.containsKey(key);
    }

    /**
     * Returns a set of all the property names.
     * @return set of all the property names.
     */
    public Set<String> keys()
    {
        return properties.keySet();
    }

    /**
     * @param name The property name to get the type for.
     * @return Property type for the property with the given name.
     */
    public Class<?> getType(String name)
    {
        validatePropertyExists(properties.get(name), name);
        return properties.get(name).getType();
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

    private static <T> Class<T> primitiveToWrapperType(Class<T> type)
    {
        return type.isPrimitive() ? (Class<T>) primitiveToWrapper(type) : type;
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
    }

    /**
     * Configuration enricher is used to add new property setters to the configuration.
     */
    private static class ConfigurationEnricher implements PropertyAccessorsWalker.PropertyAccessorsVisitor
    {
        private final Map<String, ConfigurationProperty<?>> registry;

        ConfigurationEnricher(Map<String, ConfigurationProperty<?>> registry)
        {
            this.registry = registry;
        }

        /** {@inheritDoc} */
        @Override
        public <T> void visit(String name, Class<T> clazz, PropertySetter<T> setter, PropertyGetter<T> getter)
        {
            registry.computeIfAbsent(name, key -> new ConfigurationProperty<>(key, clazz, setter, getter));
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

    /**
     * Configuration unit is a single configuration entry that can be read and written.
     * @param <V> type of the configuration setter value.
     */
    private static class ConfigurationProperty<V>
    {
        private final String name;
        private final @Nullable Class<V> type;
        private final @Nullable PropertySetter<V> setter;
        private final @Nullable PropertyGetter<V> getter;

        public ConfigurationProperty(String name, @Nullable Class<V> type, @Nullable PropertySetter<V> setter, @Nullable PropertyGetter<V> getter)
        {
            this.name = name;
            this.getter = getter;
            this.type = type;
            this.setter = setter;
        }

        public Class<V> getType()
        {
            return type;
        }

        public boolean isWritable()
        {
            return setter != null;
        }

        public String getKey()
        {
            return name;
        }

        public void setValue(Object value) {
            if (setter == null)
                throw new ConfigurationException(String.format("Property %s is read only", name));

            setter.set(primitiveToWrapperType(Objects.requireNonNull(type)).cast(value));
        }

        public V getValue()
        {
            if (getter == null)
                throw new ConfigurationException(String.format("Property %s is not accessible", name));

            return getter.get();
        }
    }
}
