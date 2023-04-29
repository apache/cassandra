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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
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
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final List<ConfigurationSourceListener> changeListeners = new CopyOnWriteArrayList<>();
    private final List<ConfigurationValidator> validators = new ArrayList<>();
    private final TypeConverterRegistry converters;
    private final Config source;
    private final Map<String, Property> properties;

    public DatabaseConfigurationSource(Config source)
    {
        this.source = source;
        properties = ImmutableMap.copyOf(defaultLoader()
                                         .flatten(Config.class)
                                         .entrySet()
                                         .stream()
                                         .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        converters = TypeConverterRegistry.instance;
        // Initialize the configuration handlers.
        addConfigurationHandler(DatabaseDescriptor::validateUpperBoundStreamingConfig);
        addConfigurationHandler(createLoggingValidator(DatabaseDescriptor::validateRepairSessionSpace));
    }

    @Override
    public <T> void set(String name, T value)
    {
        Property property = ofNullable(properties.get(name)).orElseThrow(() -> notFound(name));
        lock.writeLock().lock();
        try
        {
            Class<?> originalType = property.getType();
            // Do conversion if the value is not null and the type is not the same as the property type.
            Object convertedValue = ofNullable(value)
                                    .map(Object::getClass)
                                    .map(from -> converters.get(from, originalType)
                                                           .convert(value))
                                    .orElse(null);
            // Use given converted value if it is not null, otherwise use the default value.
            ConfigurationSource validationSource = PropertyValidationSource.create(this, name, convertedValue);
            for (ConfigurationValidator validator : validators)
                validator.validate(validationSource);
            // Do set the value only if the validation passes.
            Object validatedValue = validationSource.getRaw(name);
            Object oldValue = property.get(source);
            changeListeners.forEach(l -> l.listen(name,
                                                  ConfigurationSourceListener.EventType.BEFORE_CHANGE,
                                                  oldValue, validatedValue));
            property.set(source, validatedValue);
            changeListeners.forEach(l -> l.listen(name,
                                                  ConfigurationSourceListener.EventType.AFTER_CHANGE,
                                                  oldValue, validatedValue));
            // This potentially may expose the values that are not safe to see in logs on production.
            logger.info("Configuration property '{}' updated from '{}' to '{}'.", property.getName(), oldValue, validatedValue);
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
            lock.writeLock().unlock();
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
        return ofNullable(properties.get(name)).map(f -> primitiveToWrapper(f.getType())).orElseThrow(() -> notFound(name));
    }

    @Override
    public Object getRaw(String name)
    {
        lock.readLock().lock();
        try
        {
            return ofNullable(properties.get(name))
                   .orElseThrow(() -> notFound(name))
                   .get(source);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * @param cls Class to cast the property value to.
     * @param name the property name to get.
     * @return The value of the property with the given name.
     */
    @SuppressWarnings("unchecked")
    public <T> T get(Class<T> cls, String name)
    {
        try
        {
            Object value = getRaw(name);
            return cls.equals(String.class) ?
                   converters.get(properties.get(name).getType(), cls, (TypeConverter<T>) TypeConverter.TO_STRING).convertNullable(value) :
                   converters.get(properties.get(name).getType(), cls).convertNullable(value);
        }
        catch (ClassCastException e)
        {
            throw new ConfigurationException(String.format("Invalid value for configuration property '%s'.", name), e);
        }
    }

    @Override
    public Iterator<Pair<String, Supplier<Object>>> iterator()
    {
        return Iterators.transform(properties.entrySet().iterator(),
                                   e -> Pair.create(e.getKey(), () -> getRaw(e.getKey())));
    }

    /**
     * Adds a listener for the property with the given name.
     * @param listener listener to add.
     */
    @Override
    public ListenerUnsubscriber addSourceListener(ConfigurationSourceListener listener)
    {
        lock.writeLock().lock();
        try
        {
            changeListeners.add(listener);
            return () -> removeSourceListener(listener);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes listener for the configuration source.
     * @param listener Listener to remove.
     */
    private void removeSourceListener(ConfigurationSourceListener listener)
    {
        lock.writeLock().lock();
        try
        {
            changeListeners.remove(listener);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    private void addConfigurationHandler(ConfigurationValidator handler)
    {
        this.validators.add(handler);
    }

    private static ConfigurationValidator createLoggingValidator(BiConsumer<ConfigurationSource, Logger> validator)
    {
        return source -> validator.accept(source, logger);
    }

    private static PropertyNotFoundException notFound(String name)
    {
        return new PropertyNotFoundException(String.format("Property with name '%s' is not found.", name));
    }

    /**
     * This is a special configuration source that is used to validate the property value before it is set.
     */
    private static class PropertyValidationSource implements ConfigurationSource
    {
        private final ConfigurationSource delegate;
        private final String key;
        private @Nullable Object newValue;

        private PropertyValidationSource(ConfigurationSource delegate, String key, Object newValue)
        {
            this.delegate = delegate;
            this.key = key;
            this.newValue = newValue;
        }

        private static ConfigurationSource create(ConfigurationSource source, String key, Object newValue)
        {
            return new PropertyValidationSource(source, key, newValue);
        }

        @Override
        public <T> void set(String name, T value)
        {
            if (!name.equals(this.key))
                throw new UnsupportedOperationException("Configuration source is read-only: " + name);

            if (value == null)
                newValue = null;
            else if (value.getClass().equals(delegate.type(name)))
                newValue = value;
            else
                throw new ConfigurationException(String.format("The value type '%s' doesn't match property's '%s' type '%s'.",
                                                               value.getClass().getCanonicalName(), name, delegate.type(name).getCanonicalName()));
        }

        @Override
        public Class<?> type(String name)
        {
            return delegate.type(name);
        }

        @Override
        public Object getRaw(String name)
        {
            return name.equals(this.key) ? newValue : delegate.getRaw(name);
        }

        @Override
        public <T> T get(Class<T> clazz, String name)
        {
            // Cast is safe because the value is converted prior to this call.
            return name.equals(this.key) ? clazz.cast(newValue) : delegate.get(clazz, name);
        }

        @Override
        public Iterator<Pair<String, Supplier<Object>>> iterator()
        {
            // Configuration source iterator is not used for validation and forbidden to use.
            throw new UnsupportedOperationException();
        }
    }
}
