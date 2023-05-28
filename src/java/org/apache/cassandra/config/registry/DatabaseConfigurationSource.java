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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;

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
    private static final List<Runnable> removers = new CopyOnWriteArrayList<>();
    private final List<ConfigurationSourceListener> changeListeners = new CopyOnWriteArrayList<>();
    private final TypeConverterRegistry converters;
    private final Config source;
    private final Map<String, Property> properties;

    public DatabaseConfigurationSource(Config source)
    {
        this.source = source;
        properties = defaultLoader().flatten(Config.class);
        converters = TypeConverterRegistry.instance;
        // Initialize the configuration handlers.
        registerConfigurationValidators(this::addConfigurationValidator);
    }

    private static void registerConfigurationValidators(Consumer<ConfigurationSourceValidator> adder)
    {
        adder.accept(DatabaseDescriptor::validateUpperBoundStreamingConfig);
        adder.accept(createLoggingValidator(DatabaseDescriptor::validateRepairSessionSpace));
        adder.accept(DatabaseDescriptor::validateConcurrentCompactors);
    }

    @Override
    public <T> void set(String name, T value)
    {
        Property property = ofNullable(properties.get(name)).orElseThrow(() -> notFound(name));
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
            Object oldValue = getRaw(name);
            ConfigurationSource validationSource = PropertyValidationSource.create(this, name, convertedValue);
            changeListeners.forEach(l -> l.beforeUpdate(name, validationSource));
            Object newValue = validationSource.getRaw(name);
            property.set(source, newValue);
            changeListeners.forEach(l -> l.afterUpdate(name, this));

            // This potentially may expose the values that are not safe to see in logs on production.
            logger.info("Configuration property '{}' updated from '{}' to '{}'.", property.getName(), oldValue, newValue);
        }
        catch (ConfigurationException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            logger.error(String.format("Failed to update property '%s'", name), e);
            throw new ConfigurationException(String.format("Failed to update property '%s'. The cause: %s",
                                                           name, e.getMessage()), false);
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
        return ofNullable(properties.get(name))
               .orElseThrow(() -> notFound(name))
               .get(source);
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
            throw new ConfigurationException(String.format("Failed to get value for the property '%s'.", name), e);
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
     * @param consumer listener to add.
     */
    public void addBeforeUpdateSourceListener(BiConsumer<String, ConfigurationSource> consumer)
    {
        addSourceListener(new ConfigurationSourceListener()
        {
            @Override
            public void beforeUpdate(String name, ConfigurationSource updatedSource)
            {
                consumer.accept(name, updatedSource);
            }
        });
    }

    /**
     * Adds a listener for the property with the given name.
     * @param listener listener to add.
     */
    @Override
    public void addSourceListener(ConfigurationSourceListener listener)
    {
        changeListeners.add(listener);
        removers.add(() -> changeListeners.remove(listener));
    }

    private void addConfigurationValidator(ConfigurationSourceValidator handler)
    {
        addSourceListener(handler);
    }

    public static void shutdown()
    {
        removers.forEach(Runnable::run);
    }

    private static ConfigurationSourceValidator createLoggingValidator(BiConsumer<ConfigurationSource, Logger> validator)
    {
        return source -> validator.accept(source, logger);
    }

    private static PropertyNotFoundException notFound(String name)
    {
        return new PropertyNotFoundException(String.format("Property '%s' is not found.", name));
    }

    interface ConfigurationSourceValidator extends ConfigurationSourceListener
    {
        void validate(ConfigurationSource source);

        @Override
        default void beforeUpdate(String name, ConfigurationSource updatedSource)
        {
            validate(updatedSource);
        }
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
                throw new ConfigurationException(String.format("The value type '%s' doesn't match the '%s' property type '%s'.",
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
