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
package org.apache.cassandra.config;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.ArrayUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.yaml.snakeyaml.introspector.Property;

/**
 *
 */
public class ListenableProperty<T> extends Property
{
    private static final Logger logger = LoggerFactory.getLogger(ListenableProperty.class);
    private final Property delegate;
    private final Invoker<T> validateInvoker;
    private final List<PropertyListener<T>> listeners = new CopyOnWriteArrayList<>();

    public ListenableProperty(Property property)
    {
        super(property.getName(), property.getType());
        this.delegate = property;
        this.validateInvoker = property.getAnnotation(Validate.class) == null ?
                               (conf, val) -> val :
                               PropertyValidateInvoker.create(property);
    }

    @Override
    public boolean isWritable()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isReadable()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<?>[] getActualTypeArguments()
    {
        return delegate.getActualTypeArguments();
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized void set(Object config, Object newValue) throws Exception
    {
        if (getAnnotation(Mutable.class) == null)
            throw new ConfigurationException("Property is read-only: " + getName());

        T validatedValue = validateInvoker.invoke((Config) config, (T) newValue);
        T oldValue = get(config);
        Iterator<PropertyListener<T>> iterator = listeners.iterator();
        while (iterator.hasNext())
            iterator.next().onBeforeChange(oldValue, validatedValue);
        delegate.set(config, validatedValue);
        while (iterator.hasNext())
            iterator.next().onAfterChange(oldValue, validatedValue);
        if (logger.isInfoEnabled())
            logger.info("Updated {} from {} to {}", getName(), oldValue, validatedValue);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T get(Object conf)
    {
        return (T) delegate.get(conf);
    }

    @Override
    public List<Annotation> getAnnotations()
    {
        return delegate.getAnnotations();
    }

    @Override
    public <A extends Annotation> A getAnnotation(Class<A> aClass)
    {
        return delegate.getAnnotation(aClass);
    }

    public PropertyListener.Remover addListener(PropertyListener<T> listener)
    {
        listeners.add(listener);
        return () -> listeners.remove(listener);
    }

    interface Invoker<T>
    {
        T invoke(Config config, T newValue);
    }

    private static class PropertyValidateInvoker<T> implements Invoker<T>
    {
        private final String name;
        private final Method validateMethod;

        private PropertyValidateInvoker(String name, Method validateMethod)
        {
            this.name = name;
            this.validateMethod = validateMethod;
        }

        public static <T> PropertyValidateInvoker<T> create(Property property)
        {
            Preconditions.checkNotNull(property.getAnnotation(Validate.class));
            try
            {
                Validate annotation = property.getAnnotation(Validate.class);
                Class<?> clazz = Class.forName(annotation.useClass());
                for (Method validateMethod : clazz.getDeclaredMethods())
                {
                    if (validateMethod.getName().equals(annotation.useClassMethod()) &&
                        Arrays.equals(ArrayUtils.addAll(validateMethod.getParameterTypes(), validateMethod.getReturnType()),
                                      ArrayUtils.addAll(new Class<?>[]{Config.class, String.class, property.getType()}, property.getType())))
                    {
                        int modifiers = validateMethod.getModifiers();
                        if (Modifier.isPrivate(modifiers) || Modifier.isProtected(modifiers))
                            throw new ConfigurationException("Validate method must be public: " + validateMethod.getName(), false);
                        return new PropertyValidateInvoker<>(property.getName(), validateMethod);
                    }
                }
                throw new ConfigurationException("Validate method not found for field: " + property.getName(), false);
            }
            catch (ClassNotFoundException e)
            {
                throw new ConfigurationException("Failed to initialize: " + e.getMessage(), e);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public T invoke(Config config, T newValue)
        {
            try
            {
                return (T) validateMethod.invoke(null, config, name, newValue);
            }
            catch (Exception e)
            {
                e.printStackTrace();
                if (e.getCause() instanceof ConfigurationException)
                    throw (ConfigurationException) e.getCause();
                throw new ConfigurationException(String.format("Failed to call validation method '%s' for property '%s'.", validateMethod.getName(), name), e);
            }
        }
    }
}
