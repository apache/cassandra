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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.ArrayUtils;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;
import org.yaml.snakeyaml.introspector.FieldProperty;
import org.yaml.snakeyaml.introspector.Property;

public class ValidatedPropertyLoader extends DefaultLoader
{
    private final Predicate<Property> nonListenableProperty;

    public ValidatedPropertyLoader(Predicate<Property> nonListenableProperty)
    {
        this.nonListenableProperty = nonListenableProperty;
    }

    @Override
    protected Property createPropertyForField(Class<?> root, Field field)
    {
        Property property = new FieldProperty(field);
        if (nonListenableProperty.test(property))
            return property;

        ListenableProperty<?, ?> listenable = new ListenableProperty<>(property);
        if (property.getAnnotation(ValidatedBy.class) != null)
            listenable.addBeforeHandler(ValidationPropertyHandler.create(root, listenable.delegate()));
        return listenable;
    }

    private static class ValidationPropertyHandler<S, T> implements ListenableProperty.Handler<S, T>
    {
        private final Method method;
        private final String name;

        private ValidationPropertyHandler(Method method, String name)
        {
            this.method = method;
            this.name = name;
        }

        public static <S, T> ValidationPropertyHandler<S, T> create(Class<?> root, Property property)
        {
            Preconditions.checkNotNull(property.getAnnotation(ValidatedBy.class));
            ValidatedBy annotation = property.getAnnotation(ValidatedBy.class);
            Class<?> clazz = FBUtilities.classForName(annotation.useClass(), "validate method");
            Class<?>[] searchPattern = ArrayUtils.addAll(new Class<?>[]{ root, property.getType() });
            for (Method refMethod : clazz.getDeclaredMethods())
            {
                int modifiers = refMethod.getModifiers();
                if (Modifier.isPrivate(modifiers) || Modifier.isProtected(modifiers))
                    continue;

                if (refMethod.getName().equals(annotation.useClassMethod()) &&
                    Arrays.equals(refMethod.getParameterTypes(), searchPattern) &&
                    (refMethod.getReturnType() == Void.TYPE || refMethod.getReturnType().equals(property.getType())))
                {
                    return new ValidationPropertyHandler<>(refMethod, property.getName());
                }
            }
            throw new ConfigurationException(String.format("Required method '%s(%s)' not found for field '%s'",
                                                           annotation.useClassMethod(),
                                                           Arrays.stream(searchPattern)
                                                                 .map(Class::getCanonicalName)
                                                                 .collect(Collectors.joining(", ")),
                                                           property.getName()), false);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override
        public T handle(S source, T oldValue, T newValue)
        {
            try
            {
                Object result = method.invoke(source, source, newValue);
                return method.getReturnType() == Void.TYPE ? newValue : (T) result;
            }
            catch (Exception e)
            {
                if (e.getCause() instanceof ConfigurationException ||
                    e.getCause() instanceof IllegalArgumentException)
                    throw new ConfigurationException(String.format("Property '%s' validation failed: %s",
                                                                   name, e.getCause().getMessage()), e.getCause());
                throw new ConfigurationException(String.format("Unable to call validation method '%s' for property '%s'.",
                                                               method.getName(), name), e);
            }
        }
    }
}
