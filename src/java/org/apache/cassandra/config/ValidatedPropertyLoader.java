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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;
import org.yaml.snakeyaml.introspector.FieldProperty;
import org.yaml.snakeyaml.introspector.Property;

public final class ValidatedPropertyLoader implements Loader
{
    private final Loader loader;

    public ValidatedPropertyLoader()
    {
        this.loader = new DefaultLoader(ValidatedPropertyLoader::listenablePropertyFactory);
    }

    @Override
    public Map<String, Property> getProperties(Class<?> root)
    {
        return loader.getProperties(root);
    }

    private static Property listenablePropertyFactory(Field field)
    {
        ListenableProperty<?, ?> listenable = new ListenableProperty<>(new FieldProperty(field));
        ValidatedByList validatedByList = field.getAnnotation(ValidatedByList.class);
        ValidatedBy[] validatedByArray;
        if (validatedByList == null)
        {
            if (field.getAnnotation(ValidatedBy.class) == null)
                return listenable;
            else
                validatedByArray = new ValidatedBy[]{ field.getAnnotation(ValidatedBy.class) };
        }
        else
            validatedByArray = validatedByList.value();

        for (ValidatedBy validatedBy : validatedByArray)
            listenable.addBeforeListener(createValidationListener(field, validatedBy));

        return listenable;
    }

    private static <S, T> ListenableProperty.BeforeChangeListener<S, T> createValidationListener(Field field, ValidatedBy annotation)
    {
        Class<?> clazz = FBUtilities.classForName(annotation.useClass(), "validate method");
        List<Method> matches = new ArrayList<>();
        for (Method method : clazz.getDeclaredMethods())
        {
            if (method.getName().equals(annotation.useClassMethod()) &&
                Modifier.isStatic(method.getModifiers()) &&
                Modifier.isPublic(method.getModifiers()))
                matches.add(method);
        }

        if (matches.isEmpty())
            throw new ConfigurationException(String.format("Required public static method '%s' not found in class '%s'",
                                                           annotation.useClassMethod(),
                                                           clazz.getCanonicalName()), false);

        if (matches.size() > 1)
            throw new ConfigurationException(String.format("Ambiguous public static method '%s' found in class '%s'. " +
                                                           "You must specify a unique method name.",
                                                           annotation.useClassMethod(),
                                                           clazz.getCanonicalName()), false);
        Method method = matches.get(0);
        if (!(method.getReturnType().equals(Void.TYPE) || method.getReturnType().equals(field.getType())))
            throw new ConfigurationException(String.format("Required method '%s' in class '%s' must return '%s' or 'void', " +
                                                           "but returns '%s' instead. The field is '%s$%s'.",
                                                           annotation.useClassMethod(),
                                                           clazz.getSimpleName(),
                                                           field.getType().getCanonicalName(),
                                                           method.getReturnType().getCanonicalName(),
                                                           field.getDeclaringClass().getSimpleName(),
                                                           field.getName()), false);

        switch (method.getParameterCount())
        {
            case 1:
                return new MethodInvokeListener<>(method,
                                                  new Class<?>[]{ field.getType() },
                                                  (s, n, o, v) -> sneakyThrow(() -> method.invoke(null, v)));
            case 2:
                return new MethodInvokeListener<>(method,
                                                  new Class[]{ String.class, field.getType() },
                                                  (s, n, o, v) -> sneakyThrow(() -> method.invoke(null, n, v)));
            case 3:
                return new MethodInvokeListener<>(method,
                                                  new Class[]{ field.getDeclaringClass(), String.class, field.getType() },
                                                  (s, n, o, v) -> sneakyThrow(() -> method.invoke(null, s, n, v)));
            default:
                throw new ConfigurationException(String.format("Required method '%s' in class '%s' must have one, two, " +
                                                               "or three input parameters",
                                                               annotation.useClassMethod(),
                                                               clazz.getCanonicalName()), false);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T, E extends Exception> T sneakyThrow(Callable<?> c) throws E
    {
        try { return (T) c.call(); }
        catch (Exception ex) { throw (E) ex; }
    }

    private static class MethodInvokeListener<S, T> implements ListenableProperty.BeforeChangeListener<S, T>
    {
        private final Method method;
        private final ListenableProperty.BeforeChangeListener<S, T> delegate;

        private MethodInvokeListener(Method method, Class<?>[] arguments, ListenableProperty.BeforeChangeListener<S, T> delegate)
        {
            if (!Arrays.equals(method.getParameterTypes(), ArrayUtils.addAll(arguments)))
                throw new ConfigurationException(String.format("Method '%s' must have exactly the following '(%s)' input arguments",
                                                               method.getName(),
                                                               Arrays.stream(arguments)
                                                                     .map(Class::getCanonicalName)
                                                                     .collect(Collectors.joining(", "))), false);
            this.delegate = delegate;
            this.method = method;
        }

        /** {@inheritDoc} */
        @Override
        public T before(S source, String name, T oldValue, T newValue)
        {
            try
            {
                T result = delegate.before(source, name, oldValue, newValue);
                return method.getReturnType() == Void.TYPE ? newValue : result;
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
