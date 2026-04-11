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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.base.Objects;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@Shared(scope = SIMULATION)
public class ParameterizedClass
{
    public static final String CLASS_NAME = "class_name";
    public static final String PARAMETERS = "parameters";

    public String class_name;
    public Map<String, String> parameters;

    public ParameterizedClass()
    {
        // for snakeyaml
    }

    public ParameterizedClass(String class_name)
    {
        this(class_name, Collections.emptyMap());
    }

    public ParameterizedClass(String class_name, Map<String, String> parameters)
    {
        this.class_name = class_name;
        this.parameters = parameters;
    }

    @SuppressWarnings("unchecked")
    public ParameterizedClass(Map<String, ?> p)
    {
        this((String)p.get(CLASS_NAME),
             p.containsKey(PARAMETERS) ? (Map<String, String>)((List<?>)p.get(PARAMETERS)).get(0) : null);
    }

    static public <K> K newInstance(ParameterizedClass parameterizedClass, List<String> searchPackages)
    {
        Class<?> providerClass = null;
        if (searchPackages == null || searchPackages.isEmpty())
            searchPackages = Collections.singletonList("");
        for (String searchPackage : searchPackages)
        {
            try
            {
                if (!searchPackage.isEmpty() && !searchPackage.endsWith("."))
                    searchPackage = searchPackage + '.';
                String name = searchPackage + parameterizedClass.class_name;
                providerClass = Class.forName(name);
            }
            catch (ClassNotFoundException e)
            {
                //no-op
            }
        }

        if (providerClass == null)
        {
            String pkgList = '[' + searchPackages.stream().map(p -> '"' + p + '"').collect(Collectors.joining(",")) + ']';
            String error = "Unable to find class " + parameterizedClass.class_name + " in packages " + pkgList;
            throw new ConfigurationException(error);
        }

        try
        {
            Constructor<?> mapConstructor = filterConstructor(providerClass, c -> c.getParameterTypes().length == 1 && c.getParameterTypes()[0].equals(Map.class));
            if (mapConstructor != null)
                return (K) mapConstructor.newInstance(parameterizedClass.parameters == null ? Collections.emptyMap() : parameterizedClass.parameters);

            // Falls-back to no-arg constructor
            Constructor<?> noArgsConstructor = filterConstructor(providerClass, c -> c.getParameterTypes().length == 0);
            if (noArgsConstructor != null)
                return (K) noArgsConstructor.newInstance();

            throw new ConfigurationException("No valid constructor found for class " + parameterizedClass.class_name);
        }
        catch (IllegalAccessException | InstantiationException | ExceptionInInitializerError e)
        {
            throw new ConfigurationException("Unable to instantiate parameterized class " + parameterizedClass.class_name, e);
        }
        catch (InvocationTargetException e)
        {
            Throwable cause = e.getCause();
            String error = "Failed to instantiate class " + parameterizedClass.class_name +
                           (cause.getMessage() != null ? ": " + cause.getMessage() : "");
            throw new ConfigurationException(error, cause);
        }
    }

    private static Constructor<?> filterConstructor(Class<?> providerClass, Predicate<Constructor<?>> filter)
    {
        for (Constructor<?> constructor : providerClass.getDeclaredConstructors())
        {
            if (filter.test(constructor))
                return constructor;
        }

        return null;
    }

    @Override
    public boolean equals(Object that)
    {
        return that instanceof ParameterizedClass && equals((ParameterizedClass) that);
    }

    public boolean equals(ParameterizedClass that)
    {
        return Objects.equal(class_name, that.class_name) && Objects.equal(parameters, that.parameters);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(class_name, parameters);
    }

    @Override
    public String toString()
    {
        return class_name + parameters;
    }
}
