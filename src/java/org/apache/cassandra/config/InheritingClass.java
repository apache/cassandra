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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.exceptions.ConfigurationException;

public class InheritingClass extends ParameterizedClass
{
    public static final String DEFAULT_CONFIGURATION_KEY = "default";

    public String inherits = null;

    @SuppressWarnings("unused") // for snakeyaml
    public InheritingClass()
    {
    }

    public InheritingClass(String inherits, String class_name, Map<String, String> parameters)
    {
        super(class_name, parameters);
        this.inherits = inherits;
    }

    @SuppressWarnings("unused")
    public InheritingClass(Map<String, ?> p)
    {
        super(p);
        this.inherits = p.get("inherits").toString();
    }

    public static Map<String, ParameterizedClass> expandDefinitions(Map<String, InheritingClass> yamlConfigurations,
                                                                    InheritingClass defaultConfiguration)
    {
        if (yamlConfigurations == null)
            return ImmutableMap.of(DEFAULT_CONFIGURATION_KEY, defaultConfiguration);

        LinkedHashMap<String, ParameterizedClass> configs = new LinkedHashMap<>(yamlConfigurations.size() + 1);

        // If default is not overridden, add an entry first so that other configurations can inherit from it.
        // If it is, process it in its point of definition, so that the default can inherit from another configuration.
        if (!yamlConfigurations.containsKey(DEFAULT_CONFIGURATION_KEY))
            configs.put(DEFAULT_CONFIGURATION_KEY, defaultConfiguration);

        Map<String, InheritingClass> inheritingClasses = new LinkedHashMap<>();

        for (Map.Entry<String, InheritingClass> entry : yamlConfigurations.entrySet())
        {
            if (entry.getValue().inherits != null)
            {
                if (entry.getKey().equals(entry.getValue().inherits))
                    throw new ConfigurationException(String.format("Configuration entry %s can not inherit itself.", entry.getKey()));

                if (yamlConfigurations.get(entry.getValue().inherits) == null && !entry.getValue().inherits.equals(DEFAULT_CONFIGURATION_KEY))
                    throw new ConfigurationException(String.format("Configuration entry %s inherits non-existing entry %s.",
                                                                   entry.getKey(), entry.getValue().inherits));

                inheritingClasses.put(entry.getKey(), entry.getValue());
            }
            else
                configs.put(entry.getKey(), entry.getValue().resolve(configs));
        }

        for (Map.Entry<String, InheritingClass> inheritingEntry : inheritingClasses.entrySet())
        {
            String inherits = inheritingEntry.getValue().inherits;
            while (inherits != null)
            {
                InheritingClass nextInheritance = inheritingClasses.get(inherits);
                if (nextInheritance == null)
                    inherits = null;
                else
                    inherits = nextInheritance.inherits;

                if (inherits != null && inherits.equals(inheritingEntry.getKey()))
                    throw new ConfigurationException(String.format("Detected loop when processing key %s", inheritingEntry.getKey()));
            }
        }

        while (!inheritingClasses.isEmpty())
        {
            Set<String> forRemoval = new HashSet<>();
            for (Map.Entry<String, InheritingClass> inheritingEntry : inheritingClasses.entrySet())
            {
                if (configs.get(inheritingEntry.getValue().inherits) != null)
                {
                    configs.put(inheritingEntry.getKey(), inheritingEntry.getValue().resolve(configs));
                    forRemoval.add(inheritingEntry.getKey());
                }
            }

            assert !forRemoval.isEmpty();

            for (String toRemove : forRemoval)
                inheritingClasses.remove(toRemove);
        }

        return ImmutableMap.copyOf(configs);
    }

    public ParameterizedClass resolve(Map<String, ParameterizedClass> map)
    {
        if (inherits == null)
        {
            if (parameters == null)
                parameters = new LinkedHashMap<>();
            return this;
        }

        ParameterizedClass parent = map.get(inherits);
        if (parent == null)
            throw new ConfigurationException("Configuration definition inherits unknown " + inherits
                                             + ". A configuration can only extend one defined earlier or \"default\".");
        Map<String, String> resolvedParameters;
        if (parameters == null || parameters.isEmpty())
            resolvedParameters = parent.parameters;
        else if (parent.parameters == null || parent.parameters.isEmpty())
            resolvedParameters = this.parameters;
        else
        {
            resolvedParameters = new LinkedHashMap<>(parent.parameters);
            resolvedParameters.putAll(this.parameters);
        }

        String resolvedClass = this.class_name == null ? parent.class_name : this.class_name;
        return new InheritingClass(inherits, resolvedClass, resolvedParameters);
    }

    @Override
    public String toString()
    {
        return (inherits != null ? (inherits + "+") : "") +
               (class_name != null ? class_name : "") +
               (parameters != null ? parameters.toString() : "");
    }
}
