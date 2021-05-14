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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.cassandra.exceptions.ConfigurationException;

public class InheritingClass extends ParameterizedClass
{
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

    public ParameterizedClass resolve(Map<String, ParameterizedClass> map)
    {
        if (inherits == null)
            return this;
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
        return new ParameterizedClass(resolvedClass, resolvedParameters);
    }

    @Override
    public String toString()
    {
        return (inherits != null ? (inherits + "+") : "") +
               (class_name != null ? class_name : "") +
               (parameters != null ? parameters.toString() : "");
    }
}
