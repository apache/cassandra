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

import java.util.Objects;

import org.yaml.snakeyaml.introspector.Property;

/**
 * Holder for replacements to support backward compatibility between old and new names and types
 * of configuration parameters (CASSANDRA-15234)
 */
public final class Replacement
{
    /**
     * Currently we use Config class
     */
    public final Class<?> parent;
    /**
     * Old name of the configuration parameter
     */
    public final String oldName;
    /**
     * Old type of the configuration parameter
     */
    public final Class<?> oldType;
    /**
     * New name used for the configuration parameter
     */
    public final String newName;
    /**
     * Converter to be used according to the old default unit which was provided as a suffix of the configuration
     * parameter
     */
    public final Converters converter;
    public final boolean deprecated;

    public Replacement(Class<?> parent,
                       String oldName,
                       Class<?> oldType,
                       String newName,
                       Converters converter,
                       boolean deprecated)
    {
        this.parent = Objects.requireNonNull(parent);
        this.oldName = Objects.requireNonNull(oldName);
        this.oldType = Objects.requireNonNull(oldType);
        this.newName = Objects.requireNonNull(newName);
        this.converter = Objects.requireNonNull(converter);
        // by default deprecated is false
        this.deprecated = deprecated;
    }

    public Property toProperty(Property newProperty)
    {
        return new ForwardingProperty(oldName, oldType, newProperty)
        {
            @Override
            public void set(Object o, Object o1) throws Exception
            {
                newProperty.set(o, converter.convert(o1));
            }

            @Override
            public Object get(Object o)
            {
                return converter.unconvert(newProperty.get(o));
            }
        };
    }

    public boolean isValueFormatReplacement()
    {
        return oldName.equals(newName);
    }
}
