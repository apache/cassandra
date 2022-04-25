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
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;

import com.google.common.collect.Maps;

import org.yaml.snakeyaml.introspector.Property;

/**
 * Utility class for working with {@link Property} types.
 */
public final class Properties
{
    public static final String DELIMITER = ".";

    private Properties()
    {
    }

    /**
     * Given two properties (root, leaf), calls first go through root and passed to leaf.
     *
     * <pre>{@code leaf.get(root.get(o))}</pre>
     *
     * @param root first property in the chain
     * @param leaf last property in the chain
     * @param delimiter for joining names
     * @return new Property which combines root -> leaf
     */
    public static Property andThen(Property root, Property leaf, String delimiter)
    {
        return new AndThen(root, leaf, delimiter);
    }

    /**
     * Given two properties (root, leaf), calls first go through root and passed to leaf.
     *
     * <pre>{@code leaf.get(root.get(o))}</pre>
     *
     * @param root first property in the chain
     * @param leaf last property in the chain
     * @return new Property which combines root -> leaf
     */
    public static Property andThen(Property root, Property leaf)
    {
        return andThen(root, leaf, DELIMITER);
    }

    /**
     * Given a map of Properties, takes any "nested" property (non primitive, value-type, or collection), and
     * expands them, producing 1 or more Properties.
     *
     * @param loader for mapping type to map of properties
     * @param input map to flatten
     * @return map of all flattened properties
     */
    public static Map<String, Property> flatten(Loader loader, Map<String, Property> input)
    {
        return flatten(loader, input, DELIMITER);
    }

    /**
     * Given a map of Properties, takes any "nested" property (non primitive, value-type, or collection), and
     * expands them, producing 1 or more Properties.
     *
     * @param loader for mapping type to map of properties
     * @param input map to flatten
     * @param delimiter for joining names
     * @return map of all flattened properties
     */
    public static Map<String, Property> flatten(Loader loader, Map<String, Property> input, String delimiter)
    {
        Queue<Property> queue = new ArrayDeque<>(input.values());

        Map<String, Property> output = Maps.newHashMapWithExpectedSize(input.size());
        while (!queue.isEmpty())
        {
            Property prop = queue.poll();
            Map<String, Property> children = isPrimitive(prop) || isCollection(prop) ? Collections.emptyMap() : loader.getProperties(prop.getType());
            if (children.isEmpty())
            {
                // not nested, so assume properties can be handled
                output.put(prop.getName(), prop);
            }
            else
            {
                children.values().stream().map(p -> andThen(prop, p, delimiter)).forEach(queue::add);
            }
        }
        return output;
    }

    /**
     * @return true if proeprty type is a collection
     */
    public static boolean isCollection(Property prop)
    {
        return Collection.class.isAssignableFrom(prop.getType()) || Map.class.isAssignableFrom(prop.getType());
    }

    /**
     * @return true if property type is a primitive, or well known value type (may return false for user defined value types)
     */
    public static boolean isPrimitive(Property prop)
    {
        Class<?> type = prop.getType();
        return type.isPrimitive() || type.isEnum() || type.equals(String.class) || Number.class.isAssignableFrom(type) || Boolean.class.equals(type);
    }

    /**
     * @return default implementation of {@link Loader}
     */
    public static Loader defaultLoader()
    {
        return new DefaultLoader();
    }

    /**
     * @return a new property with an updated name
     */
    public static Property rename(String newName, Property prop)
    {
        return new ForwardingProperty(newName, prop);
    }

    private static final class AndThen extends ForwardingProperty
    {
        private final Property root;
        private final Property leaf;

        AndThen(Property root, Property leaf, String delimiter)
        {
            super(root.getName() + delimiter + leaf.getName(), leaf);
            this.root = root;
            this.leaf = leaf;
        }

        @Override
        public void set(Object object, Object value) throws Exception
        {
            Object parent = root.get(object);
            if (parent == null)
            {
                // see: org.yaml.snakeyaml.constructor.BaseConstructor.newInstance(java.lang.Class<?>, org.yaml.snakeyaml.nodes.Node, boolean)
                // That method is what creates the types, and it boils down to this call.  There is a TypeDescription
                // class that we don't use, so boils down to "null" in our existing logic... if we start using TypeDescription
                // to build the object, then we will need to rewrite this logic to work with BaseConstructor.
                Constructor<?> c = root.getType().getDeclaredConstructor();
                c.setAccessible(true);
                parent = c.newInstance();
                root.set(object, parent);
            }
            leaf.set(parent, value);
        }

        @Override
        public Object get(Object object)
        {
            try
            {
                Object parent = root.get(object);
                if (parent == null)
                    return null;
                return leaf.get(parent);
            }
            catch (Exception e)
            {
                if (!(root instanceof AndThen))
                    e.addSuppressed(new RuntimeException("Error calling get() on " + this));
                throw e;
            }
        }
    }
}
