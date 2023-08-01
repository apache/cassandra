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
package org.apache.cassandra.io.sstable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components.Types;

/**
 * SSTables are made up of multiple components in separate files. Components are
 * identified by a type and an id, but required unique components (such as the Data
 * and Index files) may have implicit ids assigned to them.
 */
public class Component
{
    public static final char separator = '-';

    /**
     * WARNING: Be careful while changing the names or string representation of the enum
     * members. Streaming code depends on the names during streaming (Ref: CASSANDRA-14556).
     */
    public final static class Type
    {
        private final static CopyOnWriteArrayList<Type> typesCollector = new CopyOnWriteArrayList<>();

        public static final List<Type> all = Collections.unmodifiableList(typesCollector);

        public final int id;
        public final String name;
        public final String repr;
        public final boolean streamable;
        private final Component singleton;

        @SuppressWarnings("rawtypes")
        public final Class<? extends SSTableFormat> formatClass;

        /**
         * Creates a new non-singleton type and registers it a global type registry - see {@link #registerType(Type)}.
         *
         * @param name         type name, must be unique for this and all parent formats
         * @param repr         the regular expression to be used to recognize a name represents this type
         * @param streamable   whether components of this type should be streamed to other nodes
         * @param formatClass  format class for which this type is defined for
         */
        public static Type create(String name, String repr, boolean streamable, Class<? extends SSTableFormat<?, ?>> formatClass)
        {
            return new Type(name, repr, false, streamable, formatClass);
        }

        /**
         * Creates a new singleton type and registers it in a global type registry - see {@link #registerType(Type)}.
         *
         * @param name         type name, must be unique for this and all parent formats
         * @param repr         the regular expression to be used to recognize a name represents this type
         * @param streamable   whether components of this type should be streamed to other nodes
         * @param formatClass  format class for which this type is defined for
         */
        public static Type createSingleton(String name, String repr, boolean streamable, Class<? extends SSTableFormat<?, ?>> formatClass)
        {
            return new Type(name, repr, true, streamable, formatClass);
        }

        private Type(String name, String repr, boolean isSingleton, boolean streamable, Class<? extends SSTableFormat<?, ?>> formatClass)
        {
            this.name = Objects.requireNonNull(name);
            this.repr = repr;
            this.streamable = streamable;
            this.id = typesCollector.size();
            this.formatClass = formatClass == null ? SSTableFormat.class : formatClass;
            this.singleton = isSingleton ? new Component(this) : null;

            registerType(this);
        }

        /**
         * If you have two formats registered, they may both define a type say `INDEX`. It is allowed even though
         * they have the same name because they are not in the same branch. Though, we cannot let a custom type
         * define a type `TOC` which is declared on the top level.
         * So, e.g. given we have `TOC@SSTableFormat`, and `BigFormat` tries to define `TOC@BigFormat`, we should
         * forbid that; but, given we have `INDEX@BigFormat`, we should allow to define `INDEX@TrieFormat` as those
         * types are be distinguishable via format type.
         *
         * @param type a type to be registered
         */
        private static void registerType(Type type)
        {
            synchronized (typesCollector)
            {
                if (typesCollector.stream().anyMatch(t -> (Objects.equals(t.name, type.name) || Objects.equals(t.repr, type.repr)) && (t.formatClass.isAssignableFrom(type.formatClass))))
                    throw new AssertionError("Type named " + type.name + " is already registered");

                typesCollector.add(type);
            }
        }

        @VisibleForTesting
        public static Type fromRepresentation(String repr, SSTableFormat<?, ?> format)
        {
            for (Type type : Type.all)
            {
                if (type.repr != null && Pattern.matches(type.repr, repr) && type.formatClass.isAssignableFrom(format.getClass()))
                    return type;
            }
            return Types.CUSTOM;
        }

        public static Component createComponent(String repr, SSTableFormat<?, ?> format)
        {
            Type type = fromRepresentation(repr, format);
            if (type.singleton != null)
                return type.singleton;
            else
                return new Component(type, repr);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Type type = (Type) o;
            return id == type.id;
        }

        @Override
        public int hashCode()
        {
            return id;
        }

        @Override
        public String toString()
        {
            return name;
        }

        public Component getSingleton()
        {
            return Objects.requireNonNull(singleton);
        }

        public Component createComponent(String repr)
        {
            Preconditions.checkArgument(singleton == null);
            return new Component(this, repr);
        }
    }

    public final Type type;
    public final String name;
    public final int hashCode;

    private Component(Type type)
    {
        this(type, type.repr);
    }

    private Component(Type type, String name)
    {
        assert name != null : "Component name cannot be null";

        this.type = type;
        this.name = name;
        this.hashCode = Objects.hash(type, name);
    }

    /**
     * @return The unique (within an sstable) name for this component.
     */
    public String name()
    {
        return name;
    }

    /**
     * Parse the component part of a sstable filename into a {@code Component} object.
     *
     * @param name a string representing a sstable component.
     * @return the component corresponding to {@code name}. Note that this always return a component as an unrecognized
     * name is parsed into a CUSTOM component.
     */
    public static Component parse(String name, SSTableFormat<?, ?> format)
    {
        return Type.createComponent(name, format);
    }

    public static Iterable<Component> getSingletonsFor(SSTableFormat<?, ?> format)
    {
        return Iterables.transform(Iterables.filter(Type.all, t -> t.singleton != null && t.formatClass.isAssignableFrom(format.getClass())), t -> t.singleton);
    }

    public static Iterable<Component> getSingletonsFor(Class<? extends SSTableFormat<?, ?>> formatClass)
    {
        return Iterables.transform(Iterables.filter(Type.all, t -> t.singleton != null && t.formatClass.isAssignableFrom(formatClass)), t -> t.singleton);
    }

    public boolean isValidFor(Descriptor descriptor)
    {
        return type.formatClass.isAssignableFrom(descriptor.version.format.getClass());
    }

    @Override
    public String toString()
    {
        return this.name();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this)
            return true;
        if (!(o instanceof Component))
            return false;
        Component that = (Component) o;
        return this.hashCode == that.hashCode && this.type == that.type && this.name.equals(that.name);
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }
}
