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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

/**
 * SSTables are made up of multiple components in separate files. Components are
 * identified by a type and an id, but required unique components (such as the Data
 * and Index files) may have implicit ids assigned to them.
 */
public class Component
{
    public static final char separator = '-';

    final static EnumSet<Type> TYPES = EnumSet.allOf(Type.class);

    public enum Type
    {
        // the base data for an sstable: the remaining components can be regenerated
        // based on the data component
        DATA("Data.db", 1),
        // index of the row keys with pointers to their positions in the data file
        PRIMARY_INDEX("Index.db", 2),
        // serialized bloom filter for the row keys in the sstable
        FILTER("Filter.db", 3),
        // file to hold information about uncompressed data length, chunk offsets etc.
        COMPRESSION_INFO("CompressionInfo.db", 4),
        // statistical metadata about the content of the sstable
        STATS("Statistics.db", 5),
        // holds CRC32 checksum of the data file
        DIGEST("Digest.crc32", 6),
        // holds the CRC32 for chunks in an a uncompressed file.
        CRC("CRC.db", 7),
        // holds SSTable Index Summary (sampling of Index component)
        SUMMARY("Summary.db", 8),
        // table of contents, stores the list of all components for the sstable
        TOC("TOC.txt", 9),
        // built-in secondary index (may be multiple per sstable)
        SECONDARY_INDEX("SI_.*.db", 10),
        // custom component, used by e.g. custom compaction strategy
        CUSTOM(null, 11);

        public final String repr;
        public final byte id;
        public static final Map<Byte, Type> idToType;

        Type(String repr, int id)
        {
            this.repr = repr;
            this.id = (byte) id;
        }

        public static Type fromRepresentation(String repr)
        {
            for (Type type : TYPES)
            {
                if (type.repr != null && Pattern.matches(type.repr, repr))
                    return type;
            }
            return CUSTOM;
        }

        public static Type fromRepresentation(byte id)
        {
            return idToType.getOrDefault(id, CUSTOM);
        }

        static
        {
            Type[] values = Type.values();
            Map<Byte, Type> result = new HashMap<>(values.length);
            for (Type t : values)
                if (!t.equals(CUSTOM)) result.put(t.id, t);

            idToType = ImmutableMap.copyOf(result);
        }
    }

    // singleton components for types that don't need ids
    public final static Component DATA = new Component(Type.DATA);
    public final static Component PRIMARY_INDEX = new Component(Type.PRIMARY_INDEX);
    public final static Component FILTER = new Component(Type.FILTER);
    public final static Component COMPRESSION_INFO = new Component(Type.COMPRESSION_INFO);
    public final static Component STATS = new Component(Type.STATS);
    public final static Component DIGEST = new Component(Type.DIGEST);
    public final static Component CRC = new Component(Type.CRC);
    public final static Component SUMMARY = new Component(Type.SUMMARY);
    public final static Component TOC = new Component(Type.TOC);

    public final Type type;
    public final String name;
    public final int hashCode;

    public Component(Type type)
    {
        this(type, type.repr);
        assert type != Type.CUSTOM;
    }

    public Component(Type type, String name)
    {
        assert name != null : "Component name cannot be null";
        this.type = type;
        this.name = name;
        this.hashCode = Objects.hashCode(type, name);
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
    public static Component parse(String name)
    {
        Type type = Type.fromRepresentation(name);
        Component c = get(type);

        if (c != null)
            return c;

        switch (type)
        {
            case SECONDARY_INDEX: return new Component(Type.SECONDARY_INDEX, name);
            case CUSTOM:          return new Component(Type.CUSTOM, name);
            default:              throw new AssertionError();
        }
    }

    public static Component get(Type type)
    {
        // Build (or retrieve singleton for) the component object
        switch (type)
        {
            case DATA:             return Component.DATA;
            case PRIMARY_INDEX:    return Component.PRIMARY_INDEX;
            case FILTER:           return Component.FILTER;
            case COMPRESSION_INFO: return Component.COMPRESSION_INFO;
            case STATS:            return Component.STATS;
            case DIGEST:           return Component.DIGEST;
            case CRC:              return Component.CRC;
            case SUMMARY:          return Component.SUMMARY;
            case TOC:              return Component.TOC;
            default:               return null;
        }
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
        Component that = (Component)o;
        return this.type == that.type && this.name.equals(that.name);
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }
}
