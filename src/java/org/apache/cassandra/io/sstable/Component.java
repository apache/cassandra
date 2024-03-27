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

import java.io.File;
import java.util.EnumSet;
import java.util.List;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;

import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.Pair;

/**
 * SSTables are made up of multiple components in separate files. Components are
 * identified by a type and an id, but required unique components (such as the Data
 * and Index files) may have implicit ids assigned to them.
 */
public class Component
{
    public static final char separator = '-';
    private static final Splitter filenameSplitter = Splitter.on(separator);

    final static EnumSet<Type> TYPES = EnumSet.allOf(Type.class);

    public enum Type
    {
        // the base data for an sstable: the remaining components can be regenerated
        // based on the data component
        DATA("Data.db"),
        // index of the row keys with pointers to their positions in the data file
        PRIMARY_INDEX("Index.db"),
        // serialized bloom filter for the row keys in the sstable
        FILTER("Filter.db"),
        // file to hold information about uncompressed data length, chunk offsets etc.
        COMPRESSION_INFO("CompressionInfo.db"),
        // statistical metadata about the content of the sstable
        STATS("Statistics.db"),
        // holds adler32 checksum of the data file
        DIGEST("Digest.crc32", "Digest.adler32", "Digest.sha1"),
        // holds the CRC32 for chunks in an a uncompressed file.
        CRC("CRC.db"),
        // holds SSTable Index Summary (sampling of Index component)
        SUMMARY("Summary.db"),
        // table of contents, stores the list of all components for the sstable
        TOC("TOC.txt"),
        // built-in secondary index (may be multiple per sstable)
        SECONDARY_INDEX("SI_.*.db"),
        // custom component, used by e.g. custom compaction strategy
        CUSTOM(new String[] { null });
        
        final String[] repr;
        Type(String repr)
        {
            this(new String[] { repr });
        }

        Type(String... repr)
        {
            this.repr = repr;
        }

        @VisibleForTesting
        public static Type fromRepresentation(String repr)
        {
            for (Type type : TYPES)
            {
                if (type.repr == null || type.repr.length == 0 || type.repr[0] == null)
                    continue;
                if (Pattern.matches(type.repr[0], repr))
                    return type;
            }
            return CUSTOM;
        }
    }

    // singleton components for types that don't need ids
    public final static Component DATA = new Component(Type.DATA);
    public final static Component PRIMARY_INDEX = new Component(Type.PRIMARY_INDEX);
    public final static Component FILTER = new Component(Type.FILTER);
    public final static Component COMPRESSION_INFO = new Component(Type.COMPRESSION_INFO);
    public final static Component STATS = new Component(Type.STATS);
    private static final String digestCrc32 = "Digest.crc32";
    private static final String digestAdler32 = "Digest.adler32";
    private static final String digestSha1 = "Digest.sha1";
    public final static Component DIGEST_CRC32 = new Component(Type.DIGEST, digestCrc32);
    public final static Component DIGEST_ADLER32 = new Component(Type.DIGEST, digestAdler32);
    public final static Component DIGEST_SHA1 = new Component(Type.DIGEST, digestSha1);
    public final static Component CRC = new Component(Type.CRC);
    public final static Component SUMMARY = new Component(Type.SUMMARY);
    public final static Component TOC = new Component(Type.TOC);

    public static Component digestFor(ChecksumType checksumType)
    {
        switch (checksumType)
        {
            case Adler32:
                return DIGEST_ADLER32;
            case CRC32:
                return DIGEST_CRC32;
        }
        throw new AssertionError();
    }

    public final Type type;
    public final String name;
    public final int hashCode;

    public Component(Type type)
    {
        this(type, type.repr[0]);
        assert type.repr.length == 1;
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
          * Parse the component part of an sstable from the full filename passed in
          */
        public static Component parseFromFullFileName(String fullFileName)
    {
            List<String> nameSplits = filenameSplitter.splitToList(fullFileName);
            return parseFromFinalToken(nameSplits.get(nameSplits.size() - 1));
        }

    /**
     * Keeping this around for potential ecosystem dependencies on the API. Use parseFromFinalToken or parseFromFullFileName instead
     * @see Component#parseFromFinalToken
     */
    @Deprecated
    public static Component parse(String name)
    {
        return parseFromFinalToken(name);
    }

    public static Component parseFromFinalToken(String name)
    {
        Type type = Type.fromRepresentation(name);
        switch(type)
        {
            case DATA:              return Component.DATA;
            case PRIMARY_INDEX:     return Component.PRIMARY_INDEX;
            case FILTER:            return Component.FILTER;
            case COMPRESSION_INFO:  return Component.COMPRESSION_INFO;
            case STATS:             return Component.STATS;
            case DIGEST:            switch (name)
            {
                case digestCrc32:   return Component.DIGEST_CRC32;
                case digestAdler32: return Component.DIGEST_ADLER32;
                case digestSha1:    return Component.DIGEST_SHA1;
                default:            throw new IllegalArgumentException("Invalid digest component " + name);
            }
            case CRC:               return Component.CRC;
            case SUMMARY:           return Component.SUMMARY;
            case TOC:               return Component.TOC;
            case SECONDARY_INDEX:   return new Component(Type.SECONDARY_INDEX, name);
            case CUSTOM:            return new Component(Type.CUSTOM, name);
            default:
                throw new IllegalStateException();
        }
    }

    /**
     * {@code
     * Filename of the form "<ksname>/<cfname>-[tmp-][<version>-]<gen>-<component>",
     * }
     * @return A Descriptor for the SSTable, and a Component for this particular file.
     * TODO move descriptor into Component field
     */
    public static Pair<Descriptor,Component> fromFilename(File directory, String name)
    {
        Pair<Descriptor,String> path = Descriptor.fromFilename(directory, name);

        // build (or retrieve singleton for) the component object
        Component component = parseFromFinalToken(path.right);
        return Pair.create(path.left, component);
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
