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

package org.apache.cassandra.index.accord;

import java.util.Collection;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;

public class IndexDescriptor
{
    public enum Version
    {
        v1("aa", c -> defaultFileNameFormat(c, "aa"));

        public static final Version CURRENT = v1;

        public final String versionString;
        public final Function<IndexComponent, String> fileNameFormatter;

        Version(String versionString, Function<IndexComponent, String> fileNameFormatter)
        {
            this.versionString = versionString;
            this.fileNameFormatter = fileNameFormatter;
        }


        private static String defaultFileNameFormat(IndexComponent indexComponent, String version)
        {
            StringBuilder sb = new StringBuilder();

            sb.append(IndexComponent.DESCRIPTOR).append(IndexComponent.SEPARATOR)
              .append(version).append(IndexComponent.SEPARATOR)
              .append(indexComponent.name).append(Descriptor.EXTENSION);

            return sb.toString();
        }
    }

    public enum IndexComponent
    {
        CINTIA_SORTED_LIST("CintiaSortedList", (byte) 1),
        CINTIA_CHECKPOINTS("CintiaCheckpoints", (byte) 2),
        SEGMENT("Segement", (byte) 3),
        METADATA("Metadata", (byte) 4);

        public static final String DESCRIPTOR = "ACCORD";
        public static final String SEPARATOR = "+";

        public final String name;
        public final Component.Type type;
        public final byte value;

        IndexComponent(String name, byte value)
        {
            this.name = name;
            this.type = componentType(name);
            this.value = value;
        }

        private static Component.Type componentType(String name)
        {
            String componentName = DESCRIPTOR + SEPARATOR + name;
            String repr = Pattern.quote(DESCRIPTOR + SEPARATOR)
                          + ".*"
                          + Pattern.quote(SEPARATOR + name + ".db");
            return Component.Type.create(componentName, repr, true, null);
        }

        public static IndexComponent fromByte(byte b)
        {
            switch (b)
            {
                case 1: return CINTIA_SORTED_LIST;
                case 2: return CINTIA_CHECKPOINTS;
                case 3: return SEGMENT;
                case 4: return METADATA;
                default:throw new IllegalArgumentException("Unknow byte: " + b);
            }
        }
    }

    public final Version version;
    public final Descriptor sstableDescriptor;
    public final IPartitioner partitioner;
    public final ClusteringComparator clusteringComparator;

    public IndexDescriptor(Version version, Descriptor sstableDescriptor, IPartitioner partitioner, ClusteringComparator clusteringComparator)
    {
        this.version = version;
        this.sstableDescriptor = sstableDescriptor;
        this.partitioner = partitioner;
        this.clusteringComparator = clusteringComparator;
    }

    public static IndexDescriptor create(SSTableReader sstable)
    {
        for (Version version : Version.values())
        {
            IndexDescriptor id = new IndexDescriptor(version, sstable.descriptor, sstable.getPartitioner(), sstable.metadata().comparator);
            if (id.isIndexBuildComplete())
                return id;
        }
        return new IndexDescriptor(Version.CURRENT, sstable.descriptor, sstable.getPartitioner(), sstable.metadata().comparator);
    }

    public static IndexDescriptor create(Descriptor descriptor, IPartitioner partitioner, ClusteringComparator comparator)
    {
        return new IndexDescriptor(Version.CURRENT, descriptor, partitioner, comparator);
    }

    public boolean isIndexBuildComplete()
    {
        return hasComponent(IndexComponent.METADATA);
    }

    public boolean hasComponent(IndexComponent indexComponent)
    {
        return fileFor(indexComponent).exists();
    }

    public File fileFor(IndexComponent indexComponent)
    {
        Component c = indexComponent.type.createComponent(version.fileNameFormatter.apply(indexComponent));
        return sstableDescriptor.fileFor(c);
    }

    public void deleteIndex()
    {
        Stream.of(IndexComponent.values()).map(this::fileFor).forEach(File::deleteIfExists);
    }

    public Collection<Component> getLiveSSTableComponents()
    {
        return Stream.of(IndexComponent.values())
                     .map(c -> c.type.createComponent(version.fileNameFormatter.apply(c)))
                     .filter(c -> sstableDescriptor.fileFor(c).exists())
                     .collect(Collectors.toList());
    }

    public Collection<IndexDescriptor.IndexComponent> getLiveComponents()
    {
        return Stream.of(IndexComponent.values())
                     .filter(c -> fileFor(c).exists())
                     .collect(Collectors.toList());
    }
}
