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
package org.apache.cassandra.index.sai.disk.format;

import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.base.Objects;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;

/**
 * Format version of indexing component, denoted as [major][minor]. Same forward-compatibility rules apply as to
 * {@link org.apache.cassandra.io.sstable.format.Version}.
 */
public class Version implements Comparable<Version>
{
    public static final String SAI_DESCRIPTOR = "SAI";
    public static final String SAI_SEPARATOR = "+";

    // Current version
    public static final Version AA = new Version("aa", V1OnDiskFormat.instance, (c, i) -> defaultFileNameFormat(c, i, "aa"));

    // These should be added in reverse order so that the latest version is used first. Version matching tests
    // are more likely to match the latest version, so we want to test that one first.
    public static final SortedSet<Version> ALL = new TreeSet<>(Comparator.reverseOrder()) {{
        add(AA);
    }};

    public static final Version EARLIEST = AA;
    // The latest version can be configured to be an earlier version to support partial upgrades that don't
    // write newer versions of the on-disk formats.
    public static final Version LATEST = CassandraRelevantProperties.SAI_LATEST_VERSION.convert(Version::parse);

    private final String version;
    private final OnDiskFormat onDiskFormat;
    private final FileNameFormatter fileNameFormatter;

    private Version(String version, OnDiskFormat onDiskFormat, FileNameFormatter fileNameFormatter)
    {
        this.version = version;
        this.onDiskFormat = onDiskFormat;
        this.fileNameFormatter = fileNameFormatter;
    }

    public static Version parse(String versionString)
    {
        for (Version version : ALL)
            if (version.version.equals(versionString))
                return version;
        throw new IllegalArgumentException("The version string " + versionString + " does not represent a valid SAI version. " +
                                           "It should be one of " + ALL.stream().map(Version::toString).collect(Collectors.joining(", ")));
    }

    @Override
    public int compareTo(Version other)
    {
        return version.compareTo(other.version);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(version);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Version other = (Version)o;
        return Objects.equal(version, other.version);
    }

    @Override
    public String toString()
    {
        return version;
    }

    public boolean onOrAfter(Version other)
    {
        return version.compareTo(other.version) >= 0;
    }

    public OnDiskFormat onDiskFormat()
    {
        return onDiskFormat;
    }

    public Component makePerSSTableComponent(IndexComponent indexComponent)
    {
        return indexComponent.type.createComponent(fileNameFormatter.format(indexComponent, null));
    }

    public Component makePerIndexComponent(IndexComponent indexComponent, IndexIdentifier indexIdentifier)
    {
        return indexComponent.type.createComponent(fileNameFormatter.format(indexComponent, indexIdentifier));
    }

    public FileNameFormatter fileNameFormatter()
    {
        return fileNameFormatter;
    }

    public interface FileNameFormatter
    {
        String format(IndexComponent indexComponent, IndexIdentifier indexIdentifier);
    }

    /**
     * SAI default filename formatter. This is the current SAI on-disk filename format
     * <p>
     * Format: {@code <sstable descriptor>-SAI+<version>(+<index name>)+<component name>.db}
     * Note: The index name is excluded for per-SSTable index files that are shared
     * across all the per-column indexes for the SSTable.
     */
    private static String defaultFileNameFormat(IndexComponent indexComponent,
                                                @Nullable IndexIdentifier indexIdentifier,
                                                String version)
    {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(SAI_DESCRIPTOR);
        stringBuilder.append(SAI_SEPARATOR).append(version);
        if (indexIdentifier != null)
            stringBuilder.append(SAI_SEPARATOR).append(indexIdentifier.indexName);
        stringBuilder.append(SAI_SEPARATOR).append(indexComponent.name);
        stringBuilder.append(Descriptor.EXTENSION);

        return stringBuilder.toString();
    }
}
