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

import java.util.List;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v2.V2OnDiskFormat;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Format version of indexing component, denoted as [major][minor]. Same forward-compatibility rules apply as to
 * {@link org.apache.cassandra.io.sstable.format.Version}.
 */
public class Version
{
    // 6.8 formats
    public static final Version AA = new Version("aa", V1OnDiskFormat.instance, Version::aaFileNameFormat);
    // Stargazer
    public static final Version BA = new Version("ba", V2OnDiskFormat.instance, (c, i) -> stargazerFileNameFormat(c, i, "ba"));

    // These are in reverse order so that the latest version is used first. Version matching tests
    // are more likely to match the latest version so we want to test that one first.
    public static final List<Version> ALL = Lists.newArrayList(AA, BA);

    public static final Version EARLIEST = AA;
    // The latest version can be configured to be an earlier version to support partial upgrades that don't
    // write newer versions of the on-disk formats.
    public static final Version LATEST = parse(System.getProperty("cassandra.sai.latest.version", "ba"));

    private final String version;
    private final OnDiskFormat onDiskFormat;
    private final FileNameFormatter fileNameFormatter;

    private Version(String version, OnDiskFormat onDiskFormat, FileNameFormatter fileNameFormatter)
    {
        this.version = version;
        this.onDiskFormat = onDiskFormat;
        this.fileNameFormatter = fileNameFormatter;
    }

    public static Version parse(String input)
    {
        checkArgument(input != null);
        checkArgument(input.length() == 2);
        if (input.equals(AA.version))
            return AA;
        if (input.equals(BA.version))
            return BA;
        throw new IllegalArgumentException();
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

    public FileNameFormatter fileNameFormatter()
    {
        return fileNameFormatter;
    }

    public static interface FileNameFormatter
    {
        public String format(IndexComponent indexComponent, IndexContext indexContext);
    }

    //
    // Version.AA filename formatter. This is the old DSE 6.8 SAI on-disk filename format
    //
    // Format: <sstable descriptor>-SAI(_<index name>)_<component name>.db
    //
    private static final String VERSION_AA_PER_SSTABLE_FORMAT = "SAI_%s.db";
    private static final String VERSION_AA_PER_INDEX_FORMAT = "SAI_%s_%s.db";

    private static String aaFileNameFormat(IndexComponent indexComponent, IndexContext indexContext)
    {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(indexContext == null ? String.format(VERSION_AA_PER_SSTABLE_FORMAT, indexComponent.representation)
                                                  : String.format(VERSION_AA_PER_INDEX_FORMAT, indexContext.getIndexName(), indexComponent.representation));

        return stringBuilder.toString();
    }

    //
    // Stargazer filename formatter. This is the current SAI on-disk filename format
    //
    // Format: <sstable descriptor>-SAI+<version>(+<index name>)+<component name>.db
    //
    private static final String SAI_DESCRIPTOR = "SAI";
    private static final String SAI_SEPARATOR = "+";
    private static final String EXTENSION = ".db";

    private static String stargazerFileNameFormat(IndexComponent indexComponent, IndexContext indexContext, String version)
    {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(SAI_DESCRIPTOR);
        stringBuilder.append(SAI_SEPARATOR).append(version);
        if (indexContext != null)
            stringBuilder.append(SAI_SEPARATOR).append(indexContext.getIndexName());
        stringBuilder.append(SAI_SEPARATOR).append(indexComponent.representation);
        stringBuilder.append(EXTENSION);

        return stringBuilder.toString();
    }

}
