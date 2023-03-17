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

package org.apache.cassandra.io.sstable.format;

import java.util.List;
import java.util.Objects;

import com.google.common.base.Splitter;

import org.apache.cassandra.io.sstable.Descriptor;

/**
 * Groups a sstable {@link Version} with a {@link SSTableFormat.Type}.
 *
 * <p>Note that both information are currently necessary to identify the exact "format" of an sstable (without having
 * its {@link Descriptor}). In particular, while {@link Version} contains its {{@link SSTableFormat}}, you cannot get
 * the {{@link SSTableFormat.Type}} from that.
 */
public final class VersionAndType
{
    private static final Splitter splitOnDash = Splitter.on('-').omitEmptyStrings().trimResults();

    private final Version version;
    private final SSTableFormat.Type formatType;

    public VersionAndType(Version version, SSTableFormat.Type formatType)
    {
        this.version = version;
        this.formatType = formatType;
    }

    public Version version()
    {
        return version;
    }

    public SSTableFormat.Type formatType()
    {
        return formatType;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        VersionAndType that = (VersionAndType) o;
        return Objects.equals(version, that.version) &&
               formatType == that.formatType;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(version, formatType);
    }

    public static VersionAndType fromString(String versionAndType)
    {
        List<String> components = splitOnDash.splitToList(versionAndType);
        if (components.size() != 2)
            throw new IllegalArgumentException("Invalid VersionAndType string: " + versionAndType + " (should be of the form 'big-bc')");

        SSTableFormat.Type formatType = SSTableFormat.Type.validate(components.get(0));
        Version version = formatType.info.getVersion(components.get(1));
        return new VersionAndType(version, formatType);
    }

    @Override
    public String toString()
    {
        return formatType.name + '-' + version;
    }
}
