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

import java.util.regex.Pattern;

import org.apache.cassandra.utils.ChecksumType;

/**
 * A set of feature flags associated with a SSTable format
 *
 * versions are denoted as [major][minor].  Minor versions must be forward-compatible:
 * new fields are allowed in e.g. the metadata component, but fields can't be removed
 * or have their size changed.
 *
 * Minor versions were introduced with version "hb" for Cassandra 1.0.3; prior to that,
 * we always incremented the major version.
 *
 */
public abstract class Version
{
    private static final Pattern VALIDATION = Pattern.compile("[a-z]+");

    protected final String version;
    protected final SSTableFormat format;
    protected Version(SSTableFormat format, String version)
    {
        this.format = format;
        this.version = version;
    }

    public abstract boolean isLatestVersion();

    public abstract boolean hasSamplingLevel();

    public abstract boolean hasNewStatsFile();

    public abstract ChecksumType compressedChecksumType();

    public abstract ChecksumType uncompressedChecksumType();

    public abstract boolean hasRepairedAt();

    public abstract boolean tracksLegacyCounterShards();

    public abstract boolean hasNewFileName();

    public abstract boolean storeRows();

    public abstract int correspondingMessagingVersion(); // Only use by storage that 'storeRows' so far

    public abstract boolean hasOldBfHashOrder();

    public abstract boolean hasCompactionAncestors();

    public abstract boolean hasBoundaries();

    public String getVersion()
    {
        return version;
    }

    public SSTableFormat getSSTableFormat()
    {
        return format;
    }

    /**
     * @param ver SSTable version
     * @return True if the given version string matches the format.
     * @see #version
     */
    public static boolean validate(String ver)
    {
        return ver != null && VALIDATION.matcher(ver).matches();
    }

    abstract public boolean isCompatible();
    abstract public boolean isCompatibleForStreaming();

    @Override
    public String toString()
    {
        return version;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Version version1 = (Version) o;

        if (version != null ? !version.equals(version1.version) : version1.version != null) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        return version != null ? version.hashCode() : 0;
    }
}
