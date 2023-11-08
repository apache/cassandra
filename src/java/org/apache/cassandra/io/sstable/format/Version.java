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

import java.util.Objects;
import java.util.regex.Pattern;


/**
 * A set of feature flags associated with a SSTable format
 * <p>
 * versions are denoted as [major][minor].  Minor versions must be forward-compatible:
 * new fields are allowed in e.g. the metadata component, but fields can't be removed
 * or have their size changed.
 * <p>
 * Minor versions were introduced with version "hb" for Cassandra 1.0.3; prior to that,
 * we always incremented the major version.
 */
public abstract class Version
{
    private static final Pattern VALIDATION = Pattern.compile("[a-z]+");

    public final String version;
    public final SSTableFormat<?, ?> format;

    protected Version(SSTableFormat format, String version)
    {
        this.format = Objects.requireNonNull(format);
        this.version = Objects.requireNonNull(version);
    }

    public abstract boolean isLatestVersion();

    public abstract int correspondingMessagingVersion(); // Only use by storage that 'storeRows' so far

    public abstract boolean hasCommitLogLowerBound();

    public abstract boolean hasCommitLogIntervals();

    public abstract boolean hasMaxCompressedLength();

    public abstract boolean hasPendingRepair();

    public abstract boolean hasIsTransient();

    public abstract boolean hasMetadataChecksum();
    
    /**
     * This format raises the legacy int year 2038 limit to 2106 by using an uint instead
     */
    public abstract boolean hasUIntDeletionTime();

    /**
     * The old bloomfilter format serializes the data as BIG_ENDIAN long's, the new one uses the
     * same format as in memory (serializes as bytes).
     *
     * @return True if the bloomfilter file is old serialization format
     */
    public abstract boolean hasOldBfFormat();

    /**
     * @deprecated it is replaced by {@link #hasImprovedMinMax()} since 'nc' and to be completetly removed since 'oa'
     */
    /** @deprecated See CASSANDRA-18134 */
    @Deprecated(since = "5.0")
    public abstract boolean hasAccurateMinMax();

    /**
     * @deprecated it is replaced by {@link #hasImprovedMinMax()} since 'nc' and to be completetly removed since 'oa'
     */
    /** @deprecated See CASSANDRA-18134 */
    @Deprecated(since = "5.0")
    public abstract boolean hasLegacyMinMax();

    public abstract boolean hasOriginatingHostId();

    public abstract boolean hasImprovedMinMax();

    /**
     * If the sstable has token space coverage data.
     */
    public abstract boolean hasTokenSpaceCoverage();

    /**
     * Records in th stats if the sstable has any partition deletions.
     */
    public abstract boolean hasPartitionLevelDeletionsPresenceMarker();

    public abstract boolean hasKeyRange();

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

    public String toFormatAndVersionString()
    {
        return format.name() + '-' + version;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;

        Version otherVersion = (Version) other;
        return Objects.equals(version, otherVersion.version) && Objects.equals(format.name(), otherVersion.format.name());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(version, format.name());
    }
}
