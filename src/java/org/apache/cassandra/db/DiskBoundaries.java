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

package org.apache.cassandra.db;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class DiskBoundaries
{
    @Nullable public final List<Directories.DataDirectory> directories;
    @Nullable private final ImmutableList<PartitionPosition> positions;
    public final SortedLocalRanges localRanges;
    final int directoriesVersion;
    private final ColumnFamilyStore cfs;
    private volatile boolean isInvalid = false;

    public DiskBoundaries(ColumnFamilyStore cfs,
                          @Nullable Directories.DataDirectory[] directories,
                          SortedLocalRanges localRanges,
                          int diskVersion)
    {
        this(cfs, directories, null, localRanges, diskVersion);
    }

    public DiskBoundaries(ColumnFamilyStore cfs,
                          @Nullable Directories.DataDirectory[] directories,
                          @Nullable List<PartitionPosition> positions,
                          SortedLocalRanges localRanges,
                          int diskVersion)
    {
        this.directories = directories == null ? null : ImmutableList.copyOf(directories);
        this.positions = positions == null ? null : ImmutableList.copyOf(positions);
        this.localRanges = localRanges;
        this.directoriesVersion = diskVersion;
        this.cfs = cfs;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DiskBoundaries that = (DiskBoundaries) o;

        return Objects.equals(localRanges, that.localRanges) &&
               directoriesVersion == that.directoriesVersion &&
               Objects.equals(directories, that.directories) &&
               Objects.equals(positions, that.positions);
    }

    public int hashCode()
    {
        int result = directories != null ? directories.hashCode() : 0;
        result = 31 * result + (positions != null ? positions.hashCode() : 0);
        result = 31 * result + localRanges.hashCode();
        result = 31 * result + directoriesVersion;
        return result;
    }

    public String toString()
    {
        return "DiskBoundaries{" +
               "directories=" + directories +
               ", positions=" + positions +
               ", localRanges=" + localRanges.toString() +
               ", directoriesVersion=" + directoriesVersion +
               '}';
    }

    /**
     * check if the given disk boundaries are out of date due not being set or to having too old diskVersion/ringVersion
     */
    public boolean isOutOfDate()
    {
        if (isInvalid)
            return true;

        int currentDiskVersion = DisallowedDirectories.getDirectoriesVersion();
        return currentDiskVersion != directoriesVersion || localRanges.isOutOfDate();
    }

    public void invalidate()
    {
        this.isInvalid = true;
    }

    public int getDiskIndexFromKey(SSTableReader sstable)
    {
        if (positions == null)
        {
            return getBoundariesFromSSTableDirectory(sstable.descriptor);
        }

        int pos = Collections.binarySearch(positions, sstable.first);
        assert pos < 0; // boundaries are .minkeybound and .maxkeybound so they should never be equal
        return -pos - 1;
    }

    /**
     * Try to figure out location based on sstable directory
     */
    public int getBoundariesFromSSTableDirectory(Descriptor descriptor)
    {
        Directories.DataDirectory actualDirectory = cfs.getDirectories().getDataDirectoryForFile(descriptor);
        for (int i = 0; i < directories.size(); i++)
        {
            Directories.DataDirectory directory = directories.get(i);
            if (actualDirectory != null && actualDirectory.equals(directory))
                return i;
        }
        return 0;
    }

    public Directories.DataDirectory getCorrectDiskForSSTable(SSTableReader sstable)
    {
        return directories.get(getDiskIndexFromKey(sstable));
    }

    public Directories.DataDirectory getCorrectDiskForKey(DecoratedKey key)
    {
        if (positions == null)
            return null;

        return directories.get(getDiskIndexFromKey(key));
    }

    public boolean isInCorrectLocation(SSTableReader sstable, Directories.DataDirectory currentLocation)
    {
        int diskIndex = getDiskIndexFromKey(sstable);
        PartitionPosition diskLast = positions.get(diskIndex);
        return directories.get(diskIndex).equals(currentLocation) && sstable.last.compareTo(diskLast) <= 0;
    }

    /**
     * Return the number of boundaries. If this instance was created with token boundaries (positions) then this
     * is the number of boundaries. If this instance was created without boundaries but only with directories, then
     * this is the number of directories.
     *
     * @return the number of boundaries.
     */
    public int getNumBoundaries()
    {
        return positions == null ? directories.size() : positions.size();
    }

    private int getDiskIndexFromKey(DecoratedKey key)
    {
        int pos = Collections.binarySearch(positions, key);
        assert pos < 0;
        return -pos - 1;
    }

    /**
     * Return the local sorted ranges, which contain the local ranges for this node, sorted.
     * See {@link SortedLocalRanges}.
     *
     * @return the local ranges, see {@link SortedLocalRanges}.
     */
    public SortedLocalRanges getLocalRanges()
    {
        return localRanges;
    }

    /**
     * Returns a non-modifiable list of the disk boundary positions. This will be null if the token space is not split
     * for the disks, this is not normally the case).
     *
     * Extracted as a method (instead of direct access to the final field) to permit mocking in tests.
     */
    @Nullable
    public List<PartitionPosition> getPositions()
    {
        return positions;
    }
}
