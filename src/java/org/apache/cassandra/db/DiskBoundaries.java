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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageService;

public class DiskBoundaries
{
    public final List<Directories.DataDirectory> directories;
    public final ImmutableList<PartitionPosition> positions;
    final long ringVersion;
    final int directoriesVersion;
    private final ColumnFamilyStore cfs;
    private volatile boolean isInvalid = false;

    public DiskBoundaries(ColumnFamilyStore cfs, Directories.DataDirectory[] directories, int diskVersion)
    {
        this(cfs, directories, null, -1, diskVersion);
    }

    @VisibleForTesting
    public DiskBoundaries(ColumnFamilyStore cfs, Directories.DataDirectory[] directories, List<PartitionPosition> positions, long ringVersion, int diskVersion)
    {
        this.directories = directories == null ? null : ImmutableList.copyOf(directories);
        this.positions = positions == null ? null : ImmutableList.copyOf(positions);
        this.ringVersion = ringVersion;
        this.directoriesVersion = diskVersion;
        this.cfs = cfs;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DiskBoundaries that = (DiskBoundaries) o;

        if (ringVersion != that.ringVersion) return false;
        if (directoriesVersion != that.directoriesVersion) return false;
        if (!directories.equals(that.directories)) return false;
        return positions != null ? positions.equals(that.positions) : that.positions == null;
    }

    public int hashCode()
    {
        int result = directories != null ? directories.hashCode() : 0;
        result = 31 * result + (positions != null ? positions.hashCode() : 0);
        result = 31 * result + (int) (ringVersion ^ (ringVersion >>> 32));
        result = 31 * result + directoriesVersion;
        return result;
    }

    public String toString()
    {
        return "DiskBoundaries{" +
               "directories=" + directories +
               ", positions=" + positions +
               ", ringVersion=" + ringVersion +
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
        long currentRingVersion = StorageService.instance.getTokenMetadata().getRingVersion();
        return currentDiskVersion != directoriesVersion || (ringVersion != -1 && currentRingVersion != ringVersion);
    }

    public void invalidate()
    {
        this.isInvalid = true;
    }

    public int getDiskIndex(SSTableReader sstable)
    {
        if (positions == null)
        {
            return getBoundariesFromSSTableDirectory(sstable.descriptor);
        }

        int pos = Collections.binarySearch(positions, sstable.getFirst());
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
        return directories.get(getDiskIndex(sstable));
    }

    public Directories.DataDirectory getCorrectDiskForKey(DecoratedKey key)
    {
        if (positions == null)
            return null;

        return directories.get(getDiskIndex(key));
    }

    public boolean isInCorrectLocation(SSTableReader sstable, Directories.DataDirectory currentLocation)
    {
        int diskIndex = getDiskIndex(sstable);
        PartitionPosition diskLast = positions.get(diskIndex);
        return directories.get(diskIndex).equals(currentLocation) && sstable.getLast().compareTo(diskLast) <= 0;
    }

    private int getDiskIndex(DecoratedKey key)
    {
        int pos = Collections.binarySearch(positions, key);
        assert pos < 0;
        return -pos - 1;
    }

    public List<Directories.DataDirectory> getDisksInBounds(DecoratedKey first, DecoratedKey last)
    {
        if (positions == null || first == null || last == null)
            return directories;
        int firstIndex = getDiskIndex(first);
        int lastIndex = getDiskIndex(last);
        return directories.subList(firstIndex, lastIndex + 1);
    }

    public boolean isEquivalentTo(DiskBoundaries oldBoundaries)
    {
        return oldBoundaries != null &&
               Objects.equals(positions, oldBoundaries.positions) &&
               Objects.equals(directories, oldBoundaries.directories);
    }
}
