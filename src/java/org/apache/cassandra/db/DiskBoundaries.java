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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageService;

public class DiskBoundaries
{
    public final List<Directories.DataDirectory> directories;
    public final ImmutableList<PartitionPosition> positions;
    final long ringVersion;
    final int directoriesVersion;
    private volatile boolean isInvalid = false;

    public DiskBoundaries(Directories.DataDirectory[] directories, int diskVersion)
    {
        this(directories, null, -1, diskVersion);
    }

    @VisibleForTesting
    public DiskBoundaries(Directories.DataDirectory[] directories, List<PartitionPosition> positions, long ringVersion, int diskVersion)
    {
        this.directories = directories == null ? null : ImmutableList.copyOf(directories);
        this.positions = positions == null ? null : ImmutableList.copyOf(positions);
        this.ringVersion = ringVersion;
        this.directoriesVersion = diskVersion;
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
        int currentDiskVersion = BlacklistedDirectories.getDirectoriesVersion();
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
            return getBoundariesFromSSTableDirectory(sstable);
        }

        int pos = Collections.binarySearch(positions, sstable.first);
        assert pos < 0; // boundaries are .minkeybound and .maxkeybound so they should never be equal
        return -pos - 1;
    }

    /**
     * Try to figure out location based on sstable directory
     */
    private int getBoundariesFromSSTableDirectory(SSTableReader sstable)
    {
        for (int i = 0; i < directories.size(); i++)
        {
            Directories.DataDirectory directory = directories.get(i);
            if (sstable.descriptor.directory.getAbsolutePath().startsWith(directory.location.getAbsolutePath()))
                return i;
        }
        return 0;
    }

    public Directories.DataDirectory getCorrectDiskForSSTable(SSTableReader sstable)
    {
        return directories.get(getDiskIndex(sstable));
    }
}