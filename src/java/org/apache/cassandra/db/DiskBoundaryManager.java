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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;

public class DiskBoundaryManager
{
    private static final Logger logger = LoggerFactory.getLogger(DiskBoundaryManager.class);
    private volatile DiskBoundaries diskBoundaries;

    public DiskBoundaries getDiskBoundaries(ColumnFamilyStore cfs)
    {
        if (diskBoundaries == null || diskBoundaries.isOutOfDate())
        {
            synchronized (this)
            {
                if (diskBoundaries == null || diskBoundaries.isOutOfDate())
                {
                    logger.debug("Refreshing disk boundary cache for {}.{}", cfs.keyspace.getName(), cfs.getTableName());
                    SortedLocalRanges localRanges = cfs.getLocalRanges();

                    DiskBoundaries oldBoundaries = diskBoundaries;
                    diskBoundaries = !cfs.getPartitioner().splitter().isPresent()
                                     ? new DiskBoundaries(cfs, cfs.getDirectories().getWriteableLocations(), localRanges, DisallowedDirectories.getDirectoriesVersion())
                                     : getDiskBoundaryValue(cfs, localRanges);

                    logger.debug("Updating boundaries from {} to {} for {}.{}", oldBoundaries, diskBoundaries, cfs.keyspace.getName(), cfs.getTableName());
                }
            }
        }
        return diskBoundaries;
    }

    public void invalidate()
    {
       if (diskBoundaries != null)
           diskBoundaries.invalidate();
    }


    private static DiskBoundaries getDiskBoundaryValue(ColumnFamilyStore cfs, SortedLocalRanges localRanges)
    {
        int directoriesVersion;
        Directories.DataDirectory[] dirs;
        do
        {
            directoriesVersion = DisallowedDirectories.getDirectoriesVersion();
            dirs = cfs.getDirectories().getWriteableLocations();
        }
        while (directoriesVersion != DisallowedDirectories.getDirectoriesVersion()); // if directoriesVersion has changed we need to recalculate

        if (localRanges == null || localRanges.getRanges().isEmpty())
            return new DiskBoundaries(cfs, dirs, null, localRanges, directoriesVersion);

        List<PartitionPosition> positions = getDiskBoundaries(localRanges.getRanges(), cfs.getPartitioner(), dirs);
        return new DiskBoundaries(cfs, dirs, positions, localRanges, directoriesVersion);
    }

    /**
     * Returns a list of disk boundaries, the result will differ depending on whether vnodes are enabled or not.
     *
     * What is returned are upper bounds for the disks, meaning everything from partitioner.minToken up to
     * getDiskBoundaries(..).get(0) should be on the first disk, everything between 0 to 1 should be on the second disk
     * etc.
     *
     * The final entry in the returned list will always be the partitioner maximum tokens upper key bound
     */
    private static List<PartitionPosition> getDiskBoundaries(List<Splitter.WeightedRange> weightedRanges, IPartitioner partitioner, Directories.DataDirectory[] dataDirectories)
    {
        assert partitioner.splitter().isPresent();

        Splitter splitter = partitioner.splitter().get();
        Splitter.SplitType splitType = DatabaseDescriptor.getNumTokens() > 1 ? Splitter.SplitType.PREFER_WHOLE : Splitter.SplitType.ALWAYS_SPLIT;

        List<Token> boundaries = splitter.splitOwnedRanges(dataDirectories.length, weightedRanges, splitType).boundaries;
        assert boundaries.size() == dataDirectories.length : "Wrong number of boundaries for directories: " + boundaries.size();

        List<PartitionPosition> diskBoundaries = new ArrayList<>();
        for (int i = 0; i < boundaries.size() - 1; i++)
            diskBoundaries.add(boundaries.get(i).maxKeyBound());
        diskBoundaries.add(partitioner.getMaximumToken().maxKeyBound());
        return diskBoundaries;
    }
}
