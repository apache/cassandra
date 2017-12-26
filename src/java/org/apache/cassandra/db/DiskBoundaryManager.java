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
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class DiskBoundaryManager
{
    private static final Logger logger = LoggerFactory.getLogger(DiskBoundaryManager.class);
    private volatile DiskBoundaries diskBoundaries;

    public DiskBoundaries getDiskBoundaries(ColumnFamilyStore cfs)
    {
        if (!cfs.getPartitioner().splitter().isPresent())
            return new DiskBoundaries(cfs.getDirectories().getWriteableLocations(), BlacklistedDirectories.getDirectoriesVersion());
        if (diskBoundaries == null || diskBoundaries.isOutOfDate())
        {
            synchronized (this)
            {
                if (diskBoundaries == null || diskBoundaries.isOutOfDate())
                {
                    logger.debug("Refreshing disk boundary cache for {}.{}", cfs.keyspace.getName(), cfs.getTableName());
                    DiskBoundaries oldBoundaries = diskBoundaries;
                    diskBoundaries = getDiskBoundaryValue(cfs);
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

    private static DiskBoundaries getDiskBoundaryValue(ColumnFamilyStore cfs)
    {
        Collection<Range<Token>> localRanges;

        long ringVersion;
        TokenMetadata tmd;
        do
        {
            tmd = StorageService.instance.getTokenMetadata();
            ringVersion = tmd.getRingVersion();
            if (StorageService.instance.isBootstrapMode()
                && !StorageService.isReplacingSameAddress()) // When replacing same address, the node marks itself as UN locally
            {
                PendingRangeCalculatorService.instance.blockUntilFinished();
                localRanges = tmd.getPendingRanges(cfs.keyspace.getName(), FBUtilities.getBroadcastAddress());
            }
            else
            {
                // Reason we use use the future settled TMD is that if we decommission a node, we want to stream
                // from that node to the correct location on disk, if we didn't, we would put new files in the wrong places.
                // We do this to minimize the amount of data we need to move in rebalancedisks once everything settled
                localRanges = cfs.keyspace.getReplicationStrategy().getAddressRanges(tmd.cloneAfterAllSettled()).get(FBUtilities.getBroadcastAddress());
            }
            logger.debug("Got local ranges {} (ringVersion = {})", localRanges, ringVersion);
        }
        while (ringVersion != tmd.getRingVersion()); // if ringVersion is different here it means that
                                                     // it might have changed before we calculated localRanges - recalculate

        int directoriesVersion;
        Directories.DataDirectory[] dirs;
        do
        {
            directoriesVersion = BlacklistedDirectories.getDirectoriesVersion();
            dirs = cfs.getDirectories().getWriteableLocations();
        }
        while (directoriesVersion != BlacklistedDirectories.getDirectoriesVersion()); // if directoriesVersion has changed we need to recalculate

        if (localRanges == null || localRanges.isEmpty())
            return new DiskBoundaries(dirs, null, ringVersion, directoriesVersion);

        List<Range<Token>> sortedLocalRanges = Range.sort(localRanges);

        List<PartitionPosition> positions = getDiskBoundaries(sortedLocalRanges, cfs.getPartitioner(), dirs);
        return new DiskBoundaries(dirs, positions, ringVersion, directoriesVersion);
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
    private static List<PartitionPosition> getDiskBoundaries(List<Range<Token>> sortedLocalRanges, IPartitioner partitioner, Directories.DataDirectory[] dataDirectories)
    {
        assert partitioner.splitter().isPresent();
        Splitter splitter = partitioner.splitter().get();
        boolean dontSplitRanges = DatabaseDescriptor.getNumTokens() > 1;
        List<Token> boundaries = splitter.splitOwnedRanges(dataDirectories.length, sortedLocalRanges, dontSplitRanges);
        // If we can't split by ranges, split evenly to ensure utilisation of all disks
        if (dontSplitRanges && boundaries.size() < dataDirectories.length)
            boundaries = splitter.splitOwnedRanges(dataDirectories.length, sortedLocalRanges, false);

        List<PartitionPosition> diskBoundaries = new ArrayList<>();
        for (int i = 0; i < boundaries.size() - 1; i++)
            diskBoundaries.add(boundaries.get(i).maxKeyBound());
        diskBoundaries.add(partitioner.getMaximumToken().maxKeyBound());
        return diskBoundaries;
    }
}
