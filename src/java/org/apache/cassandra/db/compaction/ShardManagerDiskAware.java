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

package org.apache.cassandra.db.compaction;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;

public class ShardManagerDiskAware extends ShardManagerNoDisks
{
    /**
     * Positions for the disk boundaries, in covered token range. The last number defines the total token
     * share owned by the node.
     */
    private final double[] diskBoundaryPositions;
    private final int[] diskStartRangeIndex;
    private final List<Token> diskBoundaries;

    public ShardManagerDiskAware(ColumnFamilyStore.VersionedLocalRanges localRanges, List<Token> diskBoundaries)
    {
        super(localRanges);
        assert diskBoundaries != null && !diskBoundaries.isEmpty();
        this.diskBoundaries = diskBoundaries;

        double position = 0;
        final List<Splitter.WeightedRange> ranges = localRanges;
        int diskIndex = 0;
        diskBoundaryPositions = new double[diskBoundaries.size()];
        diskStartRangeIndex = new int[diskBoundaryPositions.length];
        diskStartRangeIndex[0] = 0;

        for (int i = 0; i < localRangePositions.length; ++i)
        {
            Range<Token> range = ranges.get(i).range();
            double weight = ranges.get(i).weight();
            double span = localRangePositions[i] - position;

            Token diskBoundary = diskBoundaries.get(diskIndex);
            while (diskIndex < diskBoundaryPositions.length - 1 && (range.right.isMinimum() || diskBoundary.compareTo(range.right) < 0))
            {
                double leftPart = range.left.size(diskBoundary) * weight;
                if (leftPart > span)    // if the boundary falls on left or before it
                    leftPart = 0;
                diskBoundaryPositions[diskIndex] = position + leftPart;
                diskStartRangeIndex[diskIndex + 1] = i;
                ++diskIndex;
                diskBoundary = diskBoundaries.get(diskIndex);
            }

            position += span;
        }
        diskBoundaryPositions[diskIndex] = position;
        assert diskIndex + 1 == diskBoundaryPositions.length : "Disk boundaries are not within local ranges";
    }

    @Override
    public double shardSetCoverage()
    {
        return localSpaceCoverage() / diskBoundaryPositions.length;
        // The above is an approximation that works correctly for the normal allocation of disks.
        // This can be properly calculated if a contained token is supplied as argument and the diskBoundaryPosition
        // difference is retrieved for the disk containing that token.
        // Unfortunately we don't currently have a way to get a representative position when an sstable writer is
        // constructed for flushing.
    }

    /**
     * Construct a boundary/shard iterator for the given number of shards.
     */
    public ShardTracker boundaries(int shardCount)
    {
        return new BoundaryTrackerDiskAware(shardCount);
    }

    public class BoundaryTrackerDiskAware implements ShardTracker
    {
        private final int countPerDisk;
        private double shardStep;
        private double diskStart;
        private int diskIndex;
        private int nextShardIndex;
        private int currentRange;
        private Token currentStart;
        @Nullable
        private Token currentEnd;   // null for the last shard

        public BoundaryTrackerDiskAware(int countPerDisk)
        {
            this.countPerDisk = countPerDisk;
            currentStart = localRanges.get(0).left();
            diskIndex = -1;
        }

        void enterDisk(int diskIndex)
        {
            this.diskIndex = diskIndex;
            currentRange = 0;
            diskStart = diskIndex > 0 ? diskBoundaryPositions[diskIndex - 1] : 0;
            shardStep = (diskBoundaryPositions[diskIndex] - diskStart) / countPerDisk;
            nextShardIndex = 1;
        }

        private Token getEndToken(double toPos)
        {
            double left = currentRange > 0 ? localRangePositions[currentRange - 1] : 0;
            double right = localRangePositions[currentRange];
            while (toPos > right)
            {
                left = right;
                right = localRangePositions[++currentRange];
            }

            final Range<Token> range = localRanges.get(currentRange).range();
            return currentStart.getPartitioner().split(range.left, range.right, (toPos - left) / (right - left));
        }

        public Token shardStart()
        {
            return currentStart;
        }

        public Token shardEnd()
        {
            return currentEnd;
        }

        public Range<Token> shardSpan()
        {
            return new Range<>(currentStart, currentEnd != null ? currentEnd : currentStart.minValue());
        }

        public double shardSpanSize()
        {
            return shardStep;
        }

        /**
         * Advance to the given token (e.g. before writing a key). Returns true if this resulted in advancing to a new
         * shard, and false otherwise.
         */
        public boolean advanceTo(Token nextToken)
        {
            if (diskIndex < 0)
            {
                int search = Collections.binarySearch(diskBoundaries, nextToken);
                if (search < 0)
                    search = -1 - search;
                // otherwise (on equal) we are good as ranges are end-inclusive
                enterDisk(search);
                setEndToken();
            }

            if (currentEnd == null || nextToken.compareTo(currentEnd) <= 0)
                return false;
            do
            {
                currentStart = currentEnd;
                if (nextShardIndex == countPerDisk)
                    enterDisk(diskIndex + 1);
                else
                    ++nextShardIndex;

                setEndToken();
            }
            while (!(currentEnd == null || nextToken.compareTo(currentEnd) <= 0));
            return true;
        }

        private void setEndToken()
        {
            if (nextShardIndex == countPerDisk)
            {
                if (diskIndex + 1 == diskBoundaryPositions.length)
                    currentEnd = null;
                else
                    currentEnd = diskBoundaries.get(diskIndex);
            }
            else
                currentEnd = getEndToken(diskStart + shardStep * nextShardIndex);
        }

        public int count()
        {
            return countPerDisk;
        }

        /**
         * Returns the fraction of the given token range's coverage that falls within this shard.
         * E.g. if the span covers two shards exactly and the current shard is one of them, it will return 0.5.
         */
        public double fractionInShard(Range<Token> targetSpan)
        {
            Range<Token> shardSpan = shardSpan();
            Range<Token> covered = targetSpan.intersectionNonWrapping(shardSpan);
            if (covered == null)
                return 0;
            if (covered == targetSpan)
                return 1;
            double inShardSize = covered == shardSpan ? shardSpanSize() : ShardManagerDiskAware.this.rangeSpanned(covered);
            double totalSize = ShardManagerDiskAware.this.rangeSpanned(targetSpan);
            return inShardSize / totalSize;
        }

        public double rangeSpanned(PartitionPosition first, PartitionPosition last)
        {
            return ShardManagerDiskAware.this.rangeSpanned(first, last);
        }

        public int shardIndex()
        {
            return nextShardIndex - 1;
        }
    }
}
