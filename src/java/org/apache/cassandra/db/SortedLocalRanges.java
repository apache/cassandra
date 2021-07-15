/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

/**
 * This class contains the local ranges for a given table, sorted.
 */
public class SortedLocalRanges
{
    private static final Logger logger = LoggerFactory.getLogger(SortedLocalRanges.class);

    private final StorageService storageService;
    private final ColumnFamilyStore cfs;
    private final long ringVersion;
    private final List<Splitter.WeightedRange> ranges;
    private final Map<Integer, List<PartitionPosition>> splits;

    private volatile boolean valid;

    public SortedLocalRanges(StorageService storageService, ColumnFamilyStore cfs, long ringVersion, List<Splitter.WeightedRange> ranges)
    {
        this.storageService = storageService;
        this.cfs = cfs;
        this.ringVersion = ringVersion;

        List<Splitter.WeightedRange> sortedRanges = new ArrayList<>(ranges.size());
        for (Splitter.WeightedRange range : ranges)
        {
            for (Range<Token> unwrapped : range.range().unwrap())
            {
                sortedRanges.add(new Splitter.WeightedRange(range.weight(), unwrapped));
            }
        }
        sortedRanges.sort(Comparator.comparing(Splitter.WeightedRange::left));

        this.ranges = sortedRanges;
        this.splits = new ConcurrentHashMap<>();
        this.valid = true;
    }

    /**
     * Create a set of sorted local ranges based on the current token metadata and ring version.
     *
     * This method should preferably only be called by {@link ColumnFamilyStore} because later on,
     * ranges may need invalidating, see {@link this#invalidate()} and so a reference must be
     * kept to ranges that are passed around, and current cfs does this.
     */
    static SortedLocalRanges create(ColumnFamilyStore cfs)
    {
        StorageService storageService = StorageService.instance;
        RangesAtEndpoint localRanges;
        List<Splitter.WeightedRange> weightedRanges;
        long ringVersion;
        TokenMetadata tmd;

        do
        {
            tmd = storageService.getTokenMetadata();
            ringVersion = tmd.getRingVersion();
            localRanges = getLocalRanges(cfs, tmd);

            weightedRanges = new ArrayList<>(localRanges.size());
            for (Range<Token> r : localRanges.onlyFull().ranges())
                weightedRanges.add(new Splitter.WeightedRange(1.0, r));

            for (Range<Token> r : localRanges.onlyTransient().ranges())
                weightedRanges.add(new Splitter.WeightedRange(0.1, r));

            if (logger.isTraceEnabled())
                logger.trace("Got local ranges {} (ringVersion = {})", localRanges, ringVersion);
        }
        while (ringVersion != tmd.getRingVersion()); // if ringVersion is different here it means that
        // it might have changed before we calculated localRanges - recalculate

        return new SortedLocalRanges(storageService, cfs, ringVersion, weightedRanges);
    }

    private static RangesAtEndpoint getLocalRanges(ColumnFamilyStore cfs, TokenMetadata tmd)
    {
        RangesAtEndpoint localRanges;
        if (StorageService.instance.isBootstrapMode()
            && !StorageService.isReplacingSameAddress()) // When replacing same address, the node marks itself as UN locally
        {
            PendingRangeCalculatorService.instance.blockUntilFinished();
            localRanges = tmd.getPendingRanges(cfs.keyspace.getName(), FBUtilities.getBroadcastAddressAndPort());
        }
        else
        {
            // Reason we use use the future settled TMD is that if we decommission a node, we want to stream
            // from that node to the correct location on disk, if we didn't, we would put new files in the wrong places.
            // We do this to minimize the amount of data we need to move in rebalancedisks once everything settled
            localRanges = cfs.keyspace.getReplicationStrategy().getAddressReplicas(tmd.cloneAfterAllSettled(), FBUtilities.getBroadcastAddressAndPort());
        }
        return localRanges;
    }

    @VisibleForTesting
    public static SortedLocalRanges forTesting(ColumnFamilyStore cfs, List<Splitter.WeightedRange> ranges)
    {
        return new SortedLocalRanges(null, cfs, 0, ranges);
    }

    /**
     * check if the given disk boundaries are out of date due not being set or to having too old diskVersion/ringVersion
     */
    public boolean isOutOfDate()
    {
        return !valid || ringVersion != storageService.getTokenMetadata().getRingVersion();
    }

    public void invalidate()
    {
        this.valid = false;
    }

    public List<Splitter.WeightedRange> getRanges()
    {
        return ranges;
    }

    public long getRingVersion()
    {
        return ringVersion;
    }

    /**
     * Split the local ranges into the given number of parts.
     *
     * @param numParts the number of parts to split into
     *
     * @return a list of positions into which the local ranges were split
     */
    public List<PartitionPosition> split(int numParts)
    {
        return splits.computeIfAbsent(numParts, this::doSplit);
    }

    private List<PartitionPosition> doSplit(int numParts)
    {
        Splitter splitter = cfs.getPartitioner().splitter().orElse(null);

        List<Token> boundaries;
        if (splitter == null)
        {
            logger.debug("Could not split local ranges into {} parts for {}.{} (no splitter)", numParts, cfs.getKeyspaceName(), cfs.getTableName());
            boundaries = ranges.stream().map(Splitter.WeightedRange::right).collect(Collectors.toList());
        }
        else
        {
            logger.debug("Splitting local ranges into {} parts for {}.{}", numParts, cfs.getKeyspaceName(), cfs.getTableName());
            boundaries = splitter.splitOwnedRanges(numParts, ranges, Splitter.SplitType.ALWAYS_SPLIT).boundaries;
        }

        logger.debug("Boundaries for {}.{}: {} ({} splits)", cfs.getKeyspaceName(), cfs.getTableName(), boundaries, boundaries.size());
        return boundaries.stream().map(Token::maxKeyBound).collect(Collectors.toList());
    }

    /**
     * Returns the intersection of this list with the given range.
     */
    public List<Splitter.WeightedRange> subrange(Range<Token> range)
    {
        return ranges.stream()
                     .map(r -> {
                         Range<Token> subRange = r.range().intersectionNonWrapping(range);
                         return subRange == null ? null : new Splitter.WeightedRange(r.weight(), subRange);
                     })
                     .filter(Objects::nonNull)
                     .collect(Collectors.toList());
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SortedLocalRanges that = (SortedLocalRanges) o;
        if (ringVersion != that.ringVersion)
            return false;

        if (!cfs.equals(that.cfs))
            return false;

        return ranges.equals(that.ranges);
    }

    public int hashCode()
    {
        int result = cfs.hashCode();
        result = 31 * result + Long.hashCode(ringVersion);
        result = 31 * result + ranges.hashCode();
        return result;
    }

    public String toString()
    {
        return "LocalRanges{" +
               "table=" + cfs.getKeyspaceName() + "." + cfs.getTableName() +
               ", ring version=" + ringVersion +
               ", num ranges=" + ranges.size() + '}';
    }

    public ColumnFamilyStore getCfs()
    {
        return cfs;
    }
}
