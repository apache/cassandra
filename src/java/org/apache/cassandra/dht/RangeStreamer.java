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
package org.apache.cassandra.dht;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.cassandra.streaming.IStreamCallback;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.streaming.OperationType;
import org.apache.cassandra.streaming.StreamIn;

/**
 * Assists in streaming ranges to a node.
 */
public class RangeStreamer
{
    private static final Logger logger = LoggerFactory.getLogger(RangeStreamer.class);

    private final TokenMetadata metadata;
    private final InetAddress address;
    private final OperationType opType;
    private final Multimap<String, Map.Entry<InetAddress, Collection<Range<Token>>>> toFetch = HashMultimap.create();
    private final Set<ISourceFilter> sourceFilters = new HashSet<ISourceFilter>();
    // protected for testing.
    protected CountDownLatch latch;
    private Set<Range<Token>> completed = Collections.newSetFromMap(new ConcurrentHashMap<Range<Token>, Boolean>());

    /**
     * A filter applied to sources to stream from when constructing a fetch map.
     */
    public static interface ISourceFilter
    {
        public boolean shouldInclude(InetAddress endpoint);
    }

    /**
     * Source filter which excludes any endpoints that are not alive according to a
     * failure detector.
     */
    public static class FailureDetectorSourceFilter implements ISourceFilter
    {
        private final IFailureDetector fd;

        public FailureDetectorSourceFilter(IFailureDetector fd)
        {
            this.fd = fd;
        }

        public boolean shouldInclude(InetAddress endpoint)
        {
            return fd.isAlive(endpoint);
        }
    }

    /**
     * Source filter which excludes any endpoints that are not in a specific data center.
     */
    public static class SingleDatacenterFilter implements ISourceFilter
    {
        private final String sourceDc;
        private final IEndpointSnitch snitch;

        public SingleDatacenterFilter(IEndpointSnitch snitch, String sourceDc)
        {
            this.sourceDc = sourceDc;
            this.snitch = snitch;
        }

        public boolean shouldInclude(InetAddress endpoint)
        {
            return snitch.getDatacenter(endpoint).equals(sourceDc);
        }
    }

    public RangeStreamer(TokenMetadata metadata, InetAddress address, OperationType opType)
    {
        this.metadata = metadata;
        this.address = address;
        this.opType = opType;
    }

    public void addSourceFilter(ISourceFilter filter)
    {
        sourceFilters.add(filter);
    }

    public void addRanges(String table, Collection<Range<Token>> ranges)
    {
        Multimap<Range<Token>, InetAddress> rangesForTable = getAllRangesWithSourcesFor(table, ranges);

        if (logger.isDebugEnabled())
        {
            for (Map.Entry<Range<Token>, InetAddress> entry: rangesForTable.entries())
                logger.debug(String.format("%s: range %s exists on %s", opType, entry.getKey(), entry.getValue()));
        }

        for (Map.Entry<InetAddress, Collection<Range<Token>>> entry : getRangeFetchMap(rangesForTable, sourceFilters).asMap().entrySet())
        {
            if (logger.isDebugEnabled())
            {
                for (Range r : entry.getValue())
                    logger.debug(String.format("%s: range %s from source %s for table %s", opType, r, entry.getKey(), table));
            }
            toFetch.put(table, entry);
        }
    }

    /**
     * Get a map of all ranges and their respective sources that are candidates for streaming the given ranges
     * to us. For each range, the list of sources is sorted by proximity relative to the given destAddress.
     */
    private Multimap<Range<Token>, InetAddress> getAllRangesWithSourcesFor(String table, Collection<Range<Token>> desiredRanges)
    {
        AbstractReplicationStrategy strat = Table.open(table).getReplicationStrategy();
        Multimap<Range<Token>, InetAddress> rangeAddresses = strat.getRangeAddresses(metadata.cloneOnlyTokenMap());

        Multimap<Range<Token>, InetAddress> rangeSources = ArrayListMultimap.create();
        for (Range<Token> desiredRange : desiredRanges)
        {
            for (Range<Token> range : rangeAddresses.keySet())
            {
                if (range.contains(desiredRange))
                {
                    List<InetAddress> preferred = DatabaseDescriptor.getEndpointSnitch().getSortedListByProximity(address, rangeAddresses.get(range));
                    rangeSources.putAll(desiredRange, preferred);
                    break;
                }
            }

            if (!rangeSources.keySet().contains(desiredRange))
                throw new IllegalStateException("No sources found for " + desiredRange);
        }

        return rangeSources;
    }

    /**
     * @param rangesWithSources The ranges we want to fetch (key) and their potential sources (value)
     * @param sourceFilters A (possibly empty) collection of source filters to apply. In addition to any filters given
     *                      here, we always exclude ourselves.
     * @return
     */
    private static Multimap<InetAddress, Range<Token>> getRangeFetchMap(Multimap<Range<Token>, InetAddress> rangesWithSources,
                                                                        Collection<ISourceFilter> sourceFilters)
    {
        Multimap<InetAddress, Range<Token>> rangeFetchMapMap = HashMultimap.create();
        for (Range<Token> range : rangesWithSources.keySet())
        {
            boolean foundSource = false;

            outer:
            for (InetAddress address : rangesWithSources.get(range))
            {
                if (address.equals(FBUtilities.getBroadcastAddress()))
                {
                    // If localhost is a source, we have found one, but we don't add it to the map to avoid streaming locally
                    foundSource = true;
                    continue;
                }

                for (ISourceFilter filter : sourceFilters)
                {
                    if (!filter.shouldInclude(address))
                        continue outer;
                }

                rangeFetchMapMap.put(address, range);
                foundSource = true;
                break; // ensure we only stream from one other node for each range
            }

            if (!foundSource)
                throw new IllegalStateException("unable to find sufficient sources for streaming range " + range);
        }

        return rangeFetchMapMap;
    }

    public static Multimap<InetAddress, Range<Token>> getWorkMap(Multimap<Range<Token>, InetAddress> rangesWithSourceTarget)
    {
        return getRangeFetchMap(rangesWithSourceTarget, Collections.<ISourceFilter>singleton(new FailureDetectorSourceFilter(FailureDetector.instance)));
    }

    // For testing purposes
    Multimap<String, Map.Entry<InetAddress, Collection<Range<Token>>>> toFetch()
    {
        return toFetch;
    }

    public void fetch()
    {
        latch = new CountDownLatch(toFetch.entries().size());

        for (Map.Entry<String, Map.Entry<InetAddress, Collection<Range<Token>>>> entry : toFetch.entries())
        {
            final String table = entry.getKey();
            final InetAddress source = entry.getValue().getKey();
            final Collection<Range<Token>> ranges = entry.getValue().getValue();
            /* Send messages to respective folks to stream data over to me */
            IStreamCallback callback = new IStreamCallback()
            {
                public void onSuccess()
                {
                    completed.addAll(ranges);
                    latch.countDown();
                    if (logger.isDebugEnabled())
                        logger.debug(String.format("Removed %s/%s as a %s source; remaining is %s",
                                     source, table, opType, latch.getCount()));
                }

                public void onFailure()
                {
                    latch.countDown();
                    logger.warn("Streaming from " + source + " failed");
                }
            };
            if (logger.isDebugEnabled())
                logger.debug("" + opType + "ing from " + source + " ranges " + StringUtils.join(ranges, ", "));
            StreamIn.requestRanges(source, table, ranges, callback, opType);
        }

        try
        {
            latch.await();
            for (Map.Entry<String, Map.Entry<InetAddress, Collection<Range<Token>>>> entry : toFetch.entries())
            {
                if (!completed.containsAll(entry.getValue().getValue()))
                    throw new RuntimeException(String.format("Unable to fetch range %s for keyspace %s from any hosts", entry.getValue().getValue(), entry.getKey()));
            }
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }
}
