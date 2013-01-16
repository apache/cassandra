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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ReadRepairDecision;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.transport.ProtocolException;


public enum ConsistencyLevel
{
    ANY         (0),
    ONE         (1),
    TWO         (2),
    THREE       (3),
    QUORUM      (4),
    ALL         (5),
    LOCAL_QUORUM(6),
    EACH_QUORUM (7);

    private static final Logger logger = LoggerFactory.getLogger(ConsistencyLevel.class);

    // Used by the binary protocol
    public final int code;
    private static final ConsistencyLevel[] codeIdx;
    static
    {
        int maxCode = -1;
        for (ConsistencyLevel cl : ConsistencyLevel.values())
            maxCode = Math.max(maxCode, cl.code);
        codeIdx = new ConsistencyLevel[maxCode + 1];
        for (ConsistencyLevel cl : ConsistencyLevel.values())
        {
            if (codeIdx[cl.code] != null)
                throw new IllegalStateException("Duplicate code");
            codeIdx[cl.code] = cl;
        }
    }

    private ConsistencyLevel(int code)
    {
        this.code = code;
    }

    public static ConsistencyLevel fromCode(int code)
    {
        if (code < 0 || code >= codeIdx.length)
            throw new ProtocolException(String.format("Unknown code %d for a consistency level", code));
        return codeIdx[code];
    }

    private int localQuorumFor(Table table, String dc)
    {
        return (((NetworkTopologyStrategy) table.getReplicationStrategy()).getReplicationFactor(dc) / 2) + 1;
    }

    public int blockFor(Table table)
    {
        switch (this)
        {
            case ONE:
                return 1;
            case ANY:
                return 1;
            case TWO:
                return 2;
            case THREE:
                return 3;
            case QUORUM:
                return (table.getReplicationStrategy().getReplicationFactor() / 2) + 1;
            case ALL:
                return table.getReplicationStrategy().getReplicationFactor();
            case LOCAL_QUORUM:
                return localQuorumFor(table, DatabaseDescriptor.getLocalDataCenter());
            case EACH_QUORUM:
                NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) table.getReplicationStrategy();
                int n = 0;
                for (String dc : strategy.getDatacenters())
                    n += localQuorumFor(table, dc);
                return n;
            default:
                throw new UnsupportedOperationException("Invalid consistency level: " + toString());
        }
    }

    private boolean isLocal(InetAddress endpoint)
    {
        return DatabaseDescriptor.getLocalDataCenter().equals(DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint));
    }

    private int countLocalEndpoints(Iterable<InetAddress> liveEndpoints)
    {
        int count = 0;
        for (InetAddress endpoint : liveEndpoints)
            if (isLocal(endpoint))
                count++;
        return count;
    }

    private Map<String, Integer> countPerDCEndpoints(Table table, Iterable<InetAddress> liveEndpoints)
    {
        NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) table.getReplicationStrategy();

        Map<String, Integer> dcEndpoints = new HashMap<String, Integer>();
        for (String dc: strategy.getDatacenters())
            dcEndpoints.put(dc, 0);

        for (InetAddress endpoint : liveEndpoints)
        {
            String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint);
            dcEndpoints.put(dc, dcEndpoints.get(dc) + 1);
        }
        return dcEndpoints;
    }

    public List<InetAddress> filterForQuery(Table table, List<InetAddress> liveEndpoints)
    {
        return filterForQuery(table, liveEndpoints, ReadRepairDecision.NONE);
    }

    public List<InetAddress> filterForQuery(Table table, List<InetAddress> liveEndpoints, ReadRepairDecision readRepair)
    {
        /*
         * Endpoints are expected to be restricted to live replicas, sorted by snitch preference.
         * For LOCAL_QORUM, move local-DC replicas in front first as we need them there whether
         * we do read repair (since the first replica gets the data read) or not (since we'll take
         * the blockFor first ones).
         */
        if (this == LOCAL_QUORUM)
            Collections.sort(liveEndpoints, DatabaseDescriptor.getLocalComparator());

        switch (readRepair)
        {
            case NONE:
                return liveEndpoints.subList(0, Math.min(liveEndpoints.size(), blockFor(table)));
            case GLOBAL:
                return liveEndpoints;
            case DC_LOCAL:
                List<InetAddress> local = new ArrayList<InetAddress>();
                List<InetAddress> other = new ArrayList<InetAddress>();
                for (InetAddress add : liveEndpoints)
                {
                    if (isLocal(add))
                        local.add(add);
                    else
                        other.add(add);
                }
                // check if blockfor more than we have localep's
                int blockFor = blockFor(table);
                if (local.size() < blockFor)
                    local.addAll(other.subList(0, Math.min(blockFor - local.size(), other.size())));
                return local;
            default:
                throw new AssertionError();
        }
    }

    public boolean isSufficientLiveNodes(Table table, Iterable<InetAddress> liveEndpoints)
    {
        switch (this)
        {
            case ANY:
                // local hint is acceptable, and local node is always live
                return true;
            case LOCAL_QUORUM:
                return countLocalEndpoints(liveEndpoints) >= blockFor(table);
            case EACH_QUORUM:
                for (Map.Entry<String, Integer> entry : countPerDCEndpoints(table, liveEndpoints).entrySet())
                {
                    if (entry.getValue() < localQuorumFor(table, entry.getKey()))
                        return false;
                }
                return true;
            default:
                return Iterables.size(liveEndpoints) >= blockFor(table);
        }
    }

    public void assureSufficientLiveNodes(Table table, Iterable<InetAddress> liveEndpoints) throws UnavailableException
    {
        int blockFor = blockFor(table);
        switch (this)
        {
            case ANY:
                // local hint is acceptable, and local node is always live
                break;
            case LOCAL_QUORUM:
                int localLive = countLocalEndpoints(liveEndpoints);
                if (localLive < blockFor)
                {
                    if (logger.isDebugEnabled())
                    {
                        StringBuilder builder = new StringBuilder("Local replicas [");
                        for (InetAddress endpoint : liveEndpoints)
                        {
                            if (isLocal(endpoint))
                                builder.append(endpoint).append(",");
                        }
                        builder.append("] are insufficient to satisfy LOCAL_QUORUM requirement of ").append(blockFor).append(" live nodes in '").append(DatabaseDescriptor.getLocalDataCenter()).append("'");
                        logger.debug(builder.toString());
                    }
                    throw new UnavailableException(this, blockFor, localLive);
                }
                break;
            case EACH_QUORUM:
                for (Map.Entry<String, Integer> entry : countPerDCEndpoints(table, liveEndpoints).entrySet())
                {
                    int dcBlockFor = localQuorumFor(table, entry.getKey());
                    int dcLive = entry.getValue();
                    if (dcLive < dcBlockFor)
                        throw new UnavailableException(this, dcBlockFor, dcLive);
                }
                break;
            default:
                int live = Iterables.size(liveEndpoints);
                if (live < blockFor)
                {
                    logger.debug("Live nodes {} do not satisfy ConsistencyLevel ({} required)", Iterables.toString(liveEndpoints), blockFor);
                    throw new UnavailableException(this, blockFor, live);
                }
                break;
        }
    }

    public void validateForRead(String table) throws InvalidRequestException
    {
        switch (this)
        {
            case ANY:
                throw new InvalidRequestException("ANY ConsistencyLevel is only supported for writes");
            case LOCAL_QUORUM:
                requireNetworkTopologyStrategy(table);
                break;
            case EACH_QUORUM:
                throw new InvalidRequestException("EACH_QUORUM ConsistencyLevel is only supported for writes");
        }
    }

    public void validateForWrite(String table) throws InvalidRequestException
    {
        switch (this)
        {
            case LOCAL_QUORUM:
            case EACH_QUORUM:
                requireNetworkTopologyStrategy(table);
                break;
        }
    }

    public void validateCounterForWrite(CFMetaData metadata) throws InvalidRequestException
    {
        if (this == ConsistencyLevel.ANY)
        {
            throw new InvalidRequestException("Consistency level ANY is not yet supported for counter columnfamily " + metadata.cfName);
        }
        else if (!metadata.getReplicateOnWrite() && this != ConsistencyLevel.ONE)
        {
            throw new InvalidRequestException("cannot achieve CL > CL.ONE without replicate_on_write on columnfamily " + metadata.cfName);
        }
    }

    private void requireNetworkTopologyStrategy(String table) throws InvalidRequestException
    {
        AbstractReplicationStrategy strategy = Table.open(table).getReplicationStrategy();
        if (!(strategy instanceof NetworkTopologyStrategy))
            throw new InvalidRequestException(String.format("consistency level %s not compatible with replication strategy (%s)", this, strategy.getClass().getName()));
    }
}
