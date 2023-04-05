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


import java.util.Locale;

import com.carrotsearch.hppc.ObjectIntHashMap;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.InOurDc;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.transport.ProtocolException;

import static org.apache.cassandra.locator.Replicas.addToCountPerDc;

public enum ConsistencyLevel
{
    ANY         (0),
    ONE         (1),
    TWO         (2),
    THREE       (3),
    QUORUM      (4),
    ALL         (5),
    LOCAL_QUORUM(6, true),
    EACH_QUORUM (7),
    SERIAL      (8),
    LOCAL_SERIAL(9, true),
    LOCAL_ONE   (10, true),
    NODE_LOCAL  (11, true);

    // Used by the binary protocol
    public final int code;
    private final boolean isDCLocal;
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
        this(code, false);
    }

    private ConsistencyLevel(int code, boolean isDCLocal)
    {
        this.code = code;
        this.isDCLocal = isDCLocal;
    }

    public static ConsistencyLevel fromCode(int code)
    {
        if (code < 0 || code >= codeIdx.length)
            throw new ProtocolException(String.format("Unknown code %d for a consistency level", code));
        return codeIdx[code];
    }

    public static ConsistencyLevel fromString(String str)
    {
        return valueOf(str.toUpperCase(Locale.US));
    }

    public static int quorumFor(AbstractReplicationStrategy replicationStrategy)
    {
        return (replicationStrategy.getReplicationFactor().allReplicas / 2) + 1;
    }

    public static int localQuorumFor(AbstractReplicationStrategy replicationStrategy, String dc)
    {
        return (replicationStrategy instanceof NetworkTopologyStrategy)
             ? (((NetworkTopologyStrategy) replicationStrategy).getReplicationFactor(dc).allReplicas / 2) + 1
             : quorumFor(replicationStrategy);
    }

    public static int localQuorumForOurDc(AbstractReplicationStrategy replicationStrategy)
    {
        return localQuorumFor(replicationStrategy, DatabaseDescriptor.getLocalDataCenter());
    }

    public static ObjectIntHashMap<String> eachQuorumForRead(AbstractReplicationStrategy replicationStrategy)
    {
        if (replicationStrategy instanceof NetworkTopologyStrategy)
        {
            NetworkTopologyStrategy npStrategy = (NetworkTopologyStrategy) replicationStrategy;
            ObjectIntHashMap<String> perDc = new ObjectIntHashMap<>(((npStrategy.getDatacenters().size() + 1) * 4) / 3);
            for (String dc : npStrategy.getDatacenters())
                perDc.put(dc, ConsistencyLevel.localQuorumFor(replicationStrategy, dc));
            return perDc;
        }
        else
        {
            ObjectIntHashMap<String> perDc = new ObjectIntHashMap<>(1);
            perDc.put(DatabaseDescriptor.getLocalDataCenter(), quorumFor(replicationStrategy));
            return perDc;
        }
    }

    public static ObjectIntHashMap<String> eachQuorumForWrite(AbstractReplicationStrategy replicationStrategy, Endpoints<?> pendingWithDown)
    {
        ObjectIntHashMap<String> perDc = eachQuorumForRead(replicationStrategy);
        addToCountPerDc(perDc, pendingWithDown, 1);
        return perDc;
    }

    public int blockFor(AbstractReplicationStrategy replicationStrategy)
    {
        switch (this)
        {
            case ONE:
            case LOCAL_ONE:
                return 1;
            case ANY:
                return 1;
            case TWO:
                return 2;
            case THREE:
                return 3;
            case QUORUM:
            case SERIAL:
                return quorumFor(replicationStrategy);
            case ALL:
                return replicationStrategy.getReplicationFactor().allReplicas;
            case LOCAL_QUORUM:
            case LOCAL_SERIAL:
                return localQuorumForOurDc(replicationStrategy);
            case EACH_QUORUM:
                if (replicationStrategy instanceof NetworkTopologyStrategy)
                {
                    NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) replicationStrategy;
                    int n = 0;
                    for (String dc : strategy.getDatacenters())
                        n += localQuorumFor(replicationStrategy, dc);
                    return n;
                }
                else
                {
                    return quorumFor(replicationStrategy);
                }
            default:
                throw new UnsupportedOperationException("Invalid consistency level: " + toString());
        }
    }

    public int blockForWrite(AbstractReplicationStrategy replicationStrategy, Endpoints<?> pending)
    {
        assert pending != null;

        int blockFor = blockFor(replicationStrategy);
        switch (this)
        {
            case ANY:
                break;
            case LOCAL_ONE: case LOCAL_QUORUM: case LOCAL_SERIAL:
                // we will only count local replicas towards our response count, as these queries only care about local guarantees
                blockFor += pending.count(InOurDc.replicas());
                break;
            case ONE: case TWO: case THREE:
            case QUORUM: case EACH_QUORUM:
            case SERIAL:
            case ALL:
                blockFor += pending.size();
        }
        return blockFor;
    }

    /**
     * Determine if this consistency level meets or exceeds the consistency requirements of the given cl for the given keyspace
     * WARNING: this is not locality aware; you cannot safely use this with mixed locality consistency levels (e.g. LOCAL_QUORUM and QUORUM)
     */
    public boolean satisfies(ConsistencyLevel other, AbstractReplicationStrategy replicationStrategy)
    {
        return blockFor(replicationStrategy) >= other.blockFor(replicationStrategy);
    }

    public boolean isDatacenterLocal()
    {
        return isDCLocal;
    }

    public void validateForRead() throws InvalidRequestException
    {
        switch (this)
        {
            case ANY:
                throw new InvalidRequestException("ANY ConsistencyLevel is only supported for writes");
        }
    }

    public void validateForWrite() throws InvalidRequestException
    {
        switch (this)
        {
            case SERIAL:
            case LOCAL_SERIAL:
                throw new InvalidRequestException("You must use conditional updates for serializable writes");
        }
    }

    // This is the same than validateForWrite really, but we include a slightly different error message for SERIAL/LOCAL_SERIAL
    public void validateForCasCommit(AbstractReplicationStrategy replicationStrategy) throws InvalidRequestException
    {
        switch (this)
        {
            case EACH_QUORUM:
                requireNetworkTopologyStrategy(replicationStrategy);
                break;
            case SERIAL:
            case LOCAL_SERIAL:
                throw new InvalidRequestException(this + " is not supported as conditional update commit consistency. Use ANY if you mean \"make sure it is accepted but I don't care how many replicas commit it for non-SERIAL reads\"");
        }
    }

    public void validateForCas() throws InvalidRequestException
    {
        if (!isSerialConsistency())
            throw new InvalidRequestException("Invalid consistency for conditional update. Must be one of SERIAL or LOCAL_SERIAL");
    }

    public boolean isSerialConsistency()
    {
        return this == SERIAL || this == LOCAL_SERIAL;
    }

    public void validateCounterForWrite(TableMetadata metadata) throws InvalidRequestException
    {
        if (this == ConsistencyLevel.ANY)
            throw new InvalidRequestException("Consistency level ANY is not yet supported for counter table " + metadata.name);

        if (isSerialConsistency())
            throw new InvalidRequestException("Counter operations are inherently non-serializable");
    }

    private void requireNetworkTopologyStrategy(AbstractReplicationStrategy replicationStrategy) throws InvalidRequestException
    {
        if (!(replicationStrategy instanceof NetworkTopologyStrategy))
            throw new InvalidRequestException(String.format("consistency level %s not compatible with replication strategy (%s)",
                                                            this, replicationStrategy.getClass().getName()));
    }
}
