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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.utils.FBUtilities;

public enum ConsistencyLevel
{
    ANY,
    ONE,
    TWO,
    THREE,
    QUORUM,
    ALL,
    LOCAL_QUORUM,
    EACH_QUORUM;

    private static final String LOCAL_DC = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());

    public int blockFor(String table)
    {
        NetworkTopologyStrategy strategy = null;
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
                return (Table.open(table).getReplicationStrategy().getReplicationFactor() / 2) + 1;
            case ALL:
                return Table.open(table).getReplicationStrategy().getReplicationFactor();
            case LOCAL_QUORUM:
                strategy = (NetworkTopologyStrategy) Table.open(table).getReplicationStrategy();
                return (strategy.getReplicationFactor(LOCAL_DC) / 2) + 1;
            case EACH_QUORUM:
                strategy = (NetworkTopologyStrategy) Table.open(table).getReplicationStrategy();
                int n = 0;
                for (String dc : strategy.getDatacenters())
                    n += (strategy.getReplicationFactor(dc) / 2) + 1;
                return n;
            default:
                throw new UnsupportedOperationException("Invalid consistency level: " + toString());
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
