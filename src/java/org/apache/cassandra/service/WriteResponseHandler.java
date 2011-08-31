/**
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

package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Table;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Handles blocking writes for ONE, ANY, TWO, THREE, QUORUM, and ALL consistency levels.
 */
public class WriteResponseHandler extends AbstractWriteResponseHandler
{
    protected static final Logger logger = LoggerFactory.getLogger(WriteResponseHandler.class);

    protected final AtomicInteger responses;

    protected WriteResponseHandler(Collection<InetAddress> writeEndpoints, ConsistencyLevel consistencyLevel, String table)
    {
        super(writeEndpoints, consistencyLevel);
        responses = new AtomicInteger(determineBlockFor(table));
    }

    protected WriteResponseHandler(InetAddress endpoint)
    {
        super(Arrays.asList(endpoint), ConsistencyLevel.ALL);
        responses = new AtomicInteger(1);
    }

    public static IWriteResponseHandler create(Collection<InetAddress> writeEndpoints, ConsistencyLevel consistencyLevel, String table)
    {
        return new WriteResponseHandler(writeEndpoints, consistencyLevel, table);
    }

    public static IWriteResponseHandler create(InetAddress endpoint)
    {
        return new WriteResponseHandler(endpoint);
    }

    public void response(Message m)
    {
        if (responses.decrementAndGet() == 0)
            condition.signal();
    }

    protected int determineBlockFor(String table)
    {
        switch (consistencyLevel)
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
            default:
                throw new UnsupportedOperationException("invalid consistency level: " + consistencyLevel.toString());
        }
    }

    public void assureSufficientLiveNodes() throws UnavailableException
    {
        if (consistencyLevel == ConsistencyLevel.ANY)
        {
            // Ensure there are blockFor distinct living nodes (hints (local) are ok).
            // Thus we include the local node (coordinator) as a valid replica if it is there already.
            int effectiveEndpoints = writeEndpoints.contains(FBUtilities.getBroadcastAddress()) ? writeEndpoints.size() : writeEndpoints.size() + 1;
            if (effectiveEndpoints < responses.get())
                throw new UnavailableException();
            return;
        }

        // count destinations that are part of the desired target set
        int liveNodes = 0;
        for (InetAddress destination : writeEndpoints)
        {
            if (FailureDetector.instance.isAlive(destination))
                liveNodes++;
        }
        if (liveNodes < responses.get())
        {
            throw new UnavailableException();
        }
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }
}
