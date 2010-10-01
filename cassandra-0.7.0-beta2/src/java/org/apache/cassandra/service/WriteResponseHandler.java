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

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles blocking writes for ONE, ANY, QUORUM, and ALL consistency levels.
 */
public class WriteResponseHandler extends AbstractWriteResponseHandler
{
    protected static final Logger logger = LoggerFactory.getLogger(WriteResponseHandler.class);

    protected final AtomicInteger responses;

    protected WriteResponseHandler(Collection<InetAddress> writeEndpoints, Multimap<InetAddress, InetAddress> hintedEndpoints, ConsistencyLevel consistencyLevel, String table)
    {
        super(writeEndpoints, hintedEndpoints, consistencyLevel);
        responses = new AtomicInteger(determineBlockFor(table));
    }

    protected WriteResponseHandler(InetAddress endpoint)
    {
        super(Arrays.asList(endpoint),
              ImmutableMultimap.<InetAddress, InetAddress>builder().put(endpoint, endpoint).build(),
              ConsistencyLevel.ALL);
        responses = new AtomicInteger(1);
    }

    public static IWriteResponseHandler create(Collection<InetAddress> writeEndpoints, Multimap<InetAddress, InetAddress> hintedEndpoints, ConsistencyLevel consistencyLevel, String table)
    {
        if (consistencyLevel == ConsistencyLevel.ZERO)
        {
            return NoConsistencyWriteResponseHandler.instance;
        }
        else
        {
            return new WriteResponseHandler(writeEndpoints, hintedEndpoints, consistencyLevel, table);
        }
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
        int blockFor = 0;
        switch (consistencyLevel)
        {
            case ONE:
                blockFor = 1;
                break;
            case ANY:
                blockFor = 1;
                break;
            case QUORUM:
                blockFor = (writeEndpoints.size() / 2) + 1;
                break;
            case ALL:
                blockFor = writeEndpoints.size();
                break;
            default:
                throw new UnsupportedOperationException("invalid consistency level: " + consistencyLevel.toString());
        }
        // at most one node per range can bootstrap at a time, and these will be added to the write until
        // bootstrap finishes (at which point we no longer need to write to the old ones).
        assert 1 <= blockFor && blockFor <= 2 * DatabaseDescriptor.getReplicationFactor(table)
            : String.format("invalid response count %d for replication factor %d",
                            blockFor, DatabaseDescriptor.getReplicationFactor(table));
        return blockFor;
    }

    public void assureSufficientLiveNodes() throws UnavailableException
    {
        if (consistencyLevel == ConsistencyLevel.ANY)
        {
            // ensure there are blockFor distinct living nodes (hints are ok).
            if (hintedEndpoints.keySet().size() < responses.get())
                throw new UnavailableException();
        }

        // count destinations that are part of the desired target set
        int liveNodes = 0;
        for (InetAddress destination : hintedEndpoints.keySet())
        {
            if (writeEndpoints.contains(destination))
                liveNodes++;
        }
        if (liveNodes < responses.get())
        {
            throw new UnavailableException();
        }
    }
}
