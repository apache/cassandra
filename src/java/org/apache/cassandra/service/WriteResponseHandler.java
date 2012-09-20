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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Table;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Handles blocking writes for ONE, ANY, TWO, THREE, QUORUM, and ALL consistency levels.
 */
public class WriteResponseHandler extends AbstractWriteResponseHandler
{
    protected static final Logger logger = LoggerFactory.getLogger(WriteResponseHandler.class);

    protected final AtomicInteger responses;
    private final int blockFor;

    protected WriteResponseHandler(Collection<InetAddress> writeEndpoints, ConsistencyLevel consistencyLevel, String table, Runnable callback)
    {
        super(writeEndpoints, consistencyLevel, callback);
        blockFor = consistencyLevel.blockFor(table);
        responses = new AtomicInteger(blockFor);
    }

    protected WriteResponseHandler(InetAddress endpoint)
    {
        super(Arrays.asList(endpoint), ConsistencyLevel.ALL, null);
        blockFor = 1;
        responses = new AtomicInteger(1);
    }

    public static AbstractWriteResponseHandler create(Collection<InetAddress> writeEndpoints, ConsistencyLevel consistencyLevel, String table, Runnable callback)
    {
        return new WriteResponseHandler(writeEndpoints, consistencyLevel, table, callback);
    }

    public static AbstractWriteResponseHandler create(InetAddress endpoint)
    {
        return new WriteResponseHandler(endpoint);
    }

    public void response(MessageIn m)
    {
        if (responses.decrementAndGet() == 0)
            signal();
    }

    protected int ackCount()
    {
        return blockFor - responses.get();
    }

    protected int blockFor()
    {
        return blockFor;
    }

    public void assureSufficientLiveNodes() throws UnavailableException
    {
        if (consistencyLevel == ConsistencyLevel.ANY)
        {
            // Ensure there are blockFor distinct living nodes (hints (local) are ok).
            // Thus we include the local node (coordinator) as a valid replica if it is there already.
            int effectiveEndpoints = writeEndpoints.contains(FBUtilities.getBroadcastAddress()) ? writeEndpoints.size() : writeEndpoints.size() + 1;
            if (effectiveEndpoints < responses.get())
                throw new UnavailableException(consistencyLevel, responses.get(), effectiveEndpoints);
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
            throw new UnavailableException(consistencyLevel, responses.get(), liveNodes);
        }
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }
}
