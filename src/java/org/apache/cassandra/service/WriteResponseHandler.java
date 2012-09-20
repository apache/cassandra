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
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.db.ConsistencyLevel;

/**
 * Handles blocking writes for ONE, ANY, TWO, THREE, QUORUM, and ALL consistency levels.
 */
public class WriteResponseHandler extends AbstractWriteResponseHandler
{
    protected static final Logger logger = LoggerFactory.getLogger(WriteResponseHandler.class);

    protected final AtomicInteger responses;
    private final int blockFor;

    protected WriteResponseHandler(Collection<InetAddress> writeEndpoints, Collection<InetAddress> pendingEndpoints, ConsistencyLevel consistencyLevel, String table, Runnable callback)
    {
        super(writeEndpoints, pendingEndpoints, consistencyLevel, callback);
        blockFor = consistencyLevel.blockFor(table);
        responses = new AtomicInteger(blockFor);
    }

    protected WriteResponseHandler(InetAddress endpoint)
    {
        super(Arrays.asList(endpoint), Collections.<InetAddress>emptyList(), ConsistencyLevel.ALL, null);
        blockFor = 1;
        responses = new AtomicInteger(1);
    }

    public static AbstractWriteResponseHandler create(Collection<InetAddress> writeEndpoints, Collection<InetAddress> pendingEndpoints, ConsistencyLevel consistencyLevel, String table, Runnable callback)
    {
        return new WriteResponseHandler(writeEndpoints, pendingEndpoints, consistencyLevel, table, callback);
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

    protected int blockForCL()
    {
        return blockFor;
    }

    public void assureSufficientLiveNodes() throws UnavailableException
    {
        if (consistencyLevel == ConsistencyLevel.ANY)
        {
            // local hint is acceptable, and local node is always live
            return;
        }

        // count destinations that are part of the desired target set
        int liveNodes = 0;
        for (InetAddress destination : Iterables.concat(naturalEndpoints, pendingEndpoints))
        {
            if (FailureDetector.instance.isAlive(destination))
                liveNodes++;
        }
        if (liveNodes < blockFor)
        {
            throw new UnavailableException(consistencyLevel, blockFor, liveNodes);
        }
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }
}
