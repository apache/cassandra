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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;

/**
 * Handles blocking writes for ONE, ANY, TWO, THREE, QUORUM, and ALL consistency levels.
 */
public class WriteResponseHandler<T> extends AbstractWriteResponseHandler<T>
{
    protected static final Logger logger = LoggerFactory.getLogger(WriteResponseHandler.class);

    protected volatile int responses;
    private static final AtomicIntegerFieldUpdater<WriteResponseHandler> responsesUpdater
            = AtomicIntegerFieldUpdater.newUpdater(WriteResponseHandler.class, "responses");

    public WriteResponseHandler(Collection<InetAddress> writeEndpoints,
                                Collection<InetAddress> pendingEndpoints,
                                ConsistencyLevel consistencyLevel,
                                Keyspace keyspace,
                                Runnable callback,
                                WriteType writeType,
                                long queryStartNanoTime)
    {
        super(keyspace, writeEndpoints, pendingEndpoints, consistencyLevel, callback, writeType, queryStartNanoTime);
        responses = totalBlockFor();
    }

    public WriteResponseHandler(InetAddress endpoint, WriteType writeType, Runnable callback, long queryStartNanoTime)
    {
        this(Arrays.asList(endpoint), Collections.<InetAddress>emptyList(), ConsistencyLevel.ONE, null, callback, writeType, queryStartNanoTime);
    }

    public WriteResponseHandler(InetAddress endpoint, WriteType writeType, long queryStartNanoTime)
    {
        this(endpoint, writeType, null, queryStartNanoTime);
    }

    public void response(MessageIn<T> m)
    {
        if (responsesUpdater.decrementAndGet(this) == 0)
            signal();
    }

    protected int ackCount()
    {
        return totalBlockFor() - responses;
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }
}
