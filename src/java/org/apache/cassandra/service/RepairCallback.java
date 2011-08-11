package org.apache.cassandra.service;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.SimpleCondition;

public class RepairCallback implements IAsyncCallback
{
    public final RowRepairResolver resolver;
    private final List<InetAddress> endpoints;
    private final SimpleCondition condition = new SimpleCondition();
    private final long startTime;
    protected final AtomicInteger received = new AtomicInteger(0);

    /**
     * The main difference between this and ReadCallback is, ReadCallback has a ConsistencyLevel
     * it needs to achieve.  Repair on the other hand is happy to repair whoever replies within the timeout.
     *
     * (The other main difference of course is, this is only created once we know we have a digest
     * mismatch, and we're going to do full-data reads from everyone -- that is, this is the final
     * stage in the read process.)
     */
    public RepairCallback(RowRepairResolver resolver, List<InetAddress> endpoints)
    {
        this.resolver = resolver;
        this.endpoints = endpoints;
        this.startTime = System.currentTimeMillis();
    }

    public Row get() throws TimeoutException, DigestMismatchException, IOException
    {
        long timeout = DatabaseDescriptor.getRpcTimeout() - (System.currentTimeMillis() - startTime);
        try
        {
            condition.await(timeout, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }

        return received.get() > 1 ? resolver.resolve() : null;
    }

    public void response(Message message)
    {
        resolver.preprocess(message);
        if (received.incrementAndGet() == endpoints.size())
            condition.signal();
    }

    public boolean isLatencyForSnitch()
    {
        return true;
    }

    public int getMaxLiveColumns()
    {
        return resolver.getMaxLiveColumns();
    }
}
