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


import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.concurrent.CreationTimeAwareFuture;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SimpleCondition;

public abstract class AbstractWriteResponseHandler implements IWriteResponseHandler
{
    protected final SimpleCondition condition = new SimpleCondition();
    protected final long startTime;
    protected final Collection<InetAddress> writeEndpoints;
    protected final ConsistencyLevel consistencyLevel;
    protected List<CreationTimeAwareFuture<?>> hintFutures;

    protected AbstractWriteResponseHandler(Collection<InetAddress> writeEndpoints, ConsistencyLevel consistencyLevel)
    {
        startTime = System.currentTimeMillis();
        this.consistencyLevel = consistencyLevel;
        this.writeEndpoints = writeEndpoints;
    }

    public void get() throws TimeoutException
    {
        if (hintFutures != null)
            waitForHints(hintFutures);

        long timeout = DatabaseDescriptor.getRpcTimeout() - (System.currentTimeMillis() - startTime);
        boolean success;
        try
        {
            success = condition.await(timeout, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }

        if (!success)
        {
            throw new TimeoutException();
        }
    }

    public void addFutureForHint(CreationTimeAwareFuture<?> hintFuture)
    {
        if (hintFutures == null)
            hintFutures = new ArrayList<CreationTimeAwareFuture<?>>(writeEndpoints.size());
        hintFutures.add(hintFuture);
    }

    protected static void waitForHints(List<CreationTimeAwareFuture<?>> hintFutures) throws TimeoutException
    {
        // Wait for hints
        try
        {
            FBUtilities.waitOnFutures(hintFutures, DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
        } 
        catch (RuntimeException e)
        {
            // ExecutionEx needs a special treatment. We need to inform the client to back off because this node is overwhelmed.
            if (e.getCause() != null && e.getCause() instanceof ExecutionException)
                throw new TimeoutException();
            throw e;
        }
    }

    /** null message means "response from local write" */
    public abstract void response(Message msg);

    public abstract void assureSufficientLiveNodes() throws UnavailableException;
}
