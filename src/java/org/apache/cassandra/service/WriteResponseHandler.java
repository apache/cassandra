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

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.AbstractRackAwareSnitch;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.SimpleCondition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;

public class WriteResponseHandler implements IAsyncCallback
{
    protected static final Logger logger = LoggerFactory.getLogger( WriteResponseHandler.class );
    protected static final AbstractRackAwareSnitch endpointsnitch = (AbstractRackAwareSnitch) DatabaseDescriptor.getEndpointSnitch();
    protected final SimpleCondition condition = new SimpleCondition();
    protected final int responseCount;
    protected final Collection<Message> responses;
    protected AtomicInteger localResponses = new AtomicInteger(0);
    protected final long startTime;
	protected final ConsistencyLevel consistencyLevel;
	protected final String table;

    public WriteResponseHandler(Collection<InetAddress> writeEndpoints, Multimap<InetAddress, InetAddress> hintedEndpoints, ConsistencyLevel consistencyLevel, String table) 
    throws UnavailableException
    {
    	this.table = table;
    	this.consistencyLevel = consistencyLevel;
        this.responseCount = determineBlockFor(writeEndpoints);
        assureSufficientLiveNodes(writeEndpoints, hintedEndpoints);
        responses = new LinkedBlockingQueue<Message>();
        startTime = System.currentTimeMillis();
    }

    public void get() throws TimeoutException
    {
        try
        {
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
                throw new TimeoutException("Operation timed out - received only " + responses.size() + localResponses + " responses");
            }
        }
        finally
        {
            for (Message response : responses)
            {
                MessagingService.removeRegisteredCallback(response.getMessageId());
            }
        }
    }

    public void response(Message message)
    {
        responses.add(message);
        maybeSignal();
    }

    public void localResponse()
    {
        localResponses.addAndGet(1);
        maybeSignal();
    }

    private void maybeSignal()
    {
        if (responses.size() + localResponses.get() >= responseCount)
        {
            condition.signal();
        }
    }
    
    public int determineBlockFor(Collection<InetAddress> writeEndpoints)
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
            : "invalid response count " + responseCount;
        return blockFor;
    }
    
    public void assureSufficientLiveNodes(Collection<InetAddress> writeEndpoints, Multimap<InetAddress, InetAddress> hintedEndpoints)
    throws UnavailableException
    {
        if (consistencyLevel == ConsistencyLevel.ANY)
        {
            // ensure there are blockFor distinct living nodes (hints are ok).
            if (hintedEndpoints.keySet().size() < responseCount)
                throw new UnavailableException();
        }
        
        // count destinations that are part of the desired target set
        int liveNodes = 0;
        for (InetAddress destination : hintedEndpoints.keySet())
        {
            if (writeEndpoints.contains(destination))
                liveNodes++;
        }
        if (liveNodes < responseCount)
        {
            throw new UnavailableException();
        }
    }
}