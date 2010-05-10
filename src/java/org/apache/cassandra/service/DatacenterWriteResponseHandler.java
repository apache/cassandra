/**
 *
 */
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
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.DatacenterShardStrategy;
import org.apache.cassandra.locator.RackInferringSnitch;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.FBUtilities;

import com.google.common.collect.Multimap;

/**
 * This class will basically will block for the replication factor which is
 * provided in the input map. it will block till we recive response from (DC, n)
 * nodes.
 */
public class DatacenterWriteResponseHandler extends WriteResponseHandler
{
    private static final RackInferringSnitch snitch = (RackInferringSnitch) DatabaseDescriptor.getEndpointSnitch();
	private static final String localdc = snitch.getDatacenter(FBUtilities.getLocalAddress());
	private final AtomicInteger blockFor;

    public DatacenterWriteResponseHandler(Collection<InetAddress> writeEndpoints, Multimap<InetAddress, InetAddress> hintedEndpoints, ConsistencyLevel consistencyLevel, String table) throws UnavailableException
    {
        // Response is been managed by the map so the waitlist size really doesnt matter.
        super(writeEndpoints, hintedEndpoints, consistencyLevel, table);
        blockFor = new AtomicInteger(responseCount);
    }

    @Override
    public void response(Message message)
    {
        responses.add(message);
        //Is optimal to check if same datacenter than comparing Arrays.
        int b = -1;
        try
        {
            if (localdc.equals(endpointsnitch.getDatacenter(message.getFrom())))
            {
                b = blockFor.decrementAndGet();
            }
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
        if (b == 0)
        {
            //Singnal when Quorum is recived.
            condition.signal();
        }
        if (logger.isDebugEnabled())
            logger.debug("Processed Message: " + message.toString());
    }
    
    @Override
    public void assureSufficientLiveNodes(Collection<InetAddress> writeEndpoints, Multimap<InetAddress, InetAddress> hintedEndpoints)
    throws UnavailableException
    {
        int liveNodes = 0;
        try {
            // count destinations that are part of the desired target set
            for (InetAddress destination : hintedEndpoints.keySet())
            {
                if (writeEndpoints.contains(destination) && localdc.equals(endpointsnitch.getDatacenter(destination)))
                    liveNodes++;
            }
        } catch (Exception ex) {
            throw new UnavailableException();
        }
        if (liveNodes < responseCount)
        {
            throw new UnavailableException();
        }
    }
    
    @Override
    public int determineBlockFor(Collection<InetAddress> writeEndpoints)
    {
    	DatacenterShardStrategy stategy = (DatacenterShardStrategy) StorageService.instance.getReplicationStrategy(table);
        if (consistencyLevel.equals(ConsistencyLevel.DCQUORUM)) {
            return (stategy.getReplicationFactor(localdc, table)/2) + 1;
        }
        return super.determineBlockFor(writeEndpoints);
    }
}