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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.locator.DatacenterShardStrategy;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;

import com.google.common.collect.Multimap;

/**
 * This class will block for the replication factor which is
 * provided in the input map. it will block till we recive response from
 * n nodes in each of our data centers.
 */
public class DatacenterSyncWriteResponseHandler extends WriteResponseHandler
{
	private final DatacenterShardStrategy stategy = (DatacenterShardStrategy) StorageService.instance.getReplicationStrategy(table);
    private HashMap<String, AtomicInteger> dcResponses;

    public DatacenterSyncWriteResponseHandler(Collection<InetAddress> writeEndpoints, Multimap<InetAddress, InetAddress> hintedEndpoints, ConsistencyLevel consistencyLevel, String table)
    throws UnavailableException
    {
        // Response is been managed by the map so make it 1 for the superclass.
        super(writeEndpoints, hintedEndpoints, consistencyLevel, table);
    }

    // synchronized for the benefit of dcResponses and responseCounts.  "responses" itself
    // is inherited from WRH and is concurrent.
    @Override
    public void response(Message message)
    {
        responses.add(message);
        try
        {
            String dataCenter = endpointsnitch.getDatacenter(message.getFrom());
            // If this DC needs to be blocked then do the below.
            dcResponses.get(dataCenter).getAndDecrement();
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
        maybeSignal();
    }
    
    private void maybeSignal()
    {
    	for(AtomicInteger i : dcResponses.values()) {
    		if (0 < i.get()) {
    			return;
    		}
    	}
    	// If all the quorum conditionas are met then return back.
    	condition.signal();
    }
    
    @Override
    public int determineBlockFor(Collection<InetAddress> writeEndpoints)
    {        
        this.dcResponses = new HashMap<String, AtomicInteger>();
        for (String dc: stategy.getDatacenters(table)) {
        	int rf = stategy.getReplicationFactor(dc, table);
        	dcResponses.put(dc, new AtomicInteger((rf/2) + 1));
        }
    	// Do nothing, there is no 'one' integer to block for
        return 0;
    }
    
    @Override
    public void assureSufficientLiveNodes(Collection<InetAddress> writeEndpoints, Multimap<InetAddress, InetAddress> hintedEndpoints) throws UnavailableException
    {   
		Map<String, AtomicInteger> dcEndpoints = new HashMap<String, AtomicInteger>();
		try
		{
			for (String dc: stategy.getDatacenters(table))
				dcEndpoints.put(dc, new AtomicInteger());
			for (InetAddress destination : hintedEndpoints.keySet())
			{
				// If not just go to the next endpoint
				if (!writeEndpoints.contains(destination))
					continue;
				// figure out the destination dc
				String destinationDC = endpointsnitch.getDatacenter(destination);
				dcEndpoints.get(destinationDC).incrementAndGet();
			}
		}
		catch (UnknownHostException e)
		{
			throw new UnavailableException();
		}
        // Throw exception if any of the DC doesnt have livenodes to accept write.
        for (String dc: stategy.getDatacenters(table)) {
        	if (dcEndpoints.get(dc).get() != dcResponses.get(dc).get()) {
                throw new UnavailableException();
        	}
        }
    }
}
