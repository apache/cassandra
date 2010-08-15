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


import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Datacenter Quorum response handler blocks for a quorum of responses from the local DC
 */
public class DatacenterQuorumResponseHandler<T> extends QuorumResponseHandler<T>
{
    private static final AbstractNetworkTopologySnitch snitch = (AbstractNetworkTopologySnitch) DatabaseDescriptor.getEndpointSnitch();
	private static final String localdc = snitch.getDatacenter(FBUtilities.getLocalAddress());
    private AtomicInteger localResponses;
    
    public DatacenterQuorumResponseHandler(IResponseResolver<T> responseResolver, ConsistencyLevel consistencyLevel, String table)
    {
        super(responseResolver, consistencyLevel, table);
        localResponses = new AtomicInteger(blockfor);
    }
    
    @Override
    public void response(Message message)
    {
        responses.add(message); // we'll go ahead and resolve a reply from anyone, even if it's not from this dc

        int n;
        n = localdc.equals(snitch.getDatacenter(message.getFrom())) 
                ? localResponses.decrementAndGet()
                : localResponses.get();

        if (n == 0 && responseResolver.isDataPresent(responses))
        {
            condition.signal();
        }
    }
    
    @Override
    public int determineBlockFor(ConsistencyLevel consistency_level, String table)
	{
		NetworkTopologyStrategy stategy = (NetworkTopologyStrategy) StorageService.instance.getReplicationStrategy(table);
		return (stategy.getReplicationFactor(localdc) / 2) + 1;
	}
}
