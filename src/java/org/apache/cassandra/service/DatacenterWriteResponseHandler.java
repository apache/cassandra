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
import java.util.Collection;

import com.google.common.collect.Multimap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.FBUtilities;

/**
 * This class blocks for a quorum of responses _in the local datacenter only_ (CL.LOCAL_QUORUM).
 */
public class DatacenterWriteResponseHandler extends WriteResponseHandler
{
    private static final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();

    private static final String localdc;
    static
    {
        localdc = snitch.getDatacenter(FBUtilities.getLocalAddress());
    }

    protected DatacenterWriteResponseHandler(Iterable<InetAddress> writeEndpoints, Multimap<InetAddress, InetAddress> hintedEndpoints, Iterable<InetAddress> pendingEndpoints, ConsistencyLevel consistencyLevel, String table)
    {
        super(writeEndpoints, hintedEndpoints, pendingEndpoints, consistencyLevel, table);
        assert consistencyLevel == ConsistencyLevel.LOCAL_QUORUM;
    }

    public static IWriteResponseHandler create(Iterable<InetAddress> writeEndpoints, Multimap<InetAddress, InetAddress> hintedEndpoints, Iterable<InetAddress> pendingEndpoints, ConsistencyLevel consistencyLevel, String table)
    {
        return new DatacenterWriteResponseHandler(writeEndpoints, hintedEndpoints, pendingEndpoints, consistencyLevel, table);
    }

    @Override
    protected int determineBlockFor(String table)
    {
        NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) Table.open(table).getReplicationStrategy();
        int blockFor = (strategy.getReplicationFactor(localdc) / 2) + 1;
        // If there is any pending endpoints we went to increase blockFor to make sure we guarantee CL (see CASSANDRA-833). However, we're only
        // intersted in endpoint in the local DC. Note that we use the fact that when a node boostrap (or leave), both the source and
        // destination of a pending range will be in the same DC (this is true because strategy == NTS)
        for (InetAddress pending : pendingEndpoints)
        {
            if (localdc.equals(snitch.getDatacenter(pending)))
                blockFor++;
        }
        return blockFor;
    }


    @Override
    public void response(Message message)
    {
        if (message == null || localdc.equals(snitch.getDatacenter(message.getFrom())))
        {
            if (responses.decrementAndGet() == 0)
                condition.signal();
        }
    }
    
    @Override
    public void assureSufficientLiveNodes() throws UnavailableException
    {
        int liveNodes = 0;
        for (InetAddress destination : hintedEndpoints.keySet())
        {
            if (localdc.equals(snitch.getDatacenter(destination)) && writeEndpoints.contains(destination))
                liveNodes++;
        }

        if (liveNodes < responses.get())
        {
            throw new UnavailableException();
        }
    }
}
