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

    protected DatacenterWriteResponseHandler(Collection<InetAddress> writeEndpoints, Multimap<InetAddress, InetAddress> hintedEndpoints, ConsistencyLevel consistencyLevel, String table)
    {
        super(writeEndpoints, hintedEndpoints, consistencyLevel, table);
        assert consistencyLevel == ConsistencyLevel.LOCAL_QUORUM;
    }

    public static IWriteResponseHandler create(Collection<InetAddress> writeEndpoints, Multimap<InetAddress, InetAddress> hintedEndpoints, ConsistencyLevel consistencyLevel, String table)
    {
        return new DatacenterWriteResponseHandler(writeEndpoints, hintedEndpoints, consistencyLevel, table);
    }

    @Override
    protected int determineBlockFor(String table)
    {
        NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) Table.open(table).getReplicationStrategy();
        return (strategy.getReplicationFactor(localdc) / 2) + 1;
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
