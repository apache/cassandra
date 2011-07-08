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
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Datacenter Quorum response handler blocks for a quorum of responses from the local DC
 */
public class DatacenterReadCallback<T> extends ReadCallback<T>
{
    private static final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
    private static final String localdc = snitch.getDatacenter(FBUtilities.getBroadcastAddress());

    public DatacenterReadCallback(IResponseResolver resolver, ConsistencyLevel consistencyLevel, IReadCommand command, List<InetAddress> endpoints)
    {
        super(resolver, consistencyLevel, command, endpoints);
    }

    @Override
    protected List<InetAddress> preferredEndpoints(List<InetAddress> endpoints)
    {
        ArrayList<InetAddress> preferred = new ArrayList<InetAddress>(blockfor);
        for (InetAddress endpoint : endpoints)
        {
            if (localdc.equals(snitch.getDatacenter(endpoint)))
                preferred.add(endpoint);
            if (preferred.size() == blockfor)
                break;
        }
        return preferred;
    }

    @Override
    protected boolean waitingFor(Message message)
    {
        return localdc.equals(snitch.getDatacenter(message.getFrom()));
    }

    @Override
    protected boolean waitingFor(ReadResponse response)
    {
        // cheat and leverage our knowledge that a local read is the only way the ReadResponse
        // version of this method gets called
        return true;
    }
        
    @Override
    public int determineBlockFor(ConsistencyLevel consistency_level, String table)
	{
        NetworkTopologyStrategy stategy = (NetworkTopologyStrategy) Table.open(table).getReplicationStrategy();
		return (stategy.getReplicationFactor(localdc) / 2) + 1;
	}

    @Override
    public void assureSufficientLiveNodes() throws UnavailableException
    {
        int localEndpoints = 0;
        for (InetAddress endpoint : endpoints)
        {
            if (localdc.equals(snitch.getDatacenter(endpoint)))
                localEndpoints++;
        }

        if (localEndpoints < blockfor)
        {
            if (logger.isDebugEnabled())
            {
                StringBuilder builder = new StringBuilder("Local replicas [");
                for (InetAddress endpoint : endpoints)
                {
                    if (localdc.equals(snitch.getDatacenter(endpoint)))
                        builder.append(endpoint).append(",");
                }
                builder.append("] are insufficient to satisfy LOCAL_QUORUM requirement of ").append(blockfor).append(" live nodes in '").append(localdc).append("'");
                logger.debug(builder.toString());
            }

            throw new UnavailableException();
        }
    }
}
