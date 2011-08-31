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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.FBUtilities;

/**
 * This class blocks for a quorum of responses _in all datacenters_ (CL.EACH_QUORUM).
 */
public class DatacenterSyncWriteResponseHandler extends AbstractWriteResponseHandler
{
    private static final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();

    private static final String localdc;
    static
    {
        localdc = snitch.getDatacenter(FBUtilities.getBroadcastAddress());
    }

	private final NetworkTopologyStrategy strategy;
    private HashMap<String, AtomicInteger> responses = new HashMap<String, AtomicInteger>();

    protected DatacenterSyncWriteResponseHandler(Collection<InetAddress> writeEndpoints, ConsistencyLevel consistencyLevel, String table)
    {
        // Response is been managed by the map so make it 1 for the superclass.
        super(writeEndpoints, consistencyLevel);
        assert consistencyLevel == ConsistencyLevel.EACH_QUORUM;

        strategy = (NetworkTopologyStrategy) Table.open(table).getReplicationStrategy();

        for (String dc : strategy.getDatacenters())
        {
            int rf = strategy.getReplicationFactor(dc);
            responses.put(dc, new AtomicInteger((rf / 2) + 1));
        }
    }

    public static IWriteResponseHandler create(Collection<InetAddress> writeEndpoints, ConsistencyLevel consistencyLevel, String table)
    {
        return new DatacenterSyncWriteResponseHandler(writeEndpoints, consistencyLevel, table);
    }

    public void response(Message message)
    {
        String dataCenter = message == null
                            ? localdc
                            : snitch.getDatacenter(message.getFrom());

        responses.get(dataCenter).getAndDecrement();

        for (AtomicInteger i : responses.values())
        {
            if (0 < i.get())
                return;
        }

        // all the quorum conditions are met
        condition.signal();
    }

    public void assureSufficientLiveNodes() throws UnavailableException
    {
        Map<String, AtomicInteger> dcEndpoints = new HashMap<String, AtomicInteger>();
        for (String dc: strategy.getDatacenters())
            dcEndpoints.put(dc, new AtomicInteger());

        for (InetAddress destination : writeEndpoints)
        {
            if (FailureDetector.instance.isAlive(destination))
            {
                // figure out the destination dc
                String destinationDC = snitch.getDatacenter(destination);
                dcEndpoints.get(destinationDC).incrementAndGet();
            }
        }

        // Throw exception if any of the DC doesn't have livenodes to accept write.
        for (String dc: strategy.getDatacenters())
        {
        	if (dcEndpoints.get(dc).get() < responses.get(dc).get())
                throw new UnavailableException();
        }
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }
}
