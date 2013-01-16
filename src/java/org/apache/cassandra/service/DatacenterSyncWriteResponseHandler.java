/*
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

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterables;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.utils.FBUtilities;

/**
 * This class blocks for a quorum of responses _in all datacenters_ (CL.EACH_QUORUM).
 */
public class DatacenterSyncWriteResponseHandler extends AbstractWriteResponseHandler
{
    private static final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();

    private final NetworkTopologyStrategy strategy;
    private final HashMap<String, AtomicInteger> responses = new HashMap<String, AtomicInteger>();

    public DatacenterSyncWriteResponseHandler(Collection<InetAddress> naturalEndpoints,
                                              Collection<InetAddress> pendingEndpoints,
                                              ConsistencyLevel consistencyLevel,
                                              Table table,
                                              Runnable callback,
                                              WriteType writeType)
    {
        // Response is been managed by the map so make it 1 for the superclass.
        super(table, naturalEndpoints, pendingEndpoints, consistencyLevel, callback, writeType);
        assert consistencyLevel == ConsistencyLevel.EACH_QUORUM;

        strategy = (NetworkTopologyStrategy) table.getReplicationStrategy();

        for (String dc : strategy.getDatacenters())
        {
            int rf = strategy.getReplicationFactor(dc);
            responses.put(dc, new AtomicInteger((rf / 2) + 1));
        }
    }

    public void response(MessageIn message)
    {
        String dataCenter = message == null
                            ? DatabaseDescriptor.getLocalDataCenter()
                            : snitch.getDatacenter(message.from);

        responses.get(dataCenter).getAndDecrement();

        for (AtomicInteger i : responses.values())
        {
            if (i.get() > 0)
                return;
        }

        // all the quorum conditions are met
        signal();
    }

    protected int ackCount()
    {
        int n = 0;
        for (Map.Entry<String, AtomicInteger> entry : responses.entrySet())
        {
            String dc = entry.getKey();
            AtomicInteger i = entry.getValue();
            n += (strategy.getReplicationFactor(dc) / 2) + 1 - i.get();
        }
        return n;
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }
}
