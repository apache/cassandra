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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;

/**
 * This class blocks for a quorum of responses _in all datacenters_ (CL.EACH_QUORUM).
 */
public class DatacenterSyncWriteResponseHandler<T> extends AbstractWriteResponseHandler<T>
{
    private static final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();

    private final Map<String, AtomicInteger> responses = new HashMap<String, AtomicInteger>();
    private final AtomicInteger acks = new AtomicInteger(0);

    public DatacenterSyncWriteResponseHandler(ReplicaPlan.ForWrite replicaPlan,
                                              Runnable callback,
                                              WriteType writeType,
                                              Supplier<Mutation> hintOnFailure,
                                              long queryStartNanoTime)
    {
        // Response is been managed by the map so make it 1 for the superclass.
        super(replicaPlan, callback, writeType, hintOnFailure, queryStartNanoTime);
        assert replicaPlan.consistencyLevel() == ConsistencyLevel.EACH_QUORUM;

        if (replicaPlan.replicationStrategy() instanceof NetworkTopologyStrategy)
        {
            NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) replicaPlan.replicationStrategy();
            for (String dc : strategy.getDatacenters())
            {
                int rf = strategy.getReplicationFactor(dc).allReplicas;
                responses.put(dc, new AtomicInteger((rf / 2) + 1));
            }
        }
        else
        {
            responses.put(DatabaseDescriptor.getLocalDataCenter(), new AtomicInteger(ConsistencyLevel.quorumFor(replicaPlan.replicationStrategy())));
        }

        // During bootstrap, we have to include the pending endpoints or we may fail the consistency level
        // guarantees (see #833)
        for (Replica pending : replicaPlan.pending())
        {
            responses.get(snitch.getDatacenter(pending)).incrementAndGet();
        }
    }

    public void onResponse(Message<T> message)
    {
        try
        {
            String dataCenter = message == null
                                ? DatabaseDescriptor.getLocalDataCenter()
                                : snitch.getDatacenter(message.from());

            responses.get(dataCenter).getAndDecrement();
            acks.incrementAndGet();

            for (AtomicInteger i : responses.values())
            {
                if (i.get() > 0)
                    return;
            }

            // all the quorum conditions are met
            signal();
        }
        finally
        {
            //Must be last after all subclass processing
            logResponseToIdealCLDelegate(message);
        }
    }

    protected int ackCount()
    {
        return acks.get();
    }
}
