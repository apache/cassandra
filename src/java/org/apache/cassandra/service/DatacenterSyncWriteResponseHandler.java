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

import java.util.Map;
import java.util.function.Supplier;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.MessageParams;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.locator.CoordinationPlan;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.service.writes.thresholds.WriteWarningContext;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;

/**
 * This class blocks for a quorum of responses _in all datacenters_ (CL.EACH_QUORUM).
 *
 * The CompositeTracker handles per-datacenter counting.
 */
public class DatacenterSyncWriteResponseHandler<T> extends AbstractWriteResponseHandler<T>
{
    public DatacenterSyncWriteResponseHandler(CoordinationPlan.ForWrite coordinationPlan,
                                              Runnable callback,
                                              WriteType writeType,
                                              Supplier<Mutation> hintOnFailure,
                                              Dispatcher.RequestTime requestTime)
    {
        super(coordinationPlan, callback, writeType, hintOnFailure, requestTime);
        assert replicaPlan().consistencyLevel() == ConsistencyLevel.EACH_QUORUM;
    }

    public void onResponse(Message<T> message)
    {
        try
        {
            Map<ParamType, Object> params = message != null
                                            ? message.header.params()
                                            : MessageParams.capture();

            if (WriteWarningContext.isSupported(params.keySet()))
                getWarningContext().updateCounters(params);

            InetAddressAndPort from = message == null ? FBUtilities.getBroadcastAddressAndPort() : message.from();

            plan.responses().onResponse(from);

            if (plan.responses().isComplete())
                signal();
        }
        finally
        {
            // Must be last - forward to ideal CL delegate
            logResponseToIdealCLDelegate(message);
        }
    }

    protected int ackCount()
    {
        return plan.responses().received();
    }
}
