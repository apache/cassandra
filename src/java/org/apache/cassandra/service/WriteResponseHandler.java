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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * Handles blocking writes for ONE, ANY, TWO, THREE, QUORUM, and ALL consistency levels.
 *
 * Response tracking is delegated to coordinationPlan.tracker().
 */
public class WriteResponseHandler<T> extends AbstractWriteResponseHandler<T>
{
    protected static final Logger logger = LoggerFactory.getLogger(WriteResponseHandler.class);

    public WriteResponseHandler(CoordinationPlan.ForWrite coordinationPlan,
                                Runnable callback,
                                WriteType writeType,
                                Supplier<Mutation> hintOnFailure,
                                Dispatcher.RequestTime requestTime)
    {
        super(coordinationPlan, callback, writeType, hintOnFailure, requestTime);
    }

    public WriteResponseHandler(CoordinationPlan.ForWrite coordinationPlan, WriteType writeType, Supplier<Mutation> hintOnFailure, Dispatcher.RequestTime requestTime)
    {
        this(coordinationPlan, null, writeType, hintOnFailure, requestTime);
    }

    public void onResponse(Message<T> m)
    {
        InetAddressAndPort from = m == null ? FBUtilities.getBroadcastAddressAndPort() : m.from();
        Map<ParamType, Object> params = m != null ? m.header.params() : MessageParams.capture();

        if (WriteWarningContext.isSupported(params.keySet()))
            getWarningContext().updateCounters(params);

        replicaPlan().collectSuccess(from);

        plan.responses().onResponse(from);

        if (plan.responses().isComplete())
            signal();

        // Must be last (see comment on AbstractWriteResponseHandler.logResponseToIdealCLDelegate for why) - forward to ideal CL delegate
        logResponseToIdealCLDelegate(m);
    }

    protected int ackCount()
    {
        return plan.responses().received();
    }
}
