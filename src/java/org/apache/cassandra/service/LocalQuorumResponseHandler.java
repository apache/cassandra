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

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.locator.InOurDc;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.transport.Dispatcher;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * This class blocks for a quorum of responses _in the local datacenter only_ (CL.LOCAL_QUORUM).
 */
public class LocalQuorumResponseHandler<T> extends WriteResponseHandler<T>
{
    private final Predicate<InetAddressAndPort> waitingFor = InOurDc.endpoints();

    /** We only want to consider acks from the local DC towards our success count for LOCAL_QUORUM */
    private final AtomicInteger localDCAcks = new AtomicInteger(0);

    public LocalQuorumResponseHandler(ReplicaPlan.ForWrite replicaPlan,
                                      Runnable callback,
                                      WriteType writeType,
                                      Supplier<Mutation> hintOnFailure,
                                      Dispatcher.RequestTime requestTime)
    {
        super(replicaPlan, callback, writeType, hintOnFailure, requestTime);
        assert replicaPlan.consistencyLevel().isDatacenterLocal();
        // We don't update our tracker w/any per-DC context here since we consider success as "got enough in local DC".
    }

    @Override
    public void onResponse(Message<T> msg)
    {
        if (msg == null || waitingFor(msg.from()))
            localDCAcks.getAndIncrement();
        super.onResponse(msg);
    }

    @Override
    public boolean receivedSufficientResponses()
    {
        return localDCAcks.get() >= tracker.requiredResponses;
    }

    @Override
    protected boolean waitingFor(InetAddressAndPort from)
    {
        return waitingFor.test(from);
    }
}
