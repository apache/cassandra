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
import org.apache.cassandra.locator.InRemoteDc;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.transport.Dispatcher;

import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * This class blocks for a quorum of responses _in the target datacenters only_ (CL.REMOTE_QUORUM).
 */
public class DatacenterRemoteWriteResponseHandler<T> extends WriteResponseHandler<T>
{
    private final Predicate<InetAddressAndPort> waitingFor = InRemoteDc.endpoints();

    public DatacenterRemoteWriteResponseHandler(ReplicaPlan.ForWrite replicaPlan,
                                          Runnable callback,
                                          WriteType writeType,
                                          Supplier<Mutation> hintOnFailure,
                                          Dispatcher.RequestTime requestTime)
    {
        super(replicaPlan, callback, writeType, hintOnFailure, requestTime);
    }

    @Override
    public void onResponse(Message<T> message)
    {
        if (message != null && waitingFor.test(message.from()))
        {
            super.onResponse(message);

        }
        else
        {
            logResponseToIdealCLDelegate(message);
        }
    }

    @Override
    protected boolean waitingFor(InetAddressAndPort from)
    {
        // First check if it's in the target remote DC
        if (!waitingFor.test(from))
            return false;

        return super.waitingFor(from);
    }
}
