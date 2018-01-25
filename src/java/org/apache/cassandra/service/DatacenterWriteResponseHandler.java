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

import java.util.Collection;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;

/**
 * This class blocks for a quorum of responses _in the local datacenter only_ (CL.LOCAL_QUORUM).
 */
public class DatacenterWriteResponseHandler<T> extends WriteResponseHandler<T>
{
    public DatacenterWriteResponseHandler(Collection<InetAddressAndPort> naturalEndpoints,
                                          Collection<InetAddressAndPort> pendingEndpoints,
                                          ConsistencyLevel consistencyLevel,
                                          Keyspace keyspace,
                                          Runnable callback,
                                          WriteType writeType,
                                          long queryStartNanoTime)
    {
        super(naturalEndpoints, pendingEndpoints, consistencyLevel, keyspace, callback, writeType, queryStartNanoTime);
        assert consistencyLevel.isDatacenterLocal();
    }

    @Override
    public void response(MessageIn<T> message)
    {
        if (message == null || waitingFor(message.from))
        {
            super.response(message);
        }
        else
        {
            //WriteResponseHandler.response will call logResonseToIdealCLDelegate so only do it if not calling WriteResponseHandler.response.
            //Must be last after all subclass processing
            logResponseToIdealCLDelegate(message);
        }
    }

    @Override
    protected int totalBlockFor()
    {
        // during bootstrap, include pending endpoints (only local here) in the count
        // or we may fail the consistency level guarantees (see #833, #8058)
        return consistencyLevel.blockFor(keyspace) + consistencyLevel.countLocalEndpoints(pendingEndpoints);
    }

    @Override
    protected boolean waitingFor(InetAddressAndPort from)
    {
        return consistencyLevel.isLocal(from);
    }
}
