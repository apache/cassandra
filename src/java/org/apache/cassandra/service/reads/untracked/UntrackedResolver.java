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

package org.apache.cassandra.service.reads.untracked;

import java.util.function.Supplier;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.reads.IReadResponse;
import org.apache.cassandra.service.reads.ResponseResolver;
import org.apache.cassandra.transport.Dispatcher;

public abstract class UntrackedResolver<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>> extends ResponseResolver<E, P>
{
    public UntrackedResolver(ReadCommand command, Supplier<? extends P> replicaPlan, Dispatcher.RequestTime requestTime)
    {
        super(command, replicaPlan, requestTime);
    }

    @Override
    protected void validateResponse(Message<IReadResponse> message)
    {
        ReadResponse response = ReadResponse.fromResponse(message.payload);
        if (replicaPlan().lookup(message.from()).isTransient() &&
            response.isDigestResponse())
            throw new IllegalArgumentException("Digest response received from transient replica");
    }
}
