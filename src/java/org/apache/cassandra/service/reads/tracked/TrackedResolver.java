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
package org.apache.cassandra.service.reads.tracked;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.reads.IReadResponse;
import org.apache.cassandra.service.reads.ResponseResolver;
import org.apache.cassandra.transport.Dispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrackedResolver<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>> extends ResponseResolver<E, P>
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedResolver.class);
    private volatile Message<IReadResponse> dataResponse;

    public TrackedResolver(ReadCommand command, Supplier<? extends P> replicaPlan, Dispatcher.RequestTime requestTime)
    {
        super(command, replicaPlan, requestTime);
    }

    @Override
    protected void validateResponse(Message<IReadResponse> message)
    {
        if (replicaPlan().lookup(message.from()).isTransient())
            throw new IllegalArgumentException("Response received from transient replica");
    }

    @Override
    public void onResponseReceived(Message<IReadResponse> message)
    {
        if (logger.isTraceEnabled())
        {
            TrackedReadResponse response = TrackedReadResponse.fromResponse(message.payload);
            logger.trace("Received response summary from {}: {}", message.from(), response.summary);
        }

        if (dataResponse == null && message.payload instanceof TrackedReadResponse.Data)
            dataResponse = message;
    }

    @Override
    public boolean isDataPresent()
    {
        return dataResponse != null;
    }

    @Override
    public boolean responsesMatch()
    {
        Collection<Message<IReadResponse>> snapshot = responses.snapshot();
        Preconditions.checkState(!snapshot.isEmpty(), "Attempted response match comparison while no responses have been received.");
        if (snapshot.size() == 1)
            return true;

        byte[] digest = null;
        boolean first = true;
        for (Message<IReadResponse> message : snapshot)
        {
            TrackedReadResponse response = TrackedReadResponse.fromResponse(message.payload);

            if (first)
                digest = response.summary.digest();
            else if (!Arrays.equals(digest, response.summary.digest()))
                return false;

            first = false;
        }
        logger.trace("All mutation summaries match");
        return true;
    }

    @Override
    public PartitionIterator getData()
    {
        return UnfilteredPartitionIterators.filter(dataResponse.payload.makeIterator(command), command.nowInSec());
    }
}
