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
package org.apache.cassandra.service.reads;

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.concurrent.Accumulator;

public abstract class ResponseResolver<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>>
{
    protected static final Logger logger = LoggerFactory.getLogger(ResponseResolver.class);

    protected final ReadCommand command;
    // TODO: this doesn't need to be a full ReplicaPlan; just a replica collection
    protected final Supplier<? extends P> replicaPlan;

    // Accumulator gives us non-blocking thread-safety with optimal algorithmic constraints
    protected final Accumulator<Message<ReadResponse>> responses;
    protected final long queryStartNanoTime;

    public ResponseResolver(ReadCommand command, Supplier<? extends P> replicaPlan, long queryStartNanoTime)
    {
        this.command = command;
        this.replicaPlan = replicaPlan;
        this.responses = new Accumulator<>(replicaPlan.get().readCandidates().size());
        this.queryStartNanoTime = queryStartNanoTime;
    }

    protected P replicaPlan()
    {
        return replicaPlan.get();
    }

    public abstract boolean isDataPresent();

    public void preprocess(Message<ReadResponse> message)
    {
        if (replicaPlan().lookup(message.from()).isTransient() &&
            message.payload.isDigestResponse())
            throw new IllegalArgumentException("Digest response received from transient replica");

        try
        {
            responses.add(message);
        }
        catch (IllegalStateException e)
        {
            logger.error("Encountered error while trying to preprocess the message {}, in command {}, replica plan: {}",
                         message, command, replicaPlan);
            throw e;
        }
    }

    public Accumulator<Message<ReadResponse>> getMessages()
    {
        return responses;
    }
}
