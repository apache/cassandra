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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.utils.concurrent.Accumulator;

public abstract class ResponseResolver<E extends Endpoints<E>, L extends ReplicaLayout<E, L>>
{
    protected static final Logger logger = LoggerFactory.getLogger(ResponseResolver.class);

    protected final ReadCommand command;
    protected final L replicaLayout;
    protected final ReadRepair<E, L> readRepair;

    // Accumulator gives us non-blocking thread-safety with optimal algorithmic constraints
    protected final Accumulator<MessageIn<ReadResponse>> responses;
    protected final long queryStartNanoTime;

    public ResponseResolver(ReadCommand command, L replicaLayout, ReadRepair<E, L> readRepair, long queryStartNanoTime)
    {
        this.command = command;
        this.replicaLayout = replicaLayout;
        this.readRepair = readRepair;
        // TODO: calculate max possible replicas for the query (e.g. local dc queries won't contact remotes)
        this.responses = new Accumulator<>(replicaLayout.all().size());
        this.queryStartNanoTime = queryStartNanoTime;
    }

    public abstract boolean isDataPresent();

    public void preprocess(MessageIn<ReadResponse> message)
    {
        try
        {
            responses.add(message);
        }
        catch (IllegalStateException e)
        {
            logger.error("Encountered error while trying to preprocess the message {}: %s in command {}, replicas: {}", message, command, readRepair, replicaLayout.consistencyLevel(), replicaLayout.selected());
            throw e;
        }
    }

    public Accumulator<MessageIn<ReadResponse>> getMessages()
    {
        return responses;
    }
}
