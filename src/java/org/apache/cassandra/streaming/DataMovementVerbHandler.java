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

package org.apache.cassandra.streaming;

import java.io.IOException;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.tcm.ClusterMetadata;

public class DataMovementVerbHandler implements IVerbHandler<DataMovement>
{
    private static final Logger logger = LoggerFactory.getLogger(DataMovementVerbHandler.class);
    public static final DataMovementVerbHandler instance = new DataMovementVerbHandler();

    @Override
    public void doVerb(Message<DataMovement> message) throws IOException
    {
        MessagingService.instance().respond(NoPayload.noPayload, message); // let coordinator know we received the message
        StreamPlan streamPlan = new StreamPlan(StreamOperation.fromString(message.payload.streamOperation));
        ClusterMetadata metadata = ClusterMetadata.current();
        Schema.instance.getNonLocalStrategyKeyspaces().stream().forEach((ksm) -> {
            if (metadata.placements.get(ksm.params.replication).writes.byEndpoint().keySet().size() <= 1)
                return;

            message.payload.movements.get(ksm.params.replication).asMap().forEach((local, endpoints) -> {
                assert local.isSelf();
                boolean transientAdded = false;
                boolean fullAdded = false;
                for (Replica remote : DatabaseDescriptor.getEndpointSnitch().sortedByProximity(local.endpoint(), endpoints).filter(ep -> FailureDetector.instance.isAlive(ep.endpoint())))
                {
                    assert !remote.isSelf();
                    if (remote.isFull() && !fullAdded)
                    {
                        streamPlan.requestRanges(remote.endpoint(), ksm.name, RangesAtEndpoint.of(local), RangesAtEndpoint.empty(local.endpoint()));
                        fullAdded = true;
                    }
                    else if (remote.isTransient() && !transientAdded)
                    {
                        streamPlan.requestRanges(remote.endpoint(), ksm.name, RangesAtEndpoint.empty(local.endpoint()), RangesAtEndpoint.of(local));
                        transientAdded = true;
                    }

                    if (fullAdded && transientAdded)
                        break;
                }
                if (!fullAdded)
                {
                    if (local.isFull() || !transientAdded)
                    {
                        logger.error("Found no sources to stream from for {}", local);
                        send(false, message.from(), message.payload.streamOperation, message.payload.operationId);
                    }
                }
            });
        });

        streamPlan.execute().addEventListener(new StreamEventHandler()
        {
            @Override
            public void handleStreamEvent(StreamEvent event) {}

            @Override
            public void onSuccess(@Nullable StreamState streamState)
            {
                send(true, message.from(), message.payload.streamOperation, message.payload.operationId);
            }

            @Override
            public void onFailure(Throwable throwable)
            {
                send(false, message.from(), message.payload.streamOperation, message.payload.operationId);
            }
        });

    }

    private static void send(boolean success, InetAddressAndPort to, String operationType, String operationId)
    {
        MessagingService.instance().send(Message.out(Verb.DATA_MOVEMENT_EXECUTED_REQ,
                                                     new DataMovement.Status(success, operationType, operationId)),
                                         to);
    }
}
