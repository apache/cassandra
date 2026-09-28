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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundSink;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.ownership.MovementMap;

import static org.apache.cassandra.streaming.StreamOperation.RESTORE_REPLICA_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DataMovementVerbHandlerTest extends CQLTester
{
    @Test
    public void testRestoreReplicaCount() throws Exception
    {
        AtomicReference<StreamResultFuture> stream = new AtomicReference<>();
        StreamManager.StreamListener listener = new StreamManager.StreamListener()
        {
            @Override
            public void onRegister(StreamResultFuture result)
            {
                stream.set(result);
            }
        };
        BlockingQueue<Message<?>> replies = new LinkedBlockingQueue<>();
        OutboundSink.Filter sink = (message, to, type) -> {
            if (message.verb() == Verb.INITIATE_DATA_MOVEMENTS_RSP || message.verb() == Verb.DATA_MOVEMENT_EXECUTED_REQ)
            {
                replies.add(message);
                return false;
            }
            return true;
        };

        StreamManager.instance.addListener(listener);
        MessagingService.instance().outboundSink.add(sink);
        try
        {
            DataMovement movement = new DataMovement("restore-replicas", RESTORE_REPLICA_COUNT.name(), MovementMap.empty());
            Message<DataMovement> request = Message.builder(Verb.INITIATE_DATA_MOVEMENTS_REQ, movement)
                                                   .from(InetAddressAndPort.getByName("127.0.0.2"))
                                                   .build();
            DataMovementVerbHandler.instance.doVerb(request);

            Message<?> acknowledgement = replies.poll(10, TimeUnit.SECONDS);
            assertNotNull(acknowledgement);
            assertEquals(Verb.INITIATE_DATA_MOVEMENTS_RSP, acknowledgement.verb());
            assertEquals(request.id(), acknowledgement.id());

            Message<?> completion = replies.poll(10, TimeUnit.SECONDS);
            assertNotNull(completion);
            assertEquals(Verb.DATA_MOVEMENT_EXECUTED_REQ, completion.verb());
            DataMovement.Status status = (DataMovement.Status) completion.payload;
            assertTrue(status.success);
            assertEquals(movement.operationId, status.operationId);
            assertEquals(RESTORE_REPLICA_COUNT.name(), status.operationType);

            assertNotNull(stream.get());
            assertEquals(RESTORE_REPLICA_COUNT, stream.get().get(10, TimeUnit.SECONDS).streamOperation);
            assertFalse(stream.get().streamOperation.requiresViewBuild());
        }
        finally
        {
            MessagingService.instance().outboundSink.remove(sink);
            StreamManager.instance.removeListener(listener);
        }
    }
}
