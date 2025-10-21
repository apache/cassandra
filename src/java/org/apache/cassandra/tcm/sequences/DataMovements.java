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

package org.apache.cassandra.tcm.sequences;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.streaming.DataMovement;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.tcm.ownership.MovementMap;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

public class DataMovements implements IVerbHandler<DataMovement.Status>
{
    private static final Logger logger = LoggerFactory.getLogger(DataMovements.class);
    public static final DataMovements instance = new DataMovements();

    private final Map<StreamOperation, Map<String, ResponseTracker>> inFlightMovements = new ConcurrentHashMap<>();

    public ResponseTracker registerMovements(StreamOperation type, String operationId, MovementMap movements)
    {
        Map<String, ResponseTracker> inFlightForType = inFlightMovements.computeIfAbsent(type, (type_) -> new ConcurrentHashMap<>());
        return inFlightForType.computeIfAbsent(operationId, (id) -> new ResponseTracker(movements));
    }

    public void unregisterMovements(StreamOperation type, String operationId)
    {
        Map<String, ResponseTracker> inFlightForType = inFlightMovements.get(type);
        if (inFlightForType != null)
            inFlightForType.remove(operationId);
    }

    @Override
    public void doVerb(Message<DataMovement.Status> message) throws IOException
    {
        Map<String, ResponseTracker> inFlightForType = inFlightMovements.get(StreamOperation.valueOf(message.payload.operationType));
        ResponseTracker responseTracker = inFlightForType.get(message.payload.operationId);
        if (responseTracker != null)
        {
            if (message.payload.success)
                responseTracker.received(message.from());
            else
                responseTracker.failure(message.from());
        }
        else
        {
            logger.error("Got DataMovement executed message for {}:{} from {}, but no tracker registered for that operation",
                         message.payload.operationType, message.payload.operationId, message.from());
            logger.debug("Current in-flight movements: {}", DataMovements.instance);
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        inFlightMovements.forEach((type, byType) -> {
            sb.append(type).append('[');
            byType.forEach((id, tracker) -> {
                sb.append(id).append(':');
                tracker.remaining().forEach(i -> sb.append(i.toString(true)).append(','));
            });
            sb.append("],");
        });
        return sb.toString();
    }

    public static class ResponseTracker
    {
        private final Set<InetAddressAndPort> expected = ConcurrentHashMap.newKeySet();
        private final AsyncPromise<Object> promise = new AsyncPromise<>();

        public ResponseTracker(MovementMap movements)
        {
            movements.byEndpoint().forEach((endpoint, epMovements) -> expected.addAll(epMovements.byEndpoint().keySet()));
            if (expected.isEmpty())
                promise.setSuccess(null);
        }

        public void received(InetAddressAndPort from)
        {
            logger.info("Received stream completion from {}", from);
            expected.remove(from);
            if (expected.isEmpty())
                promise.setSuccess(null);
        }

        public void failure(InetAddressAndPort from)
        {
            logger.warn("Received stream failure from {}", from);
            if (expected.contains(from))
                promise.setFailure(new RuntimeException());
        }

        public void await()
        {
            try
            {
                promise.get();
            }
            catch (InterruptedException | ExecutionException e)
            {
                throw new RuntimeException(e);
            }
        }

        public Set<InetAddressAndPort> remaining()
        {
            return new HashSet<>(expected);
        }

        public String toString()
        {
            return expected.toString();
        }
    }
}
