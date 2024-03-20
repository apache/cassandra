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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.SystemStrategy;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.streaming.DataMovement;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.MovementMap;
import org.apache.cassandra.tcm.ownership.PlacementDeltas;
import org.apache.cassandra.tcm.ownership.ReplicaGroups;

import static org.apache.cassandra.streaming.StreamOperation.RESTORE_REPLICA_COUNT;

public class RemoveNodeStreams implements LeaveStreams
{
    private static final Logger logger = LoggerFactory.getLogger(UnbootstrapStreams.class);
    private final AtomicBoolean finished = new AtomicBoolean();
    private final AtomicBoolean failed = new AtomicBoolean();
    private DataMovements.ResponseTracker responseTracker;

    @Override
    public void execute(NodeId leaving, PlacementDeltas startLeave, PlacementDeltas midLeave, PlacementDeltas finishLeave) throws ExecutionException, InterruptedException
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        MovementMap movements = movementMap(metadata.directory.endpoint(leaving),
                                            metadata,
                                            startLeave);
        movements.forEach((params, eps) -> logger.info("Removenode movements: {}: {}", params, eps));
        String operationId = leaving.toUUID().toString();
        responseTracker = DataMovements.instance.registerMovements(RESTORE_REPLICA_COUNT, operationId, movements);
        movements.byEndpoint().forEach((endpoint, epMovements) -> {
            DataMovement msg = new DataMovement(operationId, RESTORE_REPLICA_COUNT.name(), epMovements);
            MessagingService.instance().sendWithCallback(Message.out(Verb.INITIATE_DATA_MOVEMENTS_REQ, msg), endpoint, response -> {
                logger.debug("Endpoint {} starting streams {}", response.from(), epMovements);
            });
        });

        try
        {
            responseTracker.await();
            finished.set(true);
        }
        catch (Exception e)
        {
            failed.set(true);
            throw e;
        }
        finally
        {
            DataMovements.instance.unregisterMovements(RESTORE_REPLICA_COUNT, operationId);
        }
    }

    @Override
    public Kind kind()
    {
        return Kind.REMOVENODE;
    }

    public String status()
    {
        if (finished.get())
            return "streaming finished";
        if (failed.get())
            return "streaming failed";
        if (responseTracker ==  null)
            return "streaming not yet started";
        return responseTracker.remaining()
                              .stream()
                              .map(i -> i.toString(true))
                              .collect(Collectors.joining(",", "Waiting on streaming responses from: ", ""));
    }

    /**
     * create a map where the key is the destination, and the values are possible sources
     * @return
     */
    private static MovementMap movementMap(InetAddressAndPort leaving, ClusterMetadata metadata, PlacementDeltas startDelta)
    {
        MovementMap.Builder allMovements = MovementMap.builder();
        // map of dest->src* movements, keyed by replication settings. During unbootstrap, this will be used to construct
        // a stream plan for each keyspace, based on their replication params.
        startDelta.forEach((params, delta) -> {
            // no streaming for LocalStrategy and friends
            if (SystemStrategy.class.isAssignableFrom(params.klass))
                return;

            EndpointsByReplica.Builder movements = new EndpointsByReplica.Builder();
            RangesByEndpoint startWriteAdditions = startDelta.get(params).writes.additions;
            RangesByEndpoint startWriteRemovals = startDelta.get(params).writes.removals;
            // find current placements from the metadata, we need to stream from replicas that are not changed and are therefore not in the deltas
            ReplicaGroups currentPlacements = metadata.placements.get(params).reads;
            startWriteAdditions.flattenValues()
                               .forEach(newReplica -> {
                                   EndpointsForRange.Builder candidateBuilder = new EndpointsForRange.Builder(newReplica.range());
                                   currentPlacements.forRange(newReplica.range()).get().forEach(replica -> {
                                       if (!replica.endpoint().equals(leaving) && !replica.endpoint().equals(newReplica.endpoint()))
                                           candidateBuilder.add(replica, ReplicaCollection.Builder.Conflict.NONE);
                                   });
                                   EndpointsForRange sources = candidateBuilder.build();
                                   // log if newReplica is an existing transient replica moving to a full replica
                                   if (startWriteRemovals.get(newReplica.endpoint()).contains(newReplica.range(), false))
                                       logger.debug("Streaming transient -> full conversion to {} from {}", newReplica.endpoint(), sources);
                                   movements.putAll(newReplica, sources, ReplicaCollection.Builder.Conflict.NONE);
                               });
            allMovements.put(params, movements.build());
        });
        return allMovements.build();
    }
}
