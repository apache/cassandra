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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.SystemStrategy;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.MovementMap;
import org.apache.cassandra.tcm.ownership.PlacementDeltas;
import org.apache.cassandra.utils.concurrent.Future;

public class UnbootstrapStreams implements LeaveStreams
{
    private static final Logger logger = LoggerFactory.getLogger(UnbootstrapStreams.class);
    private final AtomicBoolean started = new AtomicBoolean();

    public Kind kind()
    {
        return Kind.UNBOOTSTRAP;
    }

    @Override
    public void execute(NodeId leaving, PlacementDeltas startLeave, PlacementDeltas midLeave, PlacementDeltas finishLeave) throws ExecutionException, InterruptedException
    {
        MovementMap movements = movementMap(ClusterMetadata.current().directory.endpoint(leaving),
                                            startLeave,
                                            finishLeave);
        movements.forEach((params, eps) -> logger.info("Unbootstrap movements: {}: {}", params, eps));
        started.set(true);
        try
        {
            unbootstrap(Schema.instance.getNonLocalStrategyKeyspaces(), movements);
        }
        catch (ExecutionException e)
        {
            logger.error("Error while decommissioning node", e);
            throw e;
        }
    }

    private static MovementMap movementMap(InetAddressAndPort leaving, PlacementDeltas startDelta, PlacementDeltas finishDelta)
    {
        MovementMap.Builder allMovements = MovementMap.builder();
        // map of src->dest movements, keyed by replication settings. During unbootstrap, this will be used to construct
        // a stream plan for each keyspace, based on their replication params.
        finishDelta.forEach((params, delta) -> {
            // no streaming for LocalStrategy and friends
            if (SystemStrategy.class.isAssignableFrom(params.klass))
                return;

            // first identify ranges to be migrated off the leaving node
            Map<Range<Token>, Replica> oldReplicas = delta.writes.removals.get(leaving).byRange();

            // next go through the additions to the write groups that will be applied during the
            // first step of the plan. These represent the ranges moving to new replicas so in
            // order to construct a streaming plan we can match these up with the corresponding
            // removals to produce a src->dest mapping.
            EndpointsByReplica.Builder movements = new EndpointsByReplica.Builder();
            RangesByEndpoint startWriteAdditions = startDelta.get(params).writes.additions;
            RangesByEndpoint startWriteRemovals = startDelta.get(params).writes.removals;
            startWriteAdditions.flattenValues()
                               .forEach(newReplica -> {
                                   if (startWriteRemovals.get(newReplica.endpoint()).contains(newReplica.range(), false))
                                       logger.debug("Streaming transient -> full conversion to {} from {}", newReplica, oldReplicas.get(newReplica.range()));
                                   movements.put(oldReplicas.get(newReplica.range()), newReplica);
                               });
            allMovements.put(params, movements.build());
        });
        return allMovements.build();
    }

    private static void unbootstrap(Keyspaces keyspaces, MovementMap movements) throws ExecutionException, InterruptedException
    {
        Supplier<Future<StreamState>> startStreaming = prepareUnbootstrapStreaming(keyspaces, movements);

        StorageService.instance.repairPaxosForTopologyChange("decommission");
        logger.info("replaying batch log and streaming data to other nodes");
        // Start with BatchLog replay, which may create hints but no writes since this is no longer a valid endpoint.
        Future<?> batchlogReplay = BatchlogManager.instance.startBatchlogReplay();
        Future<StreamState> streamSuccess = startStreaming.get();

        // Wait for batch log to complete before streaming hints.
        logger.debug("waiting for batch log processing.");
        batchlogReplay.get();

        logger.info("streaming hints to other nodes");

        Future<?> hintsSuccess = StorageService.instance.streamHints();

        // wait for the transfer runnables to signal the latch.
        logger.debug("waiting for stream acks.");
        streamSuccess.get();
        hintsSuccess.get();

        logger.debug("stream acks all received.");
    }

    private static Supplier<Future<StreamState>> prepareUnbootstrapStreaming(Keyspaces keyspaces,
                                                                             MovementMap movements)
    {
        // PrepareLeave transformation gives us a map of range movements for unbootstrap, keyed on replication settings.
        // The movements themselves are maps of leavingReplica -> newReplica(s). Here we just "inflate" the outer
        // map to a set of movements per-keyspace, duplicating where keyspaces share the same replication params
        Map<String, EndpointsByReplica> byKeyspace =
        keyspaces.stream()
                 .collect(Collectors.toMap(k -> k.name,
                                           k -> movements.get(k.params.replication)));

        return () -> streamRanges(byKeyspace);
    }

    /**
     * Send data to the endpoints that will be responsible for it in the future
     *
     * @param rangesToStreamByKeyspace keyspaces and data ranges with endpoints included for each
     * @return async Future for whether stream was success
     */
    private static Future<StreamState> streamRanges(Map<String, EndpointsByReplica> rangesToStreamByKeyspace)
    {
        // First, we build a list of ranges to stream to each host, per table
        Map<String, RangesByEndpoint> sessionsToStreamByKeyspace = new HashMap<>();

        for (Map.Entry<String, EndpointsByReplica> entry : rangesToStreamByKeyspace.entrySet())
        {
            String keyspace = entry.getKey();
            EndpointsByReplica rangesWithEndpoints = entry.getValue();

            if (rangesWithEndpoints.isEmpty())
                continue;

            //Description is always Unbootstrap? Is that right?
            Map<InetAddressAndPort, Set<Range<Token>>> transferredRangePerKeyspace = SystemKeyspace.getTransferredRanges("Unbootstrap",
                                                                                                                         keyspace,
                                                                                                                         ClusterMetadata.current().tokenMap.partitioner());
            RangesByEndpoint.Builder replicasPerEndpoint = new RangesByEndpoint.Builder();
            for (Map.Entry<Replica, Replica> endPointEntry : rangesWithEndpoints.flattenEntries())
            {
                Replica local = endPointEntry.getKey();
                Replica remote = endPointEntry.getValue();
                Set<Range<Token>> transferredRanges = transferredRangePerKeyspace.get(remote.endpoint());
                if (transferredRanges != null && transferredRanges.contains(local.range()))
                {
                    logger.debug("Skipping transferred range {} of keyspace {}, endpoint {}", local, keyspace, remote);
                    continue;
                }

                replicasPerEndpoint.put(remote.endpoint(), remote.decorateSubrange(local.range()));
            }

            sessionsToStreamByKeyspace.put(keyspace, replicasPerEndpoint.build());
        }

        StreamPlan streamPlan = new StreamPlan(StreamOperation.DECOMMISSION);

        // Vinculate StreamStateStore to current StreamPlan to update transferred ranges per StreamSession
        streamPlan.listeners(StorageService.instance.streamStateStore());

        for (Map.Entry<String, RangesByEndpoint> entry : sessionsToStreamByKeyspace.entrySet())
        {
            String keyspaceName = entry.getKey();
            RangesByEndpoint replicasPerEndpoint = entry.getValue();

            for (Map.Entry<InetAddressAndPort, RangesAtEndpoint> rangesEntry : replicasPerEndpoint.asMap().entrySet())
            {
                RangesAtEndpoint replicas = rangesEntry.getValue();
                InetAddressAndPort newEndpoint = rangesEntry.getKey();

                // TODO each call to transferRanges re-flushes, this is potentially a lot of waste
                streamPlan.transferRanges(newEndpoint, keyspaceName, replicas);
            }
        }
        return streamPlan.execute();
    }

    // todo: add more details
    public String status()
    {
        return "streams" + (started.get() ? "" : " not") + " started";
    }
}
