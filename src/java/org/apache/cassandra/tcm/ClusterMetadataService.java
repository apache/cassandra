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

package org.apache.cassandra.tcm;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingService.FailureResponseException;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.service.ConsensusTableMigrationState;
import org.apache.cassandra.service.ConsensusTableMigrationState.MigrationStateSnapshot;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Simulate;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.net.Verb.REQUEST_CM;
import static org.apache.cassandra.net.Verb.UPDATE_CM;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.Simulate.With.MONITORS;

@Simulate(with=MONITORS)
public class ClusterMetadataService
{
    private static final Logger logger = LoggerFactory.getLogger(ClusterMetadataService.class);

    public static final ClusterMetadataService instance = new ClusterMetadataService();

    public volatile ClusterMetadata metadata = new ClusterMetadata(Epoch.FIRST, new ConsensusTableMigrationState.MigrationStateSnapshot(ImmutableMap.of(), Epoch.FIRST));

    // Update epoch messages can arrive out of order in simulator it seems so we need to put them back in order
    public final PriorityQueue<MigrationStateSnapshot> pendingTopologyUpdates = new PriorityQueue<>(Comparator.comparing(a -> a.epoch));

    @VisibleForTesting
    public static void reset()
    {
        instance.metadata = new ClusterMetadata(instance.metadata.epoch.nextEpoch(false), new ConsensusTableMigrationState.MigrationStateSnapshot(ImmutableMap.of(), Epoch.FIRST));
        AccordService.instance().createEpochFromConfigUnsafe();
    }

    public ClusterMetadata metadata()
    {
        return metadata;
    }

    public ClusterMetadata commit(Transformation transform)
    {
        while (true)
        {
            ClusterMetadata startingMetadata = metadata;
            Transformation.Result result = transform.execute(metadata);
            if (result.isSuccess())
            {
                synchronized (this)
                {
                    // This is mostly nonsense you can only safely update on one node
                    if (metadata != startingMetadata)
                        continue;
                    metadata = result.success().metadata;
                    AccordService.instance().createEpochFromConfigUnsafe();
                }
                propagateUpdate();
            }
            else
            {
                synchronized (this)
                {
                    if (metadata != startingMetadata)
                        continue;
                }
                logger.info("Cluster metadata transformation rejected because: {}", result.rejected().reason);
            }
            return metadata;
        }
    }

    private void propagateUpdate()
    {
        Set<InetAddressAndPort> toUpdate = new HashSet<>(StorageService.instance.getTokenMetadata().getAllEndpoints());
        toUpdate.remove(FBUtilities.getBroadcastAddressAndPort());
        Message<MigrationStateSnapshot> out = Message.out(UPDATE_CM, metadata.migrationStateSnapshot);
        Map<InetAddressAndPort, Future<?>> results = new HashMap<>();
        for (InetAddressAndPort endpoint : toUpdate)
        {
            results.put(endpoint, MessagingService.instance().sendWithResult(out, endpoint));
        }

        try
        {
            FutureCombiner.allOf(results.values()).get(1, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            // Simulator will do all sorts of things so ignore these
        }
        catch (TimeoutException e)
        {
            // Waiting so that test code knows the updates are propagated and can read the side effects
            // In simulator we don't care if they update propagates
       }
    }

    public static void log(String message)
    {
        System.out.println(FBUtilities.getBroadcastAddressAndPort().toString() + ":" + Thread.currentThread().getName() + " " + message);
    }

    public ClusterMetadata maybeCatchup(Epoch theirEpoch)
    {
        int iteration = 0;
        while (true)
        {
            // Temporary hack to make sure Accord has finished updating config for the new epoch
            synchronized (this)
            {
                if (theirEpoch.isEqualOrBefore(metadata.epoch))
                    return metadata;
            }
            log(iteration + " Need catch up our epoch " + metadata.epoch + " and their epoch " + theirEpoch);
            iteration++;
            Message<NoPayload> out = Message.out(REQUEST_CM, noPayload);
            Map<InetAddressAndPort, Future<MigrationStateSnapshot>> resultFutures = new HashMap<>();
            for (InetAddressAndPort endpoint : StorageService.instance.getTokenMetadata().getAllEndpoints())
            {
                if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
                    continue;
                resultFutures.put(endpoint, MessagingService.instance().<MigrationStateSnapshot>sendWithResult(out, endpoint).map(m -> m.payload));
            }

            long deadline = nanoTime() + TimeUnit.SECONDS.toNanos(2);
            for (Map.Entry<InetAddressAndPort, Future<MigrationStateSnapshot>> entry : resultFutures.entrySet())
            {
                MigrationStateSnapshot snapshot;
                try
                {
                    snapshot = entry.getValue().get(Math.max(0, deadline - nanoTime()), TimeUnit.NANOSECONDS);
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
                catch (ExecutionException e)
                {
                    if (e.getCause() instanceof FailureResponseException)
                    {
                        e.printStackTrace();
                        continue;
                    }
                    throw new RuntimeException(e);
                }
                catch (TimeoutException e)
                {
                    log("Timed out waiting for a response from " + entry.getKey());
                    continue;
                }
                synchronized (this)
                {
                    log("Comparing our epoch " + metadata.epoch + " to return epoch " + snapshot.epoch);
                    if (snapshot.epoch.compareTo(metadata.epoch) > 0 && !pendingTopologyUpdates.contains(snapshot))
                    {
                        log("Adding their update to the queue " + snapshot.epoch);
                        pendingTopologyUpdates.add(snapshot);
                    }
                    // Apply everything we found
                    while (!pendingTopologyUpdates.isEmpty() && pendingTopologyUpdates.peek().epoch.getEpoch() == ClusterMetadataService.instance.metadata.epoch.getEpoch() + 1)
                    {
                        snapshot = pendingTopologyUpdates.poll();
                        log("applying update " + snapshot.epoch);
                        metadata = new ClusterMetadata(snapshot.epoch, snapshot);
                        AccordService.instance().createEpochFromConfigUnsafe();
                    }
                }
            }
        }
    }

    public static final IVerbHandler<MigrationStateSnapshot> updateHandler = message ->
    {
        try
        {
            synchronized (ClusterMetadataService.instance)
            {
                if (message.payload.epoch.compareTo(ClusterMetadataService.instance.metadata.epoch) > 0)
                {
                    PriorityQueue<MigrationStateSnapshot> queuedUpdates = ClusterMetadataService.instance.pendingTopologyUpdates;
                    if (!queuedUpdates.contains(message.payload))
                    {
//                        System.out.println(Thread.currentThread().getName() + " Received new epoch " + message.payload.epoch);
                        queuedUpdates.offer(message.payload);
                    }
                    else
                    {
//                        System.out.println(Thread.currentThread().getName() + " Received previously known epoch " + message.payload.epoch);
                    }
                    while (!queuedUpdates.isEmpty() && queuedUpdates.peek().epoch.getEpoch() == ClusterMetadataService.instance.metadata.epoch.getEpoch() + 1)
                    {
                        MigrationStateSnapshot snapshot = queuedUpdates.poll();
//                        System.out.println(Thread.currentThread().getName() + " Applying epoch in update handler " + snapshot.epoch);
                        ClusterMetadataService.instance.metadata = new ClusterMetadata(snapshot.epoch, snapshot);
                        AccordService.instance().createEpochFromConfigUnsafe();
                    }
                }
            }
            MessagingService.instance().respond(noPayload, message);
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            MessagingService.instance().respondWithFailure(RequestFailureReason.forException(t), message);
        }
    };

    public static final IVerbHandler<NoPayload> catchupHandler = message ->
    {
        try
        {
//            System.out.println("Received CM request from " + message.from() + " responding with epoch " + ClusterMetadata.current().epoch);
            MessagingService.instance().respond(ClusterMetadata.current().migrationStateSnapshot, message);
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            MessagingService.instance().respondWithFailure(RequestFailureReason.forException(t), message);
        }
    };
}
