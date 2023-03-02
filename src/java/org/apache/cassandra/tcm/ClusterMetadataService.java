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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.Replication;
import org.apache.cassandra.tcm.ownership.PlacementProvider;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.transformations.SealPeriod;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.tcm.ClusterMetadataService.State.LOCAL;
import static org.apache.cassandra.tcm.compatibility.GossipHelper.emptyWithSchemaFromSystemTables;


public class ClusterMetadataService
{
    private static final Logger logger = LoggerFactory.getLogger(ClusterMetadataService.class);

    private static ClusterMetadataService instance;
    private static Throwable trace;

    public static void setInstance(ClusterMetadataService newInstance)
    {
        if (instance != null)
            throw new IllegalStateException(String.format("Cluster metadata is already initialized to %s.", instance),
                                            trace);
        instance = newInstance;
        trace = new RuntimeException("Previously initialized trace");
    }

    @VisibleForTesting
    public static ClusterMetadataService unsetInstance()
    {
        ClusterMetadataService tmp = instance();
        instance = null;
        return tmp;
    }

    public static ClusterMetadataService instance()
    {
        return instance;
    }

    public interface Processor
    {
        // TODO: remove entry id from the interface?
        /**
         * Method is _only_ responsible to commit the transformation to the cluster metadata. Implementers _have to ensure_
         * local visibility and enactment of the metadata!
         */
        Commit.Result commit(Entry.Id entryId, Transformation transform, Epoch lastKnown);
        // TODO: add a debounce to requestReplay. Right now, because of ResponseVerbHandler, it is possible to send
        // a barage of these requests.
        /**
         * Replays to the highest known epoch.
         *
         * Upon replay, all items _at least_ up to returned epoch will be visible.
         */
        ClusterMetadata replayAndWait();
    }

    private final PlacementProvider placementProvider;
    private final Processor processor;
    private final LocalLog log;
    private final Commit.Replicator replicator;
    private final MetadataSnapshots snapshots;

    private final Replication.ReplicationHandler replicationHandler;
    private final Replication.LogNotifyHandler logNotifyHandler;
    private final IVerbHandler<Replay> replayRequestHandler;
    private final IVerbHandler<Commit> commitRequestHandler;
    private final IVerbHandler<NoPayload> currentEpochHandler;

    public static State state()
    {
        return state(ClusterMetadata.current());
    }

    public static State state(ClusterMetadata clusterMetadata)
    {
        // The node is a full member of the CMS if it has started participating in reads for distributed metadata table (which
        // implies it is a write replica as well). In other words, it's a fully joined member of the replica set responsible for
        // the distributed metadata table.
        if (ClusterMetadata.current().isCMSMember(FBUtilities.getBroadcastAddressAndPort()))
            return LOCAL;
        return State.REMOTE;
    }

    ClusterMetadataService(PlacementProvider placementProvider,
                           ClusterMetadata initial,
                           Function<Processor, Processor> wrapProcessor,
                           Supplier<State> cmsStateSupplier)
    {
        this.placementProvider = placementProvider;
        this.snapshots = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots();

        log = LocalLog.async(initial);
        Processor localProcessor = wrapProcessor.apply(new PaxosBackedProcessor(log));
        replicator = new Commit.DefaultReplicator(() -> log.metadata().directory);
        currentEpochHandler = new CurrentEpochRequestHandler();
        replayRequestHandler = new SwitchableHandler<>(new Replay.Handler(), cmsStateSupplier);
        commitRequestHandler = new SwitchableHandler<>(new Commit.Handler(localProcessor, replicator), cmsStateSupplier);
        processor = new SwitchableProcessor(localProcessor,
                                            new RemoteProcessor(log, Discovery.instance::discoveredNodes),
                                            cmsStateSupplier);

        replicationHandler = new Replication.ReplicationHandler(log);
        logNotifyHandler = new Replication.LogNotifyHandler(log);
    }

    @VisibleForTesting
    public ClusterMetadataService(PlacementProvider placementProvider,
                                  MetadataSnapshots snapshots,
                                  LocalLog log,
                                  Processor processor,
                                  Commit.Replicator replicator,
                                  boolean isMemberOfOwnershipGroup)
    {
        this.placementProvider = placementProvider;
        this.log = log;
        this.processor = processor;
        this.replicator = replicator;
        this.snapshots = snapshots;

        replicationHandler = new Replication.ReplicationHandler(log);
        logNotifyHandler = new Replication.LogNotifyHandler(log);
        currentEpochHandler = new CurrentEpochRequestHandler();

        replayRequestHandler = isMemberOfOwnershipGroup ? new Replay.Handler() : null;
        commitRequestHandler = isMemberOfOwnershipGroup ? new Commit.Handler(processor, replicator) : null;
    }

    private ClusterMetadataService(PlacementProvider placementProvider,
                                   MetadataSnapshots snapshots,
                                   LocalLog log,
                                   Processor processor,
                                   Commit.Replicator replicator,
                                   Replication.ReplicationHandler replicationHandler,
                                   Replication.LogNotifyHandler logNotifyHandler,
                                   CurrentEpochRequestHandler currentEpochHandler,
                                   Replay.Handler replayRequestHandler,
                                   Commit.Handler commitRequestHandler)
    {
        this.placementProvider = placementProvider;
        this.snapshots = snapshots;
        this.log = log;
        this.processor = processor;
        this.replicator = replicator;
        this.replicationHandler = replicationHandler;
        this.logNotifyHandler = logNotifyHandler;
        this.currentEpochHandler = currentEpochHandler;
        this.replayRequestHandler = replayRequestHandler;
        this.commitRequestHandler = commitRequestHandler;
    }

    @SuppressWarnings("resource")
    public static void initializeForTools(boolean loadSSTables)
    {
        if (instance != null)
            return;
        ClusterMetadata emptyFromSystemTables = emptyWithSchemaFromSystemTables();
        emptyFromSystemTables.schema.initializeKeyspaceInstances(DistributedSchema.empty(), loadSSTables);
        emptyFromSystemTables = emptyFromSystemTables.forceEpoch(Epoch.EMPTY);
        LocalLog log = LocalLog.sync(emptyFromSystemTables, new AtomicLongBackedProcessor.InMemoryStorage(), true);
        ClusterMetadataService cms = new ClusterMetadataService(new UniformRangePlacement(),
                                                                MetadataSnapshots.NO_OP,
                                                                log,
                                                                new AtomicLongBackedProcessor(log),
                                                                Commit.Replicator.NO_OP,
                                                                new Replication.ReplicationHandler(log),
                                                                new Replication.LogNotifyHandler(log),
                                                                new CurrentEpochRequestHandler(),
                                                                null,
                                                                null);
        log.bootstrap(FBUtilities.getBroadcastAddressAndPort());
        ClusterMetadataService.setInstance(cms);
    }

    public boolean isCurrentMember(InetAddressAndPort peer)
    {
        return ClusterMetadata.current().isCMSMember(peer);
    }

    public void addToCms(List<String> ignoredEndpoints)
    {
        ClusterMetadata metadata = metadata();
        if (metadata.isCMSMember(FBUtilities.getBroadcastAddressAndPort()))
        {
            logger.info("Already in the CMS");
            throw new IllegalStateException("Already in the CMS");
        }

        // TODO
    }

    public final Supplier<Entry.Id> entryIdGen = new Entry.DefaultEntryIdGen();

    public ClusterMetadata commit(Transformation transform)
    {
        return commit(transform,
                      (metadata) -> false,
                      (metadata) -> metadata,
                      (metadata, reason) -> {
                          throw new IllegalStateException(reason);
                      });
    }

    public <T1> T1 commit(Transformation transform, Predicate<ClusterMetadata> retry, Function<ClusterMetadata, T1> onSuccess, BiFunction<ClusterMetadata, String, T1> onReject)
    {
        Retry.Backoff backoff = new Retry.Backoff();
        while (!backoff.reachedMax())
        {
            Commit.Result result = processor.commit(entryIdGen.get(), transform, null);

            if (result.isSuccess())
            {
                // TODO: we could actually move replicator to processor, if we have a source node attached to the transformation.
                // In other words, we simply know who was a submitter, so we wouldn't replicate to the submitter, since the
                // submitter is going to learn about the result of execution by other means, namely the response.
                if (state() == LOCAL) replicator.send(result, null);

                while (!backoff.reachedMax())
                {
                    try
                    {
                        return onSuccess.apply(awaitAtLeast(result.success().epoch));
                    }
                    catch (TimeoutException t)
                    {
                        logger.error("Timed out while waiting for the follower to enact the epoch {}", result.success().epoch, t);
                        backoff.maybeSleep();
                    }
                    catch (InterruptedException e)
                    {
                        throw new IllegalStateException("Couldn't commit the transformation. Is the node shutting down?", e);
                    }
                }
            }
            else
            {
                ClusterMetadata metadata = replayAndWait();

                if (result.failure().rejected)
                    return onReject.apply(metadata, result.failure().message);

                if (!retry.test(metadata))
                    throw new IllegalStateException(String.format("Committing transformation %s failed and retry criteria was not satisfied. Current tries: %s", transform, backoff.tries + 1));

                logger.info("Couldn't commit the transformation due to \"{}\". Retrying again in {}ms.", result.failure().message, backoff.backoffMs);
                // Back-off and retry
                backoff.maybeSleep();
            }
        }

        if (backoff.reachedMax())
            throw new IllegalStateException(String.format("Couldn't commit the transformation %s after %d tries", transform, backoff.maxTries()));

        throw new IllegalStateException(String.format("Could not succeed committing %s after %d tries", transform, backoff.maxTries));
    }

    /**
     * Accessors
     */

    public static Replication.ReplicationHandler replicationHandler()
    {
        return ClusterMetadataService.instance().replicationHandler;
    }

    public static Replication.LogNotifyHandler logNotifyHandler()
    {
        return ClusterMetadataService.instance().logNotifyHandler;
    }

    public static IVerbHandler<Replay> replayRequestHandler()
    {
        return ClusterMetadataService.instance().replayRequestHandler;
    }

    public static IVerbHandler<Commit> commitRequestHandler()
    {
        return ClusterMetadataService.instance().commitRequestHandler;
    }

    public static IVerbHandler<NoPayload> currentEpochRequestHandler()
    {
        return ClusterMetadataService.instance().currentEpochHandler;
    }

    public PlacementProvider placementProvider()
    {
        return this.placementProvider;
    }

    @VisibleForTesting
    public Processor processor()
    {
        return processor;
    }

    @VisibleForTesting
    public LocalLog log()
    {
        return log;
    }

    public ClusterMetadata metadata()
    {
        return log.metadata();
    }

    /**
     * Utility methods
     */

    public void maybeCatchup(Epoch theirEpoch)
    {
        Epoch ourEpoch = ClusterMetadata.current().epoch;
        if (!theirEpoch.isBefore(Epoch.FIRST) && theirEpoch.isAfter(ourEpoch))
        {
            replayAndWait();
            ourEpoch = ClusterMetadata.current().epoch;
            if (theirEpoch.isAfter(ourEpoch))
            {
                throw new IllegalArgumentException(String.format("Could not catch up to epoch %s even after replay. Highest seen after replay is %s.",
                                                                 theirEpoch, ourEpoch));
            }
        }
    }

    public ClusterMetadata replayAndWait()
    {
        return processor.replayAndWait();
    }

    public ClusterMetadata awaitAtLeast(Epoch epoch) throws InterruptedException, TimeoutException
    {
        return log.awaitAtLeast(epoch);
    }

    public MetadataSnapshots snapshotManager()
    {
        return snapshots;
    }

    public ClusterMetadata sealPeriod()
    {
        return ClusterMetadataService.instance.commit(SealPeriod.instance,
                                                      (ClusterMetadata metadata) -> metadata.lastInPeriod,
                                                      (ClusterMetadata metadata) -> metadata,
                                                      (metadata, reason) -> {
                                                          // If the transformation got rejected, someone else has beat us to seal this period
                                                          return metadata;
                                                      });
    }

    public void initRecentlySealedPeriodsIndex()
    {
        Sealed.initIndexFromSystemTables();
    }

    public boolean isMigrating()
    {
        return false;
//        return Election.instance.isMigrating();
    }

    /**
     * Switchable implementations that allow us to go between local and remote implementation whenever we need it.
     * When the node becomes a member of CMS, it switches back to being a regular member of a cluster, and all
     * the CMS handlers get disabled.
     */

    static class SwitchableHandler<T> implements IVerbHandler<T>
    {
        private final IVerbHandler<T> handler;
        private final Supplier<State> cmsStateSupplier;

        public SwitchableHandler(IVerbHandler<T> handler, Supplier<State> cmsStateSupplier)
        {
            this.handler = handler;
            this.cmsStateSupplier = cmsStateSupplier;
        }

        public void doVerb(Message<T> message) throws IOException
        {
            switch (cmsStateSupplier.get())
            {
                case LOCAL:
                    handler.doVerb(message);
                    break;
                case REMOTE:
                    throw new NotCMSException("Not currently a member of the CMS");
                default:
                    throw new IllegalStateException("Illegal state: " + cmsStateSupplier.get());
            }
        }
    }

    @VisibleForTesting
    public static class SwitchableProcessor implements Processor
    {
        private final Processor local;
        private final RemoteProcessor remote;
        private final Supplier<State> cmsStateSupplier;

        SwitchableProcessor(Processor local, RemoteProcessor remote, Supplier<State> cmsStateSupplier)
        {
            this.local = local;
            this.remote = remote;
            this.cmsStateSupplier = cmsStateSupplier;
        }

        @VisibleForTesting
        public Processor delegate()
        {
            State state = cmsStateSupplier.get();
            switch (state)
            {
                case LOCAL:
                    return local;
                case REMOTE:
                    return remote;
            }
            throw new IllegalStateException("Bad CMS state: " + state);
        }

        public Commit.Result commit(Entry.Id entryId, Transformation transform, Epoch lastKnown)
        {
            return delegate().commit(entryId, transform, lastKnown);
        }

        public ClusterMetadata replayAndWait()
        {
            return delegate().replayAndWait();
        }

        public String toString()
        {
            return "SwitchableProcessor{" +
                   delegate() + '}';
        }
    }

    public enum State
    {
        LOCAL, REMOTE, GOSSIP
    }
}
