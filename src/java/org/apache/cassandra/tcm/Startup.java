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
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.NewGossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.LogStorage;
import org.apache.cassandra.tcm.log.SystemKeyspaceStorage;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.migration.Election;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.ReconfigureCMS;
import org.apache.cassandra.tcm.sequences.ReplaceSameAddress;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.tcm.transformations.PrepareReplace;
import org.apache.cassandra.tcm.transformations.UnsafeJoin;
import org.apache.cassandra.tcm.transformations.cms.Initialize;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.tcm.ClusterMetadataService.State.LOCAL;
import static org.apache.cassandra.tcm.compatibility.GossipHelper.emptyWithSchemaFromSystemTables;
import static org.apache.cassandra.tcm.compatibility.GossipHelper.fromEndpointStates;
import static org.apache.cassandra.tcm.membership.NodeState.JOINED;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;

 /**
  * Initialize
  */
 public class Startup
{
    private static final Logger logger = LoggerFactory.getLogger(Startup.class);

    public static void initialize(Set<InetAddressAndPort> seeds) throws InterruptedException, ExecutionException, IOException, StartupException
    {
        initialize(seeds,
                   p -> p,
                   () -> MessagingService.instance().waitUntilListeningUnchecked());
    }

    public static void initialize(Set<InetAddressAndPort> seeds,
                                  Function<Processor, Processor> wrapProcessor,
                                  Runnable initMessaging) throws InterruptedException, ExecutionException, IOException, StartupException
    {
        switch (StartupMode.get(seeds))
        {
            case FIRST_CMS:
                logger.info("Initializing as first CMS node in a new cluster");
                initializeAsNonCmsNode(wrapProcessor);
                initializeAsFirstCMSNode();
                initMessaging.run();
                break;
            case NORMAL:
                logger.info("Initializing as non CMS node");
                initializeAsNonCmsNode(wrapProcessor);
                initMessaging.run();
                break;
            case VOTE:
                logger.info("Initializing for discovery");
                initializeAsNonCmsNode(wrapProcessor);
                initializeForDiscovery(initMessaging);
                break;
            case UPGRADE:
                logger.info("Initializing from gossip");
                initializeFromGossip(wrapProcessor, initMessaging);
                break;
            case BOOT_WITH_CLUSTERMETADATA:
                String fileName = CassandraRelevantProperties.TCM_UNSAFE_BOOT_WITH_CLUSTERMETADATA.getString();
                logger.warn("Initializing with cluster metadata from: {}", fileName);
                reinitializeWithClusterMetadata(fileName, wrapProcessor, initMessaging);
                break;
        }
    }

    /**
     * Make this node a _first_ CMS node.
     * <p>
     * (1) Append PreInitialize transformation to local in-memory log. When distributed metadata keyspace is initialized, a no-op transformation will
     * be added to other nodes. This is required since as of now, no node actually owns distributed metadata keyspace.
     * (2) Commit Initialize transformation, which holds a snapshot of metadata as of now.
     * <p>
     * This process is applicable for gossip upgrades as well as regular vote-and-startup process.
     */
    public static void initializeAsFirstCMSNode()
    {
        InetAddressAndPort addr = FBUtilities.getBroadcastAddressAndPort();
        String datacenter = DatabaseDescriptor.getLocator().local().datacenter;
        ClusterMetadataService.instance().log().bootstrap(addr, datacenter);
        ClusterMetadata metadata =  ClusterMetadata.current();
        assert ClusterMetadataService.state() == LOCAL : String.format("Can't initialize as node hasn't transitioned to CMS state. State: %s.\n%s", ClusterMetadataService.state(),  metadata);

        Initialize initialize = new Initialize(metadata.initializeClusterIdentifier(addr.hashCode()));
        ClusterMetadataService.instance().commit(initialize);
    }

    public static void initializeAsNonCmsNode(Function<Processor, Processor> wrapProcessor) throws StartupException
    {
        LocalLog.LogSpec logSpec = LocalLog.logSpec()
                                           .withStorage(LogStorage.SystemKeyspace)
                                           .afterReplay(Startup::scrubDataDirectories,
                                                        (metadata) -> StorageService.instance.registerMBeans())
                                           .withDefaultListeners();
        ClusterMetadataService.setInstance(new ClusterMetadataService(new UniformRangePlacement(),
                                                                      wrapProcessor,
                                                                      ClusterMetadataService::state,
                                                                      logSpec));
        ClusterMetadataService.instance().log().ready();

        NodeId nodeId = ClusterMetadata.current().myNodeId();
        UUID currentHostId = SystemKeyspace.getLocalHostId();
        if (nodeId != null && !Objects.equals(nodeId.toUUID(), currentHostId))
        {
            if (currentHostId == null)
            {
                logger.info("Taking over the host ID: {}, replacing address {}", nodeId.toUUID(), FBUtilities.getBroadcastAddressAndPort());
                SystemKeyspace.setLocalHostId(nodeId.toUUID());
                return;
            }

            String error = String.format("NodeId does not match locally set one. Check for the IP address collision: %s vs %s %s.",
                                         currentHostId, nodeId.toUUID(), FBUtilities.getBroadcastAddressAndPort());
            logger.error(error);
            throw new IllegalStateException(error);
        }
    }

    public static void scrubDataDirectories(ClusterMetadata metadata) throws StartupException
    {
        // clean up debris in the rest of the keyspaces
        for (KeyspaceMetadata keyspace : metadata.schema.getKeyspaces())
        {
            // Skip system as we've already cleaned it
            if (keyspace.name.equals(SchemaConstants.SYSTEM_KEYSPACE_NAME))
                continue;

            for (TableMetadata cfm : keyspace.tables)
            {
                ColumnFamilyStore.scrubDataDirectories(cfm);
            }
        }
    }

    public interface AfterReplay
    {
        void accept(ClusterMetadata t) throws StartupException;
    }
    /**
     * Initialization for Discovery.
     *
     * Node will attempt to discover other participants in the cluster by attempting to contact the seeds
     * it is aware of. After discovery, the node with a smallest ip address will move to propose itself as
     * a CMS initiator, and attempt to establish a CMS in via two-phase commit protocol.
     */
    public static void initializeForDiscovery(Runnable initMessaging)
    {
        initMessaging.run();
        logger.debug("Discovering other nodes in the system");
        Discovery.DiscoveredNodes candidates = Discovery.instance.discover();
        if (candidates.kind() == Discovery.DiscoveredNodes.Kind.KNOWN_PEERS)
        {
            logger.debug("Got candidates: " + candidates);
            Optional<InetAddressAndPort> option = candidates.nodes().stream().min(InetAddressAndPort::compareTo);
            InetAddressAndPort min;
            if (!option.isPresent())
            {
                if (DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddressAndPort()))
                    min = FBUtilities.getBroadcastAddressAndPort();
                else
                    throw new IllegalArgumentException(String.format("Found no candidates during initialization. Check if the seeds are up: %s", DatabaseDescriptor.getSeeds()));
            }
            else
            {
                min = option.get();
            }

             // identify if you need to start the vote
            if (min.equals(FBUtilities.getBroadcastAddressAndPort()) || FBUtilities.getBroadcastAddressAndPort().compareTo(min) < 0)
            {
                Election.instance.nominateSelf(candidates.nodes(),
                                               Collections.singleton(FBUtilities.getBroadcastAddressAndPort()),
                                               (cm) -> true,
                                               null);
            }
        }

        while (!ClusterMetadata.current().epoch.isAfter(Epoch.FIRST))
        {
            if (candidates.kind() == Discovery.DiscoveredNodes.Kind.CMS_ONLY)
            {
                RemoteProcessor.fetchLogAndWait(new RemoteProcessor.CandidateIterator(candidates.nodes(), false),
                                                ClusterMetadataService.instance().log());
            }
            else
            {
                Election.Initiator initiator = Election.instance.initiator();
                candidates = Discovery.instance.discoverOnce(initiator == null ? null : initiator.initiator);
            }
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }

        assert ClusterMetadata.current().epoch.isAfter(Epoch.FIRST);
        Election.instance.migrated();
    }

    /**
     * This should only be called during startup.
     */
    public static void initializeFromGossip(Function<Processor, Processor> wrapProcessor, Runnable initMessaging) throws StartupException
    {
        ClusterMetadata emptyFromSystemTables = emptyWithSchemaFromSystemTables(SystemKeyspace.allKnownDatacenters());
        LocalLog.LogSpec logSpec = LocalLog.logSpec()
                                           .withInitialState(emptyFromSystemTables)
                                           .afterReplay(Startup::scrubDataDirectories,
                                                        (metadata) -> StorageService.instance.registerMBeans())
                                           .withStorage(LogStorage.SystemKeyspace)
                                           .withDefaultListeners();

        ClusterMetadataService.setInstance(new ClusterMetadataService(new UniformRangePlacement(),
                                                                      wrapProcessor,
                                                                      ClusterMetadataService::state,
                                                                      logSpec));

        ClusterMetadataService.instance().log().ready();
        initMessaging.run();
        try
        {
            CommitLog.instance.recoverSegmentsOnDisk();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        logger.debug("Starting to initialize ClusterMetadata from gossip");
        Map<InetAddressAndPort, EndpointState> epStates = NewGossiper.instance.doShadowRound();
        InetAddressAndPort switchIp = null;
        if (!epStates.containsKey(getBroadcastAddressAndPort()))
        {
            UUID hostId = SystemKeyspace.getLocalHostId();
            for (Map.Entry<InetAddressAndPort, EndpointState> epstate : epStates.entrySet())
            {
                EndpointState state = epstate.getValue();
                VersionedValue gossipHostId = state.getApplicationState(ApplicationState.HOST_ID);
                if (gossipHostId != null && UUID.fromString(gossipHostId.value).equals(hostId))
                {
                    switchIp = epstate.getKey();
                    break;
                }
            }
            if (switchIp != null)
            {
                logger.info("Changing IP in gossip mode from {} to {}", switchIp, getBroadcastAddressAndPort());
                // we simply switch the key to the new ip here to make sure we grab NodeAddresses.current() for
                // this node when constructing the initial ClusterMetadata in GossipHelper#getAddressesFromEndpointState
                epStates.put(getBroadcastAddressAndPort(), epStates.remove(switchIp));
            }
        }

        logger.debug("Got epStates {}", epStates);
        ClusterMetadata initial = fromEndpointStates(emptyFromSystemTables.schema, epStates);
        logger.debug("Created initial ClusterMetadata {}", initial);
        ClusterMetadataService.instance().setFromGossip(initial);
        Gossiper.instance.clearUnsafe();
        if (switchIp != null)
        {
            // quarantine the old ip to make sure it doesn't get re-added via gossip
            InetAddressAndPort removeEp = switchIp;
            Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.removeEndpoint(removeEp));
        }
        Gossiper.instance.maybeInitializeLocalState(SystemKeyspace.incrementAndGetGeneration());
        for (Map.Entry<NodeId, NodeState> entry : initial.directory.states.entrySet())
            Gossiper.instance.mergeNodeToGossip(entry.getKey(), initial);

        // double check that everything was added, can remove once we are confident
        ClusterMetadata cmGossip = fromEndpointStates(emptyFromSystemTables.schema, Gossiper.instance.getEndpointStates());
        assert cmGossip.equals(initial) : cmGossip + " != " + initial;
    }

    public static void reinitializeWithClusterMetadata(String fileName, Function<Processor, Processor> wrapProcessor, Runnable initMessaging) throws IOException, StartupException
    {
        ClusterMetadata prev = ClusterMetadata.currentNullable();
        // First set a minimal ClusterMetadata as some deserialization depends
        // on ClusterMetadata.current() to access the partitioner
        StubClusterMetadataService initial = StubClusterMetadataService.forClientTools();
        ClusterMetadataService.unsetInstance();
        StubClusterMetadataService.setInstance(initial);

        ClusterMetadata metadata = ClusterMetadataService.deserializeClusterMetadata(fileName);
        // if the partitioners are mismatching, we probably won't even get this far
        if (metadata.partitioner != DatabaseDescriptor.getPartitioner())
            throw new IllegalStateException(String.format("When reinitializing with cluster metadata, the same " +
                                                          "partitioner must be used. Configured: %s, Serialized: %s",
                                                          DatabaseDescriptor.getPartitioner().getClass().getCanonicalName(),
                                                          metadata.partitioner.getClass().getCanonicalName()));

        if (!metadata.isCMSMember(FBUtilities.getBroadcastAddressAndPort()))
            throw new IllegalStateException("When reinitializing with cluster metadata, we must be in the CMS");

        metadata = metadata.forceEpoch(metadata.epoch.nextEpoch());
        ClusterMetadataService.unsetInstance();
        LocalLog.LogSpec logSpec = LocalLog.logSpec()
                                           .afterReplay(Startup::scrubDataDirectories,
                                                        (_metadata) -> StorageService.instance.registerMBeans())
                                           .withPreviousState(prev)
                                           .withInitialState(metadata)
                                           .withStorage(LogStorage.SystemKeyspace)
                                           .withDefaultListeners()
                                           .isReset(true);

        ClusterMetadataService.setInstance(new ClusterMetadataService(new UniformRangePlacement(),
                                                                      wrapProcessor,
                                                                      ClusterMetadataService::state,
                                                                      logSpec));

        ClusterMetadataService.instance().log().ready();
        initMessaging.run();
        ClusterMetadataService.instance().forceSnapshot(metadata.forceEpoch(metadata.nextEpoch()));
        ClusterMetadataService.instance().triggerSnapshot();
        CassandraRelevantProperties.TCM_UNSAFE_BOOT_WITH_CLUSTERMETADATA.reset();
        assert ClusterMetadataService.state() == LOCAL;
        assert ClusterMetadataService.instance() != initial : "Aborting startup as temporary metadata service is still active";
    }

    public static void startup(boolean finishJoiningRing, boolean shouldBootstrap, boolean isReplacing)
    {
        startup(() -> getInitialTransformation(finishJoiningRing, shouldBootstrap, isReplacing), finishJoiningRing, shouldBootstrap, isReplacing);
    }

    /**
     * Cassandra startup process:
     *   * assume that startup could have been interrupted at any point in time, and attempt to finish any in-process
     *     sequences
     *   * after finishing an existing in-progress sequence, if any, we should jump to JOINED
     *   * otherwise, we assume a fresh setup, in which case we grab a startup sequence (Join or Replace), and try to
     *     bootstrap
     *
     * @param initialTransformation supplier of the Transformation which initiates the startup sequence
     * @param finishJoiningRing if false, node is left in a survey mode, which means it will receive writes in all cases
     *                          except if it is replacing the node with same address
     * @param shouldBootstrap if true, bootstrap streaming will be executed
     * @param isReplacing true, if the node is replacing some other node (with same or different address).
     */
    public static void startup(Supplier<Transformation> initialTransformation, boolean finishJoiningRing, boolean shouldBootstrap, boolean isReplacing)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId self = metadata.myNodeId();

        // finish in-progress sequences first
        InProgressSequences.finishInProgressSequences(self, true);
        metadata = ClusterMetadata.current();

        switch (metadata.directory.peerState(self))
        {
            case REGISTERED:
            case LEFT:
                if (isReplacing)
                    ReconfigureCMS.maybeReconfigureCMS(metadata, DatabaseDescriptor.getReplaceAddress());

                ClusterMetadataService.instance().commit(initialTransformation.get());
                InProgressSequences.finishInProgressSequences(self, true); // potentially finish the MSO committed above
                metadata = ClusterMetadata.current();

                if (metadata.directory.peerState(self) == JOINED)
                    SystemKeyspace.setBootstrapState(SystemKeyspace.BootstrapState.COMPLETED);
                else
                {
                    StorageService.instance.markBootstrapFailed();
                    logger.info("Did not finish joining the ring; node state is {}, bootstrap state is {}",
                                metadata.directory.peerState(self),
                                SystemKeyspace.getBootstrapState());
                    break;
                }
            case JOINED:
                if (StorageService.isReplacingSameAddress())
                    ReplaceSameAddress.streamData(self, metadata, shouldBootstrap, finishJoiningRing);

                // JOINED appears before BOOTSTRAPPING & BOOT_REPLACE so we can fall
                // through when we start as REGISTERED/LEFT and complete a full startup
                logger.info("{}", StorageService.Mode.NORMAL);
                break;
            case BOOTSTRAPPING:
            case BOOT_REPLACING:
                if (finishJoiningRing)
                {
                    throw new IllegalStateException("Expected to complete startup sequence, but did not. " +
                                                    "Can't proceed from the state " + metadata.directory.peerState(self));
                }
                break;
            case LEAVING:
                logger.info("Node is currently being decommissioned, resume with `nodetool decommission`");
                StorageService.instance.markDecommissionFailed();
                break;
            case MOVING:
                logger.info("Node is currently moving, resume with nodetool move --resume or abort with nodetool move --abort");
                StorageService.instance.markMoveFailed();
                break;
            default:
                throw new IllegalStateException("Can't proceed from the state " + metadata.directory.peerState(self));
        }
    }

    /**
     * Returns:
     *   * {@link PrepareReplace}, the first step of the multi-step replacement process sequence, if {@param finishJoiningRing} is true
     *   * {@link UnsafeJoin}, a single-step join transformation, if {@param shouldBootstrap} is set to false, and the node is not
     *     in a write survey mode (in other words {@param finishJoiningRing} is true. This mode is mostly used for testing, but can
     *     also be used to quickly set up a fresh cluster.
     *   * and {@link PrepareJoin}, the first step of the multi-step join process, otherwise.
     */
    private static Transformation getInitialTransformation(boolean finishJoiningRing, boolean shouldBootstrap, boolean isReplacing)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        if (isReplacing)
        {
            InetAddressAndPort replacingEndpoint = DatabaseDescriptor.getReplaceAddress();
            if (FailureDetector.instance.isAlive(replacingEndpoint))
            {
                logger.error("Unable to replace live node {})", replacingEndpoint);
                throw new UnsupportedOperationException("Cannot replace a live node... ");
            }

            NodeId replaced = ClusterMetadata.current().directory.peerId(replacingEndpoint);

            return new PrepareReplace(replaced,
                                      metadata.myNodeId(),
                                      ClusterMetadataService.instance().placementProvider(),
                                      finishJoiningRing,
                                      shouldBootstrap);
        }
        else if (finishJoiningRing && !shouldBootstrap)
        {
            return new UnsafeJoin(metadata.myNodeId(),
                                  new HashSet<>(BootStrapper.getBootstrapTokens(ClusterMetadata.current(), getBroadcastAddressAndPort())),
                                  ClusterMetadataService.instance().placementProvider());
        }
        else
        {
            return new PrepareJoin(metadata.myNodeId(),
                                   new HashSet<>(BootStrapper.getBootstrapTokens(ClusterMetadata.current(), getBroadcastAddressAndPort())),
                                   ClusterMetadataService.instance().placementProvider(),
                                   finishJoiningRing,
                                   shouldBootstrap);
        }
    }

    /**
     * Initialization process
     */

    enum StartupMode
    {
        /**
         * Normal startup mode: node has already been a part of a CMS, and was restarted.
         */
        NORMAL,
        /**
         * An upgrade path from Gossip: node has been booted brefore and was a part of a Gossip cluster.
         */
        UPGRADE,
        /**
         * Node is starting for the first time, and should attempt to either discover existing CMS, or
         * participate in the leader election to establish a new one.
         */
        VOTE,
        /**
         * Node is starting for the first time, and is a designated first CMS node and can become a first CMS
         * node upon boot.
         */
        FIRST_CMS,
        /**
         * Node has to pick Cluster Metadata from the specified file. Used for testing and for (improbable) disaster recovery.
         */
        BOOT_WITH_CLUSTERMETADATA;

        static StartupMode get(Set<InetAddressAndPort> seeds)
        {
            if (CassandraRelevantProperties.TCM_UNSAFE_BOOT_WITH_CLUSTERMETADATA.isPresent())
            {
                logger.warn("Booting with ClusterMetadata from file: " + CassandraRelevantProperties.TCM_UNSAFE_BOOT_WITH_CLUSTERMETADATA.getString());
                return BOOT_WITH_CLUSTERMETADATA;
            }
            if (seeds.isEmpty())
                throw new IllegalArgumentException("Can not initialize CMS without any seeds");

            boolean hasAnyEpoch = SystemKeyspaceStorage.hasAnyEpoch();
            // For CCM and local dev clusters
            boolean isOnlySeed = DatabaseDescriptor.getSeeds().size() == 1
                                 && DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddressAndPort())
                                 && DatabaseDescriptor.getSeeds().iterator().next().getAddress().isLoopbackAddress();
            boolean hasBootedBefore = SystemKeyspace.getLocalHostId() != null;
            logger.info("hasAnyEpoch = {}, hasBootedBefore = {}", hasAnyEpoch, hasBootedBefore);
            if (!hasAnyEpoch && hasBootedBefore)
                return UPGRADE;
            else if (hasAnyEpoch)
                return NORMAL;
            else if (isOnlySeed)
                return FIRST_CMS;
            else
                return VOTE;
        }
    }
}