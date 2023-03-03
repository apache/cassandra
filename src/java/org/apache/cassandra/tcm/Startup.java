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
 import java.util.Map;
 import java.util.Set;
 import java.util.concurrent.ExecutionException;
 import java.util.concurrent.TimeUnit;
 import java.util.function.Function;

 import com.google.common.util.concurrent.Uninterruptibles;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;

 import org.apache.cassandra.config.DatabaseDescriptor;
 import org.apache.cassandra.db.SystemKeyspace;
 import org.apache.cassandra.db.commitlog.CommitLog;
 import org.apache.cassandra.gms.EndpointState;
 import org.apache.cassandra.gms.Gossiper;
 import org.apache.cassandra.gms.NewGossiper;
 import org.apache.cassandra.locator.InetAddressAndPort;
 import org.apache.cassandra.net.MessagingService;
 import org.apache.cassandra.schema.DistributedSchema;
 import org.apache.cassandra.tcm.compatibility.GossipHelper;
 import org.apache.cassandra.tcm.log.SystemKeyspaceStorage;
 import org.apache.cassandra.tcm.migration.Election;
 import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
 import org.apache.cassandra.tcm.transformations.cms.Initialize;
 import org.apache.cassandra.utils.FBUtilities;

 import static org.apache.cassandra.tcm.ClusterMetadataService.State.LOCAL;
 import static org.apache.cassandra.tcm.compatibility.GossipHelper.emptyWithSchemaFromSystemTables;
 import static org.apache.cassandra.tcm.compatibility.GossipHelper.fromEndpointStates;

 public class Startup
 {
     private static final Logger logger = LoggerFactory.getLogger(Startup.class);

     public static void initialize(Set<InetAddressAndPort> seeds) throws InterruptedException, ExecutionException
     {
         initialize(seeds,
                    p -> p,
                    () -> MessagingService.instance().waitUntilListeningUnchecked());
     }

     public static void initialize(Set<InetAddressAndPort> seeds,
                                   Function<ClusterMetadataService.Processor, ClusterMetadataService.Processor> wrapProcessor,
                                   Runnable initMessaging) throws InterruptedException, ExecutionException
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
         }
     }

     /**
      * Make this node a _first_ CMS node.
      *
      *   (1) Append PreInitialize transformation to local in-memory log. When distributed metadata keyspace is initialized, a no-op transformation will
      *   be added to other nodes. This is required since as of now, no node actually owns distributed metadata keyspace.
      *   (2) Commit Initialize transformation, which holds a snapshot of metadata as of now.
      *
      * This process is applicable for gossip upgrades as well as regular vote-and-startup process.
      */
     public static void initializeAsFirstCMSNode()
     {
         ClusterMetadataService.instance().log().bootstrap(FBUtilities.getBroadcastAddressAndPort());
         assert ClusterMetadataService.state() == LOCAL : String.format("Can't initialize as node hasn't transitioned to CMS state. State: %s.\n%s", ClusterMetadataService.state(), ClusterMetadata.current());

         Initialize initialize = new Initialize(ClusterMetadata.current());
         ClusterMetadataService.instance().commit(initialize);
     }

     public static void initializeAsNonCmsNode(Function<ClusterMetadataService.Processor, ClusterMetadataService.Processor> wrapProcessor)
     {
         ClusterMetadata initial = new ClusterMetadata(DatabaseDescriptor.getPartitioner());
         initial.schema.initializeKeyspaceInstances(DistributedSchema.empty());
         ClusterMetadataService.setInstance(new ClusterMetadataService(new UniformRangePlacement(),
                                                                       initial,
                                                                       wrapProcessor,
                                                                       ClusterMetadataService::state));

         ClusterMetadataService.instance().initRecentlySealedPeriodsIndex();
         ClusterMetadataService.instance().log().replayPersisted();
     }

     public static void initializeForDiscovery(Runnable initMessaging)
     {
         initMessaging.run();

         logger.debug("Discovering other nodes in the system");
         Discovery.DiscoveredNodes candidates = Discovery.instance.discover();

         if (candidates.kind() == Discovery.DiscoveredNodes.Kind.KNOWN_PEERS)
         {
             logger.debug("Got candidates: " + candidates);
             InetAddressAndPort min = candidates.nodes().stream().min(InetAddressAndPort::compareTo).get();

             // identify if you need to start the vote
             if (min.equals(FBUtilities.getBroadcastAddressAndPort()) || FBUtilities.getBroadcastAddressAndPort().compareTo(min) < 0)
             {
                 Election.instance.nominateSelf(candidates.nodes(),
                                                Collections.singleton(FBUtilities.getBroadcastAddressAndPort()),
                                                (cm) -> true);
             }
         }

         while (!ClusterMetadata.current().epoch.isAfter(Epoch.FIRST))
         {
             if (candidates.kind() == Discovery.DiscoveredNodes.Kind.CMS_ONLY)
             {
                 ClusterMetadataService.instance().processor().replayAndWait();
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
     public static void initializeFromGossip(Function<ClusterMetadataService.Processor, ClusterMetadataService.Processor> wrapProcessor, Runnable initMessaging)
     {
         ClusterMetadata emptyFromSystemTables = emptyWithSchemaFromSystemTables();
         emptyFromSystemTables.schema.initializeKeyspaceInstances(DistributedSchema.empty());
         ClusterMetadataService.setInstance(new ClusterMetadataService(new UniformRangePlacement(),
                                                                       emptyFromSystemTables,
                                                                       wrapProcessor,
                                                                       ClusterMetadataService::state));
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
         logger.debug("Got epStates {}", epStates);
         ClusterMetadata initial = fromEndpointStates(emptyFromSystemTables.schema, epStates);
         logger.debug("Created initial ClusterMetadata {}", initial);
         ClusterMetadataService.instance().setFromGossip(initial);
         Gossiper.instance.clearUnsafe();
         Gossiper.instance.maybeInitializeLocalState(SystemKeyspace.incrementAndGetGeneration());
         GossipHelper.mergeAllNodeStatesToGossip(initial);
         // double check that everything was added, can remove once we are confident
         ClusterMetadata cmGossip = fromEndpointStates(emptyFromSystemTables.schema, Gossiper.instance.getEndpointStates());
         assert cmGossip.equals(initial) : cmGossip + " != " + initial;
     }

     /**
      * Initialization process:
      */

     enum StartupMode
     {
         NORMAL,
         UPGRADE,
         VOTE,
         FIRST_CMS;

         static StartupMode get(Set<InetAddressAndPort> seeds)
         {
             if (seeds.isEmpty())
                 throw new IllegalArgumentException("Can not initialize CMS without any seeds");

             boolean hasFirstEpoch = SystemKeyspaceStorage.hasFirstEpoch();
             boolean isOnlySeed = DatabaseDescriptor.getSeeds().size() == 1 && DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddressAndPort());
             boolean hasBootedBefore = SystemKeyspace.getLocalHostId() != null;
             logger.info("hasFirstEpoch = {}, hasBootedBefore = {}", hasFirstEpoch, hasBootedBefore);
             if (!hasFirstEpoch && hasBootedBefore)
                 return UPGRADE;
             else if (hasFirstEpoch)
                 return NORMAL;
             else if (isOnlySeed)
                 return FIRST_CMS;
             else
                 return VOTE;
         }
     }
 }
