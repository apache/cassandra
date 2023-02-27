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

 import java.util.Set;
 import java.util.concurrent.ExecutionException;
 import java.util.function.Function;

 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;

 import org.apache.cassandra.config.DatabaseDescriptor;
 import org.apache.cassandra.locator.InetAddressAndPort;
 import org.apache.cassandra.net.MessagingService;
 import org.apache.cassandra.tcm.log.SystemKeyspaceStorage;
 import org.apache.cassandra.tcm.transformations.cms.Initialize;
 import org.apache.cassandra.utils.FBUtilities;

 import static org.apache.cassandra.tcm.ClusterMetadataService.State.LOCAL;

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
         ClusterMetadataService.setInstance(new ClusterMetadataService(initial,
                                                                       wrapProcessor,
                                                                       ClusterMetadataService::state));
         ClusterMetadataService.instance().log().replayPersisted();
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
             if (isOnlySeed && !hasFirstEpoch)
                return FIRST_CMS;

             return NORMAL;
         }
     }
 }
