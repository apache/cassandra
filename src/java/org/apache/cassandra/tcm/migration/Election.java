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

package org.apache.cassandra.tcm.migration;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Startup;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.ownership.TokenMap;
import org.apache.cassandra.tcm.transformations.Register;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.DistributedMetadataLogKeyspace;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.tcm.membership.NodeState.LEFT;

/**
 * Election process establishes initial CMS leader, from which you can further evolve cluster metadata.
 */
public class Election
{
    private static final Logger logger = LoggerFactory.getLogger(Election.class);
    private static final CMSInitializationRequest.Initiator MIGRATING = new CMSInitializationRequest.Initiator(null, null);
    private static final CMSInitializationRequest.Initiator MIGRATED = new CMSInitializationRequest.Initiator(null, null);

    private final AtomicReference<CMSInitializationRequest.Initiator> initiator = new AtomicReference<>();

    public static Election instance = new Election();

    public final PrepareHandler prepareHandler;
    public final AbortHandler abortHandler;

    private final MessageDelivery messaging;

    private Election()
    {
        this(MessagingService.instance());
    }

    private Election(MessageDelivery messaging)
    {
        this.messaging = messaging;
        this.prepareHandler = new PrepareHandler();
        this.abortHandler = new AbortHandler();
    }

    public void nominateSelf(Set<InetAddressAndPort> candidates, Set<InetAddressAndPort> ignoredEndpoints, ClusterMetadata metadata, boolean verifyAllPeersMetadata)
    {
        Set<InetAddressAndPort> sendTo = new HashSet<>(candidates);
        sendTo.removeAll(ignoredEndpoints);
        sendTo.remove(FBUtilities.getBroadcastAddressAndPort());

        try
        {
            initiate(sendTo, metadata, verifyAllPeersMetadata);
            finish(sendTo);
        }
        catch (Exception e)
        {
            abort(sendTo);
            throw e;
        }
    }

    private void initiate(Set<InetAddressAndPort> sendTo, ClusterMetadata metadata, boolean verifyAllPeersMetadata)
    {
        CMSInitializationRequest initializationRequest = new CMSInitializationRequest(FBUtilities.getBroadcastAddressAndPort(), UUID.randomUUID(), metadata);
        if (!updateInitiator(null, initializationRequest.initiator))
            throw new IllegalStateException("Migration already initiated by " + initiator.get());

        logger.info("No previous migration detected, initiating");
        Collection<Pair<InetAddressAndPort, CMSInitializationResponse>> metadatas = MessageDelivery.fanoutAndWait(messaging, sendTo, Verb.TCM_INIT_MIG_REQ, initializationRequest);
        if (metadatas.size() != sendTo.size())
        {
            Set<InetAddressAndPort> responded = metadatas.stream().map(p -> p.left).collect(Collectors.toSet());
            String msg = String.format("Did not get response from %s - not continuing with migration. Ignore down hosts with --ignore <host>", Sets.difference(sendTo, responded));
            logger.warn(msg);
            throw new IllegalStateException(msg);
        }

        if (verifyAllPeersMetadata)
        {
            Set<InetAddressAndPort> mismatching = metadatas.stream().filter(p -> !p.right.metadataMatches).map(p -> p.left).collect(Collectors.toSet());
            if (!mismatching.isEmpty())
            {
                String msg = String.format("Got mismatching cluster metadatas. Check logs on peers (%s) for details of mismatches. Aborting migration.", mismatching);
                throw new IllegalStateException(msg);
            }
        }
    }

    private void finish(Set<InetAddressAndPort> sendTo)
    {
        CMSInitializationRequest.Initiator currentInitiator = initiator.get();
        if (currentInitiator != null &&
            Objects.equals(currentInitiator.endpoint, FBUtilities.getBroadcastAddressAndPort()) &&
            initiator.compareAndSet(currentInitiator, MIGRATING))
        {
            Startup.initializeAsFirstCMSNode();
            Register.maybeRegister();
            SystemKeyspace.setLocalHostId(ClusterMetadata.current().myNodeId().toUUID());

            updateInitiator(MIGRATING, MIGRATED);
            MessageDelivery.fanoutAndWait(messaging, sendTo, Verb.TCM_NOTIFY_REQ, DistributedMetadataLogKeyspace.getLogState(Epoch.EMPTY, false));
        }
        else
        {
            throw new IllegalStateException("Can't finish migration, initiator="+currentInitiator);
        }
    }

    private void abort(Set<InetAddressAndPort> sendTo)
    {
        CMSInitializationRequest.Initiator init = initiator.getAndSet(null);
        for (InetAddressAndPort ep : sendTo)
            messaging.send(Message.out(Verb.TCM_ABORT_MIG, init), ep);
    }

    public CMSInitializationRequest.Initiator initiator()
    {
        return initiator.get();
    }

    public void migrated()
    {
        initiator.set(MIGRATED);
    }

    private boolean updateInitiator(CMSInitializationRequest.Initiator expected, CMSInitializationRequest.Initiator newInitiator)
    {
        CMSInitializationRequest.Initiator current = initiator.get();
        return Objects.equals(current, expected) && initiator.compareAndSet(current, newInitiator);
    }

    public boolean isMigrating()
    {
        CMSInitializationRequest.Initiator initiator = initiator();
        return initiator != null && initiator != MIGRATED;
    }

    public void abortInitialization(String initiatorEp)
    {
        InetAddressAndPort expectedInitiator = InetAddressAndPort.getByNameUnchecked(initiatorEp);
        CMSInitializationRequest.Initiator currentInitiator = initiator.get();
        if (currentInitiator != null && Objects.equals(currentInitiator.endpoint, expectedInitiator) && initiator.compareAndSet(currentInitiator, null))
        {
            ClusterMetadata metadata = ClusterMetadata.current();
            for (Map.Entry<NodeId, NodeState> entry : metadata.directory.states.entrySet())
            {
                NodeId nodeId = entry.getKey();
                if (!Objects.equals(metadata.myNodeId(), nodeId) && entry.getValue() != LEFT)
                    messaging.send(Message.out(Verb.TCM_ABORT_MIG, currentInitiator), metadata.directory.endpoint(nodeId));
            }
        }
        else
        {
            throw new IllegalStateException("Current initiator [" + currentInitiator +"] does not match provided " + expectedInitiator +
                                            " - run this command on a node where initialization has not yet been cleared, with the correct expected initiator");
        }
    }

    public class PrepareHandler implements IVerbHandler<CMSInitializationRequest>
    {
        @Override
        public void doVerb(Message<CMSInitializationRequest> message) throws IOException
        {
            logger.info("Received election initiation message {} from {}", message.payload, message.from());
            if (!updateInitiator(null, message.payload.initiator))
                throw new IllegalStateException(String.format("Got duplicate initiate migration message from %s, migration is already started by %s", message.from(), initiator()));

            logger.info("Sending initiation response");
            Directory initiatorDirectory = message.payload.directory;
            TokenMap initiatorTokenMap = message.payload.tokenMap;
            UUID initiatorSchemaVersion = message.payload.schemaVersion;
            ClusterMetadata metadata = ClusterMetadata.current();
            boolean match = true;
            if (!initiatorDirectory.equals(metadata.directory))
            {
                match = false;
                logger.warn("Initiator directory different from our");
                initiatorDirectory.dumpDiff(metadata.directory);
            }
            if (!initiatorTokenMap.equals(metadata.tokenMap))
            {
                match = false;
                logger.warn("Initiator tokenmap different from ours");
                initiatorTokenMap.dumpDiff(metadata.tokenMap);
            }
            UUID schemaDigest = SchemaKeyspace.calculateSchemaDigest();
            if (!initiatorSchemaVersion.equals(schemaDigest))
            {
                match = false;
                logger.warn("Initiator schema different from our: {} != {}", initiatorSchemaVersion, schemaDigest);
            }
            messaging.send(message.responseWith(new CMSInitializationResponse(message.payload.initiator, match)), message.from());
        }
    }

    public class AbortHandler implements IVerbHandler<CMSInitializationRequest.Initiator>
    {
        @Override
        public void doVerb(Message<CMSInitializationRequest.Initiator> message) throws IOException
        {
            logger.info("Received election abort message {} from {}", message.payload, message.from());
            CMSInitializationRequest.Initiator remoteInitiator = message.payload;
            if (initiator() == null)
                logger.info("Initiator already cleared, ignoring abort message from {}: {}", message.from(), remoteInitiator);
            else if (!remoteInitiator.endpoint.equals(initiator().endpoint) || !updateInitiator(remoteInitiator, null))
                logger.error("Could not clear initiator - initiator is set to {}, abort message received from {}: {}", initiator(), message.from(), remoteInitiator);
        }
    }
}
