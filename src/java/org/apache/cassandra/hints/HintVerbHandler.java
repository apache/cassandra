/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.hints;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;

/**
 * Verb handler used both for hint dispatch and streaming.
 *
 * With the non-sstable format, we cannot just stream hint sstables on node decommission. So sometimes, at decommission
 * time, we might have to stream hints to a non-owning host (say, if the owning host B is down during decommission of host A).
 * In that case the handler just stores the received hint in its local hint store.
 */
public final class HintVerbHandler implements IVerbHandler<HintMessage>
{
    public static final HintVerbHandler instance = new HintVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger(HintVerbHandler.class);

    public void doVerb(Message<HintMessage> message)
    {
        UUID hostId = message.payload.hostId;
        Hint hint = message.payload.hint;
        InetAddressAndPort address = StorageService.instance.getEndpointForHostId(hostId);

        // If we see an unknown table id, it means the table, or one of the tables in the mutation, had been dropped.
        // In that case there is nothing we can really do, or should do, other than log it go on.
        // This will *not* happen due to a not-yet-seen table, because we don't transfer hints unless there
        // is schema agreement between the sender and the receiver.
        if (hint == null)
        {
            logger.trace("Failed to decode and apply a hint for {}: {} - table with id {} is unknown",
                         address,
                         hostId,
                         message.payload.unknownTableID);
            respond(message);
            return;
        }

        // We must perform validation before applying the hint, and there is no other place to do it other than here.
        try
        {
            hint.mutation.getPartitionUpdates().forEach(PartitionUpdate::validate);
        }
        catch (MarshalException e)
        {
            logger.warn("Failed to validate a hint for {}: {} - skipped", address, hostId);
            respond(message);
            return;
        }

        if (!hostId.equals(StorageService.instance.getLocalHostUUID()))
        {
            // the node is not the final destination of the hint (must have gotten it from a decommissioning node),
            // so just store it locally, to be delivered later.
            HintsService.instance.write(hostId, hint);
            respond(message);
        }
        else if (!StorageProxy.instance.appliesLocally(hint.mutation))
        {
            // the topology has changed, and we are no longer a replica of the mutation - since we don't know which node(s)
            // it has been handed over to, re-address the hint to all replicas; see CASSANDRA-5902.
            HintsService.instance.writeForAllReplicas(hint);
            respond(message);
        }
        else
        {
            // the common path - the node is both the destination and a valid replica for the hint.
            hint.applyFuture().addCallback(o -> respond(message), e -> logger.debug("Failed to apply hint", e));
        }
    }

    private static void respond(Message<HintMessage> respondTo)
    {
        MessagingService.instance().send(respondTo.emptyResponse(), respondTo.from());
    }
}
