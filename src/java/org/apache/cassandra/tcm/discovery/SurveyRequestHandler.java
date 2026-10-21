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

package org.apache.cassandra.tcm.discovery;

import java.io.IOException;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.FBUtilities;

public class SurveyRequestHandler implements IVerbHandler<SurveyRequest>
{
    private static final Logger logger = LoggerFactory.getLogger(SurveyRequestHandler.class);
    private static volatile SurveyRequestHandler instance;

    final Supplier<MessageDelivery> messaging;
    final IntSupplier metadataId;

    public static SurveyRequestHandler instance()
    {
        if (instance == null)
        {
            synchronized (SurveyRequestHandler.class)
            {
                if (instance == null)
                    instance = new SurveyRequestHandler();
            }
        }
        return instance;
    }

    private SurveyRequestHandler()
    {
        this(() -> ClusterMetadata.current().metadataIdentifier, MessagingService::instance);
    }

    @VisibleForTesting
    public SurveyRequestHandler(IntSupplier metadataId, Supplier<MessageDelivery> messaging)
    {
        this.metadataId = metadataId;
        this.messaging = messaging;
    }

    @Override
    public void doVerb(Message<SurveyRequest> message) throws IOException
    {
        logger.info("Responding to {} request from {}", message.verb(), message.from());
        int localMetadataId = metadataId.getAsInt();
        if (message.payload.metadataId != localMetadataId)
            throw new InvalidRequestException(String.format("Mismatching metadata id in survey request from %s (%d)",
                                                            message.from(),
                                                            message.payload.metadataId));

        Discovery.instance.discovered(message.from());
        // Respond with the node id from system.local and not ClusterMetadata.current().myNodeId() because if
        // this node is in the process of starting up with a new broadcast address, it will not yet recognise itself
        // as being in a REGISTERED state. This results in myNodeId() returning NodeId.UNREGISTERED.
        NodeId nodeId = NodeId.fromUUID(SystemKeyspace.getLocalHostId());
        InetAddressAndPort broadcastAddress = FBUtilities.getBroadcastAddressAndPort();
        SurveyResponse response = new SurveyResponse(localMetadataId, nodeId, broadcastAddress);
        messaging.get().respond(response, message);
    }
}
