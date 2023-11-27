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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;

// The cluster metadata equivalent of a Gossip PING; used to exchange the current epochs on
// two peers via a pair of empty messages
public class CurrentEpochRequestHandler implements IVerbHandler<Epoch>
{
    private static final Logger logger = LoggerFactory.getLogger(CurrentEpochRequestHandler.class);
    public void doVerb(Message<Epoch> message) throws IOException
    {
        Message<Epoch> response = message.responseWith(ClusterMetadata.current().epoch);
        MessagingService.instance().send(response, message.from());
        // We try to catch up after responding, as watermark request is going to get retried
        ClusterMetadataService.instance().fetchLogFromPeerOrCMSAsync(ClusterMetadata.current(), message.from(), message.payload);
    }
}
