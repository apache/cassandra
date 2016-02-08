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

package org.apache.cassandra.net.async;

import java.util.Optional;

import org.apache.cassandra.auth.AllowAllInternodeAuthenticator;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.utils.CoalescingStrategies;

class NonSendingOutboundMessagingConnection extends OutboundMessagingConnection
{
    boolean sendMessageInvoked;

    NonSendingOutboundMessagingConnection(OutboundConnectionIdentifier connectionId, EncryptionOptions.ServerEncryptionOptions encryptionOptions, Optional<CoalescingStrategies.CoalescingStrategy> coalescingStrategy)
    {
        super(connectionId, encryptionOptions, coalescingStrategy, new AllowAllInternodeAuthenticator());
    }

    @Override
    boolean sendMessage(QueuedMessage queuedMessage)
    {
        sendMessageInvoked = true;
        return true;
    }
}
