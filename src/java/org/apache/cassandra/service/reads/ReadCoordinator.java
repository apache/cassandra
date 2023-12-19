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

package org.apache.cassandra.service.reads;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;

public interface ReadCoordinator
{
    ReadCoordinator DEFAULT = new ReadCoordinator()
    {
        public boolean localReadSupported()
        {
            return true;
        }

        public EndpointsForToken forNonLocalStrategyTokenRead(ClusterMetadata metadata, KeyspaceMetadata keyspace, TableId tableId, Token token)
        {
            return ReplicaLayout.forNonLocalStrategyTokenRead(metadata, keyspace, token);
        }

        public void sendReadCommand(Message<ReadCommand> message, InetAddressAndPort to, RequestCallback<ReadResponse> callback)
        {
            MessagingService.instance().sendWithCallback(message, to, callback);
        }

        public void sendReadRepairMutation(Message<Mutation> message, InetAddressAndPort to, RequestCallback<Object> callback)
        {
            MessagingService.instance().sendWithCallback(message, to, callback);
        }

        public boolean isEventuallyConsistent()
        {
            return true;
        }
    };

    boolean localReadSupported();
    EndpointsForToken forNonLocalStrategyTokenRead(ClusterMetadata metadata, KeyspaceMetadata keyspace, TableId tableId, Token token);
    default ReadCommand maybeAllowOutOfRangeReads(ReadCommand command)
    {
        return command;
    }
    void sendReadCommand(Message<ReadCommand> message, InetAddressAndPort to, RequestCallback<ReadResponse> callback);
    default void notifyOfInitialContacts(EndpointsForToken fullDataRequests, EndpointsForToken transientRequests, EndpointsForToken digestRequests) {}
    void sendReadRepairMutation(Message<Mutation> message, InetAddressAndPort to, RequestCallback<Object> callback);
    default Mutation maybeAllowOutOfRangeMutations(Mutation m)
    {
        return m;
    }
    boolean isEventuallyConsistent();
}
