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
package org.apache.cassandra.net;

import org.apache.cassandra.locator.InetAddressAndPort;

interface OutboundMessageCallbacks
{
    /** A message was not enqueued to the link because too many messages are already waiting to send */
    void onOverloaded(Message<?> message, InetAddressAndPort peer);

    /** A message was not serialized to a frame because it had expired */
    void onExpired(Message<?> message, InetAddressAndPort peer);

    /** A message was not fully or successfully serialized to a frame because an exception was thrown */
    void onFailedSerialize(Message<?> message, InetAddressAndPort peer, int messagingVersion, int bytesWrittenToNetwork, Throwable failure);

    /** A message was not sent because the connection was forcibly closed */
    void onDiscardOnClose(Message<?> message, InetAddressAndPort peer);
}
