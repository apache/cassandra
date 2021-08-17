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

interface OutboundDebugCallbacks
{
    static final OutboundDebugCallbacks NONE = new OutboundDebugCallbacks()
    {
        public void onSendSmallFrame(int messageCount, int payloadSizeInBytes) {}
        public void onSentSmallFrame(int messageCount, int payloadSizeInBytes) {}
        public void onFailedSmallFrame(int messageCount, int payloadSizeInBytes) {}
        public void onConnect(int messagingVersion, OutboundConnectionSettings settings) {}
    };

    /** A complete Frame has been handed to Netty to write to the wire. */
    void onSendSmallFrame(int messageCount, int payloadSizeInBytes);

    /** A complete Frame has been serialized to the wire */
    void onSentSmallFrame(int messageCount, int payloadSizeInBytes);

    /** Failed to send an entire frame due to network problems; presumed to be invoked in same order as onSendSmallFrame */
    void onFailedSmallFrame(int messageCount, int payloadSizeInBytes);

    void onConnect(int messagingVersion, OutboundConnectionSettings settings);
}
