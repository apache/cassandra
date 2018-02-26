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

import java.io.IOException;

import org.apache.cassandra.hints.HintResponse;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.async.OutboundConnectionIdentifier;
import org.apache.cassandra.net.async.OutboundConnectionIdentifier.ConnectionType;

/**
 * Conceptually the same as {@link org.apache.cassandra.gms.EchoMessage}, but indicates to the recipient which
 * {@link ConnectionType} should be used for the response.
 */
public class PingMessage
{
    public static IVersionedSerializer<PingMessage> serializer = new PingMessageSerializer();

    public static final PingMessage smallChannelMessage = new PingMessage(ConnectionType.SMALL_MESSAGE);
    public static final PingMessage largeChannelMessage = new PingMessage(ConnectionType.LARGE_MESSAGE);
    public static final PingMessage gossipChannelMessage = new PingMessage(ConnectionType.GOSSIP);

    public final ConnectionType connectionType;

    public PingMessage(ConnectionType connectionType)
    {
        this.connectionType = connectionType;
    }

    public static class PingMessageSerializer implements IVersionedSerializer<PingMessage>
    {
        public void serialize(PingMessage t, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(t.connectionType.getId());
        }

        public PingMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            ConnectionType connectionType = ConnectionType.fromId(in.readByte());

            // if we ever create a new connection type, then during a rolling upgrade, the old nodes won't know about
            // the new connection type (as it won't recognize the id), so just default to the small message type.
            if (connectionType ==  null)
                connectionType = ConnectionType.SMALL_MESSAGE;

            switch (connectionType)
            {
                case LARGE_MESSAGE:
                    return largeChannelMessage;
                case GOSSIP:
                    return gossipChannelMessage;
                case SMALL_MESSAGE:
                default:
                    return smallChannelMessage;
            }
        }

        public long serializedSize(PingMessage t, int version)
        {
            return 1;
        }
    }
}
