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

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.net.ConnectionType.URGENT_MESSAGES;
import static org.apache.cassandra.net.ConnectionType.SMALL_MESSAGES;
import static org.apache.cassandra.net.ConnectionType.LARGE_MESSAGES;

/**
 * Indicates to the recipient which {@link ConnectionType} should be used for the response.
 */
public class PingRequest
{
    static final PingRequest forUrgent = new PingRequest(URGENT_MESSAGES);
    static final PingRequest forSmall  = new PingRequest(SMALL_MESSAGES);
    static final PingRequest forLarge  = new PingRequest(LARGE_MESSAGES);

    final ConnectionType connectionType;

    private PingRequest(ConnectionType connectionType)
    {
        this.connectionType = connectionType;
    }

    @VisibleForTesting
    public static PingRequest get(ConnectionType type)
    {
        switch (type)
        {
            case URGENT_MESSAGES: return forUrgent;
            case  SMALL_MESSAGES: return forSmall;
            case  LARGE_MESSAGES: return forLarge;
            default: throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    static IVersionedSerializer<PingRequest> serializer = new IVersionedSerializer<PingRequest>()
    {
        public void serialize(PingRequest t, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(t.connectionType.id);
        }

        public PingRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            return get(ConnectionType.fromId(in.readByte()));
        }

        public long serializedSize(PingRequest t, int version)
        {
            return 1;
        }
    };
}
