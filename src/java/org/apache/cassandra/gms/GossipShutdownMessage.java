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
package org.apache.cassandra.gms;

import org.apache.cassandra.io.IVersionedSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This message indicates the gossiper is shutting down
 */

class GossipShutdownMessage
{
    private static final IVersionedSerializer<GossipShutdownMessage> serializer;
    static
    {
        serializer = new GossipShutdownMessageSerializer();
    }

    static IVersionedSerializer<GossipShutdownMessage> serializer()
    {
        return serializer;
    }

    GossipShutdownMessage()
    {
    }
}

class GossipShutdownMessageSerializer implements IVersionedSerializer<GossipShutdownMessage>
{
    public void serialize(GossipShutdownMessage gShutdownMessage, DataOutput dos, int version) throws IOException
    {
    }

    public GossipShutdownMessage deserialize(DataInput dis, int version) throws IOException
    {
        return new GossipShutdownMessage();
    }

    public long serializedSize(GossipShutdownMessage gossipShutdownMessage, int version)
    {
        throw new UnsupportedOperationException();
    }
}