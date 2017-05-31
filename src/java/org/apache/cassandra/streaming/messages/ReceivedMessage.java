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
package org.apache.cassandra.streaming.messages;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.StreamSession;

public class ReceivedMessage extends StreamMessage
{
    public static Serializer<ReceivedMessage> serializer = new Serializer<ReceivedMessage>()
    {
        @SuppressWarnings("resource") // Not closing constructed DataInputPlus's as the channel needs to remain open.
        public ReceivedMessage deserialize(ReadableByteChannel in, int version, StreamSession session) throws IOException
        {
            DataInputPlus input = new DataInputStreamPlus(Channels.newInputStream(in));
            return new ReceivedMessage(TableId.deserialize(input), input.readInt());
        }

        public void serialize(ReceivedMessage message, DataOutputStreamPlus out, int version, StreamSession session) throws IOException
        {
            message.tableId.serialize(out);
            out.writeInt(message.sequenceNumber);
        }
    };

    public final TableId tableId;
    public final int sequenceNumber;

    public ReceivedMessage(TableId tableId, int sequenceNumber)
    {
        super(Type.RECEIVED);
        this.tableId = tableId;
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Received (");
        sb.append(tableId).append(", #").append(sequenceNumber).append(')');
        return sb.toString();
    }
}
