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

import java.io.IOException;
import java.util.Objects;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.util.DataInputPlus;

import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.streaming.IncomingStream;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamReceiveException;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.JVMStabilityInspector;

public class IncomingStreamMessage extends StreamMessage
{
    public static Serializer<IncomingStreamMessage> serializer = new Serializer<IncomingStreamMessage>()
    {
        @SuppressWarnings("resource")
        public IncomingStreamMessage deserialize(DataInputPlus input, int version, StreamSession session) throws IOException
        {
            StreamMessageHeader header = StreamMessageHeader.serializer.deserialize(input, version);
            session = StreamManager.instance.findSession(header.sender, header.planId, header.sessionIndex);
            if (session == null)
                throw new IllegalStateException(String.format("unknown stream session: %s - %d", header.planId, header.sessionIndex));
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(header.tableId);
            if (cfs == null)
                throw new StreamReceiveException(session, "CF " + header.tableId + " was dropped during streaming");

            IncomingStream incomingData = cfs.getStreamManager().prepareIncomingStream(session, header);
            incomingData.read(input, version);

            try
            {
                return new IncomingStreamMessage(incomingData, header);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                throw new StreamReceiveException(session, t);
            }
        }

        public void serialize(IncomingStreamMessage message, DataOutputStreamPlus out, int version, StreamSession session)
        {
            throw new UnsupportedOperationException("Not allowed to call serialize on an incoming stream");
        }

        public long serializedSize(IncomingStreamMessage message, int version)
        {
            throw new UnsupportedOperationException("Not allowed to call serializedSize on an incoming stream");
        }
    };

    public StreamMessageHeader header;
    public IncomingStream stream;

    public IncomingStreamMessage(IncomingStream stream, StreamMessageHeader header)
    {
        super(Type.STREAM);
        this.stream = stream;
        this.header = header;
    }

    @Override
    public String toString()
    {
        return "IncomingStreamMessage{" +
               "header=" + header +
               ", stream=" + stream +
               '}';
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IncomingStreamMessage that = (IncomingStreamMessage) o;
        return Objects.equals(header, that.header) &&
               Objects.equals(stream, that.stream);
    }

    public int hashCode()
    {

        return Objects.hash(header, stream);
    }
}

