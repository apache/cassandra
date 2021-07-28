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

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.FBUtilities;

public class OutgoingStreamMessage extends StreamMessage
{
    public static Serializer<OutgoingStreamMessage> serializer = new Serializer<OutgoingStreamMessage>()
    {
        public OutgoingStreamMessage deserialize(DataInputPlus in, int version)
        {
            throw new UnsupportedOperationException("Not allowed to call deserialize on an outgoing stream");
        }

        public void serialize(OutgoingStreamMessage message, StreamingDataOutputPlus out, int version, StreamSession session) throws IOException
        {
            message.startTransfer();
            try
            {
                message.serialize(out, version, session);
                session.streamSent(message);
            }
            finally
            {
                message.finishTransfer();
            }
        }

        public long serializedSize(OutgoingStreamMessage message, int version)
        {
            return 0;
        }
    };

    public final StreamMessageHeader header;
    public final OutgoingStream stream;
    private boolean completed = false;
    private boolean transferring = false;

    public OutgoingStreamMessage(TableId tableId, StreamSession session, OutgoingStream stream, int sequenceNumber)
    {
        super(Type.STREAM);

        this.stream = stream;
        this.header = new StreamMessageHeader(tableId,
                                              FBUtilities.getBroadcastAddressAndPort(),
                                              session.planId(),
                                              session.isFollower(),
                                              session.sessionIndex(),
                                              sequenceNumber,
                                              stream.getRepairedAt(),
                                              stream.getPendingRepair());
    }

    public synchronized void serialize(StreamingDataOutputPlus out, int version, StreamSession session) throws IOException
    {
        if (completed)
        {
            return;
        }
        StreamMessageHeader.serializer.serialize(header, out, version);
        stream.write(session, out, version);
    }

    @VisibleForTesting
    public synchronized void finishTransfer()
    {
        transferring = false;
        //session was aborted mid-transfer, now it's safe to release
        if (completed)
        {
            stream.finish();
        }
    }

    @VisibleForTesting
    public synchronized void startTransfer()
    {
        if (completed)
            throw new RuntimeException(String.format("Transfer of stream %s already completed or aborted (perhaps session failed?).",
                                                     stream));
        transferring = true;
    }

    public synchronized void complete()
    {
        if (!completed)
        {
            completed = true;
            //release only if not transferring
            if (!transferring)
            {
                stream.finish();
            }
        }
    }

    @Override
    public String toString()
    {
        return "OutgoingStreamMessage{" +
               "header=" + header +
               ", stream=" + stream +
               '}';
    }

    public String getName()
    {
        return stream.getName();
    }
}

