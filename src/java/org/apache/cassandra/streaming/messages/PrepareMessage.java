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
import java.util.ArrayList;
import java.util.Collection;

import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.streaming.StreamRequest;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamSummary;
import org.apache.cassandra.streaming.*;

public class PrepareMessage extends StreamMessage
{
    public static Serializer<PrepareMessage> serializer = new Serializer<PrepareMessage>()
    {
        public PrepareMessage deserialize(ReadableByteChannel in, int version, StreamSession session) throws IOException
        {
            DataInput input = new DataInputStream(Channels.newInputStream(in));
            PrepareMessage message = new PrepareMessage();
            // requests
            int numRequests = input.readInt();
            for (int i = 0; i < numRequests; i++)
                message.requests.add(StreamRequest.serializer.deserialize(input, version));
            // summaries
            int numSummaries = input.readInt();
            for (int i = 0; i < numSummaries; i++)
                message.summaries.add(StreamSummary.serializer.deserialize(input, version));

            if (version >= StreamMessage.VERSION_30)
            {
                numRequests = input.readInt();
                for (int i = 0; i < numRequests; i++)
                    message.epaxosRequests.add(EpaxosRequest.serializer.deserialize(input, version));
                // summaries
                numSummaries = input.readInt();
                for (int i = 0; i < numSummaries; i++)
                    message.epaxosSummaries.add(EpaxosSummary.serializer.deserialize(input, version));
            }
            return message;
        }

        public void serialize(PrepareMessage message, DataOutputStreamPlus out, int version, StreamSession session) throws IOException
        {
            // requests
            out.writeInt(message.requests.size());
            for (StreamRequest request : message.requests)
                StreamRequest.serializer.serialize(request, out, version);
            // summaries
            out.writeInt(message.summaries.size());
            for (StreamSummary summary : message.summaries)
                StreamSummary.serializer.serialize(summary, out, version);

            if (version >= StreamMessage.VERSION_30)
            {
                out.writeInt(message.epaxosRequests.size());
                for (EpaxosRequest request: message.epaxosRequests)
                    EpaxosRequest.serializer.serialize(request, out, version);
                out.writeInt(message.epaxosSummaries.size());
                for (EpaxosSummary summary: message.epaxosSummaries)
                    EpaxosSummary.serializer.serialize(summary, out, version);
            }
        }
    };

    /**
     * Streaming requests
     */
    public final Collection<StreamRequest> requests = new ArrayList<>();

    /**
     * Summaries of streaming out
     */
    public final Collection<StreamSummary> summaries = new ArrayList<>();

    /**
     * epaxos token state requested
     */
    public final Collection<EpaxosRequest> epaxosRequests = new ArrayList<>();

    /**
     * epaxos token states to be streamed out
     */
    public final Collection<EpaxosSummary> epaxosSummaries = new ArrayList<>();

    public PrepareMessage()
    {
        super(Type.PREPARE);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Prepare (");
        sb.append(requests.size()).append(" requests, ");
        int totalFile = 0;
        for (StreamSummary summary : summaries)
            totalFile += summary.files;
        sb.append(" ").append(totalFile).append(" files");
        sb.append(epaxosRequests.size()).append(" epaxosRequests, ");
        sb.append('}');
        return sb.toString();
    }
}
