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
import java.util.ArrayList;
import java.util.Collection;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.LogStreamManifest;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamSummary;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;

public class PrepareSynAckMessage extends StreamMessage
{
    public static Serializer<PrepareSynAckMessage> serializer = new Serializer<PrepareSynAckMessage>()
    {
        public void serialize(PrepareSynAckMessage message, StreamingDataOutputPlus out, int version, StreamSession session) throws IOException
        {
            out.writeInt(message.summaries.size());
            for (StreamSummary summary : message.summaries)
                StreamSummary.serializer.serialize(summary, out, version);
            // log summary (optional, added in version 52)
            if (version >= MessagingService.VERSION_61)
            {
                out.writeBoolean(message.logSummary != null);
                if (message.logSummary != null)
                    LogStreamManifest.embedded.serialize(message.logSummary, out, version);
            }
        }

        public PrepareSynAckMessage deserialize(DataInputPlus input, int version) throws IOException
        {
            PrepareSynAckMessage message = new PrepareSynAckMessage();
            int numSummaries = input.readInt();
            for (int i = 0; i < numSummaries; i++)
                message.summaries.add(StreamSummary.serializer.deserialize(input, version));
            // log summary (optional, added in version 52)
            if (version >= MessagingService.VERSION_61)
            {
                if (input.readBoolean())
                    message.logSummary = LogStreamManifest.embedded.deserialize(input, version);
            }
            return message;
        }

        public long serializedSize(PrepareSynAckMessage message, int version)
        {
            long size = 4; // count of summaries
            for (StreamSummary summary : message.summaries)
                size += StreamSummary.serializer.serializedSize(summary, version);
            // log summary (optional, added in version 52)
            if (version >= MessagingService.VERSION_61)
            {
                size += 1; // boolean for logSummary presence
                if (message.logSummary != null)
                    size += LogStreamManifest.embedded.serializedSize(message.logSummary, version);
            }
            return size;
        }
    };

    /**
     * Summaries of streaming out
     */
    public final Collection<StreamSummary> summaries = new ArrayList<>();

    /**
     * Optional summary of log stream tx
     */
    public LogStreamManifest logSummary = null;

    public PrepareSynAckMessage()
    {
        super(Type.PREPARE_SYNACK);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Prepare SYNACK (");
        int totalFile = 0;
        for (StreamSummary summary : summaries)
            totalFile += summary.files;
        sb.append(" ").append(totalFile).append(" files");
        sb.append('}');
        return sb.toString();
    }
}
