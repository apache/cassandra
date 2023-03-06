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

package org.apache.cassandra.tcm;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.DistributedMetadataLogKeyspace;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.log.LogStorage;

public class Replay
{
    public static final Serializer serializer = new Serializer();

    public final Epoch start;
    public final boolean consistentReplay;

    public Replay(Epoch start, boolean consistentReplay)
    {
        this.start = start;
        this.consistentReplay = consistentReplay;
    }

    public String toString()
    {
        return "Replay{" +
               "consistentReplay=" + consistentReplay +
               ", start=" + start +
               '}';
    }

    static class Serializer implements IVersionedSerializer<Replay>
    {

        public void serialize(Replay t, DataOutputPlus out, int version) throws IOException
        {
            Epoch.serializer.serialize(t.start, out);
            out.writeBoolean(t.consistentReplay);
        }

        public Replay deserialize(DataInputPlus in, int version) throws IOException
        {
            Epoch epoch = Epoch.serializer.deserialize(in);
            boolean consistentReplay = in.readBoolean();
            return new Replay(epoch, consistentReplay);
        }

        public long serializedSize(Replay t, int version)
        {
            return Epoch.serializer.serializedSize(t.start)
                   + TypeSizes.BOOL_SIZE;
        }
    }

    static class Handler implements IVerbHandler<Replay>
    {
        private static final Logger logger = LoggerFactory.getLogger(Handler.class);

        public void doVerb(Message<Replay> message) throws IOException
        {
            Replay request = message.payload;

            logger.info("Received replay request {} from {}", request, message.from());
            LogState delta;
            // If both we and the other node believe it should be caught up with a linearizable read
            boolean consistentReplay = request.consistentReplay && !ClusterMetadataService.instance().isCurrentMember(message.from());

            if (consistentReplay)
                delta = DistributedMetadataLogKeyspace.getLogState(message.payload.start);
            else
                delta = LogStorage.SystemKeyspace.getLogState(message.payload.start);

            logger.info("Responding with log delta: {}", delta);
            MessagingService.instance().send(message.responseWith(delta), message.from());
        }
    }
}
