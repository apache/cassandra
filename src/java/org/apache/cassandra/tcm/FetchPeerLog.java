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

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.log.LogStorage;

public class FetchPeerLog
{
    public static final Serializer serializer = new Serializer();

    public final Epoch start;

    public FetchPeerLog(Epoch start)
    {
        this.start = start;
    }

    public String toString()
    {
        return "FetchPeerLog{" +
               ", start=" + start +
               '}';
    }

    static class Serializer implements IVersionedSerializer<FetchPeerLog>
    {

        public void serialize(FetchPeerLog t, DataOutputPlus out, int version) throws IOException
        {
            Epoch.serializer.serialize(t.start, out);
        }

        public FetchPeerLog deserialize(DataInputPlus in, int version) throws IOException
        {
            Epoch epoch = Epoch.serializer.deserialize(in);
            return new FetchPeerLog(epoch);
        }

        public long serializedSize(FetchPeerLog t, int version)
        {
            return Epoch.serializer.serializedSize(t.start);
        }
    }

    public static class Handler implements IVerbHandler<FetchPeerLog>
    {
        public static Handler instance = new Handler();
        private static final Logger logger = LoggerFactory.getLogger(Handler.class);

        public void doVerb(Message<FetchPeerLog> message) throws IOException
        {
            FetchPeerLog request = message.payload;

            ClusterMetadata metadata = ClusterMetadata.current();
            logger.info("Received peer log fetch request {} from {}: start = {}, current = {}", request, message.from(), message.payload.start, metadata.epoch);
            LogState delta = LogStorage.SystemKeyspace.getLogState(message.payload.start);
            TCMMetrics.instance.peerLogEntriesServed(message.payload.start, delta.latestEpoch());
            logger.info("Responding with log delta: {}", delta);
            MessagingService.instance().send(message.responseWith(delta), message.from());
        }
    }
}
