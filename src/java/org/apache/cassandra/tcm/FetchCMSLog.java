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
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.DistributedMetadataLogKeyspace;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.log.LogStorage;
import org.apache.cassandra.utils.FBUtilities;

public class FetchCMSLog
{
    public static final Serializer serializer = new Serializer();

    public final Epoch start;
    public final boolean consistentFetch;

    public FetchCMSLog(Epoch start, boolean consistentFetch)
    {
        this.start = start;
        this.consistentFetch = consistentFetch;
    }

    public String toString()
    {
        return "FetchCMSLog{" +
               "consistentFetch=" + consistentFetch +
               ", start=" + start +
               '}';
    }

    static class Serializer implements IVersionedSerializer<FetchCMSLog>
    {

        public void serialize(FetchCMSLog t, DataOutputPlus out, int version) throws IOException
        {
            Epoch.serializer.serialize(t.start, out);
            out.writeBoolean(t.consistentFetch);
        }

        public FetchCMSLog deserialize(DataInputPlus in, int version) throws IOException
        {
            Epoch epoch = Epoch.serializer.deserialize(in);
            boolean consistentFetch = in.readBoolean();
            return new FetchCMSLog(epoch, consistentFetch);
        }

        public long serializedSize(FetchCMSLog t, int version)
        {
            return Epoch.serializer.serializedSize(t.start)
                   + TypeSizes.BOOL_SIZE;
        }
    }

    static class Handler implements IVerbHandler<FetchCMSLog>
    {
        private static final Logger logger = LoggerFactory.getLogger(Handler.class);

        private final Function<Epoch, LogState> logStateSupplier;

        public Handler()
        {
            this(DistributedMetadataLogKeyspace::getLogState);
        }

        public Handler(Function<Epoch, LogState> logStateSupplier)
        {
            this.logStateSupplier = logStateSupplier;
        }

        public void doVerb(Message<FetchCMSLog> message) throws IOException
        {
            FetchCMSLog request = message.payload;

            logger.info("Received log fetch request {} from {}: start = {}, current = {}", request, message.from(), message.payload.start, ClusterMetadata.current().epoch);
            if (request.consistentFetch && !ClusterMetadataService.instance().isCurrentMember(FBUtilities.getBroadcastAddressAndPort()))
                throw new NotCMSException("This node is not in the CMS, can't generate a consistent log fetch response to " + message.from());

            // If both we and the other node believe it should be caught up with a linearizable read
            boolean consistentFetch = request.consistentFetch && !ClusterMetadataService.instance().isCurrentMember(message.from());

            LogState delta = consistentFetch ? logStateSupplier.apply(message.payload.start)
                                              : LogStorage.SystemKeyspace.getLogState(message.payload.start);
            TCMMetrics.instance.cmsLogEntriesServed(message.payload.start, delta.latestEpoch());
            logger.info("Responding with log delta: {}", delta);
            MessagingService.instance().send(message.responseWith(delta), message.from());
        }
    }
}
