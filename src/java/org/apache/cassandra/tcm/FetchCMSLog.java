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
import java.util.function.BiFunction;

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
import org.apache.cassandra.utils.FBUtilities;

public class FetchCMSLog
{
    public static final Serializer serializer = new Serializer();

    public final Epoch lowerBound;
    public final boolean consistentFetch;

    public FetchCMSLog(Epoch lowerBound, boolean consistentFetch)
    {
        this.lowerBound = lowerBound;
        this.consistentFetch = consistentFetch;
    }

    public String toString()
    {
        return "FetchCMSLog{" +
               "consistentFetch=" + consistentFetch +
               ", lowerBound=" + lowerBound +
               '}';
    }

    static class Serializer implements IVersionedSerializer<FetchCMSLog>
    {

        public void serialize(FetchCMSLog t, DataOutputPlus out, int version) throws IOException
        {
            Epoch.serializer.serialize(t.lowerBound, out);
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
            return Epoch.serializer.serializedSize(t.lowerBound)
                   + TypeSizes.BOOL_SIZE;
        }
    }

    static class Handler implements IVerbHandler<FetchCMSLog>
    {
        private static final Logger logger = LoggerFactory.getLogger(Handler.class);

        /**
         * Receives epoch we need to fetch state up to, and a boolean specifying whether the consistency can be downgraded
         * to node-local (which only relevant in cases of CMS expansions/shrinks, and can only be requested by the
         * CMS node that collects the highest epoch from the quorum of peers).
         */
        private final BiFunction<Epoch, Boolean, LogState> logStateSupplier;

        public Handler()
        {
            this(DistributedMetadataLogKeyspace::getLogState);
        }

        public Handler(BiFunction<Epoch, Boolean, LogState> logStateSupplier)
        {
            this.logStateSupplier = logStateSupplier;
        }

        public void doVerb(Message<FetchCMSLog> message) throws IOException
        {
            FetchCMSLog request = message.payload;

            if (logger.isTraceEnabled())
                logger.trace("Received log fetch request {} from {}: start = {}, current = {}", request, message.from(), message.payload.lowerBound, ClusterMetadata.current().epoch);

            if (request.consistentFetch && !ClusterMetadataService.instance().isCurrentMember(FBUtilities.getBroadcastAddressAndPort()))
                throw new NotCMSException("This node is not in the CMS, can't generate a consistent log fetch response to " + message.from());

            // If both we and the other node believe it should be caught up with a linearizable read
            boolean consistentFetch = request.consistentFetch && !ClusterMetadataService.instance().isCurrentMember(message.from());

            LogState delta = logStateSupplier.apply(message.payload.lowerBound, consistentFetch);
            TCMMetrics.instance.cmsLogEntriesServed(message.payload.lowerBound, delta.latestEpoch());
            logger.info("Responding to {}({}) with log delta: {}", message.from(), request, delta);
            MessagingService.instance().send(message.responseWith(delta), message.from());
        }
    }
}
