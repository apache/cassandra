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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.utils.FBUtilities;

public class ReconstructLogState
{
    public static final Serializer serializer = new Serializer();

    public final Epoch lowerBound;
    public final Epoch higherBound;
    public final boolean includeSnapshot;

    public ReconstructLogState(Epoch lowerBound, Epoch higherBound, boolean includeSnapshot)
    {
        this.lowerBound = lowerBound;
        this.higherBound = higherBound;
        this.includeSnapshot = includeSnapshot;
    }

    static class Serializer implements IVersionedSerializer<ReconstructLogState>
    {

        public void serialize(ReconstructLogState t, DataOutputPlus out, int version) throws IOException
        {
            Epoch.serializer.serialize(t.lowerBound, out);
            Epoch.serializer.serialize(t.higherBound, out);
            out.writeBoolean(t.includeSnapshot);
        }

        public ReconstructLogState deserialize(DataInputPlus in, int version) throws IOException
        {
            Epoch lowerBound = Epoch.serializer.deserialize(in);
            Epoch higherBound = Epoch.serializer.deserialize(in);
            return new ReconstructLogState(lowerBound, higherBound, in.readBoolean());
        }

        public long serializedSize(ReconstructLogState t, int version)
        {
            return Epoch.serializer.serializedSize(t.lowerBound) +
                   Epoch.serializer.serializedSize(t.higherBound) +
                   TypeSizes.BOOL_SIZE;
        }
    }

    public static class Handler implements IVerbHandler<ReconstructLogState>
    {
        public static final Handler instance = new Handler();

        private final Supplier<Processor> processor;

        public Handler()
        {
            this(() -> ClusterMetadataService.instance().processor());
        }
        public Handler(Supplier<Processor> processor)
        {
            this.processor = processor;
        }
        public void doVerb(Message<ReconstructLogState> message) throws IOException
        {
            TCMMetrics.instance.reconstructLogStateCall.mark();
            ReconstructLogState request = message.payload;

            if (!ClusterMetadataService.instance().isCurrentMember(FBUtilities.getBroadcastAddressAndPort()))
                throw new NotCMSException("This node is not in the CMS, can't generate a consistent log fetch response to " + message.from());

            LogState result = processor.get().getLogState(request.lowerBound, request.higherBound, request.includeSnapshot,
                                                          Retry.Deadline.after(DatabaseDescriptor.getCmsAwaitTimeout().to(TimeUnit.NANOSECONDS),
                                                                               new Retry.Jitter(TCMMetrics.instance.fetchLogRetries)));

            MessagingService.instance().send(message.responseWith(result), message.from());
        }
    }
}
