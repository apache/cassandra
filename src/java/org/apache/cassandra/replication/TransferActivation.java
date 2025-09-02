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

package org.apache.cassandra.replication;

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.utils.TimeUUID;

public class TransferActivation
{
    public final TimeUUID transferId;
    public final TimeUUID planId;
    public final MutationId activationId;
    public final boolean dryRun;

    public TransferActivation(CoordinatedTransfer transfer, InetAddressAndPort peer)
    {
        this(transfer, peer, false);
    }

    public TransferActivation(CoordinatedTransfer transfer, InetAddressAndPort peer, boolean dryRun)
    {
        this(transfer.transferId, transfer.streams.get(peer).planId(), transfer.activationId, dryRun);
    }

    TransferActivation(TimeUUID transferId, TimeUUID planId, MutationId activationId, boolean dryRun)
    {
        this.transferId = transferId;
        Preconditions.checkArgument(!activationId.isNone());
        Preconditions.checkNotNull(planId);
        this.planId = planId;
        this.activationId = activationId;
        this.dryRun = dryRun;
    }

    public void apply()
    {
        MutationTrackingService.instance.activateLocal(this);
    }

    public static final Serializer serializer = new Serializer();

    public static class Serializer implements IVersionedSerializer<TransferActivation>
    {
        @Override
        public void serialize(TransferActivation activate, DataOutputPlus out, int version) throws IOException
        {
            TimeUUID.Serializer.instance.serialize(activate.transferId, out, version);
            TimeUUID.Serializer.instance.serialize(activate.planId, out, version);
            MutationId.serializer.serialize(activate.activationId, out, version);
            out.writeBoolean(activate.dryRun);
        }

        @Override
        public TransferActivation deserialize(DataInputPlus in, int version) throws IOException
        {
            TimeUUID transferId = TimeUUID.Serializer.instance.deserialize(in, version);
            TimeUUID planId = TimeUUID.Serializer.instance.deserialize(in, version);
            MutationId activationId = MutationId.serializer.deserialize(in, version);
            boolean dryRun = in.readBoolean();
            return new TransferActivation(transferId, planId, activationId, dryRun);
        }

        @Override
        public long serializedSize(TransferActivation activate, int version)
        {
            long size = 0;
            size += TimeUUID.Serializer.instance.serializedSize(activate.transferId, version);
            size += TimeUUID.Serializer.instance.serializedSize(activate.planId, version);
            size += MutationId.serializer.serializedSize(activate.activationId, version);
            size += TypeSizes.BOOL_SIZE;
            return size;
        }
    }

    public static class VerbHandler implements IVerbHandler<TransferActivation>
    {
        @Override
        public void doVerb(Message<TransferActivation> msg) throws IOException
        {
            LocalTransfers.instance().executor.submit(() -> {
                msg.payload.apply();
                MessagingService.instance().respond(NoPayload.noPayload, msg);
            });
        }
    }

    public static final VerbHandler verbHandler = new VerbHandler();

    @Override
    public String toString()
    {
        return "Activate{" +
               "transferId=" + transferId +
               ", planId=" + planId +
               ", activationId=" + activationId +
               ", dryRun=" + dryRun +
               '}';
    }
}
