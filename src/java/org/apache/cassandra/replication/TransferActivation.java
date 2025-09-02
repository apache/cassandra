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
import java.util.Objects;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Activation transitions a {@link PendingLocalTransfer} from being pending (durable on disk but not yet visible to
 * reads) to live (visible to reads and compactions), by associating the streaming plan ID with a mutation ID, referred
 * to as the transfer ID.
 * <p>
 * See {@link CoordinatedTransfer} for the lifecycle of a transfer and when a {@link TransferActivation} is sent.
 */
public class TransferActivation
{
    public final TimeUUID planId;
    public final ShortMutationId transferId;
    public final NodeId coordinatorId;
    public final Phase phase;

    public enum Phase
    {
        PREPARE(0),
        COMMIT(1);

        private final int id;
        private static final Phase[] ids;
        static
        {
            ids = new Phase[values().length];
            for (Phase phase : values())
                ids[phase.id] = phase;
        }

        Phase(int id)
        {
            this.id = id;
        }

        static Phase from(int id)
        {
            Phase phase = ids[id];
            Preconditions.checkState(phase.id == id);
            return phase;
        }
    }

    public TransferActivation(CoordinatedTransfer transfer, InetAddressAndPort peer, Phase phase)
    {
        this(transfer.streamResults.get(peer).planId(), transfer.id(), ClusterMetadata.current().myNodeId(), phase);
    }

    TransferActivation(TimeUUID planId, ShortMutationId transferId, NodeId coordinatorId, Phase phase)
    {
        Preconditions.checkArgument(!transferId.isNone());
        Preconditions.checkNotNull(planId);
        Preconditions.checkNotNull(coordinatorId);
        this.planId = planId;
        this.transferId = transferId;
        this.coordinatorId = coordinatorId;
        this.phase = phase;
    }

    ShortMutationId id()
    {
        return transferId;
    }

    public boolean isPrepare()
    {
        return phase == Phase.PREPARE;
    }

    public boolean isCommit()
    {
        return phase == Phase.COMMIT;
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
            TimeUUID.Serializer.instance.serialize(activate.planId, out, version);
            ShortMutationId.serializer.serialize(activate.id(), out, version);
            NodeId.messagingSerializer.serialize(activate.coordinatorId, out, version);
            out.writeByte(activate.phase.id);
        }

        @Override
        public TransferActivation deserialize(DataInputPlus in, int version) throws IOException
        {
            TimeUUID planId = TimeUUID.Serializer.instance.deserialize(in, version);
            ShortMutationId id = ShortMutationId.serializer.deserialize(in, version);
            NodeId coordinatorId = NodeId.messagingSerializer.deserialize(in, version);
            Phase phase = Phase.from(in.readByte());
            return new TransferActivation(planId, id, coordinatorId, phase);
        }

        @Override
        public long serializedSize(TransferActivation activate, int version)
        {
            long size = 0;
            size += TimeUUID.Serializer.instance.serializedSize(activate.planId, version);
            size += ShortMutationId.serializer.serializedSize(activate.id(), version);
            size += NodeId.messagingSerializer.serializedSize(activate.coordinatorId, version);
            size += TypeSizes.BYTE_SIZE; // Enum ordinal
            return size;
        }
    }

    public static class VerbHandler implements IVerbHandler<TransferActivation>
    {
        @Override
        public void doVerb(Message<TransferActivation> msg) throws IOException
        {
            LocalTransfers.instance().executor.submit(() -> {
                try
                {
                    msg.payload.apply();
                    MessagingService.instance().respond(NoPayload.noPayload, msg);
                }
                catch (Throwable t)
                {
                    MessagingService.instance().respondWithFailure(RequestFailureReason.forException(t), msg);
                }
            });
        }
    }

    public static final VerbHandler verbHandler = new VerbHandler();

    @Override
    public String toString()
    {
        return "TransferActivation{" +
               ", planId=" + planId +
               ", transferId=" + transferId +
               ", coordinatorId=" + coordinatorId +
               ", phase=" + phase +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        TransferActivation that = (TransferActivation) o;
        return Objects.equals(planId, that.planId) && Objects.equals(transferId, that.transferId) && Objects.equals(coordinatorId, that.coordinatorId) && phase == that.phase;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(planId, transferId, coordinatorId, phase);
    }
}
