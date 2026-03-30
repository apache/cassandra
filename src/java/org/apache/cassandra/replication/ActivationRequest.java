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

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Activation transitions a {@link PendingLocalTransfer} from being pending (durable on disk but not yet visible to
 * reads) to live (visible to reads and compactions), by associating the streaming plan ID with a mutation ID, referred
 * to as the transfer ID.
 * <p>
 * For tracked repairs, activation may occur without a plan ID and therefore without a {@link PendingLocalTransfer}. In
 * this case, activation skips this step (as the data is already present) and simply updates log offsets.
 * <p>
 * See {@link CoordinatedTransfer} for the lifecycle of a transfer and when a {@link ActivationRequest} is sent.
 */
public class ActivationRequest
{
    public final StreamOperation operation;
    public final Pair<InetAddressAndPort, InetAddressAndPort> pair;
    public final Phase phase;
    public final ShortMutationId transferId;
    public final NodeId coordinatorId;
    public final String keyspace;
    public final long sinceEpoch;
    public final Range<Token> range;

    @Nullable
    public final TimeUUID planId;

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

    public ActivationRequest(StreamOperation operation,
                             Pair<InetAddressAndPort, InetAddressAndPort> pair,
                             Phase phase,
                             ShortMutationId transferId,
                             NodeId coordinatorId,
                             Range<Token> range,
                             long sinceEpoch,
                             String keyspace,
                             TimeUUID planId)
    {
        Preconditions.checkArgument(operation.isTrackable(), "Operation " + operation.getDescription() + " is not trackable");
        Preconditions.checkNotNull(pair, "Activations require an address pair");
        Preconditions.checkArgument(!transferId.isNone(), "Activations require a transfer ID");
        Preconditions.checkNotNull(coordinatorId, "Activations require a coordinator node ID");

        this.operation = operation;
        this.pair = pair;
        this.transferId = transferId;
        this.coordinatorId = coordinatorId;
        this.phase = phase;
        this.keyspace = keyspace;
        this.sinceEpoch = sinceEpoch;
        this.range = range;
        this.planId = planId;
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
        MutationTrackingService.instance().activateLocal(this);
    }

    public static final VersionedSerializer<ActivationRequest> serializer = new VersionedSerializer<>()
    {
        @Override
        public void serialize(ActivationRequest request, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(request.operation.getDescription());

            InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(request.pair.left, out, version.messagingVersion());
            InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(request.pair.right, out, version.messagingVersion());

            ShortMutationId.serializer.serialize(request.id(), out);
            NodeId.messagingSerializer.serialize(request.coordinatorId, out, version.messagingVersion());
            out.writeByte(request.phase.id);
            out.writeUTF(request.keyspace);
            out.writeLong(request.sinceEpoch);
            Range.serializer.serialize(request.range, out, null);
            TimeUUID.Serializer.nullable.serialize(request.planId, out);
        }

        @Override
        public ActivationRequest deserialize(DataInputPlus in, Version version) throws IOException
        {
            StreamOperation operation = StreamOperation.fromString(in.readUTF());

            InetAddressAndPort sender = InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version.messagingVersion());
            InetAddressAndPort receiver = InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version.messagingVersion());

            ShortMutationId id = ShortMutationId.serializer.deserialize(in);
            NodeId coordinatorId = NodeId.messagingSerializer.deserialize(in, version.messagingVersion());
            Phase phase = Phase.from(in.readByte());
            String keyspace = in.readUTF();
            long sinceEpoch = in.readLong();
            Range<Token> range = Range.serializer.deserialize(in, null);
            TimeUUID planId = TimeUUID.Serializer.nullable.deserialize(in);

            return new ActivationRequest(operation, Pair.create(sender, receiver), phase, id, coordinatorId, range, sinceEpoch, keyspace, planId);
        }

        @Override
        public long serializedSize(ActivationRequest request, Version version)
        {
            long size = 0;

            size += TypeSizes.sizeof(request.operation.getDescription());

            size += InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(request.pair.left, version.messagingVersion());
            size += InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(request.pair.right, version.messagingVersion());

            size += ShortMutationId.serializer.serializedSize(request.id());
            size += NodeId.messagingSerializer.serializedSize(request.coordinatorId, version.messagingVersion());
            size += TypeSizes.BYTE_SIZE; // Enum ordinal
            size += TypeSizes.sizeof(request.keyspace);
            size += TypeSizes.sizeof(request.sinceEpoch);
            size += Range.serializer.serializedSize(request.range, null);
            size += TimeUUID.Serializer.nullable.serializedSize(request.planId);

            return size;
        }
    };

    public static class VerbHandler implements IVerbHandler<ActivationRequest>
    {
        private static final Logger logger = LoggerFactory.getLogger(VerbHandler.class);

        @Override
        public void doVerb(Message<ActivationRequest> msg) throws IOException
        {
            TransferTrackingService.instance().executor.submit(() -> {
                try
                {
                    msg.payload.apply();
                    MessagingService.instance().respond(new ActivationResponse(msg.payload.pair), msg);
                }
                catch (Throwable t)
                {
                    logger.error("Local activation of {} failed due to error", msg.payload, t);
                    MessagingService.instance().respondWithFailure(RequestFailureReason.forException(t), msg);
                }
            });
        }
    }

    public static final VerbHandler verbHandler = new VerbHandler();

    @Override
    public String toString()
    {
        return "ActivationRequest{" +
               "operation=" + operation +
               ", pair=" + pair +
               ", phase=" + phase +
               ", transferId=" + transferId +
               ", coordinatorId=" + coordinatorId +
               ", keyspace='" + keyspace + '\'' +
               ", range=" + range +
               ", planId=" + planId +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        ActivationRequest that = (ActivationRequest) o;
        return operation == that.operation && Objects.equals(pair, that.pair) && phase == that.phase && Objects.equals(transferId, that.transferId) && Objects.equals(coordinatorId, that.coordinatorId) && Objects.equals(keyspace, that.keyspace) && Objects.equals(range, that.range) && Objects.equals(planId, that.planId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operation, pair, phase, transferId, coordinatorId, keyspace, range, planId);
    }
}
