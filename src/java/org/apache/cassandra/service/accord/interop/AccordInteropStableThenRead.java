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

package org.apache.cassandra.service.accord.interop;

import java.io.IOException;
import javax.annotation.Nullable;

import accord.local.Commands;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.messages.Commit;
import accord.messages.MessageType;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.AccordMessageSink.AccordMessageType;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.CommandSerializers.ExecuteAtSerializer;
import org.apache.cassandra.service.accord.serializers.CommitSerializers;
import org.apache.cassandra.service.accord.serializers.DepsSerializers;
import org.apache.cassandra.service.accord.serializers.IVersionedSerializer;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.serializers.Version;

import static accord.messages.Commit.WithDeps.HasDeps;
import static accord.messages.Commit.WithDeps.NoDeps;
import static accord.messages.Commit.WithTxn.HasTxn;
import static accord.messages.Commit.WithTxn.NoTxn;
import static accord.primitives.SaveStatus.PreApplied;
import static accord.primitives.SaveStatus.ReadyToExecute;

public class AccordInteropStableThenRead extends AccordInteropRead
{
    // TODO (desired): duplicates a lot of StableThenReadSerializer
    public static final IVersionedSerializer<AccordInteropStableThenRead> requestSerializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(AccordInteropStableThenRead read, DataOutputPlus out, Version version) throws IOException
        {
            CommandSerializers.txnId.serialize(read.txnId, out);
            KeySerializers.participants.serialize(read.scope, out);
            CommitSerializers.kind.serialize(read.kind, out);
            out.writeUnsignedVInt(read.minEpoch);
            ExecuteAtSerializer.serialize(read.txnId, read.executeAt, out);
            if (read.kind.withTxn != NoTxn)
                CommandSerializers.nullablePartialTxn.serialize(read.partialTxn, out, version);
            if (read.kind.withDeps == HasDeps)
                DepsSerializers.partialDeps.serialize(read.partialDeps, out);
            if (read.kind.withTxn == HasTxn)
                KeySerializers.fullRoute.serialize(read.route, out);
            ReadCommand.serializer.serialize(read.command, out, version.messageVersion());
        }

        @Override
        public AccordInteropStableThenRead deserialize(DataInputPlus in, Version version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in);
            Participants<?> scope = KeySerializers.participants.deserialize(in);
            Commit.Kind kind = CommitSerializers.kind.deserialize(in);
            long minEpoch = in.readUnsignedVInt();
            Timestamp executeAt = ExecuteAtSerializer.deserialize(txnId, in);
            PartialTxn partialTxn = kind.withTxn == NoTxn ? null : CommandSerializers.nullablePartialTxn.deserialize(in, version);
            PartialDeps partialDeps = kind.withDeps == NoDeps ? null : DepsSerializers.partialDeps.deserialize(in);
            FullRoute < ?> route = kind.withTxn == HasTxn ? KeySerializers.fullRoute.deserialize(in) : null;
            ReadCommand command = ReadCommand.serializer.deserialize(in, version.messageVersion());
            return new AccordInteropStableThenRead(txnId, scope, kind, minEpoch, executeAt, partialTxn, partialDeps, route, command);
        }

        @Override
        public long serializedSize(AccordInteropStableThenRead read, Version version)
        {
            return CommandSerializers.txnId.serializedSize(read.txnId)
                   + KeySerializers.participants.serializedSize(read.scope)
                   + CommitSerializers.kind.serializedSize(read.kind)
                   + TypeSizes.sizeofUnsignedVInt(read.minEpoch)
                   + ExecuteAtSerializer.serializedSize(read.txnId, read.executeAt)
                   + (read.kind.withTxn == NoTxn ? 0 : CommandSerializers.nullablePartialTxn.serializedSize(read.partialTxn, version))
                   + (read.kind.withDeps != HasDeps ? 0 : DepsSerializers.partialDeps.serializedSize(read.partialDeps))
                   + (read.kind.withTxn != HasTxn ? 0 : KeySerializers.fullRoute.serializedSize(read.route))
                   + ReadCommand.serializer.serializedSize(read.command, version.messageVersion());
        }
    };

    // TODO (required): why is this safe to execute at PreApplied? Document.
    private static final ExecuteOn EXECUTE_ON = new ExecuteOn(ReadyToExecute, PreApplied);

    public final long minEpoch;
    public final Commit.Kind kind;
    public final Timestamp executeAt;
    public final @Nullable PartialTxn partialTxn;
    public final @Nullable PartialDeps partialDeps;
    public final @Nullable FullRoute<?> route;

    public AccordInteropStableThenRead(Node.Id to, Topologies topologies, TxnId txnId, Commit.Kind kind, Timestamp executeAt, Txn txn, Deps deps, FullRoute<?> route, ReadCommand command)
    {
        super(to, topologies, txnId, route, executeAt.epoch(), command);
        this.kind = kind;
        this.minEpoch = topologies.oldestEpoch();
        this.executeAt = executeAt;
        this.partialTxn = kind.withTxn.select(txn, route, topologies, txnId, to);
        this.partialDeps = kind.withDeps.select(deps, route);
        this.route = kind.withTxn.select(route);
    }

    public AccordInteropStableThenRead(TxnId txnId, Participants<?> scope, Commit.Kind kind, long minEpoch, Timestamp executeAt, @Nullable PartialTxn partialTxn, @Nullable PartialDeps partialDeps, @Nullable FullRoute<?> route, ReadCommand command)
    {
        super(txnId, scope, executeAt.epoch(), command);
        this.minEpoch = minEpoch;
        this.kind = kind;
        this.executeAt = executeAt;
        this.partialTxn = partialTxn;
        this.partialDeps = partialDeps;
        this.route = route;
    }

    @Override
    public CommitOrReadNack apply(SafeCommandStore safeStore)
    {
        Route<?> route = this.route == null ? (Route)scope : this.route;
        StoreParticipants participants = StoreParticipants.execute(safeStore, route, txnId, minEpoch(), executeAtEpoch);
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        Commands.commit(safeStore, safeCommand, participants, kind.saveStatus, Ballot.ZERO, txnId, route, partialTxn, executeAt, partialDeps, kind);
        return super.apply(safeStore, safeCommand, participants);
    }

    @Override
    public ReadType kind()
    {
        return ReadType.stableThenRead;
    }

    @Override
    protected ExecuteOn executeOn()
    {
        return EXECUTE_ON;
    }

    @Override
    public MessageType type()
    {
        return AccordMessageType.INTEROP_STABLE_THEN_READ_REQ;
    }

    @Override
    public String toString()
    {
        return "AccordInteropStableThenRead{" +
               "txnId=" + txnId +
               "command=" + command +
               '}';
    }
}
