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

import accord.api.Data;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.messages.AbstractExecute;
import accord.messages.MessageType;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadRepairVerbHandler;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.accord.AccordMessageSink.AccordMessageType;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.serializers.ReadDataSerializers;
import org.apache.cassandra.service.accord.serializers.ReadDataSerializers.ReadDataSerializer;

/**
 * Applies a read repair mutation from inside the context of a CommandStore via AbstractExecute
 * ensuring that the contents of the read repair consist of data that isn't from transactions that
 * haven't been committed yet at this command store.
 */
public class AccordInteropReadRepair extends AbstractExecute
{
    public static final IVersionedSerializer<AccordInteropReadRepair> requestSerializer = new ReadDataSerializer<AccordInteropReadRepair>()
    {
        @Override
        public void serialize(AccordInteropReadRepair repair, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(repair.txnId, out, version);
            KeySerializers.participants.serialize(repair.readScope, out, version);
            out.writeUnsignedVInt(repair.waitForEpoch());
            out.writeUnsignedVInt(repair.executeAtEpoch - repair.waitForEpoch());
            Mutation.serializer.serialize(repair.mutation, out, version);
        }

        @Override
        public AccordInteropReadRepair deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version);
            Participants<?> readScope = KeySerializers.participants.deserialize(in, version);
            long waitForEpoch = in.readUnsignedVInt();
            long executeAtEpoch = in.readUnsignedVInt() + waitForEpoch;
            Mutation mutation = Mutation.serializer.deserialize(in, version);
            return new AccordInteropReadRepair(txnId, readScope, waitForEpoch, executeAtEpoch, mutation);
        }

        @Override
        public long serializedSize(AccordInteropReadRepair repair, int version)
        {
            return CommandSerializers.txnId.serializedSize(repair.txnId, version)
                   + KeySerializers.participants.serializedSize(repair.readScope, version)
                   + TypeSizes.sizeofUnsignedVInt(repair.waitForEpoch())
                   + TypeSizes.sizeofUnsignedVInt(repair.executeAtEpoch - repair.waitForEpoch())
                   + Mutation.serializer.serializedSize(repair.mutation, version);
        }
    };

    static class ReadRepairCallback extends AccordInteropReadCallback<Object>
    {
        public ReadRepairCallback(Node.Id id, InetAddressAndPort endpoint, Message<?> message, RequestCallback<Object> wrapped, MaximalCommitSender maximalCommitSender)
        {
            super(id, endpoint, message, wrapped, maximalCommitSender);
        }

        @Override
        Object convertResponse(ReadOk ok)
        {
            return NoPayload.noPayload;
        }
    }

    private final Mutation mutation;

    private static final IVersionedSerializer<Data> noop_data_serializer = new IVersionedSerializer<Data>()
    {
        @Override
        public void serialize(Data t, DataOutputPlus out, int version) throws IOException {}
        @Override
        public Data deserialize(DataInputPlus in, int version) throws IOException { return Data.NOOP_DATA; }

        public long serializedSize(Data t, int version) { return 0; }
    };

    public static final IVersionedSerializer<ReadReply> replySerializer = new ReadDataSerializers.ReplySerializer<>(noop_data_serializer);

    public AccordInteropReadRepair(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> readScope, Timestamp executeAt, Mutation mutation)
    {
        super(to, topologies, txnId, readScope, executeAt);
        this.mutation = mutation;
    }

    public AccordInteropReadRepair(TxnId txnId, Participants<?> readScope, long executeAtEpoch, long waitForEpoch, Mutation mutation)
    {
        // TODO (review): remove followup read - Is there anything left to be done for this or can I remove it?
        super(txnId, readScope, executeAtEpoch, waitForEpoch);
        this.mutation = mutation;
    }

    @Override
    protected AsyncChain<Data> execute(SafeCommandStore safeStore, Timestamp executeAt, PartialTxn txn)
    {
        return AsyncChains.ofCallable(Verb.READ_REPAIR_REQ.stage.executor(), () -> {
                                          ReadRepairVerbHandler.instance.applyMutation(mutation);
                                          return Data.NOOP_DATA;
                                      });
    }

    @Override
    protected boolean canExecutePreApplied()
    {
        return true;
    }

    @Override
    protected boolean executeIfObsoleted()
    {
        return true;
    }

    @Override
    protected ReadOk constructReadOk(Ranges unavailable, Data data)
    {
        return new InteropReadRepairOk(unavailable, data);
    }

    @Override
    public MessageType type()
    {
        return AccordMessageType.INTEROP_READ_REPAIR_REQ;
    }

    private static class InteropReadRepairOk extends ReadOk
    {
        public InteropReadRepairOk(@Nullable Ranges unavailable, @Nullable Data data)
        {
            super(unavailable, data);
        }

        @Override
        public MessageType type()
        {
            return AccordMessageType.INTEROP_READ_REPAIR_RSP;
        }
    }
}
