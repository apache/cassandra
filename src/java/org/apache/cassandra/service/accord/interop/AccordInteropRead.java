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
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.service.accord.AccordMessageSink.AccordMessageType;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.serializers.ReadDataSerializers;
import org.apache.cassandra.service.accord.serializers.ReadDataSerializers.ReadDataSerializer;

public class AccordInteropRead extends AbstractExecute
{
    public static final IVersionedSerializer<AccordInteropRead> requestSerializer = new ReadDataSerializer<AccordInteropRead>()
    {
        @Override
        public void serialize(AccordInteropRead read, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(read.txnId, out, version);
            KeySerializers.participants.serialize(read.readScope, out, version);
            out.writeUnsignedVInt(read.waitForEpoch());
            out.writeUnsignedVInt(read.executeAtEpoch - read.waitForEpoch());
            SinglePartitionReadCommand.serializer.serialize(read.command, out, version);
        }

        @Override
        public AccordInteropRead deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version);
            Participants<?> readScope = KeySerializers.participants.deserialize(in, version);
            long waitForEpoch = in.readUnsignedVInt();
            long executeAtEpoch = in.readUnsignedVInt() + waitForEpoch;
            SinglePartitionReadCommand command = (SinglePartitionReadCommand) SinglePartitionReadCommand.serializer.deserialize(in, version);
            return new AccordInteropRead(txnId, readScope, waitForEpoch, executeAtEpoch, command);
        }

        @Override
        public long serializedSize(AccordInteropRead read, int version)
        {
            return CommandSerializers.txnId.serializedSize(read.txnId, version)
                   + KeySerializers.participants.serializedSize(read.readScope, version)
                   + TypeSizes.sizeofUnsignedVInt(read.waitForEpoch())
                   + TypeSizes.sizeofUnsignedVInt(read.executeAtEpoch - read.waitForEpoch())
                   + SinglePartitionReadCommand.serializer.serializedSize(read.command, version);
        }
    };

    public static final IVersionedSerializer<ReadReply> replySerializer = new ReadDataSerializers.ReplySerializer<>(LocalReadData.serializer);

    private static class LocalReadData implements Data
    {
        static final IVersionedSerializer<LocalReadData> serializer = new IVersionedSerializer<LocalReadData>()
        {
            @Override
            public void serialize(LocalReadData data, DataOutputPlus out, int version) throws IOException
            {
                ReadResponse.serializer.serialize(data.response, out, version);
            }

            @Override
            public LocalReadData deserialize(DataInputPlus in, int version) throws IOException
            {
                return new LocalReadData(ReadResponse.serializer.deserialize(in, version));
            }

            @Override
            public long serializedSize(LocalReadData data, int version)
            {
                return ReadResponse.serializer.serializedSize(data.response, version);
            }
        };

        final ReadResponse response;

        public LocalReadData(ReadResponse response)
        {
            this.response = response;
        }

        @Override
        public String toString()
        {
            return "LocalReadData{" + response + '}';
        }

        @Override
        public Data merge(Data data)
        {
            throw new IllegalStateException("Should only ever be a single partition");
        }
    }

    static class ReadCallback extends AccordInteropReadCallback<ReadResponse>
    {
        public ReadCallback(Node.Id id, InetAddressAndPort endpoint, Message<?> message, RequestCallback<ReadResponse> wrapped, MaximalCommitSender maximalCommitSender)
        {
            super(id, endpoint, message, wrapped, maximalCommitSender);
        }

        @Override
        ReadResponse convertResponse(ReadOk ok)
        {
            return ((LocalReadData) ok.data).response;
        }
    }

    private final SinglePartitionReadCommand command;

    public AccordInteropRead(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> readScope, Timestamp executeAt, SinglePartitionReadCommand command)
    {
        super(to, topologies, txnId, readScope, executeAt);
        this.command = command;
    }

    public AccordInteropRead(TxnId txnId, Participants<?> readScope, long executeAtEpoch, long waitForEpoch, SinglePartitionReadCommand command)
    {
        super(txnId, readScope, executeAtEpoch, waitForEpoch);
        this.command = command;
    }

    @Override
    protected AsyncChain<Data> execute(SafeCommandStore safeStore, Timestamp executeAt, PartialTxn txn, Ranges unavailable)
    {
        // TODO (required): subtract unavailable ranges, either from read or from response (or on coordinator)
        return AsyncChains.ofCallable(Stage.READ.executor(), () -> new LocalReadData(ReadCommandVerbHandler.instance.doRead(command, false)));
    }

    @Override
    protected boolean canExecutePreApplied()
    {
        return true;
    }

    @Override
    protected ReadOk constructReadOk(Ranges unavailable, Data data)
    {
        return new InteropReadOk(unavailable, data);
    }

    @Override
    public MessageType type()
    {
        return AccordMessageType.INTEROP_READ_REQ;
    }

    @Override
    public String toString()
    {
        return "AccordInteropRead{" +
               "txnId=" + txnId +
               "command=" + command +
               '}';
    }

    private static class InteropReadOk extends ReadOk
    {
        public InteropReadOk(@Nullable Ranges unavailable, @Nullable Data data)
        {
            super(unavailable, data);
        }

        @Override
        public MessageType type()
        {
            return AccordMessageType.INTEROP_READ_RSP;
        }
    }
}
