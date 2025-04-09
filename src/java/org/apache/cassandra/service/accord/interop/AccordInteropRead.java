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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import accord.api.Data;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.messages.MessageType;
import accord.messages.ReadData;
import accord.primitives.AbstractRanges;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.service.accord.AccordMessageSink.AccordMessageType;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.IVersionedSerializer;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.serializers.ReadDataSerializer;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.service.accord.txn.TxnNamedRead;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.utils.Pair;

import static accord.primitives.SaveStatus.PreApplied;
import static accord.primitives.SaveStatus.ReadyToExecute;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class AccordInteropRead extends ReadData
{

    public static final IVersionedSerializer<AccordInteropRead> requestSerializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(AccordInteropRead read, DataOutputPlus out, Version version) throws IOException
        {
            CommandSerializers.txnId.serialize(read.txnId, out);
            KeySerializers.participants.serialize(read.scope, out);
            out.writeUnsignedVInt(read.executeAtEpoch);
            ReadCommand.serializer.serialize(read.command, out, version.messageVersion());
        }

        @Override
        public AccordInteropRead deserialize(DataInputPlus in, Version version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in);
            Participants<?> scope = KeySerializers.participants.deserialize(in);
            long executeAtEpoch = in.readUnsignedVInt();
            ReadCommand command = ReadCommand.serializer.deserialize(in, version.messageVersion());
            return new AccordInteropRead(txnId, scope, executeAtEpoch, command);
        }

        @Override
        public long serializedSize(AccordInteropRead read, Version version)
        {
            return CommandSerializers.txnId.serializedSize(read.txnId)
                   + KeySerializers.participants.serializedSize(read.scope)
                   + TypeSizes.sizeofUnsignedVInt(read.executeAtEpoch)
                   + ReadCommand.serializer.serializedSize(read.command, version.messageVersion());
        }
    };

    public static final IVersionedSerializer<ReadReply> replySerializer = new ReadDataSerializer.ReplySerializer<>(LocalReadData.serializer);

    private static class LocalReadData implements Data
    {
        private static final Comparator<Pair<TokenKey, ReadResponse>> RESPONSE_COMPARATOR = Comparator.comparing(Pair::left);

        static final IVersionedSerializer<LocalReadData> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(LocalReadData data, DataOutputPlus out, Version version) throws IOException
            {
                data.ensureRemoteResponse();
                ReadResponse.serializer.serialize(data.remoteResponse, out, version.messageVersion());
            }

            @Override
            public LocalReadData deserialize(DataInputPlus in, Version version) throws IOException
            {
                return new LocalReadData(ReadResponse.serializer.deserialize(in, version.messageVersion()));
            }

            @Override
            public long serializedSize(LocalReadData data, Version version)
            {
                data.ensureRemoteResponse();
                return ReadResponse.serializer.serializedSize(data.remoteResponse, version.messageVersion());
            }
        };

        // Will be null at coordinator
        List<Pair<TokenKey, ReadResponse>> localResponses;
        // Will be null at coordinator
        final ReadCommand readCommand;
        // Will be not null at coordinator, but null at the node creating the response until it serialized
        ReadResponse remoteResponse;

        public LocalReadData(@Nullable TokenKey start, @Nonnull ReadResponse response, @Nonnull ReadCommand readCommand)
        {
            checkNotNull(response, "response is null");
            checkNotNull(readCommand, "readCommand is null");
            localResponses = ImmutableList.of(Pair.create(start, response));
            this.readCommand = readCommand;
            this.remoteResponse = null;
        }

        private LocalReadData(@Nonnull List<Pair<TokenKey, ReadResponse>> responses, @Nonnull ReadCommand readCommand)
        {
            checkNotNull(readCommand, "readCommand is null");
            this.localResponses = responses;
            this.readCommand = readCommand;
            this.remoteResponse = null;
        }

        public LocalReadData(@Nonnull ReadResponse remoteResponse)
        {
            checkNotNull(remoteResponse);
            this.remoteResponse = remoteResponse;
            readCommand = null;
        }

        @Override
        public String toString()
        {
            if (localResponses != null)
               return "LocalReadData{" + localResponses + '}';
            else
                return "LocalReadData{" + remoteResponse + '}';
        }

        @Override
        public Data merge(Data data)
        {
            if (localResponses.isEmpty())
                return data;

            LocalReadData other = (LocalReadData)data;
            if (other.localResponses.isEmpty())
                return this;

            checkState(remoteResponse == null, "Already serialized");
            checkState(readCommand.isRangeRequest(), "Should only ever be a single partition");
            checkState(readCommand == other.readCommand, "Should share the same ReadCommand");
            if (localResponses.size() == 1)
            {
                List<Pair<TokenKey, ReadResponse>> merged = new ArrayList<>();
                merged.add(localResponses.get(0));
                localResponses = merged;
            }
            // TODO (required): this may be safe, but it is surprising and we should avoid it (e.g. by using immutable structure like btree)
            localResponses.addAll(other.localResponses);
            return this;
        }

        @Override
        public Data without(Ranges ranges)
        {
            List<Pair<TokenKey, ReadResponse>> copy = new ArrayList<>(localResponses.size());
            for (Pair<TokenKey, ReadResponse> r : localResponses)
            {
                if (!ranges.contains(r.left))
                    copy.add(r);
            }
            if (copy.size() == localResponses.size())
                return this;
            return new LocalReadData(localResponses, readCommand);
        }

        private void ensureRemoteResponse()
        {
            if (remoteResponse != null)
                return;
            // Range reads will be spread across command stores and need to be merged in token order
            List<Pair<TokenKey, ReadResponse>> responses = localResponses;
            if (responses.size() == 1)
            {
                remoteResponse = responses.get(0).right;
            }
            else
            {
                responses = new ArrayList(responses);
                Collections.sort(responses, RESPONSE_COMPARATOR);
                remoteResponse = ReadResponse.merge(Lists.transform(responses, Pair::right), readCommand);
            }
        }
    }

    static class ReadCallback extends AccordInteropReadCallback<ReadResponse>
    {
        public ReadCallback(Node.Id id, InetAddressAndPort endpoint, Message<?> message, RequestCallback<ReadResponse> wrapped, AccordInteropExecution interopExecution)
        {
            super(id, endpoint, message, wrapped, interopExecution);
        }

        @Override
        ReadResponse convertResponse(ReadOk ok)
        {
            return ((LocalReadData)ok.data).remoteResponse;
        }
    }

    private static final ExecuteOn EXECUTE_ON = new ExecuteOn(ReadyToExecute, PreApplied);

    protected final ReadCommand command;

    public AccordInteropRead(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> scope, long executeAtEpoch, ReadCommand command)
    {
        super(to, topologies, txnId, scope, null, null, executeAtEpoch);
        this.command = command;
    }

    public AccordInteropRead(TxnId txnId, Participants<?> scope, long executeAtEpoch, ReadCommand command)
    {
        super(txnId, scope, null, null, executeAtEpoch);
        this.command = command;
    }

    @Override
    public ReadType kind()
    {
        return ReadType.readTxnData;
    }

    @Override
    protected AsyncChain<Data> beginRead(SafeCommandStore safeStore, Timestamp executeAt, PartialTxn txn, Participants<?> execute)
    {
        TxnRead txnRead = (TxnRead)txn.read();
        long nowInSeconds = TxnNamedRead.nowInSeconds(executeAt);
        if (!command.isRangeRequest())
        {
            SinglePartitionReadCommand readCommand = ((SinglePartitionReadCommand)command);
            TokenKey key = new TokenKey(readCommand.metadata().id, readCommand.partitionKey().getToken());
            if (!execute.contains(key))
                return AsyncChains.success(new LocalReadData(new ArrayList<>(), readCommand));

            ReadCommand submit = readCommand.withTransactionalSettings(TxnNamedRead.readsWithoutReconciliation(txnRead.cassandraConsistencyLevel()), nowInSeconds);
            return AsyncChains.ofCallable(Stage.READ.executor(), () -> new LocalReadData(key, ReadCommandVerbHandler.instance.doRead(submit, false), command));
        }

        // This path can have a subrange we have never seen before provided by short read protection or read repair so we need to
        // calculate the intersection with this instance of the command store and the actual command if it is not empty we
        // will need to execute it
        TokenRange commandRange = TxnNamedRead.boundsAsAccordRange(command.dataRange().keyRange(), command.metadata().id);
        List<AsyncChain<Data>> chains = new ArrayList<>(execute.size());
        for (Range r : (AbstractRanges)execute)
        {
            Range intersection = commandRange.intersection(r);
            if (intersection == null)
                continue;
            ReadCommand submit = TxnNamedRead.commandForSubrange((PartitionRangeReadCommand) command, intersection, txnRead.cassandraConsistencyLevel(), nowInSeconds);
            TokenKey routingKey = ((TokenRange)r).start();
            chains.add(AsyncChains.ofCallable(Stage.READ.executor(), () -> new LocalReadData(routingKey, ReadCommandVerbHandler.instance.doRead(submit, false), command)));
        }

        if (chains.isEmpty())
            return AsyncChains.success(new LocalReadData(new ArrayList<>(), command));

        return AsyncChains.reduce(chains, Data::merge);
    }

    @Override
    protected ExecuteOn executeOn()
    {
        return EXECUTE_ON;
    }

    @Override
    protected void reply(Ranges unavailable, Data data, long uniqueHlc)
    {
        reply(new InteropReadOk(unavailable, data, uniqueHlc), null);
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
        public InteropReadOk(@Nullable Ranges unavailable, @Nullable Data data, long uniqueHlc)
        {
            super(unavailable, data, uniqueHlc);
        }

        @Override
        public MessageType type()
        {
            return AccordMessageType.INTEROP_READ_RSP;
        }
    }
}
