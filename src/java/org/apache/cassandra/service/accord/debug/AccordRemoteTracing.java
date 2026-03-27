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

package org.apache.cassandra.service.accord.debug;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import accord.api.Tracing;
import accord.local.CommandStore;
import accord.primitives.TxnId;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.AccordTimeService;
import org.apache.cassandra.service.accord.debug.AccordTracing.TxnEvent;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;

public class AccordRemoteTracing implements Tracing
{
    public static class AccordRemoteTrace {}
    public static final class AccordTraceOut extends AccordRemoteTrace
    {
        final TxnId txnId;
        final long idMicros;
        final long receivedAtMicros;
        final long nanosSinceReceived;
        final int commandStoreId;
        final String message;

        AccordTraceOut(TxnId txnId, long idMicros, long receivedAtMicros, long nanosSinceReceived, int commandStoreId, String message)
        {
            this.txnId = txnId;
            this.idMicros = idMicros;
            this.receivedAtMicros = receivedAtMicros;
            this.nanosSinceReceived = nanosSinceReceived;
            this.commandStoreId = commandStoreId;
            this.message = message;
        }
    }

    public static final class AccordTraceIn extends AccordRemoteTrace
    {
        final TxnId txnId;
        final long idMicros;
        final long atNanos;
        final int commandStoreId;
        final String message;

        AccordTraceIn(TxnId txnId, long idMicros, long atNanos, int commandStoreId, String message)
        {
            this.txnId = txnId;
            this.idMicros = idMicros;
            this.atNanos = atNanos;
            this.commandStoreId = commandStoreId;
            this.message = message;
        }
    }

    public static final class AccordTracingIn implements Tracing
    {
        final TxnId txnId;
        final long idMicros;
        final long onWireAtNanos;
        final List<AccordTraceIn> messages;

        public AccordTracingIn(TxnId txnId, long idMicros, long onWireAtNanos, List<AccordTraceIn> messages)
        {
            this.txnId = txnId;
            this.idMicros = idMicros;
            this.onWireAtNanos = onWireAtNanos;
            this.messages = messages;
        }

        void report(NodeId from)
        {
            AccordTracing tracing = ((AccordAgent)AccordService.unsafeInstance().agent()).tracing();
            long offWireAtNanos = nanoTime();
            long onWireAtNanos = Math.min(offWireAtNanos - 1, this.onWireAtNanos);
            int fromId = from == null ? Integer.MAX_VALUE : from.id();
            for (AccordTraceIn message : messages)
                tracing.report(message, fromId);
            tracing.report(new AccordTraceIn(txnId, idMicros, onWireAtNanos, -1, "Reply on wire"), fromId);
            tracing.report(new AccordTraceIn(txnId, idMicros, offWireAtNanos, -1, "Reply off wire from " + from) , -1);
        }

        @Override
        public void trace(CommandStore commandStore, String message)
        {
            throw new UnsupportedOperationException();
        }
    }

    public static final class AccordTracingOut implements Tracing
    {
        final TxnId txnId;
        final long idMicros;
        final long receivedAtMicros;
        final long receivedAtNanos;
        final List<BufferedMessage> messages;

        AccordTracingOut(TxnId txnId, long idMicros, long receivedAtMicros, long receivedAtNanos, List<BufferedMessage> messages)
        {
            this.txnId = txnId;
            this.idMicros = idMicros;
            this.receivedAtMicros = receivedAtMicros;
            this.receivedAtNanos = receivedAtNanos;
            this.messages = messages;
        }

        @Override
        public void trace(CommandStore commandStore, String message)
        {
            if (message.length() > 100)
                message = message.substring(0, 100);
            messages.add(new BufferedMessage(nanoTime(), commandStore == null ? -1 : commandStore.id(), message));
        }
    }

    static class BufferedMessage
    {
        final long atNanos;
        final int commandStoreId;
        final String message;

        BufferedMessage(long atNanos, int commandStoreId, String message)
        {
            this.atNanos = atNanos;
            this.commandStoreId = commandStoreId;
            this.message = message;
        }
    }

    final TxnId txnId;
    final long idMicros;
    private InetAddressAndPort replyTo;
    final long receivedAtMicros = AccordTimeService.nowMicros();
    final long receivedAtNanos = nanoTime();
    List<BufferedMessage> messages = new ArrayList<>();

    private AccordRemoteTracing(TxnId txnId, long idMicros)
    {
        this.txnId = txnId;
        this.idMicros = idMicros;
    }

    public void setReplyTo(InetAddressAndPort from)
    {
        replyTo = from;
    }

    @Override
    public synchronized void trace(CommandStore commandStore, String message)
    {
        long atNanos = nanoTime();
        int commandStoreId = commandStore == null ? -1 : commandStore.id();
        // TODO (expected): make this configurable
        if (message.length() > 100)
            message = message.substring(0, 100);

        if (messages == null)
        {
            long nanosSinceReceived = atNanos - receivedAtNanos;
            Message<?> reply = Message.out(Verb.ACCORD_REMOTE_TRACE, new AccordTraceOut(txnId, idMicros, receivedAtMicros, nanosSinceReceived, commandStoreId, message));
            MessagingService.instance().send(reply, replyTo);
        }
        else
        {
            messages.add(new BufferedMessage(atNanos, commandStoreId, message));
        }
    }

    @Override
    public synchronized Tracing send()
    {
        List<BufferedMessage> messages = this.messages;
        if (messages == null) messages = new ArrayList<>();
        else this.messages = null;
        return new AccordTracingOut(txnId, idMicros, receivedAtMicros, receivedAtNanos, messages);
    }

    public static final IVerbHandler<AccordRemoteTrace> traceMessageHandler = message -> {
        AccordTracing tracing = ((AccordAgent)AccordService.unsafeInstance().agent()).tracing();
        AccordTraceIn trace = (AccordTraceIn) message.payload;
        NodeId nodeId = ClusterMetadata.current().directory.peerId(message.from());
        tracing.report(trace, nodeId == null ? Integer.MAX_VALUE : nodeId.id());
    };

    static final int REPLY_FLAG = 1;
    public static final IVersionedSerializer<Tracing> tracingSerializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(Tracing t, DataOutputPlus out, int version) throws IOException
        {
            int flags = 0;
            if (t.getClass() == TxnEvent.class)
            {
                TxnEvent event = (TxnEvent) t;
                out.writeUnsignedVInt32(flags);
                CommandSerializers.txnId.serialize(event.txnId(), out);
                out.writeLong(event.idMicros);
            }
            else
            {
                AccordTracingOut tracing = (AccordTracingOut) t;
                flags |= REPLY_FLAG;
                out.writeUnsignedVInt32(flags);
                CommandSerializers.txnId.serialize(tracing.txnId, out);
                out.writeLong(tracing.idMicros);
                out.writeLong(tracing.receivedAtMicros);
                out.writeLong(nanoTime() - tracing.receivedAtNanos);
                out.writeUnsignedVInt32(tracing.messages.size());
                for (BufferedMessage message : tracing.messages)
                {
                    out.writeUnsignedVInt(message.atNanos - tracing.receivedAtNanos);
                    out.writeUnsignedVInt32(1 + message.commandStoreId);
                    out.writeUTF(message.message);
                }
            }
        }

        @Override
        public Tracing deserialize(DataInputPlus in, int version) throws IOException
        {
            int flags = in.readUnsignedVInt32();
            TxnId txnId = CommandSerializers.txnId.deserialize(in);
            long idMicros = in.readLong();
            if ((flags & REPLY_FLAG) == 0)
                return new AccordRemoteTracing(txnId, idMicros);

            long remoteReceivedAtMicros = in.readLong();
            long remoteReceivedAtNanos = preciseTime.translate().fromMicrosSinceEpoch(remoteReceivedAtMicros);
            long onWireAtNanos = remoteReceivedAtNanos + in.readLong();
            int messageCount = in.readUnsignedVInt32();
            List<AccordTraceIn> messages = new ArrayList<>(messageCount);
            for (int i = 0 ; i < messageCount ; ++i)
            {
                long atNanos = remoteReceivedAtNanos + in.readUnsignedVInt();
                int commandStoreId = in.readUnsignedVInt32() - 1;
                String message = in.readUTF();
                messages.add(new AccordTraceIn(txnId, idMicros, atNanos, commandStoreId, message));
            }
            return new AccordTracingIn(txnId, idMicros, onWireAtNanos, messages);
        }

        @Override
        public long serializedSize(Tracing t, int version)
        {
            int flags = 0;
            if (t.getClass() == TxnEvent.class)
            {
                TxnEvent event = (TxnEvent) t;
                long size = TypeSizes.sizeofUnsignedVInt(flags);
                size += CommandSerializers.txnId.serializedSize(event.txnId());
                size += TypeSizes.LONG_SIZE;
                return size;
            }
            else
            {
                AccordTracingOut tracing = (AccordTracingOut) t;
                flags |= REPLY_FLAG;
                long size = TypeSizes.sizeofUnsignedVInt(flags);
                size += CommandSerializers.txnId.serializedSize(tracing.txnId);
                size += 3 * TypeSizes.LONG_SIZE;
                size += TypeSizes.sizeofUnsignedVInt(tracing.messages.size());
                for (BufferedMessage message : tracing.messages)
                {
                    size += TypeSizes.sizeofUnsignedVInt(message.atNanos - tracing.receivedAtNanos);
                    size += TypeSizes.sizeofUnsignedVInt(1 + message.commandStoreId);
                    size += TypeSizes.sizeof(message.message);
                }
                return size;
            }
        }
    };

    public static final IVersionedSerializer<AccordRemoteTrace> traceSerializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(AccordRemoteTrace t, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(0);
            AccordTraceOut trace = (AccordTraceOut) t;
            CommandSerializers.txnId.serialize(trace.txnId, out);
            out.writeUnsignedVInt(trace.idMicros);
            out.writeUnsignedVInt(trace.receivedAtMicros);
            out.writeUnsignedVInt(trace.nanosSinceReceived);
            out.writeUnsignedVInt32(1 + trace.commandStoreId);
            out.writeUTF(trace.message);
        }

        @Override
        public AccordRemoteTrace deserialize(DataInputPlus in, int version) throws IOException
        {
            in.readUnsignedVInt32();
            TxnId txnId = CommandSerializers.txnId.deserialize(in);
            long idMicros = in.readUnsignedVInt();
            long receivedAtMicros = in.readUnsignedVInt();
            long nanosSinceReceived = in.readUnsignedVInt();
            long atNanos = nanosSinceReceived + preciseTime.translate().fromMicrosSinceEpoch(receivedAtMicros);
            int commandStoreId = 1 + in.readUnsignedVInt32();
            String message = in.readUTF();
            return new AccordTraceIn(txnId, idMicros, atNanos, commandStoreId, message);
        }

        @Override
        public long serializedSize(AccordRemoteTrace t, int version)
        {
            AccordTraceOut trace = (AccordTraceOut) t;
            long size = TypeSizes.sizeofUnsignedVInt(0);
            size += CommandSerializers.txnId.serializedSize(trace.txnId);
            size += TypeSizes.sizeofUnsignedVInt(trace.idMicros);
            size += TypeSizes.sizeofUnsignedVInt(trace.receivedAtMicros);
            size += TypeSizes.sizeofUnsignedVInt(trace.nanosSinceReceived);
            size += TypeSizes.sizeofUnsignedVInt(1 + trace.commandStoreId);
            size += TypeSizes.sizeof(trace.message);
            return size;
        }
    };

    public static void traceOnWire(Message.Header out, InetAddressAndPort to)
    {
        Object obj = out.params().get(ParamType.ACCORD_TRACING);
        if (obj == null)
            return;

        Tracing tracing = (Tracing) obj;
        if (tracing instanceof TxnEvent)
        {
            NodeId id = ClusterMetadata.current().directory.peerId(to);
            tracing.trace(null, "Request on wire to %s", id);
        }
    }

    public static void traceOffWire(Message.Header in)
    {
        Object obj = in.params().get(ParamType.ACCORD_TRACING);
        if (obj == null)
            return;

        if (obj instanceof AccordRemoteTracing)
        {
            AccordRemoteTracing tracing = (AccordRemoteTracing) obj;
            tracing.setReplyTo(in.from);
            tracing.trace(null, "Request off wire");
        }
        else
        {
            AccordTracingIn tracing = (AccordTracingIn) obj;
            NodeId id = ClusterMetadata.current().directory.peerId(in.from);
            tracing.report(id);
        }
    }
}
