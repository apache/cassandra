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

package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Objects;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Command;
import accord.local.CommandListener;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.primitives.TxnId;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.service.accord.async.AsyncContext;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.utils.ObjectSizes;

public abstract class ListenerProxy implements CommandListener, Comparable<ListenerProxy>
{
    private static final Logger logger = LoggerFactory.getLogger(ListenerProxy.class);

    public enum Kind { COMMAND, COMMANDS_FOR_KEY }

    public abstract Kind kind();
    public abstract ByteBuffer identifier();

    private ListenerProxy()
    {
    }

    @Override
    public int compareTo(ListenerProxy that)
    {
        return kind().compareTo(that.kind());
    }

    protected abstract long estimatedSizeOnHeap();

    static class CommandListenerProxy extends ListenerProxy
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new CommandListenerProxy(null));
        private final TxnId txnId;

        public CommandListenerProxy(TxnId txnId)
        {
            this.txnId = txnId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CommandListenerProxy that = (CommandListenerProxy) o;
            return txnId.equals(that.txnId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(txnId);
        }

        @Override
        public int compareTo(ListenerProxy that)
        {
            int cmp = super.compareTo(that);
            if (cmp != 0)
                return cmp;

            return this.txnId.compareTo(((CommandListenerProxy) that).txnId);
        }

        @Override
        public String toString()
        {
            return "CommandListenerProxy{" +
                   "txnId=" + txnId +
                   '}';
        }

        @Override
        public PreLoadContext listenerPreLoadContext(TxnId caller)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Kind kind()
        {
            return Kind.COMMAND;
        }

        @Override
        public ByteBuffer identifier()
        {
            ByteBuffer bytes = ByteBuffer.allocate(1 + CommandSerializers.txnId.serializedSize());
            ByteBufferAccessor.instance.putByte(bytes, 0, (byte) kind().ordinal());
            CommandSerializers.txnId.serialize(txnId, bytes, ByteBufferAccessor.instance, 1);
            return bytes;
        }

        @Override
        public void onChange(SafeCommandStore safeStore, Command c)
        {
            AccordCommand command = (AccordCommand) c;
            AccordCommandStore commandStore = (AccordCommandStore) safeStore;
            AsyncContext context = commandStore.getContext();
            PreLoadContext loadCtx = PreLoadContext.contextFor(ImmutableList.of(command.txnId(), txnId), Collections.emptyList());
            if (context.containsScopedItems(loadCtx))
            {
                // TODO (soon): determine if this can break anything by not waiting for the current operation to denormalize it's data
                //  the summary loader may default to commands in context, in case it wouldn't
                logger.trace("{}: synchronously updating listening command {}", c.txnId(), txnId);
                commandStore.command(txnId).onChange(safeStore, c);
            }
            else
            {
                TxnId callingTxnId = command.txnId();
                logger.trace("{}: asynchronously updating listening command {}", c.txnId(), txnId);
                commandStore.execute(loadCtx, reSafeStore -> {
                    Command caller = reSafeStore.command(callingTxnId);
                    commandStore.command(txnId).onChange(reSafeStore, caller);
                });
            }
        }

        @Override
        protected long estimatedSizeOnHeap()
        {
            return EMPTY_SIZE + AccordObjectSizes.timestamp(txnId);
        }
    }

    /**
     * These always need to be run in either the same task as the notifying command, or immediately afterwards, otherwise we
     * may use stale max timestamps for preaccept
     */
    static class CommandsForKeyListenerProxy extends ListenerProxy
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new CommandsForKeyListenerProxy(null));
        private final AccordKey.PartitionKey key;

        public CommandsForKeyListenerProxy(AccordKey.PartitionKey key)
        {
            this.key = key;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CommandsForKeyListenerProxy that = (CommandsForKeyListenerProxy) o;
            return key.equals(that.key);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(key);
        }

        @Override
        public int compareTo(ListenerProxy that)
        {
            int cmp = super.compareTo(that);
            if (cmp != 0)
                return cmp;

            return this.key.compareTo(((CommandsForKeyListenerProxy) that).key);
        }

        @Override
        public String toString()
        {
            return "CommandsForKeyListenerProxy{" +
                   "key=" + key +
                   '}';
        }

        @Override
        public PreLoadContext listenerPreLoadContext(TxnId caller)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Kind kind()
        {
            return Kind.COMMANDS_FOR_KEY;
        }

        @Override
        public ByteBuffer identifier()
        {
            ByteBuffer bytes = ByteBuffer.allocate((int) (1 + AccordKey.PartitionKey.serializer.serializedSize(key)));
            ByteBufferAccessor.instance.putByte(bytes, 0, (byte) kind().ordinal());
            AccordKey.PartitionKey.serializer.serialize(key, bytes, ByteBufferAccessor.instance, 1);
            return bytes;
        }

        @Override
        public void onChange(SafeCommandStore safeStore, Command c)
        {
            AccordCommand command = (AccordCommand) c;
            AccordCommandStore commandStore = (AccordCommandStore) safeStore;
            AsyncContext context = commandStore.getContext();
            PreLoadContext loadCtx = PreLoadContext.contextFor(ImmutableList.of(command.txnId()), ImmutableList.of(key));
            if (context.containsScopedItems(loadCtx))
            {
                logger.trace("{}: synchronously updating listening cfk {}", c.txnId(), key);
                commandStore.commandsForKey(key).onChange(safeStore, c);
            }
            else
            {
                TxnId callingTxnId = command.txnId();
                logger.trace("{}: asynchronously updating listening cfk {}", c.txnId(), key);
                commandStore.execute(loadCtx, reSafeStore -> {
                    Command caller = reSafeStore.command(callingTxnId);
                    commandStore.commandsForKey(key).onChange(reSafeStore, caller);
                });
            }
        }

        @Override
        protected long estimatedSizeOnHeap()
        {
            return EMPTY_SIZE + key.estimatedSizeOnHeap();
        }
    }

    public static <V> ListenerProxy deserialize(V src, ValueAccessor<V> accessor, int offset) throws IOException
    {
        int ordinal = accessor.getByte(src, offset);
        Kind kind = Kind.values()[ordinal];
        offset += 1;
        switch (kind)
        {
            case COMMAND:
                TxnId txnId = CommandSerializers.txnId.deserialize(src, accessor, offset);
                return new CommandListenerProxy(txnId);
            case COMMANDS_FOR_KEY:
                AccordKey.PartitionKey key = AccordKey.PartitionKey.serializer.deserialize(src, accessor, offset);
                return new CommandsForKeyListenerProxy(key);
            default:
                throw new IOException("Unknown kind ordinal " + ordinal);
        }
    }
}
