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
import java.util.Objects;

import com.google.common.base.Preconditions;

import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Listener;
import accord.txn.TxnId;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;

public abstract class ListenerProxy implements Listener
{
    public enum Kind {COMMAND, COMMANDs_FOR_KEY}

    public abstract Kind kind();
    public abstract ByteBuffer identifier();

    final CommandStore commandStore;

    private ListenerProxy(CommandStore commandStore)
    {
        this.commandStore = commandStore;
    }

    static class CommandListenerProxy extends ListenerProxy
    {
        private final TxnId txnId;

        public CommandListenerProxy(CommandStore commandStore, TxnId txnId)
        {
            super(commandStore);
            Preconditions.checkArgument(txnId != null);
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
        public String toString()
        {
            return "CommandListenerProxy{" +
                   "txnId=" + txnId +
                   '}';
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
        public void onChange(Command command)
        {
            commandStore.command(txnId).onChange(command);
        }
    }

    static class CommandsForKeyListenerProxy extends ListenerProxy
    {
        private final AccordKey.PartitionKey key;

        public CommandsForKeyListenerProxy(CommandStore commandStore, AccordKey.PartitionKey key)
        {
            super(commandStore);
            Preconditions.checkArgument(key != null);
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
        public String toString()
        {
            return "CommandsForKeyListenerProxy{" +
                   "key=" + key +
                   '}';
        }

        @Override
        public Kind kind()
        {
            return Kind.COMMANDs_FOR_KEY;
        }

        @Override
        public ByteBuffer identifier()
        {
            ByteBuffer bytes = ByteBuffer.allocate(1 + CommandSerializers.txnId.serializedSize());
            ByteBufferAccessor.instance.putByte(bytes, 0, (byte) kind().ordinal());
            AccordKey.PartitionKey.serializer.serialize(key, bytes, ByteBufferAccessor.instance, 1);
            return bytes;
        }

        @Override
        public void onChange(Command command)
        {
            commandStore.commandsForKey(key).onChange(command);
        }
    }

    public static <V> ListenerProxy deserialize(CommandStore commandStore, V src, ValueAccessor<V> accessor, int offset) throws IOException
    {
        int ordinal = accessor.getByte(src, offset);
        Kind kind = Kind.values()[ordinal];
        offset += 1;
        switch (kind)
        {
            case COMMAND:
                TxnId txnId = CommandSerializers.txnId.deserialize(src, accessor, offset);
                return new CommandListenerProxy(commandStore, txnId);
            case COMMANDs_FOR_KEY:
                AccordKey.PartitionKey key = AccordKey.PartitionKey.serializer.deserialize(src, accessor, offset);
                return new CommandsForKeyListenerProxy(commandStore, key);
            default:
                throw new IOException("Unknown kind ordinal " + ordinal);
        }
    }
}
