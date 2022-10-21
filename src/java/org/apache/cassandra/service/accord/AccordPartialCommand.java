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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import accord.local.Listener;
import accord.local.PartialCommand;
import accord.local.Status;
import accord.primitives.Deps;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.txn.Txn;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.async.AsyncContext;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;

import static org.apache.cassandra.service.accord.serializers.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.service.accord.serializers.NullableSerializer.serializeNullable;
import static org.apache.cassandra.service.accord.serializers.NullableSerializer.serializedSizeNullable;

public class AccordPartialCommand implements PartialCommand
{
    public static final PartialCommandSerializer<PartialCommand> serializer = new PartialCommandSerializer<PartialCommand>()
    {
        @Override
        public PartialCommand getCachedFull(TxnId txnId, AsyncContext context)
        {
            return context.commands.get(txnId);
        }

        @Override
        public void addToContext(PartialCommand command, AsyncContext context)
        {
            context.commands.addPartialCommand((AccordPartialCommand) command);
        }

        @Override
        public PartialCommand deserializeBody(TxnId txnId, Txn txn, Timestamp executeAt, Status status, DataInputPlus in, AccordSerializerVersion version) throws IOException
        {
            return new AccordPartialCommand(txnId, txn, executeAt, status);
        }
    };

    private final TxnId txnId;
    private final Txn txn;
    private final Timestamp executeAt;
    private final Status status;
    private List<Listener> removedListeners = null;

    public AccordPartialCommand(TxnId txnId, Txn txn, Timestamp executeAt, Status status)
    {
        this.txnId = txnId;
        this.txn = txn;
        this.executeAt = executeAt;
        this.status = status;
    }

    @Override
    public TxnId txnId()
    {
        return txnId;
    }

    @Override
    public Txn txn()
    {
        return txn;
    }

    @Override
    public Timestamp executeAt()
    {
        return executeAt;
    }

    @Override
    public Status status()
    {
        return status;
    }

    @Override
    public void removeListener(Listener listener)
    {
        if (removedListeners == null)
            removedListeners = new ArrayList<>();
        removedListeners.add(listener);
    }

    public boolean hasRemovedListeners()
    {
        return removedListeners != null && !removedListeners.isEmpty();
    }

    public void forEachRemovedListener(Consumer<Listener> consumer)
    {
        if (removedListeners != null)
            removedListeners.forEach(consumer);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccordPartialCommand that = (AccordPartialCommand) o;
        return Objects.equals(txnId, that.txnId) && Objects.equals(txn, that.txn) && Objects.equals(executeAt, that.executeAt) && status == that.status && Objects.equals(removedListeners, that.removedListeners);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(txnId, txn, executeAt, status, removedListeners);
    }

    @Override
    public String toString()
    {
        return "AccordPartialCommand{" +
               "txnId=" + txnId +
               ", txn=" + txn +
               ", executeAt=" + executeAt +
               ", status=" + status +
               ", removedListeners=" + removedListeners +
               '}';
    }

    public static class WithDeps extends AccordPartialCommand implements PartialCommand.WithDeps
    {
        public static final PartialCommandSerializer<PartialCommand.WithDeps> serializer = new PartialCommandSerializer<PartialCommand.WithDeps>()
        {
            @Override
            public PartialCommand.WithDeps getCachedFull(TxnId txnId, AsyncContext context)
            {
                return context.commands.get(txnId);
            }

            @Override
            public void addToContext(PartialCommand.WithDeps command, AsyncContext context)
            {
                context.commands.addPartialCommand((AccordPartialCommand) command);
            }

            @Override
            public PartialCommand.WithDeps deserializeBody(TxnId txnId, Txn txn, Timestamp executeAt, Status status, DataInputPlus in, AccordSerializerVersion version) throws IOException
            {
                Deps deps = deserializeNullable(in, version.msgVersion, CommandSerializers.deps);
                return new AccordPartialCommand.WithDeps(txnId, txn, executeAt, status, deps);
            }

            @Override
            public void serialize(PartialCommand.WithDeps command, DataOutputPlus out, AccordSerializerVersion version) throws IOException
            {
                super.serialize(command, out, version);
                serializeNullable(command.savedDeps(), out, version.msgVersion, CommandSerializers.deps);
            }

            @Override
            public int serializedSize(PartialCommand.WithDeps command, AccordSerializerVersion version)
            {
                int size = super.serializedSize(command, version) ;
                size += serializedSizeNullable(command.savedDeps(), version.msgVersion, CommandSerializers.deps);
                return size;
            }

            @Override
            public boolean needsUpdate(AccordCommand command)
            {
                return super.needsUpdate(command) || command.deps.hasModifications();
            }
        };

        private final Deps deps;

        public WithDeps(TxnId txnId, Txn txn, Timestamp executeAt, Status status, Deps deps)
        {
            super(txnId, txn, executeAt, status);
            this.deps = deps;
        }

        @Override
        public Deps savedDeps()
        {
            return deps;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AccordPartialCommand.WithDeps withDeps = (AccordPartialCommand.WithDeps) o;
            return super.equals(o) && Objects.equals(deps, withDeps.deps);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(super.hashCode(), deps);
        }
    }

    public static abstract class PartialCommandSerializer<T extends PartialCommand>
    {
        public void serialize(T command, DataOutputPlus out, AccordSerializerVersion version) throws IOException
        {
            AccordSerializerVersion.serializer.serialize(version, out);
            CommandSerializers.txnId.serialize(command.txnId(), out, version.msgVersion);
            CommandSerializers.status.serialize(command.status(), out, version.msgVersion);
            serializeNullable(command.txn(), out, version.msgVersion, CommandSerializers.txn);
            serializeNullable(command.executeAt(), out, version.msgVersion, CommandSerializers.timestamp);
        }

        public ByteBuffer serialize(T command)
        {
            AccordSerializerVersion version = AccordSerializerVersion.CURRENT;
            int size = serializedSize(command, version);
            try (DataOutputBuffer out = new DataOutputBuffer(size))
            {
                serialize(command, out, version);
                return out.buffer(false);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public AccordSerializerVersion deserializeVersion(DataInputPlus in) throws IOException
        {
            return AccordSerializerVersion.serializer.deserialize(in);
        }

        // check for cached command first, otherwise deserialize
        public T deserialize(AccordCommandStore commandStore, DataInputPlus in) throws IOException
        {
            AccordSerializerVersion version = deserializeVersion(in);
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version.msgVersion);
            AsyncContext context = commandStore.getContext();
            T command = getCachedFull(txnId, context);
            if (command != null)
                return command;

            Status status = CommandSerializers.status.deserialize(in, version.msgVersion);
            Txn txn = deserializeNullable(in, version.msgVersion, CommandSerializers.txn);
            Timestamp executeAt = deserializeNullable(in, version.msgVersion, CommandSerializers.timestamp);
            T partial = deserializeBody(txnId, txn, executeAt, status, in, version);
            addToContext(partial, context);
            return partial;
        }

        public T deserialize(AccordCommandStore commandStore, ByteBuffer bytes)
        {
            try (DataInputBuffer in = new DataInputBuffer(bytes, true))
            {
                return deserialize(commandStore, in);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public int serializedSize(T command, AccordSerializerVersion version)
        {
            int size = Math.toIntExact(AccordSerializerVersion.serializer.serializedSize(version));
            size += CommandSerializers.txnId.serializedSize();
            size += CommandSerializers.status.serializedSize(command.status(), version.msgVersion);
            size += serializedSizeNullable(command.txn(), version.msgVersion, CommandSerializers.txn);
            size += serializedSizeNullable(command.executeAt(), version.msgVersion, CommandSerializers.timestamp);
            return size;
        }

        public abstract T getCachedFull(TxnId txnId, AsyncContext context);
        public abstract void addToContext(T command, AsyncContext context);
        public abstract T deserializeBody(TxnId txnId, Txn txn, Timestamp executeAt, Status status, DataInputPlus in, AccordSerializerVersion version) throws IOException;

        /**
         * Determines if current modifications require updating command data duplicated elsewhere
         */
        public boolean needsUpdate(AccordCommand command)
        {
            return command.txn.hasModifications() || command.executeAt.hasModifications() || command.status.hasModifications();
        }
    }
}