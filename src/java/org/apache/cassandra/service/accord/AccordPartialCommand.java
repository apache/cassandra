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
import java.util.List;
import java.util.function.Consumer;

import accord.local.Listener;
import accord.local.PartialCommand;
import accord.local.Status;
import accord.primitives.Deps;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.txn.Txn;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.accord.async.AsyncContext;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;

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
        public PartialCommand deserializeBody(TxnId txnId, Txn txn, Timestamp executeAt, Status status, DataInputPlus in, Version version) throws IOException
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
        removedListeners.forEach(consumer);
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
            public PartialCommand.WithDeps deserializeBody(TxnId txnId, Txn txn, Timestamp executeAt, Status status, DataInputPlus in, Version version) throws IOException
            {
                Deps deps = CommandSerializers.deps.deserialize(in, version.msg_version);
                return new AccordPartialCommand.WithDeps(txnId, txn, executeAt, status, deps);
            }

            @Override
            public void serialize(PartialCommand.WithDeps command, DataOutputPlus out, Version version) throws IOException
            {
                super.serialize(command, out, version);
                CommandSerializers.deps.serialize(command.savedDeps(), out, version.msg_version);
            }

            @Override
            public int serializedSize(PartialCommand.WithDeps command, Version version)
            {
                int size = super.serializedSize(command, version) ;
                size += CommandSerializers.deps.serializedSize(command.savedDeps(), version.msg_version);
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
    }

    public static abstract class PartialCommandSerializer<T extends PartialCommand>
    {
        public enum Version
        {
            VERSION_0(0, MessagingService.current_version);
            final byte version;
            final int msg_version;

            Version(int version, int msg_version)
            {
                this.version = (byte) version;
                this.msg_version = msg_version;
            }

            public static final Version current = VERSION_0;

            public static Version fromByte(byte b)
            {
                switch (b)
                {
                    case 0:
                        return VERSION_0;
                    default:
                        throw new IllegalArgumentException();
                }
            }
        }

        public void serialize(T command, DataOutputPlus out, Version version) throws IOException
        {
            out.write(version.version);
            CommandSerializers.txnId.serialize(command.txnId(), out, version.msg_version);
            CommandSerializers.txn.serialize(command.txn(), out, version.msg_version);
            CommandSerializers.timestamp.serialize(command.executeAt(), out, version.msg_version);
            CommandSerializers.status.serialize(command.status(), out, version.msg_version);
        }

        public ByteBuffer serialize(T command)
        {
            Version version = Version.current;
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

        public Version deserializeVersion(DataInputPlus in) throws IOException
        {
            return Version.fromByte(in.readByte());
        }

        // check for cached command first, otherwise deserialize
        public T deserialize(AccordCommandStore commandStore, DataInputPlus in) throws IOException
        {
            Version version = deserializeVersion(in);
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version.msg_version);
            AsyncContext context = commandStore.getContext();
            T command = getCachedFull(txnId, context);
            if (command != null)
                return command;

            Txn txn = CommandSerializers.txn.deserialize(in, version.msg_version);
            Timestamp executeAt = CommandSerializers.timestamp.deserialize(in, version.msg_version);
            Status status = CommandSerializers.status.deserialize(in, version.msg_version);
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

        public int serializedSize(T command, Version version)
        {
            int size = TypeSizes.sizeof(version.version);
            size += CommandSerializers.txnId.serializedSize();
            size += CommandSerializers.txn.serializedSize(command.txn(), version.msg_version);
            size += CommandSerializers.timestamp.serializedSize(command.executeAt(), version.msg_version);
            size += CommandSerializers.status.serializedSize(command.status(), version.msg_version);
            return size;
        }

        public abstract T getCachedFull(TxnId txnId, AsyncContext context);
        public abstract void addToContext(T command, AsyncContext context);
        public abstract T deserializeBody(TxnId txnId, Txn txn, Timestamp executeAt, Status status, DataInputPlus in, Version version) throws IOException;

        /**
         * Determines if current modifications require updating command data duplicated elsewhere
         */
        public boolean needsUpdate(AccordCommand command)
        {
            return command.txn.hasModifications() || command.executeAt.hasModifications() || command.status.hasModifications();
        }
    }
}