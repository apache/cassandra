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
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;

import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandsForKey;
import accord.local.Status;
import accord.txn.Dependencies;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.service.accord.async.AsyncContext;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.store.StoredNavigableMap;
import org.apache.cassandra.service.accord.store.StoredValue;
import org.apache.cassandra.utils.ObjectSizes;

public class AccordCommandsForKey extends CommandsForKey implements AccordStateCache.AccordState<PartitionKey, AccordCommandsForKey>
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new AccordCommandsForKey(null, null));

    public enum SeriesKind {
        UNCOMMITTED(Command::txnId),
        COMMITTED_BY_ID(Command::txnId),
        COMMITTED_BY_EXECUTE_AT(Command::executeAt);

        private final Function<Command, Timestamp> getTimestamp;

        SeriesKind(Function<Command, Timestamp> timestampFunction)
        {
            this.getTimestamp = timestampFunction;
        }
    }

    /**
     * serializes:
         txn_id
         txn
         txn is_write
         status <- to support deps
         execute_at
         deps (txn_ids only)
     */
    public static class Summary
    {
        public static ByteBuffer serialize(AccordCommand command)
        {
            AccordCommand.SummaryVersion version = AccordCommand.SummaryVersion.current;
            int size = serializedSize(command);
            try (DataOutputBuffer out = new DataOutputBuffer(size))
            {
                out.write(version.version);
                CommandSerializers.txnId.serialize(command.txnId(), out, version.msg_version);
                CommandSerializers.txn.serialize(command.txn(), out, version.msg_version);
                out.write(command.status().ordinal());
                if (command.hasBeen(Status.Committed))
                    CommandSerializers.timestamp.serialize(command.executeAt(), out, version.msg_version);
                Dependencies deps = command.savedDeps();
                out.writeInt(deps.size());
                for (Map.Entry<TxnId, Txn> entry : deps)
                    CommandSerializers.txnId.serialize(entry.getKey(), out, version.msg_version);
                return out.buffer(false);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public static AccordCommand deserialize(AccordCommandStore commandStore, ByteBuffer bytes)
        {
            try (DataInputBuffer in = new DataInputBuffer(bytes, true))
            {
                AccordCommand.SummaryVersion version = AccordCommand.SummaryVersion.fromByte(in.readByte());
                TxnId txnId = CommandSerializers.txnId.deserialize(in, version.msg_version);
                AsyncContext context = commandStore.getContext();
                AccordCommand command = context.command(txnId);
                if (command != null)
                    return command;

                command = commandStore.commandCache().getOrCreate(txnId);
                context.addCommand(command);
                if (command.isLoaded())
                    return command;

                command.txn.load(CommandSerializers.txn.deserialize(in, version.msg_version));
                command.status.load(Status.values()[in.readByte()]);
                if (command.hasBeen(Status.Committed))
                    command.executeAt.load(CommandSerializers.timestamp.deserialize(in, version.msg_version));

                TreeMap<TxnId, Txn> depsMap = new TreeMap<>();
                int numDeps = in.readInt();
                for (int i=0; i<numDeps; i++)
                    depsMap.put(CommandSerializers.txnId.deserialize(in, version.msg_version), null);
                command.deps.load(new Dependencies(depsMap));

                return command;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public static int serializedSize(AccordCommand command)
        {
            AccordCommand.SummaryVersion version = AccordCommand.SummaryVersion.current;
            int size = TypeSizes.sizeof(version.version);
            size += CommandSerializers.txnId.serializedSize();
            size += TypeSizes.INT_SIZE; // txn size
            size += TypeSizes.sizeof((byte) command.status.get().ordinal());
            size += TypeSizes.BOOL_SIZE;
            if (command.hasBeen(Status.Committed))
                size += CommandSerializers.timestamp.serializedSize();
            int numDeps = command.deps.get().size();
            size += TypeSizes.sizeof(numDeps);
            size += numDeps * CommandSerializers.txnId.serializedSize();
            return size;
        }
    }

    public class Series implements CommandTimeseries
    {
        public final SeriesKind kind;
        public final StoredNavigableMap<Timestamp, ByteBuffer> map = new StoredNavigableMap<>();

        public Series(SeriesKind kind)
        {
            this.kind = kind;
        }

        @Override
        public Command get(Timestamp timestamp)
        {
            ByteBuffer bytes = map.getView().get(timestamp);
            return Summary.deserialize(commandStore, bytes);
        }

        @Override
        public void add(Timestamp timestamp, Command command)
        {
            map.blindPut(timestamp, Summary.serialize((AccordCommand) command));
        }

        @Override
        public void remove(Timestamp timestamp)
        {
            map.blindRemove(timestamp);
        }

        private Stream<Command> idsToCommands(Collection<ByteBuffer> blobs)
        {
            return blobs.stream().map(blob -> Summary.deserialize(commandStore, blob));
        }

        @Override
        public boolean isEmpty()
        {
            return map.getView().isEmpty();
        }

        @Override
        public Stream<Command> before(Timestamp timestamp)
        {
            return idsToCommands(map.getView().headMap(timestamp, false).values());
        }

        @Override
        public Stream<Command> after(Timestamp timestamp)
        {
            return idsToCommands(map.getView().tailMap(timestamp, false).values());
        }

        @Override
        public Stream<Command> between(Timestamp min, Timestamp max)
        {
            return idsToCommands(map.getView().subMap(min, true, max, true).values());
        }

        @Override
        public Stream<Command> all()
        {
            return idsToCommands(map.getView().values());
        }
    }

    private final AccordCommandStore commandStore;
    private final PartitionKey key;
    public final StoredValue<Timestamp> maxTimestamp = new StoredValue<>();
    public final Series uncommitted = new Series(SeriesKind.UNCOMMITTED);
    public final Series committedById = new Series(SeriesKind.COMMITTED_BY_ID);
    public final Series committedByExecuteAt = new Series(SeriesKind.COMMITTED_BY_EXECUTE_AT);

    public AccordCommandsForKey(AccordCommandStore commandStore, PartitionKey key)
    {
        this.commandStore = commandStore;
        this.key = key;
    }

    public AccordCommandsForKey initialize()
    {
        maxTimestamp.set(Timestamp.NONE);
        uncommitted.map.load(new TreeMap<>());
        committedById.map.load(new TreeMap<>());
        committedByExecuteAt.map.load(new TreeMap<>());
        return this;
    }

    public boolean hasModifications()
    {
        return maxTimestamp.hasModifications()
               || uncommitted.map.hasModifications()
               || committedById.map.hasModifications()
               || committedByExecuteAt.map.hasModifications();
    }

    public boolean isLoaded()
    {
        return maxTimestamp.isLoaded()
               && uncommitted.map.isLoaded()
               && committedById.map.isLoaded()
               && committedByExecuteAt.map.isLoaded();
    }

    public CommandStore commandStore()
    {
        return commandStore;
    }

    @Override
    public AccordStateCache.Node<PartitionKey, AccordCommandsForKey> createNode()
    {
        return new AccordStateCache.Node<>(this)
        {
            @Override
            long sizeInBytes(AccordCommandsForKey value)
            {
                return unsharedSizeOnHeap();
            }
        };
    }

    @Override
    public PartitionKey key()
    {
        return key;
    }

    private long unsharedSizeOnHeap()
    {
        // FIXME (metadata): calculate
        return EMPTY_SIZE + 400;
    }

    @Override
    public CommandTimeseries uncommitted()
    {
        return uncommitted;
    }

    @Override
    public CommandTimeseries committedById()
    {
        return committedById;
    }

    @Override
    public CommandTimeseries committedByExecuteAt()
    {
        return committedByExecuteAt;
    }

    @Override
    public Timestamp max()
    {
        return maxTimestamp.get();
    }

    @Override
    public void updateMax(Timestamp timestamp)
    {
        if (maxTimestamp.get().compareTo(timestamp) >= 0)
            return;
        maxTimestamp.set(timestamp);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccordCommandsForKey that = (AccordCommandsForKey) o;
        return commandStore == that.commandStore
               && key.equals(that.key)
               && maxTimestamp.equals(that.maxTimestamp)
               && uncommitted.map.equals(that.uncommitted.map)
               && committedById.map.equals(that.committedById.map)
               && committedByExecuteAt.map.equals(that.committedByExecuteAt.map);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(commandStore, key, maxTimestamp, uncommitted, committedById, committedByExecuteAt);
    }

    @Override
    public String toString()
    {
        return "AccordCommandsForKey{" +
               "key=" + key +
               ", maxTs=" + max() +
               ", uncommitted=" + uncommitted.map +
               ", committedById=" + committedById.map +
               ", committedByExecuteAt=" + committedByExecuteAt.map +
               '}';
    }
}
