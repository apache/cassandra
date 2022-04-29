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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;

import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandsForKey;
import accord.local.Status;
import accord.txn.Timestamp;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.service.accord.serializers.CommandSummaries;
import org.apache.cassandra.service.accord.store.StoredNavigableMap;
import org.apache.cassandra.service.accord.store.StoredValue;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.service.accord.AccordState.WriteOnly.applyMapChanges;

public class AccordCommandsForKey extends CommandsForKey implements AccordState<PartitionKey>
{
    private static final long EMPTY_SIZE = ObjectSizes.measureDeep(new AccordCommandsForKey(null, null));

    public static class WriteOnly extends AccordCommandsForKey implements AccordState.WriteOnly<PartitionKey, AccordCommandsForKey>
    {
        private Future<?> future = null;

        public WriteOnly(AccordCommandStore commandStore, PartitionKey key)
        {
            super(commandStore, key);
        }

        @Override
        public void future(Future<?> future)
        {
            Preconditions.checkArgument(this.future == null);
            this.future = future;

        }

        @Override
        public Future<?> future()
        {
            return future;
        }

        @Override
        public void applyChanges(AccordCommandsForKey instance)
        {
            applyMapChanges(this, instance, cfk -> cfk.uncommitted.map);
            applyMapChanges(this, instance, cfk -> cfk.committedById.map);
            applyMapChanges(this, instance, cfk -> cfk.committedByExecuteAt.map);
        }
    }

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
            if (bytes == null)
                return null;
            return CommandSummaries.commandsPerKey.deserialize(commandStore, bytes);
        }

        @Override
        public void add(Timestamp timestamp, Command command)
        {
            map.blindPut(timestamp, CommandSummaries.commandsPerKey.serialize((AccordCommand) command));
        }

        @Override
        public void remove(Timestamp timestamp)
        {
            map.blindRemove(timestamp);
        }

        private Stream<Command> idsToCommands(Collection<ByteBuffer> blobs)
        {
            return blobs.stream().map(blob -> CommandSummaries.commandsPerKey.deserialize(commandStore, blob));
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

    @Override
    public boolean hasModifications()
    {
        return maxTimestamp.hasModifications()
               || uncommitted.map.hasModifications()
               || committedById.map.hasModifications()
               || committedByExecuteAt.map.hasModifications();
    }

    @Override
    public void clearModifiedFlag()
    {
        maxTimestamp.clearModifiedFlag();
        uncommitted.map.clearModifiedFlag();
        committedById.map.clearModifiedFlag();
        committedByExecuteAt.map.clearModifiedFlag();
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
    public PartitionKey key()
    {
        return key;
    }

    @Override
    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE;
        size += maxTimestamp.estimatedSizeOnHeap(AccordObjectSizes::timestamp);
        size += uncommitted.map.estimatedSizeOnHeap(AccordObjectSizes::timestamp, ByteBufferUtil::estimatedSizeOnHeap);
        size += committedById.map.estimatedSizeOnHeap(AccordObjectSizes::timestamp, ByteBufferUtil::estimatedSizeOnHeap);
        size += committedByExecuteAt.map.estimatedSizeOnHeap(AccordObjectSizes::timestamp, ByteBufferUtil::estimatedSizeOnHeap);
        return size;
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

    public void updateSummaries(AccordCommand command)
    {
        if (command.status.get().hasBeen(Status.Committed))
        {
            if (!command.status.previous().hasBeen(Status.Committed))
                uncommitted.map.blindRemove(command.txnId());

            committedById.map.blindPut(command.txnId(), CommandSummaries.commandsPerKey.serialize(command));
            committedByExecuteAt.map.blindPut(command.executeAt(), CommandSummaries.commandsPerKey.serialize(command));
        }
        else
        {
            uncommitted.map.blindPut(command.txnId(), CommandSummaries.commandsPerKey.serialize(command));
        }
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
