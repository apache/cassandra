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

import java.util.Collection;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;

import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandsForKey;
import accord.txn.Timestamp;
import accord.txn.TxnId;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
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

    public class Series implements CommandTimeseries
    {
        public final SeriesKind kind;
        public final StoredNavigableMap<Timestamp, TxnId> map = new StoredNavigableMap<>();

        public Series(SeriesKind kind)
        {
            this.kind = kind;
        }

        @Override
        public Command get(Timestamp timestamp)
        {
            TxnId txnId = map.getView().get(timestamp);
            return commandStore.command(txnId);  // FIXME: will need to lock these commands
        }

        @Override
        public void add(Timestamp timestamp, Command command)
        {
            map.blindPut(timestamp, command.txnId());
        }

        @Override
        public void remove(Timestamp timestamp)
        {
            map.blindRemove(timestamp);
        }

        private Stream<Command> idsToCommands(Collection<TxnId> ids)
        {
            return ids.stream().map(commandStore::command);  // FIXME: will need to lock these commands, unless they're read only...
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

    private final CommandStore commandStore;
    private final PartitionKey key;
    public final StoredValue<Timestamp> maxTimestamp = new StoredValue<>();
    public final Series uncommitted = new Series(SeriesKind.UNCOMMITTED);
    public final Series committedById = new Series(SeriesKind.COMMITTED_BY_ID);
    public final Series committedByExecuteAt = new Series(SeriesKind.COMMITTED_BY_EXECUTE_AT);

    public AccordCommandsForKey(CommandStore commandStore, PartitionKey key)
    {
        this.commandStore = commandStore;
        this.key = key;
    }

    public void loadEmpty()
    {
        maxTimestamp.load(Timestamp.NONE);
        uncommitted.map.load(new TreeMap<>());
        committedById.map.load(new TreeMap<>());
        committedByExecuteAt.map.load(new TreeMap<>());
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
