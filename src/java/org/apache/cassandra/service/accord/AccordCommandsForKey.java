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
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandsForKey;
import accord.local.PartialCommand;
import accord.local.Status;
import accord.primitives.Timestamp;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.service.accord.store.StoredLong;
import org.apache.cassandra.service.accord.store.StoredNavigableMap;
import org.apache.cassandra.service.accord.store.StoredSet;
import org.apache.cassandra.service.accord.store.StoredValue;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.service.accord.AccordState.WriteOnly.applyMapChanges;
import static org.apache.cassandra.service.accord.AccordState.WriteOnly.applySetChanges;

public class AccordCommandsForKey extends CommandsForKey implements AccordState<PartitionKey>
{
    private static final Logger logger = LoggerFactory.getLogger(AccordCommandsForKey.class);

    private static final long EMPTY_SIZE = ObjectSizes.measureDeep(new AccordCommandsForKey(null, null));

    public static class Defaults
    {
        public static final Timestamp maxTimestamp = Timestamp.NONE;
        public static final Timestamp lastExecutedTimestamp = Timestamp.NONE;
        public static final Timestamp lastWriteTimestamp = Timestamp.NONE;
        public static final long lastExecutedMicros = 0;
    }

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
            applySetChanges(this, instance, cfk -> cfk.blindWitnessed);
            applyMapChanges(this, instance, cfk -> cfk.uncommitted.map);
            applyMapChanges(this, instance, cfk -> cfk.committedById.map);
            applyMapChanges(this, instance, cfk -> cfk.committedByExecuteAt.map);
        }
    }

    public enum SeriesKind
    {
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
        public final StoredNavigableMap<Timestamp, ByteBuffer> map;

        public Series(AccordState.Kind stateKind, SeriesKind kind)
        {
            this.kind = kind;
            map = new StoredNavigableMap<>(stateKind);
        }

        @Override
        public PartialCommand.WithDeps get(Timestamp timestamp)
        {
            ByteBuffer bytes = map.getView().get(timestamp);
            if (bytes == null)
                return null;
            return AccordPartialCommand.WithDeps.serializer.deserialize(commandStore, bytes);
        }

        @Override
        public void add(Timestamp timestamp, Command command)
        {
            map.blindPut(timestamp, AccordPartialCommand.WithDeps.serializer.serialize(command));
        }

        @Override
        public void remove(Timestamp timestamp)
        {
            map.blindRemove(timestamp);
        }

        private Stream<PartialCommand.WithDeps> idsToCommands(Collection<ByteBuffer> blobs)
        {
            return blobs.stream().map(blob -> AccordPartialCommand.WithDeps.serializer.deserialize(commandStore, blob));
        }

        @Override
        public boolean isEmpty()
        {
            return map.getView().isEmpty();
        }

        @Override
        public Stream<PartialCommand.WithDeps> before(Timestamp timestamp)
        {
            return idsToCommands(map.getView().headMap(timestamp, false).values());
        }

        @Override
        public Stream<PartialCommand.WithDeps> after(Timestamp timestamp)
        {
            return idsToCommands(map.getView().tailMap(timestamp, false).values());
        }

        @Override
        public Stream<PartialCommand.WithDeps> between(Timestamp min, Timestamp max)
        {
            return idsToCommands(map.getView().subMap(min, true, max, true).values());
        }

        @Override
        public Stream<PartialCommand.WithDeps> all()
        {
            return idsToCommands(map.getView().values());
        }
    }

    private final AccordCommandStore commandStore;
    private final PartitionKey key;
    public final StoredValue<Timestamp> maxTimestamp;
    public final StoredValue<Timestamp> lastExecutedTimestamp;
    public final StoredLong lastExecutedMicros;
    public final StoredValue<Timestamp> lastWriteTimestamp;
    public final StoredSet.Navigable<Timestamp> blindWitnessed;
    public final Series uncommitted;
    public final Series committedById;
    public final Series committedByExecuteAt;

    public AccordCommandsForKey(AccordCommandStore commandStore, PartitionKey key)
    {
        this.commandStore = commandStore;
        this.key = key;
        maxTimestamp = new StoredValue<>(kind());
        lastExecutedTimestamp = new StoredValue<>(kind());
        lastExecutedMicros = new StoredLong(kind());
        lastWriteTimestamp = new StoredValue<>(kind());
        blindWitnessed = new StoredSet.Navigable<>(kind());
        uncommitted = new Series(kind(), SeriesKind.UNCOMMITTED);
        committedById = new Series(kind(), SeriesKind.COMMITTED_BY_ID);
        committedByExecuteAt = new Series(kind(), SeriesKind.COMMITTED_BY_EXECUTE_AT);
    }

    @Override
    public boolean isEmpty()
    {
        return maxTimestamp.isEmpty()
               && lastExecutedTimestamp.isEmpty()
               && lastExecutedMicros.isEmpty()
               && lastWriteTimestamp.isEmpty()
               && blindWitnessed.isEmpty()
               && uncommitted.map.isEmpty()
               && committedById.map.isEmpty()
               && committedByExecuteAt.map.isEmpty();
    }

    public void setEmpty()
    {
        maxTimestamp.setEmpty();
        lastExecutedTimestamp.setEmpty();
        lastExecutedMicros.setEmpty();
        lastWriteTimestamp.setEmpty();
        blindWitnessed.setEmpty();
        uncommitted.map.setEmpty();
        committedById.map.setEmpty();
        committedByExecuteAt.map.setEmpty();
    }

    public AccordCommandsForKey initialize()
    {
        maxTimestamp.set(Defaults.maxTimestamp);
        lastExecutedTimestamp.load(Defaults.lastExecutedTimestamp);
        lastExecutedMicros.load(Defaults.lastExecutedMicros);
        lastWriteTimestamp.load(Defaults.lastWriteTimestamp);
        blindWitnessed.load(new TreeSet<>());
        uncommitted.map.load(new TreeMap<>());
        committedById.map.load(new TreeMap<>());
        committedByExecuteAt.map.load(new TreeMap<>());
        return this;
    }

    @Override
    public boolean hasModifications()
    {
        return maxTimestamp.hasModifications()
               || lastExecutedTimestamp.hasModifications()
               || lastExecutedMicros.hasModifications()
               || lastWriteTimestamp.hasModifications()
               || blindWitnessed.hasModifications()
               || uncommitted.map.hasModifications()
               || committedById.map.hasModifications()
               || committedByExecuteAt.map.hasModifications();
    }

    @Override
    public void clearModifiedFlag()
    {
        maxTimestamp.clearModifiedFlag();
        lastExecutedTimestamp.clearModifiedFlag();
        lastExecutedMicros.clearModifiedFlag();
        lastWriteTimestamp.clearModifiedFlag();
        blindWitnessed.clearModifiedFlag();
        uncommitted.map.clearModifiedFlag();
        committedById.map.clearModifiedFlag();
        committedByExecuteAt.map.clearModifiedFlag();
    }

    @Override
    public boolean isLoaded()
    {
        return maxTimestamp.isLoaded()
               && lastExecutedTimestamp.isLoaded()
               && lastExecutedMicros.isLoaded()
               && lastWriteTimestamp.isLoaded()
               && blindWitnessed.isLoaded()
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
        size += lastExecutedTimestamp.estimatedSizeOnHeap(AccordObjectSizes::timestamp);
        size += lastExecutedMicros.estimatedSizeOnHeap();
        size += lastWriteTimestamp.estimatedSizeOnHeap(AccordObjectSizes::timestamp);
        size += blindWitnessed.estimatedSizeOnHeap(AccordObjectSizes::timestamp);
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
        if (isFullInstance())
        {
            if (maxTimestamp.get().compareTo(timestamp) >= 0)
                return;
            maxTimestamp.set(timestamp);
        }
        else
        {
            Preconditions.checkState(isWriteOnlyInstance());
            blindWitnessed.blindAdd(timestamp);
        }
    }

    public void applyBlindWitnessedTimestamps()
    {
        if (isEmpty() || blindWitnessed.getView().isEmpty())
            return;

        logger.trace("Applying blind witnessed timestamps for {}: {}", key(), blindWitnessed.getView());
        blindWitnessed.getView().forEach(this::updateMax);
        blindWitnessed.clear();
    }

    public void updateSummaries(AccordCommand command)
    {
        if (command.status.get().hasBeen(Status.Committed))
        {
            if (command.status.previous() == null || !command.status.previous().hasBeen(Status.Committed))
                uncommitted.map.blindRemove(command.txnId());

            ByteBuffer bb = AccordPartialCommand.WithDeps.serializer.serialize(command);
            committedById.map.blindPut(command.txnId(), bb);
            committedByExecuteAt.map.blindPut(command.executeAt(), bb);
        }
        else
        {
            uncommitted.map.blindPut(command.txnId(), AccordPartialCommand.WithDeps.serializer.serialize(command));
        }
    }

    private static long getTimestampMicros(Timestamp timestamp)
    {
        return timestamp.real + timestamp.logical;
    }

    private void maybeUpdatelastTimestamp(Timestamp executeAt, boolean isForWriteTxn)
    {
        Timestamp lastWrite = lastWriteTimestamp.get();

        if (executeAt.compareTo(lastWrite) < 0)
            throw new IllegalArgumentException(String.format("%s is less than the most recent write timestamp %s", executeAt, lastWrite));

        Timestamp lastExecuted = lastExecutedTimestamp.get();
        int cmp = executeAt.compareTo(lastExecuted);
        // execute can be in the past if it's for a read and after the most recent write
        if (cmp == 0 || (!isForWriteTxn && cmp < 0))
            return;
        if (cmp < 0)
            throw new IllegalArgumentException(String.format("%s is less than the most recent executed timestamp %s", executeAt, lastExecuted));

        long micros = getTimestampMicros(executeAt);
        long lastMicros = lastExecutedMicros.get();
        lastExecutedTimestamp.set(executeAt);
        lastExecutedMicros.set(Math.max(micros, lastMicros + 1));
        if (isForWriteTxn)
            lastWriteTimestamp.set(executeAt);
    }

    public int nowInSecondsFor(Timestamp executeAt, boolean isForWriteTxn)
    {
        maybeUpdatelastTimestamp(executeAt, isForWriteTxn);
        // we use the executeAt time instead of the monotonic database timestamp to prevent uneven
        // ttl expiration in extreme cases, ie 1M+ writes/second to a key causing timestamps to overflow
        // into the next second on some keys and not others.
        return (int) TimeUnit.MICROSECONDS.toSeconds(getTimestampMicros(lastExecutedTimestamp.get()));
    }

    public long timestampMicrosFor(Timestamp executeAt, boolean isForWriteTxn)
    {
        maybeUpdatelastTimestamp(executeAt, isForWriteTxn);
        return lastExecutedMicros.get();
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
               && lastExecutedTimestamp.equals(that.lastExecutedTimestamp)
               && lastExecutedMicros.equals(that.lastExecutedMicros)
               && lastWriteTimestamp.equals(that.lastWriteTimestamp)
               && blindWitnessed.equals(that.blindWitnessed)
               && uncommitted.map.equals(that.uncommitted.map)
               && committedById.map.equals(that.committedById.map)
               && committedByExecuteAt.map.equals(that.committedByExecuteAt.map);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(commandStore, key, blindWitnessed, maxTimestamp, lastExecutedTimestamp, lastExecutedMicros, lastWriteTimestamp, uncommitted, committedById, committedByExecuteAt);
    }

    @Override
    public String toString()
    {
        return "AccordCommandsForKey{" +
               "key=" + key +
               ", maxTs=" + maxTimestamp +
               ", lastExecutedTimestamp=" + lastExecutedTimestamp +
               ", lastExecutedMicros=" + lastExecutedMicros +
               ", lastWriteTimestamp=" + lastWriteTimestamp +
//               ", blindWitnessed=" + blindWitnessed +
//               ", uncommitted=" + uncommitted.map +
//               ", committedById=" + committedById.map +
//               ", committedByExecuteAt=" + committedByExecuteAt.map +
               '}';
    }
}
