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
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Command;
import accord.local.CommandStore;
import accord.impl.CommandsForKey;
import accord.local.SafeCommandStore;
import accord.local.SafeCommandStore.TestDep;
import accord.local.SafeCommandStore.TestKind;
import accord.local.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.service.accord.api.PartitionKey;
import accord.utils.async.AsyncResult;
import org.apache.cassandra.service.accord.store.StoredLong;
import org.apache.cassandra.service.accord.store.StoredNavigableMap;
import org.apache.cassandra.service.accord.store.StoredSet;
import org.apache.cassandra.service.accord.store.StoredValue;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.assertj.core.util.VisibleForTesting;

import static accord.local.SafeCommandStore.TestDep.ANY_DEPS;
import static accord.local.SafeCommandStore.TestDep.WITH;
import static accord.local.SafeCommandStore.TestKind.Ws;
import static accord.local.Status.KnownDeps.DepsUnknown;
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
        private AsyncResult<Void> result = null;

        public WriteOnly(AccordCommandStore commandStore, PartitionKey key)
        {
            super(commandStore, key);
        }

        @Override
        public void asyncResult(AsyncResult<Void> result)
        {
            Preconditions.checkArgument(this.result == null);
            this.result = result;

        }

        @Override
        public AsyncResult<Void> asyncResult()
        {
            return result;
        }

        @Override
        public void applyChanges(AccordCommandsForKey instance)
        {
            applySetChanges(this, instance, cfk -> cfk.blindWitnessed);
            applyMapChanges(this, instance, cfk -> cfk.byId.map);
            applyMapChanges(this, instance, cfk -> cfk.byExecuteAt.map);
        }
    }

    public enum SeriesKind
    {
        BY_ID, BY_EXECUTE_AT;
    }

    public class Series implements CommandTimeseries
    {
        public final SeriesKind kind;
        public final StoredNavigableMap<Timestamp, ByteBuffer> map;

        public Series(ReadWrite readWrite, SeriesKind kind)
        {
            this.kind = kind;
            map = new StoredNavigableMap<>(readWrite);
        }

        @Override
        public void add(Timestamp timestamp, Command command)
        {
            map.blindPut(timestamp, AccordPartialCommand.serializer.serialize(new AccordPartialCommand(key, command)));
        }

        @Override
        public void remove(Timestamp timestamp)
        {
            map.blindRemove(timestamp);
        }

        private Stream<AccordPartialCommand> idsToCommands(Collection<ByteBuffer> blobs)
        {
            return blobs.stream().map(blob -> AccordPartialCommand.serializer.deserialize(AccordCommandsForKey.this, commandStore, blob));
        }

        @Override
        public boolean isEmpty()
        {
            return map.getView().isEmpty();
        }

        public <T> T mapReduce(TestKind testKind, TestTimestamp testTimestamp, Timestamp timestamp,
                        TestDep testDep, @Nullable TxnId depId,
                        @Nullable Status minStatus, @Nullable Status maxStatus,
                        SafeCommandStore.CommandFunction<T, T> map, T initialValue, T terminalValue)
        {

            for (ByteBuffer buffer : (testTimestamp == TestTimestamp.BEFORE ? this.map.getView().headMap(timestamp, false) : this.map.getView().tailMap(timestamp, false)).values())
            {
                AccordPartialCommand cmd = AccordPartialCommand.serializer.deserialize(AccordCommandsForKey.this, commandStore, buffer);
                if (testKind == Ws && cmd.txnId().isRead()) continue;
                // If we don't have any dependencies, we treat a dependency filter as a mismatch
                if (testDep != ANY_DEPS && (cmd.known().deps == DepsUnknown || (cmd.deps().contains(depId) != (testDep == WITH))))
                    continue;
                if (minStatus != null && minStatus.compareTo(cmd.status()) > 0)
                    continue;
                if (maxStatus != null && maxStatus.compareTo(cmd.status()) < 0)
                    continue;
                initialValue = map.apply(key, cmd.txnId(), cmd.executeAt(), initialValue);
                if (initialValue.equals(terminalValue))
                    break;
            }
            return initialValue;
        }

        @VisibleForTesting
        public Stream<AccordPartialCommand> all()
        {
            return idsToCommands(map.getView().values());
        }

        public AccordPartialCommand get(Timestamp timestamp)
        {
            ByteBuffer blob = map.getView().get(timestamp);
            if (blob == null)
                return null;
            return AccordPartialCommand.serializer.deserialize(AccordCommandsForKey.this, commandStore, blob);
        }
    }

    private final AccordCommandStore commandStore;
    private final PartitionKey key;
    public final StoredValue<Timestamp> maxTimestamp;
    public final StoredValue<Timestamp> lastExecutedTimestamp;
    public final StoredLong lastExecutedMicros;
    public final StoredValue<Timestamp> lastWriteTimestamp;
    public final StoredSet.Navigable<Timestamp> blindWitnessed;
    public final Series byId;
    public final Series byExecuteAt;

    public AccordCommandsForKey(AccordCommandStore commandStore, PartitionKey key)
    {
        this.commandStore = commandStore;
        this.key = key;
        maxTimestamp = new StoredValue<>(rw());
        lastExecutedTimestamp = new StoredValue<>(rw());
        lastExecutedMicros = new StoredLong(rw());
        lastWriteTimestamp = new StoredValue<>(rw());
        blindWitnessed = new StoredSet.Navigable<>(rw());
        byId = new Series(rw(), SeriesKind.BY_ID);
        byExecuteAt = new Series(rw(), SeriesKind.BY_EXECUTE_AT);
    }

    @Override
    public boolean isEmpty()
    {
        return maxTimestamp.isEmpty()
               && lastExecutedTimestamp.isEmpty()
               && lastExecutedMicros.isEmpty()
               && lastWriteTimestamp.isEmpty()
               && blindWitnessed.isEmpty()
               && byId.map.isEmpty()
               && byExecuteAt.map.isEmpty();
    }

    public void setEmpty()
    {
        maxTimestamp.setEmpty();
        lastExecutedTimestamp.setEmpty();
        lastExecutedMicros.setEmpty();
        lastWriteTimestamp.setEmpty();
        blindWitnessed.setEmpty();
        byId.map.setEmpty();
        byExecuteAt.map.setEmpty();
    }

    public AccordCommandsForKey initialize()
    {
        maxTimestamp.set(Defaults.maxTimestamp);
        lastExecutedTimestamp.load(Defaults.lastExecutedTimestamp);
        lastExecutedMicros.load(Defaults.lastExecutedMicros);
        lastWriteTimestamp.load(Defaults.lastWriteTimestamp);
        blindWitnessed.load(new TreeSet<>());
        byId.map.load(new TreeMap<>());
        byExecuteAt.map.load(new TreeMap<>());
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
               || byId.map.hasModifications()
               || byExecuteAt.map.hasModifications();
    }

    @Override
    public void clearModifiedFlag()
    {
        maxTimestamp.clearModifiedFlag();
        lastExecutedTimestamp.clearModifiedFlag();
        lastExecutedMicros.clearModifiedFlag();
        lastWriteTimestamp.clearModifiedFlag();
        blindWitnessed.clearModifiedFlag();
        byId.map.clearModifiedFlag();
        byExecuteAt.map.clearModifiedFlag();
    }

    @Override
    public boolean isLoaded()
    {
        return maxTimestamp.isLoaded()
               && lastExecutedTimestamp.isLoaded()
               && lastExecutedMicros.isLoaded()
               && lastWriteTimestamp.isLoaded()
               && blindWitnessed.isLoaded()
               && byId.map.isLoaded()
               && byExecuteAt.map.isLoaded();
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
        size += byId.map.estimatedSizeOnHeap(AccordObjectSizes::timestamp, ByteBufferUtil::estimatedSizeOnHeap);
        size += byExecuteAt.map.estimatedSizeOnHeap(AccordObjectSizes::timestamp, ByteBufferUtil::estimatedSizeOnHeap);
        return size;
    }

    @Override
    public Series byId()
    {
        return byId;
    }

    @Override
    public Series byExecuteAt()
    {
        return byExecuteAt;
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
        ByteBuffer partialCommand = AccordPartialCommand.serializer.serialize(new AccordPartialCommand(key, command));
        byId.map.blindPut(command.txnId(), partialCommand);
        byExecuteAt.map.blindPut(command.executeAt(), partialCommand);
    }

    private static long getTimestampMicros(Timestamp timestamp)
    {
        return timestamp.hlc();
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
        return Math.toIntExact(TimeUnit.MICROSECONDS.toSeconds(getTimestampMicros(lastExecutedTimestamp.get())));
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
               && byId.map.equals(that.byId.map)
               && byExecuteAt.map.equals(that.byExecuteAt.map);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(commandStore, key, blindWitnessed, maxTimestamp, lastExecutedTimestamp, lastExecutedMicros, lastWriteTimestamp, byId, byExecuteAt);
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
               '}';
    }
}
