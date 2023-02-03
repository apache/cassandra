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
import java.util.Map;
import java.util.function.ToLongFunction;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;

import accord.api.Key;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.impl.CommandsForKey;
import accord.local.Command;
import accord.local.CommandListener;
import accord.local.CommonAttributes;
import accord.local.Node;
import accord.local.SaveStatus;
import accord.primitives.AbstractKeys;
import accord.primitives.AbstractRanges;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullKeyRoute;
import accord.primitives.FullRangeRoute;
import accord.primitives.KeyDeps;
import accord.primitives.Keys;
import accord.primitives.PartialKeyRoute;
import accord.primitives.PartialRangeRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.accord.txn.TxnUpdate;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

import static org.apache.cassandra.utils.ObjectSizes.measure;

public class AccordObjectSizes
{
    public static long key(Key key)
    {
        return ((PartitionKey) key).estimatedSizeOnHeap();
    }

    public static long key(RoutingKey key)
    {
        return ((AccordRoutingKey) key).estimatedSizeOnHeap();
    }

    private static final long EMPTY_RANGE_SIZE = measure(TokenRange.fullRange(""));
    public static long range(Range range)
    {
        return EMPTY_RANGE_SIZE + key(range.start()) + key(range.end());
    }

    private static final long EMPTY_RANGES_SIZE = measure(Ranges.of());
    public static long ranges(Ranges ranges)
    {
        long size = EMPTY_RANGES_SIZE;
        size += ObjectSizes.sizeOfReferenceArray(ranges.size());
        // TODO: many ranges are fixed size, can compute by multiplication
        for (int i = 0, mi = ranges.size() ; i < mi ; i++)
            size += range(ranges.get(i));
        return size;
    }

    private static final long EMPTY_KEYS_SIZE = measure(Keys.of());
    public static long keys(Keys keys)
    {
        long size = EMPTY_KEYS_SIZE;
        size += ObjectSizes.sizeOfReferenceArray(keys.size());
        for (int i=0, mi=keys.size(); i<mi; i++)
            size += key(keys.get(i));
        return size;
    }

    public static long seekables(Seekables<?, ?> seekables)
    {
        switch (seekables.domain())
        {
            default: throw new AssertionError();
            case Key: return keys((Keys) seekables);
            case Range: return ranges((Ranges) seekables);
        }
    }

    private static long routingKeysOnly(AbstractKeys<RoutingKey, ?> keys)
    {
        // TODO: many routing keys are fixed size, can compute by multiplication
        long size = ObjectSizes.sizeOfReferenceArray(keys.size());
        for (int i=0, mi=keys.size(); i<mi; i++)
            size += key(keys.get(i));
        return size;
    }

    private static final long EMPTY_ROUTING_KEYS_SIZE = measure(RoutingKeys.of());
    public static long routingKeys(RoutingKeys keys)
    {
        return EMPTY_ROUTING_KEYS_SIZE + routingKeysOnly(keys);
    }

    private static final long EMPTY_FULL_KEY_ROUTE_SIZE = measure(new FullKeyRoute(new TokenKey(null, null), new RoutingKey[0]));
    public static long fullKeyRoute(FullKeyRoute route)
    {
        return EMPTY_FULL_KEY_ROUTE_SIZE
               + routingKeysOnly(route)
               + key(route.homeKey()); // TODO: we will probably dedup homeKey, serializer dependent, but perhaps this is an acceptable error
    }

    private static final long EMPTY_PARTIAL_KEY_ROUTE_KEYS_SIZE = measure(new PartialKeyRoute(Ranges.EMPTY, new TokenKey(null, null), new RoutingKey[0]));
    public static long partialKeyRoute(PartialKeyRoute route)
    {
        return EMPTY_PARTIAL_KEY_ROUTE_KEYS_SIZE
               + routingKeysOnly(route)
               + ranges(route.covering())
               + key(route.homeKey());
    }

    private static long rangesOnly(AbstractRanges<?> ranges)
    {
        long size = ObjectSizes.sizeOfReferenceArray(ranges.size());
        for (int i=0, mi=ranges.size(); i<mi; i++)
            size += range(ranges.get(i));
        return size;
    }

    private static final long EMPTY_FULL_RANGE_ROUTE_SIZE = measure(new FullRangeRoute(new TokenKey(null, null), new Range[0]));
    public static long fullRangeRoute(FullRangeRoute route)
    {
        return EMPTY_FULL_RANGE_ROUTE_SIZE
               + rangesOnly(route)
               + key(route.homeKey()); // TODO: we will probably dedup homeKey, serializer dependent, but perhaps this is an acceptable error
    }

    private static final long EMPTY_PARTIAL_RANGE_ROUTE_KEYS_SIZE = measure(new PartialRangeRoute(Ranges.EMPTY, new TokenKey(null, null), new Range[0]));
    public static long partialRangeRoute(PartialRangeRoute route)
    {
        return EMPTY_PARTIAL_RANGE_ROUTE_KEYS_SIZE
               + rangesOnly(route)
               + ranges(route.covering())
               + key(route.homeKey());
    }

    public static long route(Unseekables<?, ?> unseekables)
    {
        switch (unseekables.kind())
        {
            default: throw new AssertionError();
            case RoutingKeys: return routingKeys((RoutingKeys) unseekables);
            case PartialKeyRoute: return partialKeyRoute((PartialKeyRoute) unseekables);
            case FullKeyRoute: return fullKeyRoute((FullKeyRoute) unseekables);
            case RoutingRanges: return ranges((Ranges) unseekables);
            case PartialRangeRoute: return partialRangeRoute((PartialRangeRoute) unseekables);
            case FullRangeRoute: return fullRangeRoute((FullRangeRoute) unseekables);
        }
    }

    private static final long EMPTY_TXN = measure(new PartialTxn.InMemory(null, null, null, null, null, null));
    public static long txn(PartialTxn txn)
    {
        long size = EMPTY_TXN;
        size += seekables(txn.keys());
        size += ((TxnRead) txn.read()).estimatedSizeOnHeap();
        if (txn.update() != null)
            size += ((TxnUpdate) txn.update()).estimatedSizeOnHeap();
        if (txn.query() != null)
            size += ((TxnQuery) txn.query()).estimatedSizeOnHeap();
        return size;
    }

    private static final long TIMESTAMP_SIZE = ObjectSizes.measureDeep(Timestamp.fromBits(0, 0, new Node.Id(0)));

    public static long timestamp()
    {
        return TIMESTAMP_SIZE;
    }
    public static long timestamp(Timestamp timestamp)
    {
        return TIMESTAMP_SIZE;
    }

    private static final long EMPTY_DEPS_SIZE = ObjectSizes.measureDeep(Deps.NONE);
    public static long dependencies(Deps dependencies)
    {
        // TODO (expected): this doesn't measure the backing arrays, is inefficient;
        //      doesn't account for txnIdToKeys, txnIdToRanges, and searchable fields;
        //      fix to accunt for, in case caching isn't redone
        long size = EMPTY_DEPS_SIZE - EMPTY_KEYS_SIZE - ObjectSizes.sizeOfReferenceArray(0);
        size += keys(dependencies.keyDeps.keys());
        for (int i = 0 ; i < dependencies.rangeDeps.rangeCount() ; ++i)
            size += range(dependencies.rangeDeps.range(i));
        size += ObjectSizes.sizeOfReferenceArray(dependencies.rangeDeps.rangeCount());

        for (int i = 0 ; i < dependencies.keyDeps.txnIdCount() ; ++i)
            size += timestamp(dependencies.keyDeps.txnId(i));
        for (int i = 0 ; i < dependencies.rangeDeps.txnIdCount() ; ++i)
            size += timestamp(dependencies.rangeDeps.txnId(i));

        size += KeyDeps.SerializerSupport.keysToTxnIdsCount(dependencies.keyDeps) * 4L;
        size += RangeDeps.SerializerSupport.rangesToTxnIdsCount(dependencies.rangeDeps) * 4L;
        return size;
    }

    private static final long EMPTY_WRITES_SIZE = measure(new Writes(null, null, null));
    public static long writes(Writes writes)
    {
        long size = EMPTY_WRITES_SIZE;
        size += timestamp(writes.executeAt);
        size += seekables(writes.keys);
        if (writes.write != null)
            size += ((TxnWrite) writes.write).estimatedSizeOnHeap();
        return size;
    }

    private static final long EMPTY_COMMAND_LISTENER = measure(new Command.Listener(null));
    private static final long EMPTY_CFK_LISTENER = measure(new CommandsForKey.Listener((Key) null));
    public static long listener(CommandListener listener)
    {
        if (listener.isTransient())
            return 0;
        if (listener instanceof Command.Listener)
            return EMPTY_COMMAND_LISTENER + timestamp(((Command.Listener) listener).txnId());
        if (listener instanceof CommandsForKey.Listener)
            return EMPTY_CFK_LISTENER + key(((CommandsForKey.Listener) listener).key());
        throw new IllegalArgumentException("Unhandled listener type: " + listener.getClass());
    }

    private static class CommandEmptySizes
    {
        private static final CommonAttributes EMPTY_ATTRS = new CommonAttributes.Mutable((TxnId) null);
        final static long NOT_WITNESSED = measure(Command.SerializerSupport.notWitnessed(EMPTY_ATTRS, Ballot.ZERO));
        final static long PREACCEPTED = measure(Command.SerializerSupport.preaccepted(EMPTY_ATTRS, null, null));
        final static long ACCEPTED = measure(Command.SerializerSupport.accepted(EMPTY_ATTRS, SaveStatus.Accepted, null, null, null));
        final static long COMMITTED = measure(Command.SerializerSupport.committed(EMPTY_ATTRS, SaveStatus.Committed, null, null, null, ImmutableSortedSet.of(), ImmutableSortedMap.of()));
        final static long EXECUTED = measure(Command.SerializerSupport.executed(EMPTY_ATTRS, SaveStatus.Applied, null, null, null, ImmutableSortedSet.of(), ImmutableSortedMap.of(), null, null));


        private static long emptySize(Command command)
        {
            switch (command.status())
            {
                case NotWitnessed:
                    return NOT_WITNESSED;
                case PreAccepted:
                    return PREACCEPTED;
                case AcceptedInvalidate:
                case Accepted:
                case PreCommitted:
                    return ACCEPTED;
                case Committed:
                case ReadyToExecute:
                    return COMMITTED;
                case PreApplied:
                case Applied:
                case Invalidated:
                    return EXECUTED;
                default:
                    throw new IllegalStateException("Unhandled status " + command.status());
            }
        }
    }

    private static <T> long sizeNullable(T value, ToLongFunction<T> measure)
    {
        if (value == null)
            return 0;
        return measure.applyAsLong(value);
    }

    public static long command(Command command)
    {
        long size = CommandEmptySizes.emptySize(command);
        size += sizeNullable(command.homeKey(), AccordObjectSizes::key);
        size += sizeNullable(command.progressKey(), AccordObjectSizes::key);
        size += sizeNullable(command.route(), AccordObjectSizes::route);
        size += sizeNullable(command.promised(), AccordObjectSizes::timestamp);
        for (CommandListener listener : command.listeners())
            size += listener(listener);

        if (!command.isWitnessed())
            return size;

        Command.PreAccepted preaccepted = command.asWitnessed();
        size += timestamp(preaccepted.executeAt());
        size += sizeNullable(preaccepted.partialTxn(), AccordObjectSizes::txn);
        size += sizeNullable(preaccepted.partialDeps(), AccordObjectSizes::dependencies);

        if (!command.isAccepted())
            return size;

        Command.Accepted accepted = command.asAccepted();
        size += timestamp(accepted.accepted());

        if (!command.isCommitted())
            return size;

        Command.Committed committed = command.asCommitted();
        size += TIMESTAMP_SIZE * committed.waitingOnCommit().size();
        size += TIMESTAMP_SIZE * 2 * committed.waitingOnApply().size();

        if (!command.isExecuted())
            return size;

        Command.Executed executed = command.asExecuted();
        size += sizeNullable(executed.writes(), AccordObjectSizes::writes);
        Result result = executed.result();
        if (result != null)
            size += ((TxnData) result).estimatedSizeOnHeap();

        return size;
    }

    private static long cfkSeriesSize(ImmutableSortedMap<Timestamp, ByteBuffer> series)
    {
        long size = 0;
        for (Map.Entry<Timestamp, ByteBuffer> entry : series.entrySet())
        {
            size += timestamp(entry.getKey());
            size += ByteBufferUtil.estimatedSizeOnHeap(entry.getValue());
        }
        return size;
    }

    private static long EMPTY_CFK_SIZE = measure(CommandsForKey.SerializerSupport.create(null, null, null, 0, null, null,
                                                                                         ImmutableSortedMap.of(),
                                                                                         ImmutableSortedMap.of()));
    public static long commandsForKey(CommandsForKey cfk)
    {
        long size = EMPTY_CFK_SIZE;
        size += key(cfk.key());
        size += timestamp(cfk.max());
        size += timestamp(cfk.lastExecutedTimestamp());
        size += timestamp(cfk.lastWriteTimestamp());
        size += cfkSeriesSize((ImmutableSortedMap<Timestamp, ByteBuffer>) cfk.byId().commands);
        size += cfkSeriesSize((ImmutableSortedMap<Timestamp, ByteBuffer>) cfk.byExecuteAt().commands);
        return size;
    }
}
