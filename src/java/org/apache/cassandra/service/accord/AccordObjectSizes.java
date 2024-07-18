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

import java.util.UUID;
import java.util.function.ToLongFunction;

import accord.api.Key;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.impl.TimestampsForKey;
import accord.local.Command;
import accord.local.Command.WaitingOn;
import accord.local.cfk.CommandsForKey.TxnInfoExtra;
import accord.local.CommonAttributes;
import accord.local.Node;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.primitives.AbstractKeys;
import accord.primitives.AbstractRanges;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullKeyRoute;
import accord.primitives.FullRangeRoute;
import accord.primitives.KeyDeps;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialKeyRoute;
import accord.primitives.PartialRangeRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.RoutingKeys;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.WaitingOnSerializer;
import org.apache.cassandra.service.accord.txn.AccordUpdate;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.utils.ObjectSizes;

import static accord.local.cfk.CommandsForKey.NO_TXNIDS;
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

    private static final TableId EMPTY_ID = TableId.fromUUID(new UUID(0, 0));
    private static final long EMPTY_RANGE_SIZE = measure(TokenRange.fullRange(EMPTY_ID));
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

    private static long routingKeysOnly(AbstractKeys<RoutingKey> keys)
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

    private static final long EMPTY_FULL_KEY_ROUTE_SIZE = measure(new FullKeyRoute(new TokenKey(null, null), true, new RoutingKey[0]));
    public static long fullKeyRoute(FullKeyRoute route)
    {
        return EMPTY_FULL_KEY_ROUTE_SIZE
               + routingKeysOnly(route)
               + key(route.homeKey()); // TODO: we will probably dedup homeKey, serializer dependent, but perhaps this is an acceptable error
    }

    private static final long EMPTY_PARTIAL_KEY_ROUTE_KEYS_SIZE = measure(new PartialKeyRoute(Ranges.EMPTY, new TokenKey(null, null), true, new RoutingKey[0]));
    public static long partialKeyRoute(PartialKeyRoute route)
    {
        return EMPTY_PARTIAL_KEY_ROUTE_KEYS_SIZE
               + routingKeysOnly(route)
               + ranges(route.covering())
               + key(route.homeKey());
    }

    private static long rangesOnly(AbstractRanges ranges)
    {
        long size = ObjectSizes.sizeOfReferenceArray(ranges.size());
        for (int i=0, mi=ranges.size(); i<mi; i++)
            size += range(ranges.get(i));
        return size;
    }

    private static final long EMPTY_FULL_RANGE_ROUTE_SIZE = measure(new FullRangeRoute(new TokenKey(null, null), true, new Range[0]));
    public static long fullRangeRoute(FullRangeRoute route)
    {
        return EMPTY_FULL_RANGE_ROUTE_SIZE
               + rangesOnly(route)
               + key(route.homeKey()); // TODO: we will probably dedup homeKey, serializer dependent, but perhaps this is an acceptable error
    }

    private static final long EMPTY_PARTIAL_RANGE_ROUTE_KEYS_SIZE = measure(new PartialRangeRoute(Ranges.EMPTY, new TokenKey(null, null), true, new Range[0]));
    public static long partialRangeRoute(PartialRangeRoute route)
    {
        return EMPTY_PARTIAL_RANGE_ROUTE_KEYS_SIZE
               + rangesOnly(route)
               + ranges(route.covering())
               + key(route.homeKey());
    }

    public static long route(Unseekables<?> unseekables)
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
            size += ((AccordUpdate) txn.update()).estimatedSizeOnHeap();
        if (txn.query() != null)
            size += ((TxnQuery) txn.query()).estimatedSizeOnHeap();
        return size;
    }

    // don't count Id size, as should normally be shared
    private static final long TIMESTAMP_SIZE = ObjectSizes.measure(Timestamp.fromBits(0, 0, new Node.Id(0)));
    private static final long BALLOT_SIZE = ObjectSizes.measure(Ballot.ZERO);

    public static long timestamp()
    {
        return TIMESTAMP_SIZE;
    }

    public static long timestamp(Timestamp timestamp)
    {
        return TIMESTAMP_SIZE;
    }

    public static long ballot()
    {
        return BALLOT_SIZE;
    }

    public static long ballot(Ballot ballot)
    {
        return ballot == Ballot.ZERO ? 0 : BALLOT_SIZE;
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

    private static final long EMPTY_WRITES_SIZE = measure(new Writes(null, null, null, null));
    public static long writes(Writes writes)
    {
        long size = EMPTY_WRITES_SIZE;
        size += timestamp(writes.executeAt);
        size += seekables(writes.keys);
        if (writes.write != null)
            size += ((TxnWrite) writes.write).estimatedSizeOnHeap();
        return size;
    }

    public static long results(Result result)
    {
        return ((TxnResult) result).estimatedSizeOnHeap();
    }

    private static final long EMPTY_COMMAND_LISTENER = measure(new Command.ProxyListener(null));
    public static long listener(Command.DurableAndIdempotentListener listener)
    {
        if (listener instanceof Command.ProxyListener)
            return EMPTY_COMMAND_LISTENER + timestamp(((Command.ProxyListener) listener).txnId());
        throw new IllegalArgumentException("Unhandled listener type: " + listener.getClass());
    }

    private static class CommandEmptySizes
    {
        private final static TokenKey EMPTY_KEY = new TokenKey(EMPTY_ID, null);
        private final static TxnId EMPTY_TXNID = new TxnId(42, 42, Kind.Read, Domain.Key, new Node.Id(42));

        private static CommonAttributes attrs(boolean hasDeps, boolean hasTxn)
        {
            CommonAttributes.Mutable attrs = new CommonAttributes.Mutable(EMPTY_TXNID).route(new FullKeyRoute(EMPTY_KEY, true, new RoutingKey[]{ EMPTY_KEY }));
            attrs.durability(Status.Durability.NotDurable);
            if (hasDeps)
                attrs.partialDeps(PartialDeps.NONE);

            if (hasTxn)
                attrs.partialTxn(new PartialTxn.InMemory(null, null, null, null, null, null));

            return attrs;
        }

        private static final Writes EMPTY_WRITES = new Writes(EMPTY_TXNID, EMPTY_TXNID, Keys.EMPTY, (key, safeStore, executeAt, store, txn) -> null);
        private static final Result EMPTY_RESULT = new Result() {};

        final static long NOT_DEFINED = measure(Command.SerializerSupport.notDefined(attrs(false, false), Ballot.ZERO));
        final static long PREACCEPTED = measure(Command.SerializerSupport.preaccepted(attrs(false, true), EMPTY_TXNID, null));;
        final static long ACCEPTED = measure(Command.SerializerSupport.accepted(attrs(true, false), SaveStatus.Accepted, EMPTY_TXNID, Ballot.ZERO, Ballot.ZERO));
        final static long COMMITTED = measure(Command.SerializerSupport.committed(attrs(true, true), SaveStatus.Committed, EMPTY_TXNID, Ballot.ZERO, Ballot.ZERO, null));
        final static long EXECUTED = measure(Command.SerializerSupport.executed(attrs(true, true), SaveStatus.Applied, EMPTY_TXNID, Ballot.ZERO, Ballot.ZERO, WaitingOn.EMPTY, EMPTY_WRITES, EMPTY_RESULT));
        final static long TRUNCATED = measure(Command.SerializerSupport.truncatedApply(attrs(false, false), SaveStatus.TruncatedApply,  EMPTY_TXNID, null, null));
        final static long INVALIDATED = measure(Command.SerializerSupport.invalidated(EMPTY_TXNID, null));

        private static long emptySize(Command command)
        {
            switch (command.status())
            {
                case NotDefined:
                    return NOT_DEFINED;
                case PreAccepted:
                    return PREACCEPTED;
                case AcceptedInvalidate:
                case Accepted:
                case PreCommitted:
                    return ACCEPTED;
                case Committed:
                case Stable:
                    return COMMITTED;
                case PreApplied:
                case Applied:
                    return EXECUTED;
                case Truncated:
                    return TRUNCATED;
                case Invalidated:
                    return INVALIDATED;
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
        size += sizeNullable(command.route(), AccordObjectSizes::route);
        size += sizeNullable(command.promised(), AccordObjectSizes::timestamp);
        for (Command.DurableAndIdempotentListener listener : command.durableListeners())
            size += listener(listener);
        size += sizeNullable(command.executeAt(), AccordObjectSizes::timestamp);
        size += sizeNullable(command.partialTxn(), AccordObjectSizes::txn);
        size += sizeNullable(command.partialDeps(), AccordObjectSizes::dependencies);
        size += sizeNullable(command.acceptedOrCommitted(), AccordObjectSizes::timestamp);
        size += sizeNullable(command.writes(), AccordObjectSizes::writes);

        if (command.result() instanceof TxnResult)
            size += sizeNullable(command.result(), AccordObjectSizes::results);

        if (!(command instanceof Command.Committed && command.saveStatus().hasBeen(Status.Stable)))
            return size;

        Command.Committed committed = command.asCommitted();
        size += WaitingOnSerializer.serializedSize(committed.waitingOn);

        return size;
    }

    private static long EMPTY_TFK_SIZE = measure(TimestampsForKey.SerializerSupport.create(null, null, 0, null));

    public static long timestampsForKey(TimestampsForKey timestamps)
    {
        long size = EMPTY_TFK_SIZE;
        size += timestamp(timestamps.lastExecutedTimestamp());
        size += timestamp(timestamps.lastWriteTimestamp());
        return size;
    }

    private static long EMPTY_CFK_SIZE = measure(new CommandsForKey(null));
    private static long EMPTY_INFO_SIZE = measure(TxnInfo.createMock(TxnId.NONE, null, null, NO_TXNIDS, Ballot.ZERO));
    private static long EMPTY_INFO_EXTRA_ADDITIONAL_SIZE = EMPTY_INFO_SIZE - measure(TxnInfo.createMock(TxnId.NONE, null, null, null, null));
    public static long commandsForKey(CommandsForKey cfk)
    {
        long size = EMPTY_CFK_SIZE;
        size += key(cfk.key());
        size += ObjectSizes.sizeOfReferenceArray(cfk.size());
        size += cfk.size() * EMPTY_INFO_SIZE;
        for (int i = 0 ; i < cfk.size() ; ++i)
        {
            TxnInfo info = cfk.get(i);
            if (info.getClass() != TxnInfoExtra.class) continue;
            TxnInfoExtra infoExtra = (TxnInfoExtra) info;
            if (infoExtra.missing.length > 0)
            {
                size += EMPTY_INFO_EXTRA_ADDITIONAL_SIZE;
                size += ObjectSizes.sizeOfReferenceArray(infoExtra.missing.length);
                size += infoExtra.missing.length * TIMESTAMP_SIZE;
                size += ballot(infoExtra.ballot);
            }
        }
        return size;
    }
}
