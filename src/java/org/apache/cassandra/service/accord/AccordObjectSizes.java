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
import accord.local.Command;
import accord.local.ICommand;
import accord.local.Node;
import accord.local.StoreParticipants;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.local.cfk.CommandsForKey.TxnInfoExtra;
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
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.RoutingKeys;
import accord.primitives.SaveStatus;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.utils.ImmutableBitSet;
import accord.utils.UnhandledEnum;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.ResultSerializers;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.txn.AccordUpdate;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.utils.ObjectSizes;

import static accord.local.Command.Accepted.accepted;
import static accord.local.Command.Committed.committed;
import static accord.local.Command.Executed.executed;
import static accord.local.Command.NotAcceptedWithoutDefinition.notAccepted;
import static accord.local.Command.NotDefined.notDefined;
import static accord.local.Command.PreAccepted.preaccepted;
import static accord.local.Command.Truncated.*;
import static accord.local.cfk.CommandsForKey.InternalStatus.ACCEPTED;
import static accord.primitives.Status.Durability.NotDurable;
import static accord.primitives.TxnId.NO_TXNIDS;
import static org.apache.cassandra.utils.ObjectSizes.measure;

public class AccordObjectSizes
{
    public static long key(Key key)
    {
        return ((PartitionKey) key).estimatedSizeOnHeap();
    }

    public static long key(RoutingKey key)
    {
        return ((TokenKey) key).estimatedSizeOnHeap();
    }

    private static final TableId EMPTY_ID = TableId.fromUUID(new UUID(0, 0));
    private static final long EMPTY_RANGE_SIZE = measure(TokenRange.fullRange(EMPTY_ID, Murmur3Partitioner.instance));
    public static long range(Range range)
    {
        return EMPTY_RANGE_SIZE + key(range.start()) + key(range.end());
    }

    public static long seekable(Seekable seekable)
    {
        switch (seekable.domain())
        {
            default: throw new AssertionError();
            case Key: return key((Key) seekable);
            case Range: return range((Range) seekable);
        }
    }

    private static final long EMPTY_RANGES_SIZE = measure(Ranges.of());
    public static long ranges(Ranges ranges)
    {
        long size = EMPTY_RANGES_SIZE;
        int numberOfRanges = ranges.size();
        size += ObjectSizes.sizeOfReferenceArray(numberOfRanges);
        if (numberOfRanges > 1 && DatabaseDescriptor.getPartitioner().isFixedLength())
            return size + numberOfRanges * range(ranges.get(0));

        for (int i = 0 ; i < numberOfRanges ; i++)
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
        int numberOfKeys = keys.size();
        long size = ObjectSizes.sizeOfReferenceArray(numberOfKeys);
        if (numberOfKeys > 1 && DatabaseDescriptor.getPartitioner().isFixedLength())
            return size + numberOfKeys * key(keys.get(0));

        for (int i=0 ; i < numberOfKeys; i++)
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
               + key(route.homeKey()); // TODO (desired): we will probably dedup homeKey, serializer dependent, but perhaps this is an acceptable error
    }

    private static final long EMPTY_PARTIAL_KEY_ROUTE_KEYS_SIZE = measure(new PartialKeyRoute(new TokenKey(null, null), new RoutingKey[0]));
    public static long partialKeyRoute(PartialKeyRoute route)
    {
        return EMPTY_PARTIAL_KEY_ROUTE_KEYS_SIZE
               + routingKeysOnly(route)
               + key(route.homeKey());
    }

    public static long ranges(AbstractRanges ranges)
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
               + ranges(route)
               + key(route.homeKey()); // TODO (desired): we will probably dedup homeKey, serializer dependent, but perhaps this is an acceptable error
    }

    private static final long EMPTY_PARTIAL_RANGE_ROUTE_KEYS_SIZE = measure(new PartialRangeRoute(new TokenKey(null, null), new Range[0]));
    public static long partialRangeRoute(PartialRangeRoute route)
    {
        return EMPTY_PARTIAL_RANGE_ROUTE_KEYS_SIZE
               + ranges(route)
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

    private static final long EMPTY_TXN = measure(new PartialTxn.InMemory(Kind.Read, null, null, null, null, TableMetadatasAndKeys.none(Domain.Key)));
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
        size += routingKeys(dependencies.keyDeps.keys());
        for (int i = 0 ; i < dependencies.rangeDeps.rangeCount() ; ++i)
            size += range(dependencies.rangeDeps.range(i));
        size += ObjectSizes.sizeOfReferenceArray(dependencies.rangeDeps.rangeCount());

        size += dependencies.keyDeps.txnIdCount() * TIMESTAMP_SIZE;
        size += dependencies.rangeDeps.txnIdCount() * TIMESTAMP_SIZE;
        size += KeyDeps.SerializerSupport.keysToTxnIds(dependencies.keyDeps).length * 4L;
        size += RangeDeps.SerializerSupport.rangesToTxnIds(dependencies.rangeDeps).length * 4L;
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
        if (result == ResultSerializers.APPLIED)
            return 0;
        return ((TxnResult) result).estimatedSizeOnHeap();
    }

    private static class CommandEmptySizes
    {
        private final static TokenKey EMPTY_KEY = new TokenKey(EMPTY_ID, null);
        private final static TxnId EMPTY_TXNID = new TxnId(42, 42, 0, Kind.Read, Domain.Key, new Node.Id(42));

        private static ICommand attrs(boolean hasDeps, boolean hasTxn, boolean executes)
        {
            FullKeyRoute route = new FullKeyRoute(EMPTY_KEY, new RoutingKey[]{ EMPTY_KEY });
            Participants<?> empty = route.slice(0, 0);
            ICommand.Builder builder = new ICommand.Builder(EMPTY_TXNID)
                                       .setParticipants(StoreParticipants.create(route, empty, executes ? empty : null, executes ? empty : null, empty, route))
                                       .durability(NotDurable)
                                       .executeAt(EMPTY_TXNID)
                                       .promised(Ballot.ZERO);
            if (hasDeps)
                builder.partialDeps(PartialDeps.NONE);

            if (hasTxn)
                builder.partialTxn(new PartialTxn.InMemory(Kind.Read, null, null, null, null, TableMetadatasAndKeys.none(Domain.Key)));

            if (executes)
            {
                builder.waitingOn(WaitingOn.empty(Domain.Key));
                builder.result(new TxnData());
            }

            return builder;
        }

        final static long NOT_DEFINED = measure(notDefined(attrs(false, false, false)));
        final static long PREACCEPTED = measure(preaccepted(attrs(false, true, false), SaveStatus.PreAccepted));
        final static long NOTACCEPTED = measure(notAccepted(attrs(false, false, false), SaveStatus.AcceptedInvalidate));
        final static long ACCEPTED = measure(accepted(attrs(true, false, false), SaveStatus.AcceptedMedium));
        final static long COMMITTED = measure(committed(attrs(true, true, false), SaveStatus.Committed));
        final static long EXECUTED = measure(executed(attrs(true, true, true), SaveStatus.Applied));
        // TODO (expected): TruncatedAwaitsOnlyDeps
        final static long TRUNCATED = measure(vestigial(EMPTY_TXNID, attrs(false, false, false).participants()));
        final static long INVALIDATED = measure(invalidated(EMPTY_TXNID, attrs(false, false, false).participants()));

        private static long emptySize(Command command)
        {
            switch (command.saveStatus())
            {
                case Uninitialised:
                case NotDefined:
                    return NOT_DEFINED;
                case PreAccepted:
                case PreAcceptedWithDeps:
                case PreAcceptedWithVote:
                    return PREACCEPTED;
                case AcceptedInvalidate:
                    return NOTACCEPTED;
                case AcceptedInvalidateWithDefinition:
                case AcceptedMedium:
                case AcceptedMediumWithDefinition:
                case AcceptedSlow:
                case AcceptedSlowWithDefAndVote:
                case AcceptedSlowWithDefinition:
                case PreCommitted:
                case PreCommittedWithDeps:
                case PreCommittedWithFixedDeps:
                case PreCommittedWithDefinition:
                case PreCommittedWithDefAndDeps:
                case PreCommittedWithDefAndFixedDeps:
                    return ACCEPTED;
                case Committed:
                case ReadyToExecute:
                case Stable:
                    return COMMITTED;
                case PreApplied:
                case Applying:
                case Applied:
                    return EXECUTED;
                case TruncatedApply:
                case TruncatedUnapplied:
                case TruncatedApplyWithOutcome:
                case Vestigial:
                case Erased:
                    return TRUNCATED;
                case Invalidated:
                    return INVALIDATED;
                default:
                    throw new UnhandledEnum(command.saveStatus());
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
        size += sizeNullable(command.executeAt(), AccordObjectSizes::timestamp);
        size += sizeNullable(command.partialTxn(), AccordObjectSizes::txn);
        size += sizeNullable(command.partialDeps(), AccordObjectSizes::dependencies);
        size += sizeNullable(command.acceptedOrCommitted(), AccordObjectSizes::timestamp);
        size += sizeNullable(command.writes(), AccordObjectSizes::writes);
        size += sizeNullable(command.result(), AccordObjectSizes::results);
        size += sizeNullable(command.waitingOn(), AccordObjectSizes::waitingOn);
        return size;
    }

    private static long EMPTY_WAITING_ON_SIZE = measure(new WaitingOn(null, null, null, null));
    private static long EMPTY_BIT_SET_SIZE = measure(new ImmutableBitSet(0));
    private static long waitingOn(WaitingOn waitingOn)
    {
        // TODO (desired): this doesn't correctly account for object padding of bitset arrays
        long size =  EMPTY_WAITING_ON_SIZE + EMPTY_BIT_SET_SIZE + (waitingOn.waitingOn.size() * 8L);
        if (waitingOn.appliedOrInvalidated != null)
            size += EMPTY_BIT_SET_SIZE + (waitingOn.appliedOrInvalidated.size() * 8L);
        return size;
    }

    private static long EMPTY_CFK_SIZE = measure(new CommandsForKey(null));
    private static long EMPTY_INFO_SIZE = measure(CommandsForKey.NO_INFO);
    private static long EMPTY_INFO_EXTRA_ADDITIONAL_SIZE = measure(TxnInfo.create(TxnId.NONE, ACCEPTED, false, TxnId.NONE, NO_TXNIDS, Ballot.MAX)) - EMPTY_INFO_SIZE;
    public static long commandsForKey(CommandsForKey cfk)
    {
        long size = EMPTY_CFK_SIZE;
        size += key(cfk.key());
        size += ObjectSizes.sizeOfReferenceArray(cfk.size());
        size += cfk.size() * EMPTY_INFO_SIZE;
        for (int i = 0 ; i < cfk.size() ; ++i)
        {
            TxnInfo info = cfk.get(i);
            if (info.executeAt != info) size += TIMESTAMP_SIZE;
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
