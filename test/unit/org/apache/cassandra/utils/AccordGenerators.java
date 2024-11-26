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

package org.apache.cassandra.utils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;

import accord.local.Command;
import accord.local.CommonAttributes;
import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.local.StoreParticipants;
import accord.primitives.SaveStatus;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.KeyDeps;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import accord.utils.ReducingRangeMap;
import accord.utils.TriFunction;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.AccordSplitter;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.quicktheories.impl.JavaRandom;

import static accord.local.CommandStores.RangesForEpoch;
import static accord.primitives.Status.Durability.NotDurable;
import static org.apache.cassandra.service.accord.AccordTestUtils.TABLE_ID1;
import static org.apache.cassandra.service.accord.AccordTestUtils.createPartialTxn;

public class AccordGenerators
{
    private static final Gen<IPartitioner> PARTITIONER_GEN = fromQT(CassandraGenerators.nonLocalPartitioners());

    private AccordGenerators()
    {
    }

    public static Gen<IPartitioner> partitioner()
    {
        return PARTITIONER_GEN;
    }

    private enum SupportedCommandTypes
    {notDefined, preaccepted, committed, stable}

    public static Gen<Command> commands()
    {
        Gen<TxnId> ids = AccordGens.txnIds();
        //TODO switch to Status once all types are supported
        Gen<SupportedCommandTypes> supportedTypes = Gens.enums().all(SupportedCommandTypes.class);
        //TODO goes against fuzz testing, and also limits to a very specific table existing...
        // There is a branch that can generate random transactions, so maybe look into that?
        PartialTxn txn = createPartialTxn(0);

        return rs -> {
            TxnId id = ids.next(rs);
            TxnId executeAt = id;
            if (rs.nextBoolean())
                executeAt = ids.next(rs);
            if (executeAt.compareTo(id) < 0)
            {
                TxnId tmp = id;
                id = executeAt;
                executeAt = tmp;
            }
            SupportedCommandTypes targetType = supportedTypes.next(rs);
            switch (targetType)
            {
                case notDefined:
                    return AccordTestUtils.Commands.notDefined(id, txn);
                case preaccepted:
                    return AccordTestUtils.Commands.preaccepted(id, txn, executeAt);
                case committed:
                    return AccordTestUtils.Commands.committed(id, txn, executeAt);
                case stable:
                    return AccordTestUtils.Commands.stable(id, txn, executeAt);
                default:
                    throw new UnsupportedOperationException("Unexpected type: " + targetType);
            }
        };
    }

    public enum RecoveryStatus { None, Started, Complete }

    public static Gen<CommandBuilder> commandsBuilder()
    {
        return commandsBuilder(AccordGens.txnIds(), Gens.bools().all(), Gens.enums().all(RecoveryStatus.class), (rs, txnId, txn) -> AccordGens.depsFor(txnId, txn).next(rs));
    }

    public static Gen<CommandBuilder> commandsBuilder(Gen<TxnId> txnIdGen, Gen<Boolean> fastPath, Gen<RecoveryStatus> recover, TriFunction<RandomSource, TxnId, Txn, Deps> depsGen)
    {
        return rs -> {
            TxnId txnId = txnIdGen.next(rs);
            Txn txn = AccordTestUtils.createTxn(0, 0);
            Deps deps = depsGen.apply(rs, txnId, txn);
            Timestamp executeAt = fastPath.next(rs) ? txnId
                                                    : AccordGens.timestamps(AccordGens.epochs(txnId.epoch()),
                                                                            AccordGens.hlcs(txnId.hlc()),
                                                                            AccordGens.flags(),
                                                                            RandomSource::nextInt).next(rs);
            Ranges slice = AccordTestUtils.fullRange(txn);
            PartialTxn partialTxn = txn.slice(slice, true); //TODO (correctness): find the case where includeQuery=false and replicate
            PartialDeps partialDeps = deps.intersecting(slice);
            Ballot promised;
            Ballot accepted;
            switch (recover.next(rs))
            {
                case None:
                {
                    promised = Ballot.ZERO;
                    accepted = Ballot.ZERO;
                }
                break;
                case Started:
                {
                    promised = AccordGens.ballot(AccordGens.epochs(executeAt.epoch()),
                                                 AccordGens.hlcs(executeAt.hlc()),
                                                 AccordGens.flags(),
                                                 RandomSource::nextInt).next(rs);
                    accepted = Ballot.ZERO;
                }
                break;
                case Complete:
                {
                    promised = accepted = AccordGens.ballot(AccordGens.epochs(executeAt.epoch()),
                                                            AccordGens.hlcs(executeAt.hlc()),
                                                            AccordGens.flags(),
                                                            RandomSource::nextInt).next(rs);
                }
                break;
                default:
                    throw new UnsupportedOperationException();
            }

            Command.WaitingOn waitingOn = Command.WaitingOn.none(txnId.domain(), deps);
            return new CommandBuilder(txnId, txn, executeAt, partialTxn, partialDeps, promised, accepted, waitingOn);
        };
    }

    public static class CommandBuilder
    {
        public final TxnId txnId;
        public final FullRoute<?> route;
        public final Seekables<?, ?> keysOrRanges;
        private final Timestamp executeAt;
        private final PartialTxn partialTxn;
        private final PartialDeps partialDeps;
        private final Ballot promised, accepted;
        private final Command.WaitingOn waitingOn;

        public CommandBuilder(TxnId txnId, Txn txn, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Ballot promised, Ballot accepted, Command.WaitingOn waitingOn)
        {
            this.txnId = txnId;
            this.executeAt = executeAt;
            this.partialTxn = partialTxn;
            this.partialDeps = partialDeps;
            this.promised = promised;
            this.accepted = accepted;
            this.waitingOn = waitingOn;
            this.route = txn.keys().toRoute(txn.keys().get(0).someIntersectingRoutingKey(null));
            this.keysOrRanges = txn.keys();
        }

        private CommonAttributes attributes(SaveStatus saveStatus)
        {
            CommonAttributes.Mutable mutable = new CommonAttributes.Mutable(txnId);
            if (saveStatus.known.isDefinitionKnown())
                mutable.partialTxn(partialTxn);
            if (saveStatus.known.deps.hasProposedOrDecidedDeps())
                mutable.partialDeps(partialDeps);

            mutable.setParticipants(StoreParticipants.all(route));
            mutable.durability(NotDurable);

            return mutable;
        }

        public Command build(SaveStatus saveStatus)
        {
            switch (saveStatus)
            {
                default: throw new AssertionError("Unhandled saveStatus: " + saveStatus);
                case TruncatedApplyWithDeps:
                    throw new IllegalArgumentException("TruncatedApplyWithDeps is not a valid state for a Command to be in, its for FetchData");
                case Uninitialised:
                case NotDefined:
                    return Command.SerializerSupport.notDefined(attributes(saveStatus), Ballot.ZERO);
                case PreAccepted:
                    return Command.SerializerSupport.preaccepted(attributes(saveStatus), executeAt, Ballot.ZERO);
                case AcceptedInvalidate:
                    return Command.AcceptedInvalidateWithoutDefinition.acceptedInvalidate(attributes(saveStatus), promised, Ballot.ZERO);

                case Accepted:
                case AcceptedWithDefinition:
                case AcceptedInvalidateWithDefinition:
                case PreCommittedWithDefinition:
                case PreCommittedWithDefinitionAndAcceptedDeps:
                case PreCommittedWithAcceptedDeps:
                case PreCommitted:
                    return Command.SerializerSupport.accepted(attributes(saveStatus), saveStatus, executeAt, promised, accepted);

                case Committed:
                    return Command.SerializerSupport.committed(attributes(saveStatus), saveStatus, executeAt, promised, accepted, null);

                case Stable:
                case ReadyToExecute:
                    return Command.SerializerSupport.committed(attributes(saveStatus), saveStatus, executeAt, promised, accepted, waitingOn);

                case PreApplied:
                case Applying:
                case Applied:
                    return Command.SerializerSupport.executed(attributes(saveStatus), saveStatus, executeAt, promised, accepted, waitingOn, new Writes(txnId, executeAt, keysOrRanges, new TxnWrite(Collections.emptyList(), true)), new TxnData());

                case TruncatedApply:
                    if (txnId.kind().awaitsOnlyDeps()) return Command.SerializerSupport.truncatedApply(attributes(saveStatus), saveStatus, executeAt, null, null, txnId);
                    else return Command.SerializerSupport.truncatedApply(attributes(saveStatus), saveStatus, executeAt, null, null);

                case TruncatedApplyWithOutcome:
                    if (txnId.kind().awaitsOnlyDeps()) return Command.SerializerSupport.truncatedApply(attributes(saveStatus), saveStatus, executeAt, new Writes(txnId, executeAt, keysOrRanges, new TxnWrite(Collections.emptyList(), true)), new TxnData(), txnId);
                    else return Command.SerializerSupport.truncatedApply(attributes(saveStatus), saveStatus, executeAt, new Writes(txnId, executeAt, keysOrRanges, new TxnWrite(Collections.emptyList(), true)), new TxnData());

                case Erased:
                case ErasedOrVestigial:
                case Invalidated:
                    return Command.SerializerSupport.invalidated(txnId);
            }
        }
    }

    public static Gen<PartitionKey> keys()
    {
        return keys(fromQT(CassandraGenerators.TABLE_ID_GEN),
                    fromQT(CassandraGenerators.decoratedKeys()));
    }

    public static Gen<PartitionKey> keys(IPartitioner partitioner)
    {
        return keys(fromQT(CassandraGenerators.TABLE_ID_GEN),
                    fromQT(CassandraGenerators.decoratedKeys(ignore -> partitioner)));
    }

    public static Gen<PartitionKey> keys(Gen<TableId> tableIdGen, Gen<DecoratedKey> key)
    {
        return rs -> new PartitionKey(tableIdGen.next(rs), key.next(rs));
    }

    public static Gen<AccordRoutingKey> routingKeys()
    {
        return routingKeyGen(fromQT(CassandraGenerators.TABLE_ID_GEN),
                    fromQT(CassandraGenerators.token()));
    }

    public static Gen<AccordRoutingKey> routingKeys(IPartitioner partitioner)
    {
        return routingKeyGen(fromQT(CassandraGenerators.TABLE_ID_GEN),
                             fromQT(CassandraGenerators.token(partitioner)));
    }

    public static Gen<AccordRoutingKey> routingKeyGen(Gen<TableId> tableIdGen, Gen<Token> tokenGen)
    {
        return routingKeyGen(tableIdGen, Gens.enums().all(AccordRoutingKey.RoutingKeyKind.class), tokenGen);
    }

    public static Gen<AccordRoutingKey> routingKeyGen(Gen<TableId> tableIdGen, Gen<AccordRoutingKey.RoutingKeyKind> kindGen, Gen<Token> tokenGen)
    {
        return rs -> {
            TableId tableId = tableIdGen.next(rs);
            AccordRoutingKey.RoutingKeyKind kind = kindGen.next(rs);
            switch (kind)
            {
                case TOKEN:
                    return new AccordRoutingKey.TokenKey(tableId, tokenGen.next(rs));
                case SENTINEL:
                    return rs.nextBoolean() ? AccordRoutingKey.SentinelKey.min(tableId) : AccordRoutingKey.SentinelKey.max(tableId);
                default:
                    throw new AssertionError("Unknown kind: " + kind);
            }
        };
    }

    public static Gen<Range> range()
    {
        return PARTITIONER_GEN.flatMap(partitioner -> range(fromQT(CassandraGenerators.TABLE_ID_GEN), fromQT(CassandraGenerators.token(partitioner))));
    }

    public static Gen<Range> range(IPartitioner partitioner)
    {
        return range(fromQT(CassandraGenerators.TABLE_ID_GEN), fromQT(CassandraGenerators.token(partitioner)));
    }

    public static Gen<Range> range(Gen<TableId> tables, Gen<Token> tokenGen)
    {
        return rs -> {
            Gen<AccordRoutingKey> gen = routingKeyGen(Gens.constant(tables.next(rs)), tokenGen);
            AccordRoutingKey a = gen.next(rs);
            AccordRoutingKey b = gen.next(rs);
            while (a.equals(b))
                b = gen.next(rs);
            if (a.compareTo(b) < 0) return new TokenRange(a, b);
            else                    return new TokenRange(b, a);
        };
    }

    public static Gen<Ranges> ranges()
    {
        // javac couldn't pick the right constructor with HashSet::new, so had to create new lambda...
        return ranges(Gens.lists(fromQT(CassandraGenerators.TABLE_ID_GEN)).unique().ofSizeBetween(1, 10).map(l -> new HashSet<>(l)), PARTITIONER_GEN);
    }

    public static Gen<Ranges> ranges(Gen<Set<TableId>> tableIdGen, Gen<IPartitioner> partitionerGen)
    {
        return rs -> {
            Set<TableId> tables = tableIdGen.next(rs);
            IPartitioner partitioner = partitionerGen.next(rs);
            List<Range> ranges = new ArrayList<>();
            int numSplits = rs.nextInt(10, 100);
            TokenRange range = new TokenRange(AccordRoutingKey.SentinelKey.min(TABLE_ID1), AccordRoutingKey.SentinelKey.max(TABLE_ID1));
            AccordSplitter splitter = partitioner.accordSplitter().apply(Ranges.of(range));
            BigInteger size = splitter.sizeOf(range);
            BigInteger update = splitter.divide(size, numSplits);
            BigInteger offset = BigInteger.ZERO;
            while (offset.compareTo(size) < 0)
            {
                BigInteger end = offset.add(update);
                TokenRange r = splitter.subRange(range, offset, end);
                for (TableId id : tables)
                {
                    ranges.add(r.withTable(id));
                }
                offset = end;
            }
            return Ranges.of(ranges.toArray(new Range[0]));
        };
    }

    public static Gen<Ranges> ranges(IPartitioner partitioner)
    {
        return ranges(Gens.lists(fromQT(CassandraGenerators.TABLE_ID_GEN)).unique().ofSizeBetween(1, 10).map(l -> new HashSet<>(l)), ignore -> partitioner);
    }

    public static Gen<Ranges> rangesArbitrary(IPartitioner partitioner)
    {
        Gen<Range> rangeGen = range(partitioner);
        Gen.IntGen sizeGen = Gens.ints().between(0, 10);
        return rs -> {
            int targetSize = sizeGen.nextInt(rs);
            List<Range> ranges = new ArrayList<>(targetSize);
            for (int i = 0; i < targetSize; i++)
                ranges.add(rangeGen.next(rs));
            return Ranges.of(ranges.toArray(Range[]::new));
        };
    }

    public static Gen<KeyDeps> keyDepsGen()
    {
        return AccordGens.keyDeps(AccordGenerators.routingKeys());
    }

    public static Gen<KeyDeps> keyDepsGen(IPartitioner partitioner)
    {
        return AccordGens.keyDeps(AccordGenerators.routingKeys(partitioner));
    }

    public static Gen<KeyDeps> directKeyDepsGen()
    {
        return AccordGens.directKeyDeps(AccordGenerators.routingKeys());
    }

    public static Gen<KeyDeps> directKeyDepsGen(IPartitioner partitioner)
    {
        return AccordGens.directKeyDeps(AccordGenerators.routingKeys(partitioner));
    }

    public static Gen<RangeDeps> rangeDepsGen()
    {
        return AccordGens.rangeDeps(AccordGenerators.range());
    }

    public static Gen<RangeDeps> rangeDepsGen(IPartitioner partitioner)
    {
        return AccordGens.rangeDeps(AccordGenerators.range(partitioner));
    }

    public static Gen<Deps> depsGen()
    {
        return AccordGens.deps(keyDepsGen(), rangeDepsGen(), directKeyDepsGen());
    }

    public static Gen<Deps> depsGen(IPartitioner partitioner)
    {
        return AccordGens.deps(keyDepsGen(partitioner), rangeDepsGen(partitioner), directKeyDepsGen(partitioner));
    }

    public static Gen<RedundantBefore.Entry> redundantBeforeEntry(IPartitioner partitioner)
    {
        return redundantBeforeEntry(Gens.bools().all(), range(partitioner), AccordGens.txnIds(Gens.pick(Txn.Kind.ExclusiveSyncPoint), ignore -> Routable.Domain.Range));
    }

    public static Gen<RedundantBefore.Entry> redundantBeforeEntry(Gen<Boolean> emptyGen, Gen<Range> rangeGen, Gen<TxnId> txnIdGen)
    {
        return rs -> {
            Range range = rangeGen.next(rs);
            TxnId locallyWitnessedOrInvalidatedBefore = emptyGen.next(rs) ? TxnId.NONE : txnIdGen.next(rs); // emptyable or range
            TxnId locallyAppliedOrInvalidatedBefore = TxnId.nonNullOrMin(locallyWitnessedOrInvalidatedBefore, emptyGen.next(rs) ? TxnId.NONE : txnIdGen.next(rs)); // emptyable or range
            TxnId locallyDecidedAndAppliedOrInvalidatedBefore = TxnId.nonNullOrMin(locallyAppliedOrInvalidatedBefore, emptyGen.next(rs) ? TxnId.NONE : txnIdGen.next(rs)); // emptyable or range
            TxnId shardOnlyAppliedOrInvalidatedBefore = emptyGen.next(rs) ? TxnId.NONE : txnIdGen.next(rs); // emptyable or range
            TxnId shardAppliedOrInvalidatedBefore = TxnId.nonNullOrMin(locallyAppliedOrInvalidatedBefore, TxnId.nonNullOrMin(shardOnlyAppliedOrInvalidatedBefore, emptyGen.next(rs) ? TxnId.NONE : txnIdGen.next(rs))); // emptyable or range
            TxnId gcBefore = TxnId.nonNullOrMin(shardAppliedOrInvalidatedBefore, emptyGen.next(rs) ? TxnId.NONE : txnIdGen.next(rs)); // emptyable or range
            TxnId bootstrappedAt = txnIdGen.next(rs);
            Timestamp staleUntilAtLeast = emptyGen.next(rs) ? null : txnIdGen.next(rs); // nullable

            long maxEpoch = Stream.of(locallyAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, bootstrappedAt, staleUntilAtLeast).filter(t -> t != null).mapToLong(Timestamp::epoch).max().getAsLong();
            long startEpoch = rs.nextLong(maxEpoch);
            long endEpoch = emptyGen.next(rs) ? Long.MAX_VALUE : 1 + rs.nextLong(startEpoch, Long.MAX_VALUE);
            return new RedundantBefore.Entry(range, startEpoch, endEpoch, locallyWitnessedOrInvalidatedBefore, locallyAppliedOrInvalidatedBefore, locallyDecidedAndAppliedOrInvalidatedBefore, shardOnlyAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, gcBefore, bootstrappedAt, staleUntilAtLeast);
        };
    }

    public static Gen<RedundantBefore> redundantBefore(IPartitioner partitioner)
    {
        Gen<Ranges> rangeGen = rangesArbitrary(partitioner);
        Gen<TxnId> txnIdGen = AccordGens.txnIds(Gens.pick(Txn.Kind.ExclusiveSyncPoint), ignore -> Routable.Domain.Range);
        BiFunction<RandomSource, Range, RedundantBefore.Entry> entryGen = (rs, range) -> redundantBeforeEntry(Gens.bools().all(), i -> range, txnIdGen).next(rs);
        return AccordGens.redundantBefore(rangeGen, entryGen);
    }

    public static Gen<DurableBefore> durableBeforeGen(IPartitioner partitioner)
    {
        Gen<Ranges> rangeGen = rangesArbitrary(partitioner);
        Gen<TxnId> txnIdGen = AccordGens.txnIds(Gens.pick(Txn.Kind.ExclusiveSyncPoint), ignore -> Routable.Domain.Range);

        return (rs) -> {
            Ranges ranges = rangeGen.next(rs);
            TxnId majority = txnIdGen.next(rs);
            TxnId universal = majority;
            return DurableBefore.create(ranges, majority, universal);
        };
    }

    public static Gen<ReducingRangeMap<Timestamp>> rejectBeforeGen(IPartitioner partitioner)
    {
        Gen<Ranges> rangeGen = rangesArbitrary(partitioner);
        Gen<Timestamp> timestampGen = AccordGens.timestamps();

        return (rs) -> {
            ReducingRangeMap<Timestamp> initial = new ReducingRangeMap<>();
            int size = rs.nextInt(10);
            for (int i = 0; i < size; i++)
                initial = ReducingRangeMap.add(initial, rangeGen.next(rs), timestampGen.next(rs));

            return initial;
        };
    }

    public static Gen<NavigableMap<Timestamp, Ranges>> safeToReadGen(IPartitioner partitioner)
    {
        Gen<Ranges> rangeGen = ranges(partitioner);
        Gen<Timestamp> timestampGen = AccordGens.timestamps();

        return (rs) -> {
            ImmutableMap.Builder<Timestamp, Ranges> initial = new ImmutableSortedMap.Builder<>(Comparator.comparing(o -> o));
            int size = rs.nextInt(10);
            for (int i = 0; i < size; i++)
                initial.put(timestampGen.next(rs), rangeGen.next(rs));

            return (NavigableMap<Timestamp, Ranges>) initial.build();
        };
    }

    public static Gen<RangesForEpoch> rangesForEpoch(IPartitioner partitioner)
    {
        Gen<Ranges> rangesGen = ranges(partitioner);

        return rs -> {
            int size = rs.nextInt(1, 5);
            long[] epochs = new long[size];
            for (int i = 0; i < size; i++)
                epochs[i] = rs.nextLong(1, 10_000);
            Ranges[] ranges = new Ranges[size];
            for (int i = 0; i < size; i++)
                ranges[i] = rangesGen.next(rs);
            return new RangesForEpoch(epochs, ranges);
        };
    }

    public static <T> Gen<T> fromQT(org.quicktheories.core.Gen<T> qt)
    {
        return rs -> {
            JavaRandom r = new JavaRandom(rs.asJdkRandom());
            return qt.generate(r);
        };
    }
}
