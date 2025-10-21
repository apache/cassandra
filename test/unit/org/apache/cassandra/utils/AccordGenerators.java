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

import java.lang.reflect.Array;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Sets;

import accord.local.Command;
import accord.local.Command.Truncated;
import accord.local.ICommand;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.RedundantBefore;
import accord.local.RedundantBefore.Bounds;
import accord.local.StoreParticipants;
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
import accord.primitives.SaveStatus;
import accord.primitives.Seekables;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.topology.TopologyManager;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import accord.utils.ReducingRangeMap;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.TinyEnumSet;
import accord.utils.TriFunction;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.AccordSplitter;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.accord.FetchTopologies;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.quicktheories.impl.JavaRandom;

import static accord.local.CommandStores.RangesForEpoch;
import static accord.local.RedundantStatus.Property.GC_BEFORE;
import static accord.local.RedundantStatus.Property.PRE_BOOTSTRAP;
import static accord.local.RedundantStatus.SomeStatus.LOCALLY_APPLIED_ONLY;
import static accord.local.RedundantStatus.SomeStatus.LOCALLY_WITNESSED_ONLY;
import static accord.local.RedundantStatus.SomeStatus.SHARD_APPLIED_ONLY;
import static accord.local.RedundantStatus.oneSlow;
import static accord.primitives.Status.Durability.NotDurable;
import static accord.primitives.Timestamp.Flag.SHARD_BOUND;
import static accord.primitives.Txn.Kind.Write;
import static org.apache.cassandra.service.accord.AccordTestUtils.TABLE_ID1;
import static org.apache.cassandra.service.accord.AccordTestUtils.createPartialTxn;

public class AccordGenerators
{
    public static final Gen<IPartitioner> PARTITIONER_GEN = fromQT(CassandraGenerators.nonLocalPartitioners());
    public static final Gen<TableId> TABLE_ID_GEN = fromQT(CassandraGenerators.TABLE_ID_GEN);

    private AccordGenerators()
    {
    }

    public static boolean maybeUpdatePartitioner(List<Topology> topologies)
    {
        for (var t : topologies)
        {
            if (maybeUpdatePartitioner(t))
                return true;
        }
        return false;
    }

    public static boolean maybeUpdatePartitioner(Topology topology)
    {
        return maybeUpdatePartitioner(topology.ranges());
    }

    public static boolean maybeUpdatePartitioner(Ranges ranges)
    {
        if (ranges.isEmpty()) return false;
        for (Range range : ranges)
        {
            TokenRange tr = (TokenRange) range;
            maybeUpdatePartitioner(tr.start());
            return true;
        }
        return false;
    }

    public static void maybeUpdatePartitioner(TokenKey key)
    {
        DatabaseDescriptor.setPartitionerUnsafe(key.token().getPartitioner());
    }

    public static Gen<IPartitioner> partitioner()
    {
        return PARTITIONER_GEN.filter(IPartitioner::accordSupported);
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

        private ICommand attributes(SaveStatus saveStatus)
        {
            ICommand.Builder builder = new ICommand.Builder(txnId);
            if (saveStatus.known.isDefinitionKnown())
                builder.partialTxn(partialTxn);
            if (saveStatus.known.deps().hasPreAcceptedOrProposedOrDecidedDeps())
                builder.partialDeps(partialDeps);

            builder.setParticipants(StoreParticipants.all(route));
            builder.durability(NotDurable);
            if (saveStatus.compareTo(SaveStatus.PreAccepted) >= 0)
                builder.executeAt(executeAt);
            builder.promised(promised);
            if (saveStatus.status.compareTo(Status.PreAccepted) > 0)
                builder.acceptedOrCommitted(accepted);
            else
                builder.acceptedOrCommitted(Ballot.ZERO);
            if (saveStatus.compareTo(SaveStatus.Stable) >= 0 && !saveStatus.hasBeen(Status.Truncated))
                builder.waitingOn(waitingOn);
            if (saveStatus.hasBeen(Status.PreApplied) && !saveStatus.hasBeen(Status.Truncated))
            {
                if (txnId.is(Write))
                    builder.writes(new Writes(txnId, executeAt, keysOrRanges, new TxnWrite(TableMetadatas.none(), Collections.emptyList(), true)));
                builder.result(new TxnData());
            }
            return builder;
        }

        public Command build(SaveStatus saveStatus)
        {
            ICommand command = attributes(saveStatus);
            switch (saveStatus)
            {
                default: throw new AssertionError("Unhandled saveStatus: " + saveStatus);
                case Uninitialised:
                case NotDefined:
                    return Command.NotDefined.notDefined(command, Ballot.ZERO);
                case PreAccepted:
                case PreAcceptedWithVote:
                case PreAcceptedWithDeps:
                    return Command.PreAccepted.preaccepted(command, saveStatus);
                case AcceptedInvalidate:
                    return Command.NotAcceptedWithoutDefinition.acceptedInvalidate(command);

                case AcceptedMedium:
                case AcceptedMediumWithDefinition:
                case AcceptedMediumWithDefAndVote:
                case AcceptedInvalidateWithDefinition:
                case AcceptedSlow:
                case AcceptedSlowWithDefinition:
                case AcceptedSlowWithDefAndVote:
                case PreCommittedWithDefinition:
                case PreCommittedWithDeps:
                case PreCommittedWithFixedDeps:
                case PreCommittedWithDefAndDeps:
                case PreCommittedWithDefAndFixedDeps:
                case PreCommitted:
                    return Command.Accepted.accepted(command, saveStatus);

                case Committed:
                    return Command.Committed.committed(command, saveStatus);

                case Stable:
                case ReadyToExecute:
                    return Command.Committed.committed(command, saveStatus);

                case PreApplied:
                case Applying:
                case Applied:
                    return Command.Executed.executed(command, saveStatus);

                case TruncatedApply:
                case TruncatedUnapplied:
                    if (txnId.kind().awaitsOnlyDeps()) return Truncated.truncated(command, saveStatus, executeAt, null, null, null, txnId);
                    else return Truncated.truncated(command, saveStatus, executeAt, null, null, null, null);

                case TruncatedApplyWithOutcome:
                    if (txnId.kind().awaitsOnlyDeps()) return Truncated.truncated(command, saveStatus, executeAt, command.partialDeps(), txnId.is(Write) ? new Writes(txnId, executeAt, keysOrRanges, new TxnWrite(TableMetadatas.none(), Collections.emptyList(), true)) : null, new TxnData(), txnId);
                    else return Truncated.truncated(command, saveStatus, executeAt, command.partialDeps(), txnId.is(Write) ? new Writes(txnId, executeAt, keysOrRanges, new TxnWrite(TableMetadatas.none(), Collections.emptyList(), true)) : null, new TxnData(), null);

                case Erased:
                case Vestigial:
                case Invalidated:
                    return Truncated.invalidated(txnId, command.participants());
            }
        }
    }

    public static Gen<PartitionKey> keys()
    {
        return keys(TABLE_ID_GEN,
                    fromQT(CassandraGenerators.decoratedKeys()));
    }

    public static Gen<PartitionKey> keys(IPartitioner partitioner)
    {
        return keys(TABLE_ID_GEN,
                    fromQT(CassandraGenerators.decoratedKeys(ignore -> partitioner)));
    }

    public static Gen<PartitionKey> keys(IPartitioner partitioner, List<TableId> tables)
    {
        //TODO (correctness): fix Gens.pick to not fail with lists of size 1
        return keys(tables.size() == 1 ? Gens.constant(tables.get(0)) : Gens.pick(tables),
                    fromQT(CassandraGenerators.decoratedKeys(ignore -> partitioner)));
    }

    public static Gen<PartitionKey> keys(Gen<TableId> tableIdGen, Gen<DecoratedKey> key)
    {
        return rs -> new PartitionKey(tableIdGen.next(rs), key.next(rs));
    }

    public static Gen<TokenKey> routingKeysGen(IPartitioner partitioner)
    {
        return routingKeyGen(TABLE_ID_GEN,
                             fromQT(CassandraGenerators.token(partitioner)),
                             partitioner);
    }

    public static Gen<TokenKey> routingKeyGen(Gen<TableId> tableIdGen, Gen<Token> tokenGen, IPartitioner partitioner)
    {
        return routingKeyGen(tableIdGen, Gens.enums().all(RoutingKeyKind.class), tokenGen, partitioner);
    }

    public enum RoutingKeyKind
    {
        TOKEN, SENTINEL
    }

    public static Gen<TokenKey> routingKeyGen(Gen<TableId> tableIdGen, Gen<RoutingKeyKind> kindGen, Gen<Token> tokenGen, IPartitioner partitioner)
    {
        return rs -> {
            TableId tableId = tableIdGen.next(rs);
            RoutingKeyKind kind = kindGen.next(rs);
            switch (kind)
            {
                case TOKEN:
                    return new TokenKey(tableId, tokenGen.next(rs));
                case SENTINEL:
                    return rs.nextBoolean() ? TokenKey.min(tableId, partitioner) : TokenKey.max(tableId, partitioner);
                default:
                    throw new AssertionError("Unknown kind: " + kind);
            }
        };
    }

    public static Gen<TokenKey> allowBeforeAndAfter(Gen<TokenKey> gen)
    {
        return gen.map((rs, key) -> {
            if (key.isTokenSentinel()) return key;
            switch (rs.nextInt(0, 3))
            {
                case 0:  return key;
                case 1:  return key.before();
                case 2:  return key.after();
                default: throw new AssertionError();
            }
        });
    }

    public static Gen<Range> range()
    {
        return partitioner().flatMap(partitioner -> range(TABLE_ID_GEN, fromQT(CassandraGenerators.token(partitioner)), partitioner));
    }

    public static Gen<Range> range(IPartitioner partitioner)
    {
        return range(TABLE_ID_GEN, fromQT(CassandraGenerators.token(partitioner)), partitioner);
    }

    public static Gen<Range> range(IPartitioner partitioner, Gen<TableId> tables)
    {
        return range(tables, fromQT(CassandraGenerators.token(partitioner)), partitioner);
    }

    public static Gen<Range> range(Gen<TableId> tables, Gen<Token> tokenGen, IPartitioner partitioner)
    {
        return rs -> {
            Gen<TokenKey> gen = allowBeforeAndAfter(routingKeyGen(Gens.constant(tables.next(rs)), tokenGen, partitioner));
            TokenKey a = gen.next(rs);
            TokenKey b = gen.next(rs);
            while (same(a, b))
                b = gen.next(rs);
            return a.compareTo(b) < 0 ? TokenRange.create(a, b) : TokenRange.create(b, a);
        };
    }

    private static boolean same(TokenKey a, TokenKey b)
    {
        if (a.equals(b)) return true;
        // define +Inf == before(+Inf) as these are not actionable ranges
        return a.isTableSentinel() && b.isTableSentinel()
               && a.isMin() == b.isMin()
               && a.isMax() == b.isMax();
    }

    public static Gen<Ranges> ranges()
    {
        // javac couldn't pick the right constructor with HashSet::new, so had to create new lambda...
        return ranges(Gens.lists(TABLE_ID_GEN).unique().ofSizeBetween(1, 10), partitioner());
    }

    public static Gen<Ranges> ranges(Gen<List<TableId>> tableIdGen, Gen<IPartitioner> partitionerGen)
    {
        Gen.IntGen splitsGen = Gens.ints().between(10, 99);
        return ranges(tableIdGen, partitionerGen, splitsGen);
    }

    public static Gen<Ranges> ranges(Gen<List<TableId>> tableIdGen, Gen<IPartitioner> partitionerGen, Gen.IntGen splitsGen)
    {
        return rs -> {
            List<TableId> tables = tableIdGen.next(rs);
            IPartitioner partitioner = partitionerGen.next(rs);
            List<Range> ranges = new ArrayList<>();
            int numSplits = splitsGen.nextInt(rs);
            if (numSplits == 0) return Ranges.EMPTY;
            TokenRange range = TokenRange.create(TokenKey.min(TABLE_ID1, partitioner), TokenKey.max(TABLE_ID1, partitioner));
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
        return ranges(Gens.lists(TABLE_ID_GEN).unique().ofSizeBetween(1, 10), ignore -> partitioner);
    }

    public static Gen<Ranges> ranges(IPartitioner partitioner, Gen.IntGen splitsGen)
    {
        return ranges(Gens.lists(TABLE_ID_GEN).unique().ofSizeBetween(1, 10), ignore -> partitioner, splitsGen);
    }

    public static Gen<Ranges> ranges(TableId tableId, IPartitioner partitioner)
    {
        List<TableId> tables = Collections.singletonList(tableId);
        return ranges(i -> tables, i -> partitioner);
    }

    public static Gen<Ranges> rangesArbitrary(IPartitioner partitioner)
    {
        Gen.IntGen sizeGen = Gens.ints().between(0, 10);
        return rangesArbitrary(partitioner, sizeGen);
    }

    public static Gen<Ranges> rangesArbitrary(IPartitioner partitioner, Gen.IntGen sizeGen)
    {
        return rangesArbitrary(partitioner, TABLE_ID_GEN, sizeGen);
    }

    public static Gen<Ranges> rangesArbitrary(IPartitioner partitioner, Gen<TableId> tables, Gen.IntGen sizeGen)
    {
        Gen<Range> rangeGen = range(partitioner, tables);
        return rs -> {
            int targetSize = sizeGen.nextInt(rs);
            List<Range> ranges = new ArrayList<>(targetSize);
            for (int i = 0; i < targetSize; i++)
                ranges.add(rangeGen.next(rs));
            return Ranges.of(ranges.toArray(Range[]::new));
        };
    }

    public static Gen<Ranges> rangesSplitOrArbitrary(IPartitioner partitioner)
    {
        Gen<Ranges> split = ranges(partitioner);
        Gen<Ranges> arbitrary = rangesArbitrary(partitioner);
        return rs -> rs.nextBoolean() ? split.next(rs) : arbitrary.next(rs);
    }

    public static Gen<Ranges> rangesSplitOrArbitrary(IPartitioner partitioner, Gen.IntGen sizeGen)
    {
        return rangesSplitOrArbitrary(partitioner, sizeGen, Gens.lists(TABLE_ID_GEN).unique().ofSizeBetween(1, 10));
    }

    public static Gen<Ranges> rangesSplitOrArbitrary(IPartitioner partitioner, Gen.IntGen sizeGen, Gen<List<TableId>> tableIdGen)
    {
        Gen<Ranges> split = ranges(tableIdGen, i -> partitioner, sizeGen);
        Gen<Ranges> arbitrary = rangesArbitrary(partitioner, tableIdGen.map((rs, l) -> rs.pick(l)), sizeGen);
        return rs -> rs.nextBoolean() ? split.next(rs) : arbitrary.next(rs);
    }

    public static Gen<KeyDeps> keyDepsGen(IPartitioner partitioner)
    {
        return AccordGens.keyDeps(AccordGenerators.routingKeysGen(partitioner));
    }

    public static Gen<KeyDeps> directKeyDepsGen(IPartitioner partitioner)
    {
        return AccordGens.directKeyDeps(AccordGenerators.routingKeysGen(partitioner));
    }

    public static Gen<RangeDeps> rangeDepsGen(IPartitioner partitioner)
    {
        return AccordGens.rangeDeps(AccordGenerators.range(partitioner));
    }

    public static Gen<Deps> depsGen(IPartitioner partitioner)
    {
        return AccordGens.deps(keyDepsGen(partitioner), rangeDepsGen(partitioner));
    }

    public static Gen<Bounds> redundantBeforeEntry(IPartitioner partitioner)
    {
        return redundantBeforeEntry(Gens.bools().all(), range(partitioner), AccordGens.txnIds(Gens.pick(Txn.Kind.ExclusiveSyncPoint), ignore -> Routable.Domain.Range));
    }

    public static Gen<Bounds> redundantBeforeEntry(Gen<Boolean> emptyGen, Gen<Range> rangeGen, Gen<TxnId> txnIdGen)
    {
        return rs -> {
            Range range = rangeGen.next(rs);

            List<Bounds> bounds = new ArrayList<>();
            if (rs.nextBoolean())
                bounds.add(Bounds.create(range, txnIdGen.next(rs), LOCALLY_WITNESSED_ONLY, null ));
            if (rs.nextBoolean())
                bounds.add(Bounds.create(range, txnIdGen.next(rs), LOCALLY_APPLIED_ONLY, null ));
            if (rs.nextBoolean())
                bounds.add(Bounds.create(range, txnIdGen.next(rs), SHARD_APPLIED_ONLY, null ));
            if (rs.nextBoolean())
                bounds.add(Bounds.create(range, txnIdGen.next(rs).addFlag(SHARD_BOUND), oneSlow(GC_BEFORE), null ));
            if (rs.nextBoolean())
                bounds.add(Bounds.create(range, txnIdGen.next(rs), oneSlow(PRE_BOOTSTRAP), null ));
            if (rs.nextBoolean())
                bounds.add(new Bounds(range, Long.MIN_VALUE, Long.MAX_VALUE, new TxnId[0], new short[0], txnIdGen.next(rs)));

            Collections.shuffle(bounds);
            long endEpoch = emptyGen.next(rs) ? Long.MAX_VALUE : rs.nextLong(0, Long.MAX_VALUE);
            long minEpoch = Long.MAX_VALUE;
            Bounds result = null;
            for (Bounds b : bounds)
            {
                if (b.bounds.length > 0)
                    minEpoch = Math.min(minEpoch, b.bounds[0].epoch());
                if (result == null) result = b;
                else result = Bounds.reduce(result, b);
            }

            long startEpoch = rs.nextLong(Math.min(minEpoch, endEpoch));
            Bounds epochBounds = new Bounds(range, startEpoch, endEpoch, new TxnId[0], new short[0], null);
            if (result == null)
                return epochBounds;
            return Bounds.reduce(result, epochBounds);
        };
    }

    public static Gen<RedundantBefore> redundantBefore(IPartitioner partitioner)
    {
        Gen<Ranges> rangeGen = rangesArbitrary(partitioner);
        Gen<TxnId> txnIdGen = AccordGens.txnIds(Gens.pick(Txn.Kind.ExclusiveSyncPoint), ignore -> Routable.Domain.Range);
        BiFunction<RandomSource, Range, Bounds> entryGen = (rs, range) -> redundantBeforeEntry(Gens.bools().all(), i -> range, txnIdGen).next(rs);
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

    public static Gen<TinyEnumSet<Shard.Flag>> shardFlagsGen()
    {
        return rs -> {
            if (rs.nextBoolean()) return Shard.NO_FLAGS;
            EnumSet<Shard.Flag> flags = EnumSet.noneOf(Shard.Flag.class);
            for (Shard.Flag v : Shard.Flag.values())
            {
                if (rs.nextBoolean())
                    flags.add(v);
            }
            return new TinyEnumSet<>(flags.toArray(Shard.Flag[]::new));
        };
    }

    public static <T extends Comparable<? super T>> Gen<SortedArrayList<T>> sortedArrayList(Class<T> klass, Gen.IntGen sizeGen, Gen<T> valueGen)
    {
        return rs -> {
            int size = sizeGen.nextInt(rs);
            if (size == 0) return SortedArrayList.ofSorted();
            return SortedArrayList.copyUnsorted(Gens.lists(valueGen).unique().ofSize(size).next(rs), s -> (T[]) Array.newInstance(klass, s));
        };
    }

    private static <T> Gen<List<T>> select(List<T> list, int size)
    {
        // This is better in Gens, but didn't want to alter Accord in this patch...
        if (size < 0 || size > list.size())
            throw new IllegalArgumentException("Unexpected size: " + size + ", list size is " + list.size());
        if (size == 0) return i -> List.of();
        if (size == list.size()) return i -> list;
        return rs -> {
            List<T> toSelect = new ArrayList<>(list);
            List<T> selected = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
            {
                int idx = rs.nextInt(0, toSelect.size());
                selected.add(toSelect.remove(idx));
            }
            return selected;
        };
    }

    private static Gen<Shard> shardGen(Range range)
    {
        Gen<SortedArrayList<Node.Id>> nodesGen = sortedArrayList(Node.Id.class, Gens.ints().between(1, 10), AccordGens.nodes());
        Gen<TinyEnumSet<Shard.Flag>> shardFlagsGen = shardFlagsGen();
        return rs -> {
            SortedArrayList<Node.Id> nodes = nodesGen.next(rs);
            int maxFailures = Shard.maxToleratedFailures(nodes.size());
            int slowQuorumSize = Shard.slowQuorumSize(nodes.size());
            Set<Node.Id> fastPathElectorate = new TreeSet<>(select(nodes, nodes.size() == slowQuorumSize ? slowQuorumSize : rs.nextInt(slowQuorumSize, nodes.size())).next(rs));
            List<Node.Id> nonFastPath = new ArrayList<>(Sets.difference(new HashSet<>(nodes), fastPathElectorate));
            nonFastPath.sort(Comparator.naturalOrder());
            Set<Node.Id> joining = new TreeSet<>(select(nonFastPath, nonFastPath.size() == 0 ? 0 : rs.nextInt(0, nonFastPath.size())).next(rs));
            return Shard.create(range, nodes, fastPathElectorate, joining, shardFlagsGen.next(rs));
        };
    }

    public static Gen<Topology> topologyGen(IPartitioner partitioner)
    {
        return topologyGen(AccordGens.epochs(), partitioner);
    }

    public static Gen<Topology> topologyGen(Gen.LongGen epochGen, IPartitioner partitioner)
    {
        return topologyGen(epochGen, ranges(partitioner));
    }

    public static Gen<Topology> topologyGen(Gen<Ranges> rangesGen)
    {
        return topologyGen(AccordGens.epochs(), rangesGen);
    }

    public static Gen<Topology> topologyGen(Gen.LongGen epochGen, Gen<Ranges> rangesGen)
    {
        return rs -> {
            long epoch = epochGen.nextLong(rs);
            Ranges ranges = rangesGen.next(rs);
            if (ranges.isEmpty()) return new Topology(epoch, new Shard[0]);

            List<Shard> shards = new ArrayList<>(ranges.size());
            for (Range range : ranges)
                shards.add(shardGen(range).next(rs));
            //TODO (coverage): staleNodes
            return new Topology(epoch, shards.toArray(Shard[]::new));
        };
    }

    public static Gen<FetchTopologies> fetchTopologiesGen()
    {
        Gen.LongGen epochGen = AccordGens.epochs();
        Gen.LongGen maxEpochGen = rs -> {
            if (rs.decide(0.3))
                return Long.MAX_VALUE;
            return epochGen.nextLong(rs);
        };
        return rs -> {
            long a = epochGen.nextLong(rs);
            long b = maxEpochGen.nextLong(rs);
            while (a == b)
                b = maxEpochGen.nextLong(rs);
            if (a > b)
            {
                long tmp = a;
                a = b;
                b = tmp;
            }
            return new FetchTopologies(a, b);
        };
    }

    public static Gen<TopologyManager.TopologyRange> topologyRangeGen()
    {
        Gen.LongGen epochGen = AccordGens.epochs();
        return rs -> {
            // settle on 1 partitioner
            IPartitioner partitioner = partitioner().next(rs);
            Supplier<Topology> topologyGen = () -> {
                if (rs.decide(.3)) return Topology.EMPTY;
                return topologyGen(partitioner).next(rs);
            };

            // first figure out the min epoch, then generate a list of topologies
            long minEpoch = epochGen.nextLong(rs);
            if (minEpoch == Timestamp.MAX_EPOCH)
            {
                // not possible to have a list of values, so to simplfiy just return empty
                return new TopologyManager.TopologyRange(Timestamp.MAX_EPOCH, Timestamp.MAX_EPOCH, -1, Collections.emptyList());
            }
            long epochsRemaining = Timestamp.MAX_EPOCH - minEpoch;
            int size = rs.nextInt(1, Math.toIntExact(Math.min(100, epochsRemaining)));
            int numEmpty = rs.nextInt(0, size);

            List<Topology> topologies = new ArrayList<>(size);
            int offset = 0;
            for (int i = 0; i < numEmpty; i++)
                topologies.add(Topology.EMPTY.withEpoch(minEpoch + offset++));
            long firstNonEmpty = -1;
            for (int i = offset; i < size; i++)
            {
                Topology t = topologyGen.get().withEpoch(minEpoch + offset++);
                if (firstNonEmpty == -1 && !t.isEmpty())
                    firstNonEmpty = t.epoch();
                topologies.add(t);
            }
            return new TopologyManager.TopologyRange(topologies.get(0).epoch(), topologies.get(topologies.size() - 1).epoch(), firstNonEmpty, topologies);
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
