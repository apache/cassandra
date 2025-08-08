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

package org.apache.cassandra.service.accord.serializers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.Agent;
import accord.api.AsyncExecutor;
import accord.api.DataStore;
import accord.api.Journal;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.api.Timeouts;
import accord.impl.AbstractSafeCommandStore;
import accord.impl.DefaultLocalListeners;
import accord.impl.DefaultRemoteListeners;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.DurableBefore;
import accord.local.ICommand;
import accord.local.Node;
import accord.local.NodeCommandStoreService;
import accord.local.PreLoadContext;
import accord.local.RedundantBefore;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SequentialAsyncExecutor;
import accord.local.StoreParticipants;
import accord.local.TimeService;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.CommandsForKey.InternalStatus;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.local.cfk.CommandsForKey.Unmanaged;
import accord.local.cfk.SafeCommandsForKey;
import accord.local.cfk.Serialize;
import accord.local.durability.DurabilityService;
import accord.messages.ReplyContext;
import accord.primitives.Ballot;
import accord.primitives.KeyDeps;
import accord.primitives.Known;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.topology.TopologyManager;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import accord.utils.SortedArrays;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncChain;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.simulator.RandomSource.Choices;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.CassandraGenerators;

import static accord.api.ProtocolModifiers.Toggles.setTransitiveDependenciesAreVisible;
import static accord.local.cfk.CommandsForKey.NO_BOUNDS_INFO;
import static accord.primitives.Known.KnownExecuteAt.ExecuteAtErased;
import static accord.primitives.Known.KnownExecuteAt.ExecuteAtUnknown;
import static accord.primitives.Status.Durability.AllQuorums;
import static accord.primitives.Status.Durability.NotDurable;
import static accord.utils.Property.qt;
import static accord.utils.SortedArrays.Search.FAST;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTestUtils.createPartialTxn;

// TODO (required): test statusOverrides
public class CommandsForKeySerializerTest
{
    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        // need to create the accord test table as generating random txn is not currently supported
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks"));
        setTransitiveDependenciesAreVisible(Kind.values());
        StorageService.instance.initServer();
    }

    @Before
    public void before() throws Throwable
    {
        CommandsForKey.disableLinearizabilityViolationsReporting();
    }

    @After
    public void after() throws Throwable
    {
        CommandsForKey.enableLinearizabilityViolationsReporting();
    }

    static class Cmd
    {
        final TxnId txnId;
        final SaveStatus saveStatus;
        final PartialTxn txn;
        final Timestamp executeAt;
        final Ballot ballot;
        final boolean isDurable;
        final List<TxnId> deps = new ArrayList<>();
        final List<TxnId> missing = new ArrayList<>();
        boolean invisible;

        Cmd(TxnId txnId, PartialTxn txn, SaveStatus saveStatus, boolean isDurable, Timestamp executeAt, Ballot ballot)
        {
            this.txnId = txnId;
            this.saveStatus = saveStatus;
            this.txn = txn;
            this.executeAt = executeAt;
            this.ballot = ballot;
            this.isDurable = isDurable;
        }

        ICommand.Builder builder()
        {
            ICommand.Builder builder = new ICommand.Builder(txnId);
            if (saveStatus.known.isDefinitionKnown())
                builder.partialTxn(txn);

            builder.setParticipants(StoreParticipants.all(txn.keys().toRoute(txn.keys().get(0).someIntersectingRoutingKey(null))));
            builder.durability(isDurable ? AllQuorums : NotDurable);
            if (saveStatus.known.deps().hasPreAcceptedOrProposedOrDecidedDeps())
            {
                try (KeyDeps.Builder keyBuilder = KeyDeps.builder();)
                {
                    for (TxnId id : deps)
                        keyBuilder.add(((Key)txn.keys().get(0)).toUnseekable(), id);
                    builder.partialDeps(new PartialDeps(AccordTestUtils.fullRange(txn), keyBuilder.build(), RangeDeps.NONE));
                }
            }

            builder.executeAt(executeAt);
            builder.promised(ballot);
            builder.acceptedOrCommitted(ballot);
            builder.durability(isDurable ? AllQuorums : NotDurable);
            if (saveStatus.compareTo(SaveStatus.Stable) >= 0 && !saveStatus.hasBeen(Status.Truncated))
                builder.waitingOn(Command.WaitingOn.empty(txnId.domain()));

            if (saveStatus.known.outcome() == Known.Outcome.Apply)
            {
                if (txnId.is(Kind.Write))
                    builder.writes(new Writes(txnId, executeAt, txn.keys(), new TxnWrite(TableMetadatas.none(), Collections.emptyList(), true)));
                builder.result(new TxnData());
            }
            return builder;
        }

        Command toCommand()
        {
            switch (saveStatus)
            {
                default: throw new AssertionError("Unhandled saveStatus: " + saveStatus);
                case Uninitialised:
                case NotDefined:
                    return Command.NotDefined.notDefined(builder(), Ballot.ZERO);
                case PreAccepted:
                case PreAcceptedWithVote:
                case PreAcceptedWithDeps:
                    return Command.PreAccepted.preaccepted(builder(), saveStatus);
                case AcceptedInvalidate:
                    return Command.NotAcceptedWithoutDefinition.notAccepted(builder(), saveStatus);
                case AcceptedMedium:
                case AcceptedMediumWithDefinition:
                case AcceptedMediumWithDefAndVote:
                case AcceptedSlow:
                case AcceptedSlowWithDefinition:
                case AcceptedSlowWithDefAndVote:
                case AcceptedInvalidateWithDefinition:
                case PreCommittedWithDefinition:
                case PreCommittedWithDefAndDeps:
                case PreCommittedWithDefAndFixedDeps:
                case PreCommittedWithDeps:
                case PreCommittedWithFixedDeps:
                case PreCommitted:
                    return Command.Accepted.accepted(builder(), saveStatus);

                case Committed:
                    return Command.Committed.committed(builder(), saveStatus);

                case Stable:
                case ReadyToExecute:
                    return Command.Committed.committed(builder(), saveStatus);

                case PreApplied:
                case Applied:
                    return Command.Executed.executed(builder(), saveStatus);

                case Invalidated:
                    return Command.Truncated.invalidated(txnId, builder().participants());
            }
        }

        @Override
        public String toString()
        {
            return "Cmd{" +
                   "txnId=" + txnId +
                   ", saveStatus=" + saveStatus +
                   ", txn=" + txn +
                   ", executeAt=" + executeAt +
                   ", deps=" + deps +
                   ", missing=" + missing +
                   ", invisible=" + invisible +
                   '}';
        }
    }

    static class ObjectGraph
    {
        final Cmd[] cmds;
        ObjectGraph(Cmd[] cmds)
        {
            this.cmds = cmds;
        }

        List<Command> toCommands()
        {
            List<Command> commands = new ArrayList<>(cmds.length);
            for (int i = 0 ; i < cmds.length ; ++i)
                commands.add(cmds[i].toCommand());
            return commands;
        }
    }

    private static ObjectGraph generateObjectGraph(int txnIdCount, Supplier<TxnId> txnIdSupplier, Supplier<SaveStatus> saveStatusSupplier, Function<TxnId, PartialTxn> txnSupplier, Function<TxnId, Timestamp> timestampSupplier, Supplier<Ballot> ballotSupplier, IntSupplier missingCountSupplier, RandomSource source)
    {
        Cmd[] cmds = new Cmd[txnIdCount];
        for (int i = 0 ; i < txnIdCount ; ++i)
        {
            TxnId txnId = txnIdSupplier.get();
            SaveStatus saveStatus = saveStatusSupplier.get();
            Timestamp executeAt = txnId;
            if (!txnId.kind().awaitsOnlyDeps() && !saveStatus.known.is(ExecuteAtErased) && !saveStatus.known.is(ExecuteAtUnknown))
                executeAt = timestampSupplier.apply(txnId);

            boolean isDurable = false;
            Ballot ballot;
            switch (saveStatus.status)
            {
                default: throw new UnhandledEnum(saveStatus.status);
                case NotDefined:
                case PreAccepted:
                case Invalidated:
                case Truncated:
                    ballot = Ballot.ZERO;
                    break;
                case PreApplied:
                case Applied:
                    isDurable = source.nextBoolean();
                case AcceptedInvalidate:
                case AcceptedMedium:
                case AcceptedSlow:
                case PreCommitted:
                case Committed:
                case Stable:
                    ballot = ballotSupplier.get();
            }

            cmds[i] = new Cmd(txnId, txnSupplier.apply(txnId), saveStatus, isDurable, executeAt, ballot);
        }
        Arrays.sort(cmds, Comparator.comparing(o -> o.txnId));
        for (int i = 0 ; i < txnIdCount ; ++i)
        {
            if (!cmds[i].saveStatus.known.deps().hasPreAcceptedOrProposedOrDecidedDeps())
                continue;

            Timestamp knownBefore = cmds[i].saveStatus.known.deps().hasCommittedOrDecidedDeps() ? cmds[i].executeAt : cmds[i].txnId;
            int limit = SortedArrays.binarySearch(cmds, 0, cmds.length, knownBefore, (a, b) -> a.compareTo(b.txnId), FAST);
            if (limit < 0) limit = -1 - limit;

            List<TxnId> deps = cmds[i].deps;
            List<TxnId> missing = cmds[i].missing;
            for (int j = 0 ; j < limit ; ++j)
            {
                if (i != j && cmds[i].txnId.kind().witnesses(cmds[j].txnId))
                    deps.add(cmds[j].txnId);
            }

            int missingCount = Math.min(deps.size(), missingCountSupplier.getAsInt());
            while (missingCount > 0)
            {
                int remove = source.nextInt(deps.size());
                int cmdIndex = SortedArrays.binarySearch(cmds, 0, cmds.length, deps.get(remove), (a, b) -> a.compareTo(b.txnId), FAST);
                if (!cmds[cmdIndex].saveStatus.hasBeen(Status.Committed))
                    missing.add(deps.get(remove));
                deps.set(remove, deps.get(deps.size() - 1));
                deps.remove(deps.size() - 1);
                --missingCount;
            }
            deps.sort(TxnId::compareTo);
            missing.sort(TxnId::compareTo);
        }

        outer: for (int i = 0 ; i < cmds.length ; ++i)
        {
            if (null != InternalStatus.from(cmds[i].saveStatus))
                continue;

            for (int j = 0 ; j < i ; ++j)
            {
                InternalStatus status = InternalStatus.from(cmds[j].saveStatus);
                if (status == null || !status.hasExecuteAtOrDeps()) continue;
                if (cmds[j].txnId.kind().witnesses(cmds[i].txnId) && status.depsKnownBefore(cmds[j].txnId, cmds[j].executeAt).compareTo(cmds[i].txnId) > 0 && Collections.binarySearch(cmds[j].missing, cmds[i].txnId) < 0)
                    continue outer;
            }
            for (int j = i + 1 ; j < cmds.length ; ++j)
            {
                InternalStatus status = InternalStatus.from(cmds[j].saveStatus);
                if (status == null || !status.hasExecuteAtOrDeps()) continue;
                if (cmds[j].txnId.kind().witnesses(cmds[i].txnId) && Collections.binarySearch(cmds[j].missing, cmds[i].txnId) < 0)
                    continue outer;
            }
            cmds[i].invisible = true;
            for (int j = 0 ; j < i ; ++j)
            {
                if (cmds[j].executeAt.compareTo(cmds[i].txnId) > 0)
                {
                    int remove = Collections.binarySearch(cmds[j].missing, cmds[i].txnId);
                    if (remove >= 0) cmds[j].missing.remove(remove);
                }
            }
            for (int j = i + 1 ; j < cmds.length ; ++j)
            {
                int remove = Collections.binarySearch(cmds[j].missing, cmds[i].txnId);
                if (remove >= 0) cmds[j].missing.remove(remove);
            }
        }
        return new ObjectGraph(cmds);
    }

    private static Function<Timestamp, TxnId> txnIdSupplier(LongUnaryOperator epochSupplier, LongUnaryOperator hlcSupplier, Supplier<Kind> kindSupplier, Supplier<Node.Id> idSupplier)
    {
        return min -> new TxnId(epochSupplier.applyAsLong(min == null ? 1 : min.epoch()), hlcSupplier.applyAsLong(min == null ? 1 : min.hlc() + 1), kindSupplier.get(), Routable.Domain.Key, idSupplier.get());
    }

    private static Function<Timestamp, Timestamp> timestampSupplier(LongUnaryOperator epochSupplier, LongUnaryOperator hlcSupplier, IntSupplier flagSupplier, Supplier<Node.Id> idSupplier)
    {
        return min -> Timestamp.fromValues(epochSupplier.applyAsLong(min == null ? 1 : min.epoch()), hlcSupplier.applyAsLong(min == null ? 1 : min.hlc() + 1), flagSupplier.getAsInt(), idSupplier.get());
    }

    private static Supplier<Ballot> ballotSupplier(LongUnaryOperator epochSupplier, LongUnaryOperator hlcSupplier, IntSupplier flagSupplier, Supplier<Node.Id> idSupplier)
    {
        return () -> Ballot.fromValues(epochSupplier.applyAsLong(1), hlcSupplier.applyAsLong(1), flagSupplier.getAsInt(), idSupplier.get());
    }

    private static <T extends Timestamp> Function<Timestamp, T> timestampSupplier(Set<Timestamp> unique, Function<Timestamp, T> supplier)
    {
        return min -> {
            T candidate = supplier.apply(min);
            while (!unique.add(candidate))
            {
                T next = supplier.apply(min);
                if (next.equals(candidate)) min = candidate;
                else candidate = next;
            }
            return candidate;
        };
    }

    @Test
    public void serde()
    {
        testOne(7082228630293368049L);
        Random random = new Random();
        for (int i = 0 ; i < 10000 ; ++i)
        {
            long seed = random.nextLong();
            testOne(seed);
        }
    }

    private static void testOne(long seed)
    {
        try
        {
            System.out.println(seed);
            RandomSource source = RandomSource.wrap(new Random(seed));

            // TODO (required): produce broader variety of distributions, including executeAt with lower HLC but higher epoch
            final LongUnaryOperator epochSupplier; {
                long maxEpoch = source.nextLong(1, 10);
                epochSupplier = min -> min >= maxEpoch ? min : maxEpoch == 1 ? 1 : source.nextLong(min, maxEpoch);
            }
            final LongUnaryOperator hlcSupplier; {
                long maxHlc = source.nextLong(10, 1000000);
                hlcSupplier = min -> min >= maxHlc ? min : source.nextLong(min, maxHlc);
            }
            final Supplier<Node.Id> idSupplier; {
                int maxId = source.nextInt(1, 10);
                Int2ObjectHashMap<Node.Id> lookup = new Int2ObjectHashMap<>();
                idSupplier = () -> lookup.computeIfAbsent(maxId == 1 ? 1 : source.nextInt(1, maxId), Node.Id::new);
            }
            final IntSupplier flagSupplier = () -> 0;
            final Supplier<Kind> kindSupplier = () -> {
                float v = source.nextFloat();
                if (v < 0.5) return Kind.Read;
                if (v < 0.97) return Kind.Write;
                return Kind.ExclusiveSyncPoint;
            };

            boolean permitMissing = source.decide(0.75f);
            final IntSupplier missingCountSupplier; {
                if (!permitMissing)
                {
                    missingCountSupplier = () -> 0;
                }
                else
                {
                    float zeroChance = source.nextFloat();
                    int maxMissing = source.nextInt(1, 10);
                    missingCountSupplier = () -> {
                        float v = source.nextFloat();
                        if (v < zeroChance) return 0;
                        return source.nextInt(0, maxMissing);
                    };
                }
            }

            Choices<SaveStatus> saveStatusChoices = Choices.uniform(EnumSet.complementOf(EnumSet.of(SaveStatus.Applying, SaveStatus.TruncatedApply, SaveStatus.TruncatedUnapplied, SaveStatus.TruncatedApplyWithOutcome)).toArray(SaveStatus[]::new));
            Supplier<SaveStatus> saveStatusSupplier = () -> {
                SaveStatus result = saveStatusChoices.choose(source);
                while (result.is(Status.Truncated)) // we don't currently process truncations
                    result = saveStatusChoices.choose(source);
                return result;
            };

            Set<Timestamp> uniqueTs = new TreeSet<>();
            final Function<Timestamp, TxnId> txnIdSupplier = timestampSupplier(uniqueTs, txnIdSupplier(epochSupplier, hlcSupplier, kindSupplier, idSupplier));
            boolean permitExecuteAt = source.decide(0.75f);
            final Function<TxnId, Timestamp> executeAtSupplier;
            {
                if (!permitExecuteAt)
                {
                    executeAtSupplier = id -> id;
                }
                else
                {
                    Function<Timestamp, Timestamp> rawTimestampSupplier = timestampSupplier(uniqueTs, timestampSupplier(epochSupplier, hlcSupplier, flagSupplier, idSupplier));
                    float useTxnIdChance = source.nextFloat();
                    BooleanSupplier useTxnId = () -> source.decide(useTxnIdChance);
                    executeAtSupplier = txnId -> useTxnId.getAsBoolean() ? txnId : rawTimestampSupplier.apply(txnId);
                }
            }

            Supplier<Ballot> ballotSupplier;
            {
                Supplier<Ballot> delegate = ballotSupplier(epochSupplier, hlcSupplier, flagSupplier, idSupplier);
                ballotSupplier =  () -> source.decide(0.5f) ? Ballot.ZERO : delegate.get();
            }

            PartialTxn txn = createPartialTxn(0);
            RoutingKey key = ((Key) txn.keys().get(0)).toUnseekable();
            ObjectGraph graph = generateObjectGraph(source.nextInt(0, 100), () -> txnIdSupplier.apply(null), saveStatusSupplier, ignore -> txn, executeAtSupplier, ballotSupplier, missingCountSupplier, source);
            List<Command> commands = graph.toCommands();
            CommandsForKey cfk = new CommandsForKey(key);
            while (commands.size() > 0)
            {
                int next = source.nextInt(commands.size());
                Command command = commands.get(next);
                cfk = cfk.update(new TestSafeCommandStore(PreLoadContext.contextFor(command.txnId(), "Test")), command).cfk();
                commands.set(next, commands.get(commands.size() - 1));
                commands.remove(commands.size() - 1);
            }

            for (int i = 0, j = 0 ; j < graph.cmds.length ; ++j)
            {
                Cmd cmd = graph.cmds[j];
                if (i >= cfk.size() || !cfk.txnId(i).equals(cmd.txnId))
                {
                    Assert.assertTrue(cmd.invisible);
                    continue;
                }
                TxnInfo info = cfk.get(i);
                InternalStatus expectStatus = InternalStatus.from(cmd.saveStatus);
                if (expectStatus == InternalStatus.APPLIED_NOT_DURABLE && cmd.isDurable)
                    expectStatus = InternalStatus.APPLIED_DURABLE;
                if (expectStatus == null) expectStatus = InternalStatus.TRANSITIVE_VISIBLE;
                if (expectStatus.hasExecuteAt())
                    Assert.assertEquals(cmd.executeAt, info.executeAt);
                Assert.assertEquals(expectStatus, info.status());
                Assert.assertArrayEquals(cmd.missing.toArray(TxnId[]::new), info.missing());
                if (expectStatus.hasBallot)
                    Assert.assertEquals(cmd.ballot, info.ballot());
                ++i;
            }

            cfk = cfk.updateUniqueHlc(source.nextLong(Long.MAX_VALUE));
            ByteBuffer buffer = Serialize.toBytesWithoutKey(cfk);
            CommandsForKey roundTrip = Serialize.fromBytes(key, buffer);
            Assert.assertEquals(cfk, roundTrip);
        }
        catch (Throwable t)
        {
            throw new AssertionError(seed + " seed failed", t);
        }
    }

    @Test
    public void test()
    {
        var tableGen = AccordGenerators.fromQT(CassandraGenerators.TABLE_ID_GEN);
        var txnIdGen = AccordGens.txnIds((Gen.LongGen) rs -> rs.nextLong(0, 100), rs -> rs.nextLong(100), rs -> rs.nextInt(10));
        qt().check(rs -> {
            TableId table = tableGen.next(rs);
            TokenKey pk = new TokenKey(table, new Murmur3Partitioner.LongToken(rs.nextLong()));
            var redudentBefore = txnIdGen.next(rs);
            TxnId[] ids = Gens.arrays(TxnId.class, rs0 -> {
                TxnId next = txnIdGen.next(rs0);
                while (next.compareTo(redudentBefore) <= 0)
                    next = txnIdGen.next(rs0);
                return next;
            }).unique().ofSizeBetween(0, 10).next(rs);
            Arrays.sort(ids, Comparator.naturalOrder());
            TxnInfo[] info = new TxnInfo[ids.length];
            InternalStatus[] statuses = Stream.of(InternalStatus.values()).filter(s -> s != InternalStatus.PRUNED).toArray(InternalStatus[]::new);
            for (int i = 0; i < info.length; i++)
            {
                InternalStatus status = rs.pick(statuses);
                info[i] = TxnInfo.create(ids[i], status, true, ids[i], TxnId.NO_TXNIDS, Ballot.ZERO);
            }

            Gen<Unmanaged.Pending> pendingGen = Gens.enums().allMixedDistribution(Unmanaged.Pending.class).next(rs);

            Unmanaged[] unmanaged = Gens.lists(txnIdGen)
                                        .unique()
                                        .ofSizeBetween(0, 10)
                                        .map((rs0, txnIds) -> txnIds.stream().map(i -> new Unmanaged(pendingGen.next(rs0), i, i)).toArray(Unmanaged[]::new))
                                        .next(rs);
            Arrays.sort(unmanaged, Comparator.naturalOrder());
            if (unmanaged.length > 0)
            {
                // when registering unmanaged, if the txn is "missing" in TxnInfo we add it
                List<TxnInfo> missing = new ArrayList<>(unmanaged.length);
                for (Unmanaged u : unmanaged)
                {
                    int idx = Arrays.binarySearch(ids, u.txnId);
                    if (idx < 0)
                        missing.add(TxnInfo.create(u.txnId, InternalStatus.TRANSITIVE, true, u.txnId, Ballot.ZERO));
                }
                if (!missing.isEmpty())
                {
                    info = ArrayUtils.addAll(info, missing.toArray(TxnInfo[]::new));
                    Arrays.sort(info, Comparator.naturalOrder());
                }
            }
            else unmanaged = CommandsForKey.NO_PENDING_UNMANAGED;

            long maxUniqueHlc = rs.nextLong(0, Long.MAX_VALUE);
            CommandsForKey expected = CommandsForKey.SerializerSupport.create(pk, info, maxUniqueHlc, unmanaged, TxnId.NONE, NO_BOUNDS_INFO, true);

            ByteBuffer buffer = Serialize.toBytesWithoutKey(expected);
            CommandsForKey roundTrip = Serialize.fromBytes(pk, buffer);
            Assert.assertEquals(expected, roundTrip);
        });
    }

    @Test
    public void thereAndBackAgain()
    {
        long tokenValue = -2311778975040348869L;
        Token token = new Murmur3Partitioner.LongToken(tokenValue);
        TokenKey pk = new TokenKey(TableId.fromString("1b255f4d-ef25-40a6-0000-000000000009"), token);
        TxnId txnId = TxnId.fromValues(11,34052499,2,1);
        CommandsForKey expected = CommandsForKey.SerializerSupport.create(pk,
                                                     new TxnInfo[] { TxnInfo.create(txnId, InternalStatus.PREACCEPTED_WITHOUT_DEPS, true, txnId, TxnId.NO_TXNIDS, Ballot.ZERO) },
                                                                          0, CommandsForKey.NO_PENDING_UNMANAGED, TxnId.NONE, NO_BOUNDS_INFO, true);

        ByteBuffer buffer = Serialize.toBytesWithoutKey(expected);
        CommandsForKey roundTrip = Serialize.fromBytes(pk, buffer);
        Assert.assertEquals(expected, roundTrip);
    }

    static class TestCommandStore extends CommandStore implements Agent
    {
        static final TestCommandStore INSTANCE = new TestCommandStore();
        protected TestCommandStore()
        {
            super(0,
                  null,
                  null,
                  null,
                  ignore -> new ProgressLog.NoOpProgressLog(),
                  ignore -> new DefaultLocalListeners(new DefaultRemoteListeners((a, b, c, d, e)->{}), DefaultLocalListeners.DefaultNotifySink.INSTANCE),
                  new EpochUpdateHolder());
        }

        @Override public boolean inStore() { return true; }
        @Override public Journal.Replayer replayer() { throw new UnsupportedOperationException(); }

        @Override protected void ensureDurable(Ranges ranges, RedundantBefore onSuccess) {}
        @Override public Agent agent() { return this; }
        @Override public AsyncChain<Void> build(PreLoadContext context, Consumer<? super SafeCommandStore> consumer) { return null; }
        @Override public <T> AsyncChain<T> build(PreLoadContext context, Function<? super SafeCommandStore, T> apply) { throw new UnsupportedOperationException(); }
        @Override public void shutdown() { }
        @Override public <T> AsyncChain<T> build(Callable<T> task) { throw new UnsupportedOperationException(); }
        @Override public void onInconsistentTimestamp(Command command, Timestamp prev, Timestamp next) { throw new UnsupportedOperationException(); }
        @Override public void onFailedBootstrap(int attempts, String phase, Ranges ranges, Runnable retry, Throwable failure) { throw new UnsupportedOperationException(); }
        @Override public void onStale(Timestamp staleSince, Ranges ranges) { throw new UnsupportedOperationException(); }
        @Override public void onUncaughtException(Throwable t) { throw new UnsupportedOperationException(); }
        @Override public void onCaughtException(Throwable t, String context) { throw new UnsupportedOperationException(); }
        @Override public boolean rejectPreAccept(TimeService time, TxnId txnId) { throw new UnsupportedOperationException(); }
        @Override public long cfkHlcPruneDelta() { return 0; }
        @Override public int cfkPruneInterval() { return 0; }
        @Override public long maxConflictsHlcPruneDelta() { return 0; }
        @Override public long maxConflictsPruneInterval() { return 0; }
        @Override public Txn emptySystemTxn(Kind kind, Routable.Domain domain) { throw new UnsupportedOperationException(); }
        @Override public long slowCoordinatorDelay(Node node, SafeCommandStore safeStore, TxnId txnId, TimeUnit units, int retryCount) { return 0; }
        @Override public long slowReplicaDelay(Node node, SafeCommandStore safeStore, TxnId txnId, int retryCount, ProgressLog.BlockedUntil blockedUntil, TimeUnit units) { return 0; }
        @Override public long slowAwaitDelay(Node node, SafeCommandStore safeStore, TxnId txnId, int retryCount, ProgressLog.BlockedUntil retrying, TimeUnit units) { return 0; }
        @Override public long retrySyncPointDelay(Node node, int attempt, TimeUnit units) { return 0; }
        @Override public long retryDurabilityDelay(Node node, int attempt, TimeUnit units) { return 0; }
        @Override public long expireEpochWait(TimeUnit units) { return 0; }
        @Override public long expiresAt(ReplyContext replyContext, TimeUnit unit) { return 0; }
        @Override public long selfSlowAt(TxnId txnId, Status.Phase phase, TimeUnit unit) { return 0; }
        @Override public long selfExpiresAt(TxnId txnId, Status.Phase phase, TimeUnit unit) { return 0; }
        @Override public AsyncChain<TxnId> awaitStaleId(Node node, TxnId staleId, boolean isRequested) { return null; }
        @Override public long minStaleHlc(Node node, boolean requested) { return 0; }
    }

    public static class TestSafeCommandStore extends AbstractSafeCommandStore
    {
        public TestSafeCommandStore(PreLoadContext context)
        {
            super(context, TestCommandStore.INSTANCE);
        }

        @Override protected CommandStoreCaches tryGetCaches() { return null; }
        @Override protected SafeCommand add(SafeCommand safeCommand, CommandStoreCaches caches) { return null; }
        @Override protected SafeCommandsForKey add(SafeCommandsForKey safeCfk, CommandStoreCaches caches) { return null; }
        @Override protected SafeCommand getInternal(TxnId txnId) { return null; }
        @Override protected SafeCommandsForKey getInternal(RoutingKey key) { return null; }
        @Override public DataStore dataStore() { return null; }
        @Override public Agent agent() { return null; }
        @Override public ProgressLog progressLog() { return null; }
        @Override public NodeCommandStoreService node() { return new NodeCommandStoreService()
        {
            @Override public AsyncExecutor someExecutor() { throw new UnsupportedOperationException(); }
            @Override public SequentialAsyncExecutor someSequentialExecutor() { throw new UnsupportedOperationException(); }
            @Override public long epoch() { return 0;}
            @Override public Node.Id id() { return Node.Id.NONE; }
            @Override public Timeouts timeouts() { return null; }
            @Override public DurableBefore durableBefore() { return null;}
            @Override public DurabilityService durability() { return null; }
            @Override public long uniqueNow(long atLeast) { return 0; }
            @Override public TopologyManager topology() { return null; }
            @Override public long currentStamp() { return 0; }
            @Override public void updateStamp() { throw new UnsupportedOperationException(); }
            @Override public long now() { return 0; }
            @Override public long elapsed(TimeUnit unit) { return 0; }
        }; }
        @Override public boolean visit(Unseekables<?> keysOrRanges, TxnId testTxnId, Kind.Kinds testKind, TestStartedAt testStartedAt, Timestamp testStartAtTimestamp, ComputeIsDep computeIsDep, AllCommandVisitor visit) { return false; }
        @Override public <P1, P2> void visit(Unseekables<?> keysOrRanges, Timestamp startedBefore, Kind.Kinds testKind, ActiveCommandVisitor<P1, P2> visit, P1 p1, P2 p2) { }
    }

}
