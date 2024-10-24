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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;

import accord.api.RoutingKey;
import accord.impl.basic.SimulatedFault;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.messages.PreAccept;
import accord.primitives.FullRoute;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.SimulatedAccordCommandStore.FunctionWrapper;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.utils.Pair;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

public class SimulatedAccordTaskTest extends SimulatedAccordCommandStoreTestBase
{
    @Before
    public void precondition()
    {
        Assertions.assertThat(intTbl.partitioner).isEqualTo(Murmur3Partitioner.instance);
        Assertions.assertThat(reverseTokenTbl.partitioner).isEqualTo(Murmur3Partitioner.instance);
    }

    @Test
    public void happyPath()
    {
        qt().withExamples(100).check(rs -> test(rs, 100, intTbl, ignore -> Action.SUCCESS, ignore -> 0L));
    }

    @Test
    public void fuzz()
    {
        Gen<Action> actionGen = Gens.enums().allWithWeights(Action.class, 10, 1, 1);
        Gen.LongGen delaysNanos = Gens.longs().between(0, TimeUnit.MILLISECONDS.toNanos(10));
        qt().withExamples(100).check(rs -> test(rs, 100, intTbl, actionGen, delaysNanos));
    }

    enum Operation { Task, PreAccept }

    private static void test(RandomSource rs, int numSamples, TableMetadata tbl, Gen<Action> actionGen, Gen.LongGen delaysNanos) throws Exception
    {
        AccordKeyspace.unsafeClear();
        Gen<Operation> operationGen = Gens.enums().all(Operation.class);

        int numKeys = rs.nextInt(20, 1000);
        long minToken = 0;
        long maxToken = numKeys;

        Gen<RoutingKey> keyGen = Gens.longs().between(minToken + 1, maxToken).map(t -> new TokenKey(tbl.id, new LongToken(t)));
        Gen<RoutingKeys> keysGen = Gens.lists(keyGen).unique().ofSizeBetween(1, 10).map(l -> RoutingKeys.of(l));
        Gen<Ranges> rangesGen = Gens.lists(rangeInsideRange(tbl.id, minToken, maxToken)).uniqueBestEffort().ofSizeBetween(1, 10).map(l -> Ranges.of(l.toArray(Range[]::new)));
        Gen<Unseekables<?>> unseekablesGen = Gens.oneOf(keysGen, rangesGen);
        Gen<Pair<Txn, FullRoute<?>>> txnGen = randomTxn(mixedDomainGen.next(rs), mixedTokenGen.next(rs));

        try (var instance = new SimulatedAccordCommandStore(rs, new SimulatedLoadFunctionWrapper(actionGen.asSupplier(rs), delaysNanos.asLongSupplier(rs))))
        {
            instance.ignoreExceptions = t -> t instanceof SimulatedFault;
            Counter counter = new Counter();
            for (int i = 0; i < numSamples; i++)
            {
                Operation op = operationGen.next(rs);
                switch (op)
                {
                    case Task:
                    {
                        PreLoadContext ctx = PreLoadContext.contextFor(unseekablesGen.next(rs));
                        instance.maybeCacheEvict(ctx.keys());
                        operation(instance, ctx, actionGen.next(rs), rs::nextBoolean).chain().begin(counter);
                    }
                    break;
                    case PreAccept:
                    {
                        Pair<Txn, FullRoute<?>> txnWithRoute = txnGen.next(rs);
                        Txn txn = txnWithRoute.left;
                        Action action = actionGen.next(rs);
                        TxnId txnId = instance.nextTxnId(txn.kind(), txn.keys().domain());
                        FullRoute<?> route = txnWithRoute.right;
                        PreAccept preAccept = new PreAccept(nodeId, instance.topologies, txnId, txn, route) {
                            @Override
                            public PreAcceptReply apply(SafeCommandStore safeStore)
                            {
                                PreAcceptReply result = super.apply(safeStore);
                                if (action == Action.FAILURE)
                                    throw new SimulatedFault("PreAccept failed for keys " + keys());
                                return result;
                            }
                        };
                        instance.maybeCacheEvict(txn.keys().toParticipants());
                        instance.processAsync(preAccept).begin(counter);
                    }
                    break;
                    default:
                        throw new UnsupportedOperationException(op.name());
                }
            }
            instance.processAll();
            Assertions.assertThat(counter.counter).isEqualTo(numSamples);
            instance.commandStore.cachesUnsafe().commands().forEach(e -> {
                Assertions.assertThat(e.references()).isEqualTo(0);
            });
            instance.commandStore.cachesUnsafe().commandsForKeys().forEach(e -> {
                Assertions.assertThat(e.references()).isEqualTo(0);
            });
            instance.commandStore.cachesUnsafe().timestampsForKeys().forEach(e -> {
                Assertions.assertThat(e.references()).isEqualTo(0);
            });
        }
    }

    private static Gen<Range> rangeInsideRange(TableId tableId, long minToken, long maxToken)
    {
        if (minToken + 1 == maxToken)
        {
            // only one range is possible...
            return Gens.constant(range(tableId, minToken, maxToken));
        }
        return rs -> {
            long a = rs.nextLong(minToken, maxToken + 1);
            long b = rs.nextLong(minToken, maxToken + 1);
            while (a == b)
                b = rs.nextLong(minToken, maxToken + 1);
            if (a > b)
            {
                long tmp = a;
                a = b;
                b = tmp;
            }
            return range(tableId, a, b);
        };
    }

    private static TokenRange range(TableId tableId, long start, long end)
    {
        return new TokenRange(new TokenKey(tableId, new LongToken(start)), new TokenKey(tableId, new LongToken(end)));
    }

    private enum Action { SUCCESS, FAILURE, LOAD_FAILURE }

    private static AccordTask<Void> operation(SimulatedAccordCommandStore instance, PreLoadContext ctx, Action action, BooleanSupplier delay)
    {
        return new SimulatedOperation(instance.commandStore, ctx, action == Action.FAILURE ? SimulatedOperation.Action.FAILURE : SimulatedOperation.Action.SUCCESS);
    }

    private static class Counter implements BiConsumer<Object, Throwable>
    {
        int counter = 0;

        @Override
        public void accept(Object o, Throwable failure)
        {
            counter++;
            if (failure != null && !(failure instanceof SimulatedFault))
                throw new AssertionError("Unexpected error", failure);
        }
    }

    private static class SimulatedOperation extends AccordTask<Void>
    {
        enum Action { SUCCESS, FAILURE}
        private final Action action;

        public SimulatedOperation(AccordCommandStore commandStore, PreLoadContext preLoadContext, Action action)
        {
            super(commandStore, preLoadContext);
            this.action = action;
        }

        @Override
        public Void apply(SafeCommandStore safe)
        {
            if (action == Action.FAILURE)
                throw new SimulatedFault("Operation failed for keys " + keys());
            return null;
        }
    }

    private static class SimulatedLoadFunctionWrapper implements FunctionWrapper
    {
        final Supplier<Action> actions;
        final LongSupplier delayNanos;

        private SimulatedLoadFunctionWrapper(Supplier<Action> actions, LongSupplier delayNanos)
        {
            this.actions = actions;
            this.delayNanos = delayNanos;
        }

        @Override
        public <I1, I2, O> BiFunction<I1, I2, O> wrap(BiFunction<I1, I2, O> f)
        {
            return new SimulatedLoadFunction<>(f, actions, delayNanos);
        }
    }

    private static class SimulatedLoadFunction<I1, I2, V> implements BiFunction<I1, I2, V>
    {
        private final BiFunction<I1, I2, V> load;
        private final Supplier<Action> actions;
        private final LongSupplier delaysNanos;
        SimulatedLoadFunction(BiFunction<I1, I2, V> load, Supplier<Action> actions, LongSupplier delaysNanos)
        {
            this.load = load;
            this.actions = actions;
            this.delaysNanos = delaysNanos;
        }

        @Override
        public V apply(I1 i1, I2 i2)
        {
            long delayNanos = delaysNanos.getAsLong();
            if (delayNanos > 0)
                LockSupport.parkNanos(delayNanos);
            Action action = actions.get();
            if (action == Action.SUCCESS) return load.apply(i1, i2);
            throw new SimulatedFault("Failure loading " + i2);
        }
    }
}
