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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import accord.api.RoutingKey;
import accord.impl.basic.SimulatedFault;
import accord.local.ExecutionContext;
import accord.local.LoadKeys;
import accord.local.SafeCommandStore;
import accord.messages.PreAccept;
import accord.messages.PreAccept.PreAcceptReply;
import accord.messages.ReplyList;
import accord.primitives.FullRoute;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import accord.utils.async.AsyncResult;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.SimulatedAccordCommandStore.FunctionWrapper;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.execution.SafeTask;
import org.apache.cassandra.utils.Pair;

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
        qt().withExamples(100).check(rs -> test(rs, 100, reverseTokenTbl, ignore -> Action.SUCCESS, ignore -> 0L, null));
    }

    /**
     * Faults injected, but no incremental tasks: a task that fails on a key that failed to load keeps its reference to
     * that key when it is atomic/incremental, so that the work can be retried (see
     * {@code SafeTask.releaseResourcesExclusiveNoExcept}, which diverts such state into {@code OptionalState.retry}).
     * That is deliberate, so the "everything was released" check below is only meaningful if either nothing is
     * incremental (here) or nothing fails ({@link #fuzzIncrementalWithoutFailures}).
     */
    @Test
    public void fuzz()
    {
        Gen<Action> actionGen = Gens.enums().allWithWeights(Action.class, 10, 1, 1);
        Gen.LongGen delaysNanos = Gens.longs().between(0, TimeUnit.MILLISECONDS.toNanos(10));
        qt().withExamples(100).check(rs -> test(rs, 100, reverseTokenTbl, actionGen, delaysNanos, LoadKeys.SYNC));
    }

    /** the same workload with incremental tasks left alone, but nothing failing, so all state must be released */
    @Test
    public void fuzzIncrementalWithoutFailures()
    {
        Gen.LongGen delaysNanos = Gens.longs().between(0, TimeUnit.MILLISECONDS.toNanos(10));
        qt().withExamples(100).check(rs -> test(rs, 100, reverseTokenTbl, ignore -> Action.SUCCESS, delaysNanos, null));
    }

    enum Operation { Task, PreAccept }

    /**
     * @param forceLoadKeys if not null, every request runs with these {@link LoadKeys} rather than the ones it asks for,
     *                      so that {@code LoadKeys.SYNC} can exclude incremental execution from the workload
     */
    private static void test(RandomSource rs, int numSamples, TableMetadata tbl, Gen<Action> actionGen, Gen.LongGen delaysNanos, LoadKeys forceLoadKeys) throws Exception
    {
        AccordKeyspace.unsafeClear();
        Gen<Operation> operationGen = Gens.enums().all(Operation.class);

        int numKeys = rs.nextInt(20, 1000);
        long minToken = 0;
        long maxToken = numKeys;

        Gen<RoutingKey> keyGen = Gens.longs().between(minToken + 1, maxToken).map(t -> new TokenKey(tbl.id, new LongToken(t)));
        Gen<RoutingKeys> keysGen = Gens.lists(keyGen).unique().ofSizeBetween(1, 10).map(l -> RoutingKeys.of(l));
        Gen<Ranges> rangesGen = Gens.lists(rangeInsideRange(tbl.id, minToken, maxToken)).uniqueBestEffort().ofSizeBetween(1, 10).map(l -> Ranges.of(l.toArray(Range[]::new)));
        Gen<Pair<Txn, FullRoute<?>>> txnGen = randomTxn(tbl, mixedDomainGen.next(rs), mixedTokenGen.next(rs));

        try (var instance = new SimulatedAccordCommandStore(tbl.id, rs, new SimulatedLoadFunctionWrapper(actionGen.asSupplier(rs), delaysNanos.asLongSupplier(rs))))
        {
            instance.ignoreExceptions = t -> t instanceof SimulatedFault;
            Counter counter = new Counter();
            // what we submitted, so that a submission that never completes can be named (and its state shown)
            List<Pair<SafeTask<?>, AsyncResult<?>>> submitted = new ArrayList<>();
            for (int i = 0; i < numSamples; i++)
            {
                Operation op = operationGen.next(rs);
                switch (op)
                {
                    case Task:
                    {
                        ExecutionContext ctx = (ExecutionContext.Empty)()->"Test";
                        instance.maybeCacheEvict(ctx.keys());
                        SafeTask<Void> task = operation(instance, ctx, actionGen.next(rs), rs::nextBoolean);
                        AsyncResult<Void> result = task.chain().beginAsResult();
                        submitted.add(Pair.create(task, result));
                        result.invoke(counter);
                    }
                    break;
                    case PreAccept:
                    {
                        Pair<Txn, FullRoute<?>> txnWithRoute = txnGen.next(rs);
                        Txn txn = txnWithRoute.left;
                        Action action = actionGen.next(rs);
                        TxnId txnId = instance.nextTxnId(txn.kind(), txn.keys().domain());
                        FullRoute<?> route = txnWithRoute.right;
                        PreAccept preAccept = new PreAccept(nodeId, instance.topologies, txnId, txn, null, false, route) {
                            @Override
                            public LoadKeys loadKeys()
                            {
                                return forceLoadKeys != null ? forceLoadKeys : super.loadKeys();
                            }

                            @Override
                            public ReplyList<PreAcceptReply> applyInternal(SafeCommandStore safeStore)
                            {
                                unsafeSetNode(emptyNode());
                                ReplyList<PreAcceptReply> result = super.applyInternal(safeStore);
                                if (action == Action.FAILURE)
                                    throw new SimulatedFault("PreAccept failed for keys " + keys());
                                return result;
                            }
                        };
                        instance.maybeCacheEvict(txn.keys().toParticipants());
                        SafeTask<ReplyList<PreAcceptReply>> task = SafeTask.create(instance.commandStore, preAccept, preAccept);
                        // a PreAccept reply is not necessarily complete when the task returns it: deps are computed
                        // incrementally and may finish in a continuation, so await the reply itself
                        AsyncResult<PreAcceptReply> result = task.chain()
                                                                 .beginAsResult()
                                                                 .flatMap(SimulatedAccordCommandStore::awaitReply);
                        submitted.add(Pair.create(task, result));
                        result.invoke(counter);
                    }
                    break;
                    default:
                        throw new UnsupportedOperationException(op.name());
                }
            }
            instance.processAll();
            Assertions.assertThat(counter.counter)
                      .describedAs("%d of %d submissions never completed; still outstanding: %s",
                                   numSamples - counter.counter, numSamples, describeIncomplete(submitted))
                      .isEqualTo(numSamples);
            instance.commandStore.cachesUnsafe().commands().forEach(e -> {
                Assertions.assertThat(e.references()).isEqualTo(0);
            });
            instance.commandStore.cachesUnsafe().commandsForKeys().forEach(e -> {
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
        return TokenRange.create(new TokenKey(tableId, new LongToken(start)), new TokenKey(tableId, new LongToken(end)));
    }

    /** the submissions that have not reached a terminal state, with the state each is stuck in */
    private static String describeIncomplete(List<Pair<SafeTask<?>, AsyncResult<?>>> submitted)
    {
        StringBuilder sb = new StringBuilder();
        for (Pair<SafeTask<?>, AsyncResult<?>> submission : submitted)
        {
            if (!submission.right.isDone())
                sb.append("\n  ").append(submission.left);
        }
        return sb.length() == 0 ? "nothing" : sb.toString();
    }

    private enum Action { SUCCESS, FAILURE, LOAD_FAILURE }

    private static SafeTask<Void> operation(SimulatedAccordCommandStore instance, ExecutionContext ctx, Action action, BooleanSupplier delay)
    {

        Function<SafeCommandStore, Void> function = action == Action.FAILURE ? safeStore -> { throw new SimulatedFault("Operation failed for keys " + ctx.keys()); }
                                                                             : safeStore -> null;
        return SafeTask.create(instance.commandStore, ctx, function);
    }

    private static class Counter implements BiConsumer<Object, Throwable>
    {
        int counter = 0;

        @Override
        public void accept(Object o, Throwable failure)
        {
            counter++;
            if (failure != null && !isExpected(failure))
                throw new AssertionError("Unexpected error", failure);
        }

        /**
         * An injected load failure leaves the entry {@code FAILED_TO_LOAD}, and any later task that needs that key
         * fails with "Failed to load &lt;key&gt;" caused by our {@link SimulatedFault}. Anything else is a real failure.
         */
        private static boolean isExpected(Throwable failure)
        {
            for (Throwable t = failure ; t != null ; t = t.getCause())
            {
                if (t instanceof SimulatedFault)
                    return true;
            }
            return false;
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
