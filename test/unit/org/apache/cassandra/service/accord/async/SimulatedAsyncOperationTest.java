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

package org.apache.cassandra.service.accord.async;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

import org.junit.Before;
import org.junit.Test;

import accord.api.Key;
import accord.impl.basic.SimulatedFault;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.SimulatedAccordCommandStore;
import org.apache.cassandra.service.accord.SimulatedAccordCommandStoreTestBase;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

public class SimulatedAsyncOperationTest extends SimulatedAccordCommandStoreTestBase
{
    @Before
    public void precondition()
    {
        Assertions.assertThat(intTbl.partitioner).isEqualTo(Murmur3Partitioner.instance);
    }

    @Test
    public void happyPath()
    {
        qt().withExamples(100).check(rs -> test(rs, 100, intTbl, ignore -> Action.SUCCESS));
    }

    @Test
    public void fuzz()
    {
        Gen<Action> actionGen = Gens.enums().allWithWeights(Action.class, 10, 1, 1);
        qt().withExamples(100).check(rs -> test(rs, 100, intTbl, actionGen));
    }

    private static void test(RandomSource rs, int numSamples, TableMetadata tbl, Gen<Action> actionGen) throws Exception
    {
        AccordKeyspace.unsafeClear();

        int numKeys = rs.nextInt(20, 1000);
        long minToken = 0;
        long maxToken = numKeys;

        Gen<Key> keyGen = Gens.longs().between(minToken + 1, maxToken).map(t -> new PartitionKey(tbl.id, tbl.partitioner.decorateKey(LongToken.keyForToken(t))));


        Gen<Keys> keysGen = Gens.lists(keyGen).unique().ofSizeBetween(1, 10).map(l -> Keys.of(l));
        Gen<Ranges> rangesGen = Gens.lists(rangeInsideRange(tbl.id, minToken, maxToken)).uniqueBestEffort().ofSizeBetween(1, 10).map(l -> Ranges.of(l.toArray(Range[]::new)));
        Gen<Seekables<?, ?>> seekablesGen = Gens.oneOf(keysGen, rangesGen);

        try (var instance = new SimulatedAccordCommandStore(rs))
        {
            instance.ignoreExceptions = t -> t instanceof SimulatedFault;
            Counter counter = new Counter();
            for (int i = 0; i < numSamples; i++)
            {
                PreLoadContext ctx = PreLoadContext.contextFor(seekablesGen.next(rs));
                operation(instance, ctx, actionGen.next(rs), rs::nextBoolean).begin((ignore, failure) -> {
                    counter.counter++;
                    if (failure != null && !(failure instanceof SimulatedFault)) throw new AssertionError("Unexpected error", failure);
                });
            }
            instance.processAll();
            Assertions.assertThat(counter.counter).isEqualTo(numSamples);
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

    private enum Action {SUCCESS, FAILURE, LOAD_FAILURE}

    private static AsyncOperation<Void> operation(SimulatedAccordCommandStore instance, PreLoadContext ctx, Action action, BooleanSupplier delay)
    {
        return new SimulatedOperation(instance.store, ctx, action == Action.FAILURE ? SimulatedOperation.Action.FAILURE : SimulatedOperation.Action.SUCCESS)
        {
            @Override
            AsyncLoader createAsyncLoader(AccordCommandStore commandStore, PreLoadContext preLoadContext)
            {
                return new SimulatedLoader(action == SimulatedAsyncOperationTest.Action.LOAD_FAILURE ? SimulatedLoader.Action.FAILURE : SimulatedLoader.Action.SUCCESS, delay.getAsBoolean(), instance.unorderedScheduled);
            }
        };
    }

    private static class Counter
    {
        int counter = 0;
    }

    private static class SimulatedOperation extends AsyncOperation<Void>
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

    private static class SimulatedLoader extends AsyncLoader
    {

        enum Action { SUCCESS, FAILURE}

        private final Action action;
        private boolean delay;
        private final ScheduledExecutorService executor;
        SimulatedLoader(Action action, boolean delay, ScheduledExecutorService executor)
        {
            super(null, null, null, null);
            this.action = action;
            this.delay = delay;
            this.executor = executor;
        }

        @Override
        public boolean load(AsyncOperation.Context context, BiConsumer<Object, Throwable> callback)
        {
            if (delay)
            {
                executor.schedule(() -> {
                    callback.accept(null, action == Action.FAILURE ? new SimulatedFault("Failure loading " + context) : null);
                }, 1, TimeUnit.SECONDS);
                delay = false;
                return false;
            }
            if (action == Action.FAILURE)
                throw new SimulatedFault("Failure loading " + context);

            return true;
        }
    }
}
