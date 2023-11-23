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

package org.apache.cassandra.distributed.test.log;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;

public class ModelChecker<STATE, SUT>
{
    private final List<StepExecutor<STATE, SUT>> steps;
    private final List<Precondition<STATE, SUT>> invariants;
    private Precondition<STATE, SUT> exitCondition;
    private BiConsumer<STATE, SUT> beforeAll;
    private Pair<STATE, SUT> init;

    public ModelChecker()
    {
        steps = new ArrayList<>();
        invariants = new ArrayList<>();
    }

    public void run() throws Throwable
    {
        run(Integer.MAX_VALUE);
    }

    public void run(int maxSteps) throws Throwable
    {
        assert exitCondition != null : "Exit condition is not specified";
        assert init != null : "Initial condition is not specified";

        Ref<Pair<STATE, SUT>> state = new Ref<>(init);
        EntropySource entropySource = new FakeEntropySource(new Random(88));
        if (beforeAll != null)
            beforeAll.accept(state.get().l, state.get().r);

        for (int i = 0; i < maxSteps; i++)
        {
            if (exitCondition.test(state.get()))
                return;

            // TODO: add randomisation / probability for triggering a specific step
            steps.get(entropySource.nextInt(steps.size())).execute(state, entropySource.derive());
            for (Precondition<STATE, SUT> invariant : invariants)
                invariant.test(state.get());
        }
    }
    public ModelChecker<STATE, SUT> init(STATE state, SUT sut)
    {
        this.init = new Pair<>(state, sut);
        return this;
    }

    public ModelChecker<STATE, SUT> beforeAll(BiConsumer<STATE, SUT> precondition)
    {
        this.beforeAll = precondition;
        return this;
    }

    public ModelChecker<STATE, SUT> exitCondition(Precondition<STATE, SUT> precondition)
    {
        this.exitCondition = precondition;
        return this;
    }

    public ModelChecker<STATE, SUT> step(Precondition<STATE, SUT> precondition, Step<STATE, SUT> step)
    {
        steps.add((ref, entropySource) -> {
            ref.map(state -> {
                if (!precondition.test(state))
                    return state;

                Pair<STATE, SUT> next = step.next(state.l, state.r, entropySource);
                if (next == Pair.unchanged())
                    return state;
                else
                    return next;
            });
        });

        return this;
    }

    public ModelChecker<STATE, SUT> invariant(Precondition<STATE, SUT> invariant)
    {
        invariants.add(invariant);
        return this;
    }

    public ModelChecker<STATE, SUT> step(Step<STATE, SUT> step)
    {
        return step(Precondition.alwaysTrue(), step);
    }

    public ModelChecker<STATE, SUT> step(StatePrecondition<STATE> precondition, ThrowingFunction<STATE, STATE> step)
    {
        steps.add((ref, entropySource) -> {
            ref.map(state -> {
                if (!precondition.test(state.l))
                    return state;

                STATE newState = step.apply(state.l);
                if (state.l == newState)
                    return state;

                return state.next(newState);
            });
        });

        return this;
    }

    public ModelChecker<STATE, SUT> step(StatePrecondition<STATE> precondition, ThrowingBiFunction<STATE, SUT, STATE> step)
    {
        steps.add((ref, entropySource) -> {
            ref.map(state -> {
                if (!precondition.test(state.l))
                    return state;

                STATE newState = step.apply(state.l, state.r);
                if (state.l == newState)
                    return state;

                return state.next(newState);
            });
        });

        return this;
    }

    public ModelChecker<STATE, SUT> step(ThrowingFunction<STATE, STATE> step)
    {
        return step((t, sut, entropySource) -> {
            return new Pair<>(step.apply(t), sut);
        });
    }

    static interface StepExecutor<STATE, SUT>
    {
        void execute(Ref<Pair<STATE, SUT>> state, EntropySource entropySource) throws Throwable;
    }

    public static interface StatePrecondition<STATE>
    {
        boolean test(STATE state) throws Throwable;
    }

    public static interface Precondition<STATE, SUT>
    {
        boolean test(STATE state, SUT sut) throws Throwable;

        default boolean test(Pair<STATE, SUT> state) throws Throwable
        {
            return test(state.l, state.r);
        }

        public static <STATE, SUT> Precondition<STATE, SUT> alwaysTrue()
        {
            return (a,b) -> true;
        }
    }

    public static interface Step<STATE, SUT>
    {
        Pair<STATE, SUT> next(STATE t, SUT sut, EntropySource entropySource) throws Throwable;
    }

    public static interface ThrowingFunction<I, O>
    {
        O apply(I t) throws Throwable;
    }

    public static interface ThrowingBiFunction<I1, I2, O>
    {
        O apply(I1 t1, I2 t2) throws Throwable;
    }

    // Borrowed from Harry
    public static interface EntropySource
    {
        long next();
        // We derive from entropy source here to avoid letting the step change state for other states
        // For example, if you start drawing more entropy bits from one of the steps, but won't change
        // other steps, their states won't change either.
        EntropySource derive();

        Random seededRandom();
        default long[] next(int n)
        {
            long[] next = new long[n];
            for (int i = 0; i < n; i++)
                next[i] = next();
            return next;
        }

        default int nextInt()
        {
            return RngUtils.asInt(next());
        }

        default int nextInt(int max)
        {
            return RngUtils.asInt(next(), max);
        }

        default int nextInt(int min, int max)
        {
            return RngUtils.asInt(next(), min, max);
        }

        default boolean nextBoolean()
        {
            return RngUtils.asBoolean(next());
        }
    }

    public static class FakeEntropySource implements EntropySource
    {
        private final Random rng;

        public FakeEntropySource(Random rng)
        {
            this.rng = rng;
        }

        public long next()
        {
            return rng.nextLong();
        }

        public EntropySource derive()
        {
            return new FakeEntropySource(new Random(rng.nextLong()));
        }

        public Random seededRandom()
        {
            return new Random(rng.nextLong());
        }
    }

    public static class Ref<T>
    {
        public T ref;

        public Ref(T init)
        {
            this.ref = init;
        }

        public T get()
        {
            return ref;
        }

        public void set(T v)
        {
            this.ref = v;
        }

        public void map(ThrowingFunction<T, T> fn) throws Throwable
        {
            this.ref = fn.apply(ref);
        }
    }

    public static class Pair<L, R>
    {
        private static Pair<?,?> UNCHANGED = new Pair<>(null,null);
        public static <L, R> Pair<L, R> unchanged()
        {
            return (Pair<L, R>) UNCHANGED;
        }

        public final L l;
        public final R r;

        public Pair(L l, R r)
        {
            this.l = l;
            this.r = r;
        }

        public Pair<L, R> next(L state)
        {
            return new Pair<>(state, this.r);
        }
    }
}
