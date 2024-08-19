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

package org.apache.cassandra.harry.checker;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;

public class ModelChecker<STATE, SUT>
{
    private final List<StepExecutor<STATE, SUT>> steps;
    private final List<Precondition<STATE, SUT>> invariants;
    private Precondition<STATE, SUT> exitCondition;
    private Step<STATE, SUT> beforeAll;
    private Step<STATE, SUT> afterAll;
    private Pair<STATE, SUT> init;

    public ModelChecker()
    {
        steps = new ArrayList<>();
        invariants = new ArrayList<>();
    }

    public void run() throws Throwable
    {
        run(0, Long.MAX_VALUE, new JdkRandomEntropySource(System.currentTimeMillis()));
    }

    public void run(int minSteps, long maxSteps) throws Throwable
    {
        run(minSteps, maxSteps, new JdkRandomEntropySource(System.currentTimeMillis()));
    }

    public void run(int minSteps, long maxSteps, EntropySource entropySource) throws Throwable
    {
        assert init != null : "Initial condition is not specified";

        Ref<Pair<STATE, SUT>> state = new Ref<>(init, Pair.unchanged());
        if (beforeAll != null)
            state.map((s) -> beforeAll.next(s.l, s.r, entropySource));

        for (int i = 0; i < maxSteps; i++)
        {
            if (i > minSteps && exitCondition.test(state.get()))
                return;

            // TODO: add randomisation / probability for triggering a specific step
            steps.get(entropySource.nextInt(steps.size())).execute(state, entropySource.derive());
            for (Precondition<STATE, SUT> invariant : invariants)
                invariant.test(state.get());
        }

        if (afterAll != null)
            state.map((s) -> afterAll.next(s.l, s.r, entropySource));
    }

    public ModelChecker<STATE, SUT> init(STATE state, SUT sut)
    {
        this.init = new Pair<>(state, sut);
        return this;
    }

    public Simple init(STATE state)
    {
        Simple simple = new Simple();
        simple.init(state);
        return simple;
    }

    @SuppressWarnings("unused")
    public ModelChecker<STATE, SUT> beforeAll(Step<STATE, SUT> beforeAll)
    {
        this.beforeAll = beforeAll;
        return this;
    }

    @SuppressWarnings("unused")
    public ModelChecker<STATE, SUT> afterAll(Step<STATE, SUT> afterAll)
    {
        this.afterAll = afterAll;
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

                return step.next(state.l, state.r, entropySource);
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
        return step((t, sut, entropySource) -> new Pair<>(step.apply(t), sut));
    }

    interface StepExecutor<STATE, SUT>
    {
        void execute(Ref<Pair<STATE, SUT>> state, EntropySource entropySource) throws Throwable;
    }

    public interface StatePrecondition<STATE>
    {
        boolean test(STATE state) throws Throwable;
    }

    public interface Precondition<STATE, SUT>
    {
        boolean test(STATE state, SUT sut) throws Throwable;

        default boolean test(Pair<STATE, SUT> state) throws Throwable
        {
            return test(state.l, state.r);
        }

        static <STATE, SUT> Precondition<STATE, SUT> alwaysTrue()
        {
            return (a, b) -> true;
        }
    }

    public interface Step<STATE, SUT>
    {
        Pair<STATE, SUT> next(STATE t, SUT sut, EntropySource entropySource) throws Throwable;
    }

    public interface ThrowingConsumer<I>
    {
        void accept(I t) throws Throwable;
    }

    public interface ThrowingBiConsumer<I1, I2>
    {
        void accept(I1 t1, I2 t2) throws Throwable;
    }

    public interface ThrowingFunction<I, O>
    {
        O apply(I t) throws Throwable;
    }

    public interface ThrowingBiFunction<I1, I2, O>
    {
        O apply(I1 t1, I2 t2) throws Throwable;
    }

    private static class Ref<T>
    {
        public T ref;
        private final T unchanged;

        public Ref(T init, T unchanged)
        {
            this.ref = init;
            this.unchanged = unchanged;
        }

        public T get()
        {
            return ref;
        }

        public void set(T v)
        {
            if (v == unchanged)
                return;
            this.ref = v;
        }

        public void map(ThrowingFunction<T, T> fn) throws Throwable
        {
            set(fn.apply(ref));
        }
    }

    public static class Pair<L, R>
    {
        private static final Pair<?, ?> UNCHANGED = new Pair<>(null, null);

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

    public class Simple
    {
        public Simple init(STATE state)
        {
            ModelChecker.this.init = new Pair<>(state, null);
            return this;
        }

        @SuppressWarnings("unused")
        public Simple beforeAll(ThrowingConsumer<STATE> beforeAll)
        {
            ModelChecker.this.beforeAll = (t, sut, entropySource) -> {
                beforeAll.accept(t);
                return Pair.unchanged();
            };
            return this;
        }

        public Simple beforeAll(ThrowingBiConsumer<STATE, EntropySource> beforeAll)
        {
            ModelChecker.this.beforeAll = (t, sut, entropySource) -> {
                beforeAll.accept(t, entropySource);
                return Pair.unchanged();
            };
            return this;
        }

        @SuppressWarnings("unused")
        public Simple beforeAll(ThrowingFunction<STATE, STATE> beforeAll)
        {
            ModelChecker.this.beforeAll = (t, sut, entropySource) -> new Pair<>(beforeAll.apply(t), sut);
            return this;
        }

        public Simple afterAll(ThrowingConsumer<STATE> afterAll)
        {
            ModelChecker.this.afterAll = (t, sut, entropySource) -> {
                afterAll.accept(t);
                return Pair.unchanged();
            };
            return this;
        }

        @SuppressWarnings("unused")
        public Simple afterAll(ThrowingFunction<STATE, STATE> afterAll)
        {
            ModelChecker.this.afterAll = (t, sut, entropySource) -> new Pair(afterAll.apply(t), sut);
            return this;
        }

        public Simple exitCondition(Predicate<STATE> precondition)
        {
            ModelChecker.this.exitCondition = (state, sut) -> precondition.test(state);
            return this;
        }

        @SuppressWarnings("unused")
        public Simple invariant(Predicate<STATE> invariant)
        {
            invariants.add((state, sut) -> invariant.test(state));
            return this;
        }

        public Simple step(ThrowingFunction<STATE, STATE> step)
        {
            ModelChecker.this.step((state, sut, entropySource) -> new Pair<>(step.apply(state), sut));
            return this;
        }

        public Simple step(ThrowingConsumer<STATE> step)
        {
            ModelChecker.this.step((state, sut, entropySource) -> {
                step.accept(state);
                return Pair.unchanged();
            });
            return this;
        }

        public Simple step(ThrowingBiConsumer<STATE, EntropySource> step)
        {
            ModelChecker.this.step((state, sut, entropySource) -> {
                step.accept(state, entropySource);
                return Pair.unchanged();
            });
            return this;
        }

        public Simple step(BiPredicate<STATE, EntropySource> precondition, BiConsumer<STATE, EntropySource> step)
        {
            ModelChecker.this.step((state, sut, entropySource) -> {
                if (!precondition.test(state, entropySource))
                    return Pair.unchanged();

                step.accept(state, entropySource);
                return Pair.unchanged();
            });

            return this;
        }

        public Simple step(Predicate<STATE> precondition, Consumer<STATE> step)
        {
            ModelChecker.this.step((state, ignore) -> precondition.test(state),
                                   (t, sut, entropySource) -> {
                                       step.accept(t);
                                       return Pair.unchanged();
                                   });

            return this;
        }

        public void run() throws Throwable
        {
            ModelChecker.this.run();
        }

        public void run(int minSteps, long maxSteps, EntropySource entropySource) throws Throwable
        {
            ModelChecker.this.run(minSteps, maxSteps, entropySource);
        }
    }
}
