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
import java.util.Random;
import java.util.function.Consumer;

import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;

public class ModelChecker<STATE>
{
    private final List<StepExecutor<STATE>> steps;
    private final List<Precondition<STATE>> invariants;
    private Precondition<STATE> exitCondition;
    private Consumer<STATE> beforeAll;
    private Consumer<STATE> afterAll;
    private STATE init;

    public ModelChecker()
    {
        steps = new ArrayList<>();
        invariants = new ArrayList<>();
    }

    public void run() throws Throwable
    {
        run(Integer.MAX_VALUE, System.currentTimeMillis());
    }

    public void run(int maxSteps, long seed) throws Throwable
    {
        assert init != null : "Initial condition is not specified";

        Ref<STATE> state = new Ref<>(init);
        EntropySource entropySource = new JdkRandomEntropySource(new Random(seed));
        if (beforeAll != null)
            beforeAll.accept(state.get());

        for (int i = 0; i < maxSteps; i++)
        {
            if (exitCondition != null && exitCondition.test(state.get()))
                return;

            // TODO: add randomisation / probability for triggering a specific step
            steps.get(entropySource.nextInt(steps.size())).execute(state, entropySource.derive());
            for (Precondition<STATE> invariant : invariants)
                invariant.test(state.get());
        }

        if (afterAll != null)
            afterAll.accept(state.get());
    }
    public ModelChecker<STATE> init(STATE state)
    {
        this.init = state;
        return this;
    }

    public ModelChecker<STATE> beforeAll(Consumer<STATE> precondition)
    {
        this.beforeAll = precondition;
        return this;
    }

    public ModelChecker<STATE> afterAll(Consumer<STATE> postcondition)
    {
        this.afterAll = postcondition;
        return this;
    }

    public ModelChecker<STATE> exitCondition(Precondition<STATE> precondition)
    {
        this.exitCondition = precondition;
        return this;
    }

    public ModelChecker<STATE> step(Precondition<STATE> precondition, Step<STATE> step)
    {
        steps.add((ref, entropySource) -> {
            ref.map(state -> {
                if (!precondition.test(state))
                    return state;

                return step.next(state, entropySource);
            });
        });

        return this;
    }

    public ModelChecker<STATE> invariant(Precondition<STATE> invariant)
    {
        invariants.add(invariant);
        return this;
    }

    public ModelChecker<STATE> step(Step<STATE> step)
    {
        return step(Precondition.alwaysTrue(), step);
    }

    public ModelChecker<STATE> step(StatePrecondition<STATE> precondition, ThrowingFunction<STATE, STATE> step)
    {
        steps.add((ref, entropySource) -> {
            ref.map(state -> {
                if (!precondition.test(state))
                    return state;

                return step.apply(state);
            });
        });

        return this;
    }

    public ModelChecker<STATE> step(ThrowingConsumer<STATE> step)
    {
        return step((t, entropySource) -> {
            step.consume(t);
            return t;
        });
    }
    public ModelChecker<STATE> step(ThrowingFunction<STATE, STATE> step)
    {
        return step((t, entropySource) -> {
            return step.apply(t);
        });
    }

    interface StepExecutor<STATE>
    {
        void execute(Ref<STATE> state, EntropySource entropySource) throws Throwable;
    }

    public interface StatePrecondition<STATE>
    {
        boolean test(STATE state) throws Throwable;
    }

    public interface Precondition<STATE>
    {
        boolean test(STATE state) throws Throwable;

        static <STATE> Precondition<STATE> alwaysTrue()
        {
            return (a) -> true;
        }
    }

    public interface Step<STATE>
    {
        STATE next(STATE t, EntropySource entropySource) throws Throwable;
    }

    public interface ThrowingConsumer<I>
    {
        void consume(I t) throws Throwable;
    }

    public interface ThrowingFunction<I, O>
    {
        O apply(I t) throws Throwable;
    }

    public interface ThrowingBiFunction<I1, I2, O>
    {
        O apply(I1 t1, I2 t2) throws Throwable;
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

    public static class State<MODEL, SUT>
    {
        public final MODEL model;
        public final SUT sut;

        public State(MODEL model, SUT sut)
        {
            this.model = model;
            this.sut = sut;
        }

        public State<MODEL, SUT> next(MODEL state)
        {
            return new State<>(state, this.sut);
        }
    }
}
