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

package accord.utils;

import accord.utils.async.TimeoutUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class Property
{
    public static abstract class Common<T extends Common<T>>
    {
        protected long seed = SeedProvider.instance.nextSeed();
        protected int examples = 1000;

        protected boolean pure = true;
        @Nullable
        protected Duration timeout = null;

        protected Common() {
        }

        protected Common(Common<?> other) {
            this.seed = other.seed;
            this.examples = other.examples;
            this.pure = other.pure;
            this.timeout = other.timeout;
        }

        public T withSeed(long seed)
        {
            this.seed = seed;
            return (T) this;
        }

        public T withExamples(int examples)
        {
            if (examples <= 0)
                throw new IllegalArgumentException("Examples must be positive");
            this.examples = examples;
            return (T) this;
        }

        public T withPure(boolean pure)
        {
            this.pure = pure;
            return (T) this;
        }

        public T withTimeout(Duration timeout)
        {
            this.timeout = timeout;
            this.pure = false;
            return (T) this;
        }

        protected void checkWithTimeout(Runnable fn)
        {
            try
            {
                TimeoutUtils.runBlocking(timeout, "property with timeout", fn::run);
            }
            catch (ExecutionException e)
            {
                throw new PropertyError(propertyError(this, e.getCause()));
            }
            catch (InterruptedException e)
            {
                throw new PropertyError(propertyError(this, e));
            }
            catch (TimeoutException e)
            {
                TimeoutException override = new TimeoutException("property test did not complete within " + this.timeout);
                override.setStackTrace(new StackTraceElement[0]);
                throw new PropertyError(propertyError(this, override));
            }
        }
    }

    public static class ForBuilder extends Common<ForBuilder>
    {
        public void check(FailingConsumer<RandomSource> fn)
        {
            forAll(Gens.random()).check(fn);
        }

        public <T> SingleBuilder<T> forAll(Gen<T> gen)
        {
            return new SingleBuilder<>(gen, this);
        }

        public <A, B> DoubleBuilder<A, B> forAll(Gen<A> a, Gen<B> b)
        {
            return new DoubleBuilder<>(a, b, this);
        }

        public <A, B, C> TrippleBuilder<A, B, C> forAll(Gen<A> a, Gen<B> b, Gen<C> c)
        {
            return new TrippleBuilder<>(a, b, c, this);
        }
    }

    private static Object normalizeValue(Object value)
    {
        if (value == null)
            return null;
        // one day java arrays will have a useful toString... one day...
        if (value.getClass().isArray())
        {
            Class<?> subType = value.getClass().getComponentType();
            if (!subType.isPrimitive())
                return Arrays.asList((Object[]) value);
            if (Byte.TYPE == subType)
                return Arrays.toString((byte[]) value);
            if (Character.TYPE == subType)
                return Arrays.toString((char[]) value);
            if (Short.TYPE == subType)
                return Arrays.toString((short[]) value);
            if (Integer.TYPE == subType)
                return Arrays.toString((int[]) value);
            if (Long.TYPE == subType)
                return Arrays.toString((long[]) value);
            if (Float.TYPE == subType)
                return Arrays.toString((float[]) value);
            if (Double.TYPE == subType)
                return Arrays.toString((double[]) value);
        }
        try
        {
            String result = value.toString();
            if (result != null && result.length() > 100 && value instanceof Collection)
                result = ((Collection<?>) value).stream().map(o -> "\n\t     " + o).collect(Collectors.joining(",", "[", "]"));
            return result;
        }
        catch (Throwable t)
        {
            return "Object.toString failed: " + t.getClass().getCanonicalName() + ": " + t.getMessage();
        }
    }

    private static StringBuilder propertyErrorCommon(Common<?> input, Throwable cause)
    {
        StringBuilder sb = new StringBuilder();
        // return "Seed=" + seed + "\nExamples=" + examples;
        sb.append("Property error detected:\nSeed = ").append(input.seed).append('\n');
        sb.append("Examples = ").append(input.examples).append('\n');
        sb.append("Pure = ").append(input.pure).append('\n');
        if (cause != null)
        {
            String msg = cause.getMessage();
            sb.append("Error: ");
            // to improve readability, if a newline is detected move the error msg to the next line
            if (msg != null && msg.contains("\n"))
                msg = "\n\t" + msg.replace("\n", "\n\t");
            if (msg == null)
                msg = cause.getClass().getCanonicalName();
            sb.append(msg).append('\n');
        }
        return sb;
    }

    private static String propertyError(Common<?> input, Throwable cause, Object... values)
    {
        StringBuilder sb = propertyErrorCommon(input, cause);
        if (values != null)
        {
            sb.append("Values:\n");
            for (int i = 0; i < values.length; i++)
                sb.append('\t').append(i).append(" = ").append(normalizeValue(values[i])).append(": ").append(values[i] == null ? "unknown type" : values[i].getClass().getCanonicalName()).append('\n');
        }
        return sb.toString();
    }

    private static String statefulPropertyError(StatefulBuilder input, Throwable cause, Object state, List<String> history)
    {
        StringBuilder sb = propertyErrorCommon(input, cause);
        sb.append("Steps: ").append(input.steps).append('\n');
        sb.append("Values:\n");
        String stateStr = state == null ? null : state.toString().replace("\n", "\n\t\t");
        sb.append("\tState: ").append(stateStr).append(": ").append(state == null ? "unknown type" : state.getClass().getCanonicalName()).append('\n');
        sb.append("\tHistory:").append('\n');
        int idx = 0;
        for (var event : history)
            sb.append("\t\t").append(++idx).append(": ").append(event).append('\n');
        return sb.toString();
    }

    public interface FailingConsumer<A>
    {
        void accept(A value) throws Exception;
    }

    public static class SingleBuilder<T> extends Common<SingleBuilder<T>>
    {
        private final Gen<T> gen;

        private SingleBuilder(Gen<T> gen, Common<?> other) {
            super(other);
            this.gen = Objects.requireNonNull(gen);
        }

        public void check(FailingConsumer<T> fn)
        {
            if (timeout != null)
            {
                checkWithTimeout(() -> checkInternal(fn));
                return;
            }
            checkInternal(fn);
        }

        private void checkInternal(FailingConsumer<T> fn)
        {
            RandomSource random = new DefaultRandom(seed);
            for (int i = 0; i < examples; i++)
            {
                T value = null;
                try
                {
                    checkInterrupted();
                    fn.accept(value = gen.next(random));
                }
                catch (Throwable t)
                {
                    throw new PropertyError(propertyError(this, t, value), t);
                }
                if (pure)
                {
                    seed = random.nextLong();
                    random.setSeed(seed);
                }
            }
        }
    }

    public interface FailingBiConsumer<A, B>
    {
        void accept(A a, B b) throws Exception;
    }

    public static class DoubleBuilder<A, B> extends Common<DoubleBuilder<A, B>>
    {
        private final Gen<A> aGen;
        private final Gen<B> bGen;

        private DoubleBuilder(Gen<A> aGen, Gen<B> bGen, Common<?> other) {
            super(other);
            this.aGen = Objects.requireNonNull(aGen);
            this.bGen = Objects.requireNonNull(bGen);
        }

        public void check(FailingBiConsumer<A, B> fn)
        {
            if (timeout != null)
            {
                checkWithTimeout(() -> checkInternal(fn));
                return;
            }
            checkInternal(fn);
        }

        private void checkInternal(FailingBiConsumer<A, B> fn)
        {
            RandomSource random = new DefaultRandom(seed);
            for (int i = 0; i < examples; i++)
            {
                A a = null;
                B b = null;
                try
                {
                    checkInterrupted();
                    fn.accept(a = aGen.next(random), b = bGen.next(random));
                }
                catch (Throwable t)
                {
                    throw new PropertyError(propertyError(this, t, a, b), t);
                }
                if (pure)
                {
                    seed = random.nextLong();
                    random.setSeed(seed);
                }
            }
        }
    }

    public interface FailingTriConsumer<A, B, C>
    {
        void accept(A a, B b, C c) throws Exception;
    }

    public static class TrippleBuilder<A, B, C> extends Common<TrippleBuilder<A, B, C>>
    {
        private final Gen<A> as;
        private final Gen<B> bs;
        private final Gen<C> cs;

        public TrippleBuilder(Gen<A> as, Gen<B> bs, Gen<C> cs, Common<?> other)
        {
            super(other);
            this.as = as;
            this.bs = bs;
            this.cs = cs;
        }

        public void check(FailingTriConsumer<A, B, C> fn)
        {
            if (timeout != null)
            {
                checkWithTimeout(() -> checkInternal(fn));
                return;
            }
            checkInternal(fn);
        }

        private void checkInternal(FailingTriConsumer<A, B, C> fn)
        {
            RandomSource random = new DefaultRandom(seed);
            for (int i = 0; i < examples; i++)
            {
                A a = null;
                B b = null;
                C c = null;
                try
                {
                    checkInterrupted();
                    fn.accept(a = as.next(random), b = bs.next(random), c = cs.next(random));
                }
                catch (Throwable t)
                {
                    throw new PropertyError(propertyError(this, t, a, b, c), t);
                }
                if (pure)
                {
                    seed = random.nextLong();
                    random.setSeed(seed);
                }
            }
        }
    }

    private static void checkInterrupted() throws InterruptedException
    {
        if (Thread.currentThread().isInterrupted())
            throw new InterruptedException();
    }

    public static class PropertyError extends AssertionError
    {
        public PropertyError(String message, Throwable cause)
        {
            super(message, cause);
        }

        public PropertyError(String message)
        {
            super(message);
        }
    }

    public static ForBuilder qt()
    {
        return new ForBuilder();
    }

    public static StatefulBuilder stateful()
    {
        return new StatefulBuilder();
    }

    public static class StatefulBuilder extends Common<StatefulBuilder>
    {
        protected int steps = 1000;
        @Nullable
        protected Duration stepTimeout = null;

        public StatefulBuilder()
        {
            examples = 500;
        }

        public StatefulBuilder withSteps(int steps)
        {
            this.steps = steps;
            return this;
        }

        public StatefulBuilder withStepTimeout(Duration duration)
        {
            stepTimeout = duration;
            return this;
        }

        @SuppressWarnings("rawtypes")
        public <State, SystemUnderTest> void check(Commands<State, SystemUnderTest> commands)
        {
            RandomSource rs = new DefaultRandom(seed);
            for (int i = 0; i < examples; i++)
            {
                State state = null;
                List<String> history = new ArrayList<>(steps);
                try
                {
                    checkInterrupted();

                    state = commands.genInitialState().next(rs);
                    SystemUnderTest sut = commands.createSut(state);

                    try
                    {
                        for (int j = 0; j < steps; j++)
                        {
                            Gen<Command<State, SystemUnderTest, ?>> cmdGen = commands.commands(state);
                            Command cmd = cmdGen.next(rs);
                            for (int a = 0; cmd.checkPreconditions(state) != PreCheckResult.Ok && a < 42; a++)
                            {
                                if (a == 41)
                                    throw new IllegalArgumentException("Unable to find next command");
                                cmd = cmdGen.next(rs);
                            }
                            if (cmd instanceof MultistepCommand)
                            {
                                for (Command<State, SystemUnderTest, ?> sub : ((MultistepCommand<State, SystemUnderTest>) cmd))
                                {
                                    history.add(sub.detailed(state));
                                    process(sub, state, sut, history.size());
                                }
                            }
                            else
                            {
                                history.add(cmd.detailed(state));
                                process(cmd, state, sut, history.size());
                            }
                        }
                        commands.destroySut(sut, null);
                        commands.destroyState(state, null);
                    }
                    catch (Throwable t)
                    {
                        try
                        {
                            commands.destroySut(sut, t);
                            commands.destroyState(state, t);
                        }
                        catch (Throwable t2)
                        {
                            t.addSuppressed(t2);
                        }
                        throw t;
                    }
                }
                catch (Throwable t)
                {
                    throw new PropertyError(statefulPropertyError(this, t, state, history), t);
                }
                if (pure)
                {
                    seed = rs.nextLong();
                    rs.setSeed(seed);
                }
            }
        }

        private <State, SystemUnderTest> void process(Command cmd, State state, SystemUnderTest sut, int id) throws Throwable
        {
            if (stepTimeout == null)
            {
                cmd.process(state, sut);
                return;
            }
            TimeoutUtils.runBlocking(stepTimeout, "Stateful Step " + id, () -> cmd.process(state, sut));
        }
    }

    public enum PreCheckResult { Ok, Ignore }
    public interface Command<State, SystemUnderTest, Result>
    {
        default PreCheckResult checkPreconditions(State state) {return PreCheckResult.Ok;}
        Result apply(State state) throws Throwable;
        Result run(SystemUnderTest sut) throws Throwable;
        default void checkPostconditions(State state, Result expected,
                                         SystemUnderTest sut, Result actual) throws Throwable {}
        default String detailed(State state) {return this.toString();}
        default void process(State state, SystemUnderTest sut) throws Throwable
        {
            checkPostconditions(state, apply(state),
                                sut, run(sut));
        }
    }

    public static <State, SystemUnderTest> MultistepCommand<State, SystemUnderTest> multistep(Command<State, SystemUnderTest, ?>... cmds)
    {
        return multistep(Arrays.asList(cmds));
    }

    public static <State, SystemUnderTest> MultistepCommand<State, SystemUnderTest> multistep(List<Command<State, SystemUnderTest, ?>> cmds)
    {
        List<Command<State, SystemUnderTest, ?>> result = new ArrayList<>(cmds.size());
        for (Command<State, SystemUnderTest, ?> c : cmds)
        {
            if (c instanceof MultistepCommand) result.addAll(flatten((MultistepCommand<State, SystemUnderTest>) c));
            else                               result.add(c);
        }
        return result::iterator;
    }

    private static <State, SystemUnderTest> Collection<? extends Command<State, SystemUnderTest, ?>> flatten(MultistepCommand<State, SystemUnderTest> mc)
    {
        List<Command<State, SystemUnderTest, ?>> result = new ArrayList<>();
        for (Command<State, SystemUnderTest, ?> c : mc)
        {
            if (c instanceof MultistepCommand) result.addAll(flatten((MultistepCommand<State, SystemUnderTest>) c));
            else                               result.add(c);
        }
        return result;
    }

    public interface MultistepCommand<State, SystemUnderTest> extends Command<State, SystemUnderTest, Object>, Iterable<Command<State, SystemUnderTest, ?>>
    {
        @Override
        default PreCheckResult checkPreconditions(State state)
        {
            for (Command<State, SystemUnderTest, ?> cmd : this)
            {
                PreCheckResult result = cmd.checkPreconditions(state);
                if (result != PreCheckResult.Ok) return result;
            }
            return PreCheckResult.Ok;
        }

        @Override
        default Object apply(State state) throws Throwable
        {
            throw new UnsupportedOperationException();
        }

        @Override
        default Object run(SystemUnderTest sut) throws Throwable
        {
            throw new UnsupportedOperationException();
        }

        @Override
        default void checkPostconditions(State state, Object expected, SystemUnderTest sut, Object actual) throws Throwable
        {
            throw new UnsupportedOperationException();
        }

        @Override
        default String detailed(State state)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        default void process(State state, SystemUnderTest sut) throws Throwable
        {
            throw new UnsupportedOperationException();
        }
    }

    public static <State, SystemUnderTest, Result> Command<State, SystemUnderTest, Result> ignoreCommand()
    {
        return new Command<>()
        {
            @Override
            public PreCheckResult checkPreconditions(State state)
            {
                return PreCheckResult.Ignore;
            }

            @Override
            public Result apply(State state) throws Throwable
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Result run(SystemUnderTest sut) throws Throwable
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public String detailed(State state)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public interface UnitCommand<State, SystemUnderTest> extends Command<State, SystemUnderTest, Void>
    {
        void applyUnit(State state) throws Throwable;
        void runUnit(SystemUnderTest sut) throws Throwable;

        @Override
        default Void apply(State state) throws Throwable
        {
            applyUnit(state);
            return null;
        }

        @Override
        default Void run(SystemUnderTest sut) throws Throwable
        {
            runUnit(sut);
            return null;
        }
    }

    public interface StateOnlyCommand<State> extends UnitCommand<State, Void>
    {
        @Override
        default void runUnit(Void sut) throws Throwable {}
    }

    public static class SimpleCommand<State> implements StateOnlyCommand<State>
    {
        private final Function<State, String> name;
        private final Consumer<State> fn;

        public SimpleCommand(String name, Consumer<State> fn)
        {
            this.name = ignore -> name;
            this.fn = fn;
        }

        public SimpleCommand(Function<State, String> name, Consumer<State> fn)
        {
            this.name = name;
            this.fn = fn;
        }

        @Override
        public String detailed(State state)
        {
            return name.apply(state);
        }

        @Override
        public void applyUnit(State state)
        {
            fn.accept(state);
        }
    }

    public interface Commands<State, SystemUnderTest>
    {
        Gen<State> genInitialState() throws Throwable;
        SystemUnderTest createSut(State state) throws Throwable;
        default void destroyState(State state, @Nullable Throwable cause) throws Throwable {}
        default void destroySut(SystemUnderTest sut, @Nullable Throwable cause) throws Throwable {}
        Gen<Command<State, SystemUnderTest, ?>> commands(State state) throws Throwable;
    }

    public static <State, SystemUnderTest> CommandsBuilder<State, SystemUnderTest> commands(Supplier<Gen<State>> stateGen, Function<State, SystemUnderTest> sutFactory)
    {
        return new CommandsBuilder<>(stateGen, sutFactory);
    }

    public static <State> CommandsBuilder<State, Void> commands(Supplier<Gen<State>> stateGen)
    {
        return new CommandsBuilder<>(stateGen, ignore -> null);
    }

    public static class CommandsBuilder<State, SystemUnderTest>
    {
        public interface Setup<State, SystemUnderTest>
        {
            Command<State, SystemUnderTest, ?> setup(RandomSource rs, State state);
        }
        private final Supplier<Gen<State>> stateGen;
        private final Function<State, SystemUnderTest> sutFactory;
        private final Map<Setup<State, SystemUnderTest>, Integer> knownWeights = new LinkedHashMap<>();
        @Nullable
        private Set<Setup<State, SystemUnderTest>> unknownWeights = null;
        private Gen.IntGen unknownWeightGen = Gens.ints().between(1, 10);
        @Nullable
        private FailingConsumer<State> preCommands = null;
        @Nullable
        private FailingBiConsumer<State, Throwable> destroyState = null;
        @Nullable
        private FailingBiConsumer<SystemUnderTest, Throwable> destroySut = null;

        public CommandsBuilder(Supplier<Gen<State>> stateGen, Function<State, SystemUnderTest> sutFactory)
        {
            this.stateGen = stateGen;
            this.sutFactory = sutFactory;
        }

        public CommandsBuilder<State, SystemUnderTest> preCommands(FailingConsumer<State> preCommands)
        {
            this.preCommands = preCommands;
            return this;
        }

        public CommandsBuilder<State, SystemUnderTest> destroyState(FailingConsumer<State> destroyState)
        {
            return destroyState((success, failure) -> {
                if (failure == null)
                    destroyState.accept(success);
            });
        }

        public CommandsBuilder<State, SystemUnderTest> destroyState(FailingBiConsumer<State, Throwable> destroyState)
        {
            this.destroyState = destroyState;
            return this;
        }

        public CommandsBuilder<State, SystemUnderTest> destroySut(FailingConsumer<SystemUnderTest> destroySut)
        {
            return destroySut((success, failure) -> {
                if (failure == null)
                    destroySut.accept(success);
            });
        }

        public CommandsBuilder<State, SystemUnderTest> destroySut(FailingBiConsumer<SystemUnderTest, Throwable> destroySut)
        {
            this.destroySut = destroySut;
            return this;
        }

        public CommandsBuilder<State, SystemUnderTest> add(int weight, Command<State, SystemUnderTest, ?> cmd)
        {
            return add(weight, (i1, i2) -> cmd);
        }

        public CommandsBuilder<State, SystemUnderTest> add(int weight, Gen<Command<State, SystemUnderTest, ?>> cmd)
        {
            return add(weight, (rs, state) -> cmd.next(rs));
        }

        public CommandsBuilder<State, SystemUnderTest> add(int weight, Setup<State, SystemUnderTest> cmd)
        {
            knownWeights.put(cmd, weight);
            return this;
        }

        public CommandsBuilder<State, SystemUnderTest> add(Command<State, SystemUnderTest, ?> cmd)
        {
            return add((i1, i2) -> cmd);
        }

        public CommandsBuilder<State, SystemUnderTest> add(Gen<Command<State, SystemUnderTest, ?>> cmd)
        {
            return add((rs, state) -> cmd.next(rs));
        }

        public CommandsBuilder<State, SystemUnderTest> add(Setup<State, SystemUnderTest> cmd)
        {
            if (unknownWeights == null)
                unknownWeights = new LinkedHashSet<>();
            unknownWeights.add(cmd);
            return this;
        }

        public CommandsBuilder<State, SystemUnderTest> addIf(Predicate<State> predicate, Gen<Command<State, SystemUnderTest, ?>> cmd)
        {
            return add((rs, state) -> {
                if (!predicate.test(state)) return ignoreCommand();
                return cmd.next(rs);
            });
        }

        public CommandsBuilder<State, SystemUnderTest> addIf(Predicate<State> predicate, Setup<State, SystemUnderTest> cmd)
        {
            return add((rs, state) -> {
                if (!predicate.test(state)) return ignoreCommand();
                return cmd.setup(rs, state);
            });
        }

        public CommandsBuilder<State, SystemUnderTest> addAllIf(Predicate<State> predicate, Consumer<IfBuilder<State, SystemUnderTest>> sub)
        {
            sub.accept(new IfBuilder<>()
            {
                @Override
                public IfBuilder<State, SystemUnderTest> add(Setup<State, SystemUnderTest> cmd)
                {
                    CommandsBuilder.this.addIf(predicate, cmd);
                    return this;
                }
            });
            return this;
        }

        public interface IfBuilder<State, SystemUnderTest>
        {
            IfBuilder<State, SystemUnderTest> add(Setup<State, SystemUnderTest> cmd);
        }

        public CommandsBuilder<State, SystemUnderTest> unknownWeight(Gen.IntGen unknownWeightGen)
        {
            this.unknownWeightGen = Objects.requireNonNull(unknownWeightGen);
            return this;
        }

        public Commands<State, SystemUnderTest> build()
        {
            Gen<Setup<State, SystemUnderTest>> commandsGen;
            if (unknownWeights == null)
            {
                commandsGen = Gens.pick(new LinkedHashMap<>(knownWeights));
            }
            else
            {
                class DynamicWeightsGen implements Gen<Setup<State, SystemUnderTest>>, Gens.Reset
                {
                    Gen<Setup<State, SystemUnderTest>> gen;
                    @Override
                    public Setup<State, SystemUnderTest> next(RandomSource rs)
                    {
                        if (gen == null)
                        {
                            // create random weights
                            LinkedHashMap<Setup<State, SystemUnderTest>, Integer> clone = new LinkedHashMap<>(knownWeights);
                            for (Setup<State, SystemUnderTest> s : unknownWeights)
                                clone.put(s, unknownWeightGen.nextInt(rs));
                            gen = Gens.pick(clone);
                        }
                        return gen.next(rs);
                    }

                    @Override
                    public void reset()
                    {
                        gen = null;
                    }
                }
                commandsGen = new DynamicWeightsGen();
            }
            return new Commands<>()
            {
                @Override
                public Gen<State> genInitialState() throws Throwable
                {
                    return stateGen.get();
                }

                @Override
                public SystemUnderTest createSut(State state) throws Throwable
                {
                    return sutFactory.apply(state);
                }

                @Override
                public Gen<Command<State, SystemUnderTest, ?>> commands(State state) throws Throwable
                {
                    if (preCommands != null)
                        preCommands.accept(state);
                    return commandsGen.map((rs, setup) -> setup.setup(rs, state));
                }

                @Override
                public void destroyState(State state, @Nullable Throwable cause) throws Throwable
                {
                    Gens.Reset.tryReset(commandsGen);
                    if (destroyState != null)
                        destroyState.accept(state, cause);
                }

                @Override
                public void destroySut(SystemUnderTest sut, @Nullable Throwable cause) throws Throwable
                {
                    if (destroySut != null)
                        destroySut.accept(sut, cause);
                }
            };
        }

        public interface FailingConsumer<T>
        {
            void accept(T value) throws Throwable;
        }

        public interface FailingBiConsumer<A, B>
        {
            void accept(A a, B b) throws Throwable;
        }
    }
}
