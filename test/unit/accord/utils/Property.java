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
import org.agrona.collections.LongArrayList;

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
import java.util.function.BiFunction;
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
        addList(sb, "\t\t", history);
        return sb.toString();
    }

    private static void addList(StringBuilder sb, String prefix, List<String> list)
    {
        int idx = 0;
        for (var event : list)
            sb.append(prefix).append(++idx).append(": ").append(event).append('\n');
    }

    public static String formatList(String prefix, List<String> list)
    {
        StringBuilder sb = new StringBuilder();
        addList(sb, prefix, list);
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
                LongArrayList historyTiming = stepTimeout == null ? null : new LongArrayList();
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
                                    process(sub, state, sut, history.size(), historyTiming);
                                }
                            }
                            else
                            {
                                history.add(cmd.detailed(state));
                                process(cmd, state, sut, history.size(), historyTiming);
                            }
                        }
                        commands.destroySut(sut, null);
                        commands.destroyState(state, null);
                        commands.onSuccess(state, sut, maybeRewriteHistory(history, historyTiming));
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

                    throw new PropertyError(statefulPropertyError(this, t, state, maybeRewriteHistory(history, historyTiming)), t);
                }
                if (pure)
                {
                    seed = rs.nextLong();
                    rs.setSeed(seed);
                }
            }
        }

        private static List<String> maybeRewriteHistory(List<String> history, @Nullable LongArrayList historyTiming)
        {
            if (historyTiming == null) return history;
            List<String> newHistory = new ArrayList<>(history.size());
            for (int i = 0; i < history.size(); i++)
            {
                String step = history.get(i);
                long timeNanos = historyTiming.getLong(i);
                newHistory.add(step + ";\tDuration " + Duration.ofNanos(timeNanos));
            }
            return newHistory;
        }

        private <State, SystemUnderTest> void process(Command cmd, State state, SystemUnderTest sut, int id, @Nullable LongArrayList stepTiming) throws Throwable
        {
            if (stepTimeout == null)
            {
                cmd.process(state, sut);
                return;
            }
            long startNanos = System.nanoTime();
            try
            {
                TimeoutUtils.runBlocking(stepTimeout, "Stateful Step " + id + ": " + cmd.detailed(state), () -> cmd.process(state, sut));
            }
            finally
            {
                stepTiming.add(System.nanoTime() - startNanos);
            }
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

    public static class ForwardingCommand<State, SystemUnderTest, Result> implements Command<State, SystemUnderTest, Result>
    {
        private final Command<State, SystemUnderTest, Result> delegate;

        public ForwardingCommand(Command<State, SystemUnderTest, Result> delegate)
        {
            this.delegate = delegate;
        }

        protected Command<State, SystemUnderTest, Result> delegate()
        {
            return delegate;
        }

        @Override
        public PreCheckResult checkPreconditions(State state)
        {
            return delegate().checkPreconditions(state);
        }

        @Override
        public Result apply(State state) throws Throwable
        {
            return delegate().apply(state);
        }

        @Override
        public Result run(SystemUnderTest sut) throws Throwable
        {
            return delegate().run(sut);
        }

        @Override
        public void checkPostconditions(State state, Result expected, SystemUnderTest sut, Result actual) throws Throwable
        {
            delegate().checkPostconditions(state, expected, sut, actual);
        }

        @Override
        public String detailed(State state)
        {
            return delegate().detailed(state);
        }

        @Override
        public void process(State state, SystemUnderTest sut) throws Throwable
        {
            // don't call delegate here else the process function calls the delegate and not this class
            Command.super.process(state, sut);
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
        default void onSuccess(State state, SystemUnderTest sut, List<String> history) throws Throwable {}
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

    public interface StatefulSuccess<State, SystemUnderTest>
    {
        void apply(State state, SystemUnderTest sut, List<String> history) throws Throwable;
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
        @Nullable
        private Map<Predicate<State>, List<Setup<State, SystemUnderTest>>> conditionalCommands = null;
        private Gen.IntGen unknownWeightGen = Gens.ints().between(1, 10);
        @Nullable
        private FailingConsumer<State> preCommands = null;
        @Nullable
        private FailingBiConsumer<State, Throwable> destroyState = null;
        @Nullable
        private FailingBiConsumer<SystemUnderTest, Throwable> destroySut = null;
        @Nullable
        private BiFunction<State, Gen<Command<State, SystemUnderTest, ?>>, Gen<Command<State, SystemUnderTest, ?>>> commandsTransformer = null;
        private final List<StatefulSuccess<State, SystemUnderTest>> onSuccess = new ArrayList<>();

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
            return addIf(predicate, (rs, state) -> cmd.next(rs));
        }

        public CommandsBuilder<State, SystemUnderTest> addIf(Predicate<State> predicate, Command<State, SystemUnderTest, ?> cmd)
        {
            return addIf(predicate, (rs, state) -> cmd);
        }

        public CommandsBuilder<State, SystemUnderTest> addIf(Predicate<State> predicate, Setup<State, SystemUnderTest> cmd)
        {
            if (conditionalCommands == null)
                conditionalCommands = new LinkedHashMap<>();
            conditionalCommands.computeIfAbsent(predicate, i -> new ArrayList<>()).add(cmd);
            return this;
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

                @Override
                public IfBuilder<State, SystemUnderTest> addIf(Predicate<State> nextPredicate, Setup<State, SystemUnderTest> cmd) {
                    CommandsBuilder.this.addIf(predicate.and(nextPredicate), cmd);
                    return this;
                }
            });
            return this;
        }

        public interface IfBuilder<State, SystemUnderTest>
        {
            IfBuilder<State, SystemUnderTest> add(Setup<State, SystemUnderTest> cmd);
            IfBuilder<State, SystemUnderTest> addIf(Predicate<State> predicate, Setup<State, SystemUnderTest> cmd);
        }

        public CommandsBuilder<State, SystemUnderTest> unknownWeight(Gen.IntGen unknownWeightGen)
        {
            this.unknownWeightGen = Objects.requireNonNull(unknownWeightGen);
            return this;
        }

        public CommandsBuilder<State, SystemUnderTest> commandsTransformer(BiFunction<State, Gen<Command<State, SystemUnderTest, ?>>, Gen<Command<State, SystemUnderTest, ?>>> commandsTransformer)
        {
            this.commandsTransformer = commandsTransformer;
            return this;
        }

        public CommandsBuilder<State, SystemUnderTest> onSuccess(StatefulSuccess<State, SystemUnderTest> fn)
        {
            onSuccess.add(fn);
            return this;
        }

        public Commands<State, SystemUnderTest> build()
        {
            Gen<Setup<State, SystemUnderTest>> commandsGen;
            if (unknownWeights == null && conditionalCommands == null)
            {
                commandsGen = Gens.pick(new LinkedHashMap<>(knownWeights));
            }
            else
            {
                class DynamicWeightsGen implements Gen<Setup<State, SystemUnderTest>>, Gens.Reset
                {
                    LinkedHashMap<Setup<State, SystemUnderTest>, Integer> weights;
                    LinkedHashMap<Setup<State, SystemUnderTest>, Integer> conditionalWeights;
                    Gen<Setup<State, SystemUnderTest>> nonConditional;
                    @Override
                    public Setup<State, SystemUnderTest> next(RandomSource rs)
                    {
                        if (weights == null)
                        {
                            // create random weights
                            weights = new LinkedHashMap<>(knownWeights);
                            if (unknownWeights != null)
                            {
                                for (Setup<State, SystemUnderTest> s : unknownWeights)
                                    weights.put(s, unknownWeightGen.nextInt(rs));
                            }
                            nonConditional = Gens.pick(weights);
                            if (conditionalCommands != null)
                            {
                                conditionalWeights = new LinkedHashMap<>();
                                for (List<Setup<State, SystemUnderTest>> commands : conditionalCommands.values())
                                {
                                    for (Setup<State, SystemUnderTest> c : commands)
                                        conditionalWeights.put(c, unknownWeightGen.nextInt(rs));
                                }
                            }
                        }
                        if (conditionalWeights == null) return nonConditional.next(rs);
                        return (r, s) -> {
                            // need to figure out what conditions apply...
                            LinkedHashMap<Setup<State, SystemUnderTest>, Integer> clone = new LinkedHashMap<>(weights);
                            for (Map.Entry<Predicate<State>, List<Setup<State, SystemUnderTest>>> e : conditionalCommands.entrySet())
                            {
                                if (e.getKey().test(s))
                                    e.getValue().forEach(c -> clone.put(c, conditionalWeights.get(c)));
                            }
                            Setup<State, SystemUnderTest> select = Gens.pick(clone).next(r);
                            return select.setup(r, s);
                        };
                    }

                    @Override
                    public void reset()
                    {
                        weights = null;
                        nonConditional = null;
                        conditionalWeights = null;
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
                    Gen<Command<State, SystemUnderTest, ?>> map = commandsGen.map((rs, setup) -> setup.setup(rs, state));
                    return commandsTransformer == null ? map : commandsTransformer.apply(state, map);
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

                @Override
                public void onSuccess(State state, SystemUnderTest sut, List<String> history) throws Throwable
                {
                    for (var fn : onSuccess)
                        fn.apply(state, sut, history);
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
