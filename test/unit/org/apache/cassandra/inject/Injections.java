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
package org.apache.cassandra.inject;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NativeLibrary;
import org.jboss.byteman.agent.install.Install;
import org.jboss.byteman.agent.submit.Submit;
import org.jboss.byteman.rule.helper.Helper;

import static org.apache.cassandra.inject.ActionBuilder.newActionBuilder;
import static org.apache.cassandra.inject.Expression.expr;
import static org.apache.cassandra.inject.Expression.method;
import static org.apache.cassandra.inject.Expression.quote;

public class Injections
{
    private static Submit submitter;

    public static void inject(Injection...injections) throws Throwable
    {
        String script = Arrays.stream(injections).map(Injection::format).collect(Collectors.joining("\n"));
        getSubmitter().addRulesFromResources(Lists.newArrayList(IOUtils.toInputStream(script)));
    }

    public static void deleteAll()
    {
        try
        {
            getSubmitter().deleteAllRules();
        }
        catch (Throwable ignore)
        {
            // Ignore because it will throw if there aren't any injections
        }
    }

    private static Submit getSubmitter() throws Throwable
    {
        if (submitter == null)
        {
            submitter = new Submit(FBUtilities.getBroadcastAddressAndPort().getAddress().getHostAddress(), loadAgent());
        }
        return submitter;
    }

    private static int loadAgent() throws Throwable
    {
        int port = getPort();
        long pid = getProcessId();
        List<String> properties = new ArrayList<>();
        properties.add("org.jboss.byteman.transform.all=true");
        Install.install(Long.toString(pid), true, true, FBUtilities.getBroadcastAddressAndPort().getAddress().getHostAddress(), port, properties.toArray(new String[0]));
        return port;
    }

    private static int getPort()
    {
        try (ServerSocket serverSocket = new ServerSocket(0))
        {
            return serverSocket.getLocalPort();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieves the process ID or <code>null</code> if the process ID cannot be retrieved.
     * @return the process ID or <code>null</code> if the process ID cannot be retrieved.
     */
    private static Long getProcessId()
    {
        long pid = NativeLibrary.getProcessID();
        if (pid >= 0)
            return pid;

        return getProcessIdFromJvmName();
    }

    /**
     * Retrieves the process ID from the JVM name.
     * @return the process ID or <code>null</code> if the process ID cannot be retrieved.
     */
    private static Long getProcessIdFromJvmName()
    {
        // the JVM name in Oracle JVMs is: '<pid>@<hostname>' but this might not be the case on all JVMs
        String jvmName = ManagementFactory.getRuntimeMXBean().getName();
        try
        {
            return Long.valueOf(jvmName.split("@")[0]);
        }
        catch (NumberFormatException e)
        {
            // ignore
        }
        return null;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface CallMe {}

    public abstract static class MultiInvokePointInjectionBuilder<T extends Injection, B extends InjectionBuilder<T>>
            extends InjectionBuilder<T>
    {
        final List<InvokePointBuilder> invokePointBuilders = new LinkedList<>();

        MultiInvokePointInjectionBuilder(String id)
        {
            super(id);
        }

        /**
         * Adds a new invoke point to the injection. Adding a new invoke point cause creation of a new rule because
         * a single rule can only have a single invoke point.
         */
        public B add(InvokePointBuilder ipb)
        {
            invokePointBuilders.add(ipb);
            return (B) this;
        }
    }

    public abstract static class CrossProductInjectionBuilder<T extends Injection, B extends InjectionBuilder<T>>
            extends MultiInvokePointInjectionBuilder<T, B>
    {
        final LinkedList<ActionBuilder> actionBuilders = new LinkedList<>();

        CrossProductInjectionBuilder(String id)
        {
            super(id);
        }

        Rule[] getRules()
        {
            Rule[] rules = new Rule[actionBuilders.size() * invokePointBuilders.size()];
            int i = 0;
            for (InvokePointBuilder ipb : invokePointBuilders)
            {
                for (ActionBuilder ab : actionBuilders)
                {
                    rules[i++] = Rule.newRule(id + "_" + i, ab, ipb);
                }
            }
            return rules;
        }

        /**
         * Adds a new action to the injection. Adding a new action cause creation of a new rule because a single rule
         * can only have a single action. Do not confuse an action with a statement - an action is bundle of bindings,
         * condition under which it can be invoked and a sequence of statements.
         */
        public B add(ActionBuilder builder)
        {
            if (builder.getHelperClass() == null)
            {
                builder.withHelperClass(Helper.class);
            }
            builder.conditions().when(getIsEnabledExpression());
            actionBuilders.add(builder);
            return (B) this;
        }

        /**
         * @see #add(ActionBuilder)
         */
        public B add(ActionBuilder.Builder builder)
        {
            return add(builder.toActionBuilder());
        }
    }

    /**
     * Creates a new {@link Counter} injection.
     *
     * @param name name of the internal counter
     */
    public static Counter.CounterBuilder newCounter(String name)
    {
        return new Counter.CounterBuilder(name);
    }

    /**
     * Creates an injection along with a distributed counter. It increments the counter whenever an invoke point
     * is reached. You can add multiple invoke points and you can also bundle additional actions - for example,
     * throw an exception and count how many times it happened.
     */
    public static class Counter extends Injection
    {
        private static final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
        private final String name;
        private final AtomicLong internalCounter;

        private Counter(String id, String name, Rule[] rules)
        {
            super(id, rules);
            this.name = name;
            this.internalCounter = counters.computeIfAbsent(name, n -> new AtomicLong());
            reset();
        }

        /**
         * Get a current value of the counter.
         */
        public long get()
        {
            return internalCounter.get();
        }

        /**
         * Reset counter value to 0.
         */
        public void reset()
        {
            internalCounter.set(0);
        }

        @CallMe
        public static void increment(String name)
        {
            counters.get(name).incrementAndGet();
        }

        public static class CounterBuilder extends CrossProductInjectionBuilder<Counter, CounterBuilder>
        {
            private final String name;

            private CounterBuilder(String name)
            {
                super(String.format("counter/%s/%s", name, UUID.randomUUID()));
                this.name = name;
                add(newActionBuilder().actions().doAction(method(Counter.class, CallMe.class).args(quote(name))));
            }

            @Override
            public Counter build()
            {
                return new Counter(id, name, getRules());
            }
        }
    }

    /**
     * Creates a new {@link Barrier} injection.
     *
     * @param name name of the barrier
     * @param parties how many parties the barrier should wait for
     * @param cyclic whether the barrier should reset after resume
     */
    public static Barrier.BarrierBuilder newBarrier(String name, int parties, boolean cyclic)
    {
        return new Barrier.BarrierBuilder(name, parties, cyclic, true, true);
    }

    /**
     * Creates a {@link Barrier} injection point where the number of parties is just counted down but it does not await.
     *
     * @param name name of the barrier
     * @param parties how many parties a new barrier should wait for
     * @param cyclic whether the barrier should reset after resume
     */
    public static Barrier.BarrierBuilder newBarrierCountDown(String name, int parties, boolean cyclic)
    {
        return new Barrier.BarrierBuilder(name, parties, cyclic, true, false);
    }

    /**
     * Creates a {@link Barrier} injection point where it just awaits but it does not decrement the number of parties
     * it is awaiting for.
     *
     * @param name name of the barrier
     * @param parties how many parties a new barrier should wait for
     * @param cyclic whether the barrier should reset after resume
     */
    public static Barrier.BarrierBuilder newBarrierAwait(String name, int parties, boolean cyclic)
    {
        return new Barrier.BarrierBuilder(name, parties, cyclic, false, true);
    }

    /**
     * Creates an injection with a distributed barrier awaiting for a defined number of parties to reach it
     * (including test node). It can be used to synchronize multiple nodes
     */
    public static class Barrier extends Injection
    {
        private static final Map<String, CyclicBarrier> barriers = new ConcurrentHashMap<>();
        private final boolean doCountDown;
        private final boolean doAwait;
        private final CyclicBarrier internalBarrier;

        private Barrier(String id, String name, int parties, boolean cyclic, boolean doCountDown, boolean doAwait, Rule[] rules)
        {
            super(id, rules);
            this.internalBarrier = barriers.computeIfAbsent(name, n -> new CyclicBarrier(name, parties, cyclic));
            this.doCountDown = doCountDown;
            this.doAwait = doAwait;
            reset();
        }

        /**
         * Do a single step for this barrier that is, decrement the number of parties and await, depending on which
         * actions are enabled. I may just decrement the number of parties, without awaiting as well as it may just
         * await, without decrementing the number of parties.
         */
        public void arrive() throws InterruptedException
        {
            internalBarrier.await(doCountDown, doAwait);
        }

        /**
         * Just wait for this barrier to be released. Do not decrement the number of parties regardless of how this
         * barrier was setup - this is force await.
         */
        public void await() throws InterruptedException
        {
            internalBarrier.await(false, true);
        }

        /**
         * Just decrement the number of parties this barrier is waiting for but do not wait regardless of how
         * this barrier was setup - this is force countdown.
         */
        public void countDown()
        {
            try
            {
                internalBarrier.await(true, false);
            }
            catch (InterruptedException ignored)
            {
                // will not happen in case just a countdown
            }
        }

        /**
         * Get the number of parties the barrier is currently awaiting for.
         */
        public long getCount()
        {
            return internalBarrier.getCount();
        }

        /**
         * Reset the barrier, that is, let all waiting threads resume and reset the number of parties
         * (if cyclic is enabled).
         */
        public void reset()
        {
            internalBarrier.reset();
        }

        @CallMe
        public static void doAction(String name, boolean doCountDown, boolean doAwait) throws InterruptedException
        {
            CyclicBarrier barrier = barriers.get(name);
            barrier.await(doCountDown, doAwait);
        }

        public static class BarrierBuilder extends CrossProductInjectionBuilder<Barrier, BarrierBuilder>
        {
            private final String name;
            private final int parties;
            private final boolean cyclic;
            private final boolean doAwait;
            private final boolean doCountDown;

            private BarrierBuilder(String name, int parties, boolean cyclic, boolean doCountDown, boolean doAwait)
            {
                super(String.format("barrier/%s/%s", name, UUID.randomUUID().toString()));
                this.name = name;
                this.parties = parties;
                this.cyclic = cyclic;
                this.doAwait = doAwait;
                this.doCountDown = doCountDown;
                add(newActionBuilder().actions().doAction(method(Barrier.class, CallMe.class)
                        .args(quote(name), doCountDown, doAwait)));
            }

            @Override
            public Barrier build()
            {
                return new Barrier(id, name, parties, cyclic, doCountDown, doAwait, getRules());
            }
        }
    }

    public abstract static class SingleActionBuilder<T extends Injection, B extends InjectionBuilder<T>>
            extends MultiInvokePointInjectionBuilder<T, B>
    {
        protected final ActionBuilder actionBuilder = newActionBuilder();

        public SingleActionBuilder(String id)
        {
            super(id);
            actionBuilder.conditions().when(getIsEnabledExpression());
        }

        @SuppressWarnings("unchecked")
        public B action(Consumer<ActionBuilder> builder)
        {
            builder.accept(actionBuilder);
            return (B) this;
        }

        protected Rule[] getRules()
        {
            Rule[] rules = new Rule[invokePointBuilders.size()];
            int i = 0;
            for (InvokePointBuilder ipb : invokePointBuilders)
            {
                rules[i++] = Rule.newRule(id + "_" + i, actionBuilder, ipb);
            }
            return rules;
        }
    }

    /**
     * Creates a custom injection which allows you to define your invoke points and actions from scratch.
     *
     * @param name name of the injection
     */
    public static CustomBuilder newCustom(String name)
    {
        return new CustomBuilder(name);
    }

    public static class CustomBuilder extends CrossProductInjectionBuilder<Injection, CustomBuilder>
    {
        public CustomBuilder(String name)
        {
            super(String.format("custom/%s/%s", name, UUID.randomUUID()));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Injection build()
        {
            return new Injection(id, getRules()) {};
        }
    }

    /**
     * Creates a pause to hold on the thread for a defined amount of time.
     *
     * @param name name of the pause
     * @param timeout time out in milliseconds
     */
    public static PauseBuilder newPause(String name, long timeout)
    {
        return new PauseBuilder(name, timeout);
    }

    public static class PauseBuilder extends SingleActionBuilder<Injection, PauseBuilder>
    {
        public PauseBuilder(String name, long timeout)
        {
            super(String.format("pause/%s/%s", name, UUID.randomUUID()));
            actionBuilder.actions().doAction(expr("Thread.sleep").args(timeout));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Injection build()
        {
            return new Injection(id, getRules());
        }
    }
}
