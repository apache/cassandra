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

package org.apache.cassandra.simulator.paxos;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Inject;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.simulator.ClusterSimulation;
import org.apache.cassandra.simulator.SimulationRunner;
import org.apache.cassandra.simulator.SimulatorUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "paxos",
         description = "Run a paxos simulation",
         subcommands = { CommandLine.HelpCommand.class,
                         PaxosSimulationRunner.Run.class,
                         PaxosSimulationRunner.VersionCommand.class,
                         PaxosSimulationRunner.Record.class,
                         PaxosSimulationRunner.Reconcile.class})
public class PaxosSimulationRunner extends SimulationRunner implements Runnable
{
    @Command(name = "run")
    public static class Run extends SimulationRunner.Run<PaxosClusterSimulation.Builder>
    {
        @Option(names = "--consistency", description = "Specify the consistency level to perform paxos operations at")
        String consistency;

        @Option(names = "--with-paxos-state-cache", description = "Run with the paxos state cache always enabled")
        boolean withStateCache;

        @Option(names = "--without-paxos-state-cache", description = "Run with the paxos state cache always disabled")
        boolean withoutStateCache;

        @Option(names = "--variant", paramLabel = "from_paxos_variant", description = "Specify the initial paxos variant to use")
        String variant;

        @Option(names = "--to-variant",  paramLabel = "to_paxos_variant", description = "Specify the paxos variant to change to at some point during the simulation")
        String toVariant;

        public Run() {}

        @Override
        protected void propagate(PaxosClusterSimulation.Builder builder)
        {
            super.propagate(builder);
            propagateTo(consistency, withStateCache, withoutStateCache, variant, toVariant, builder);
        }

        @Override
        protected void run( long seed, PaxosClusterSimulation.Builder builder) throws IOException
        {
            super.run(seed, builder);
        }
    }

    @Command(name = "record")
    public static class Record extends SimulationRunner.Record<PaxosClusterSimulation.Builder>
    {
        @Option(names = "--consistency")
        String consistency;

        @Option(names = "--with-paxos-state-cache")
        boolean withStateCache;

        @Option(names = "--without-paxos-state-cache")
        boolean withoutStateCache;

        @Option(names = "--variant", description = "paxos variant to use")
        String variant;

        @Option(names = "--to-variant", description = "paxos variant to change to during the simulation")
        String toVariant;

        public Record() {}

        @Override
        protected void propagate(PaxosClusterSimulation.Builder builder)
        {
            super.propagate(builder);
            propagateTo(consistency, withStateCache, withoutStateCache, variant, toVariant, builder);
        }
    }

    @Command(name = "reconcile")
    public static class
    Reconcile extends SimulationRunner.Reconcile<PaxosClusterSimulation.Builder>
    {
        @Option(names = "--consistency")
        String consistency;

        @Option(names = "--with-paxos-state-cache")
        boolean withStateCache;

        @Option(names = "--without-paxos-state-cache")
        boolean withoutStateCache;

        @Option(names = "--variant", description = "paxos variant to use")
        String variant;

        @Option(names = "--to-variant", description = "paxos variant to change to during the simulation")
        String toVariant;
        
        public Reconcile() {}

        @Override
        protected void propagate(PaxosClusterSimulation.Builder builder)
        {
            super.propagate(builder);
            propagateTo(consistency, withStateCache, withoutStateCache, variant, toVariant, builder);
        }
    }

    static void propagateTo(String consistency, boolean withStateCache, boolean withoutStateCache, String variant, String toVariant, PaxosClusterSimulation.Builder builder)
    {
        Optional.ofNullable(consistency).map(ConsistencyLevel::valueOf).ifPresent(builder::consistency);
        if (withStateCache) builder.stateCache(true);
        if (withoutStateCache) builder.stateCache(false);
        Optional.ofNullable(variant).map(Config.PaxosVariant::valueOf).ifPresent(builder::initialPaxosVariant);
        Optional.ofNullable(toVariant).map(Config.PaxosVariant::valueOf).ifPresent(builder::finalPaxosVariant);
    }

    // for simple unit tests so we can simply invoke main()
    private static final AtomicInteger uniqueNum = new AtomicInteger();

    @Override
    public void run()
    {
        CommandLine.usage(this, System.out);
    }

    public static class InjectPaxosClusterSimulationFactory implements CommandLine.IFactory
    {
        private final CommandLine.IFactory defaultFactory = CommandLine.defaultFactory();
        private final ClusterSimulation.Builder<?> builder;

        public InjectPaxosClusterSimulationFactory(ClusterSimulation.Builder<?> builder)
        {
            this.builder = builder;
        }

        @Override
        public <K> K create(Class<K> cls) throws Exception
        {
            try
            {
                K bean = this.defaultFactory.create(cls);
                Class<?> beanClass = bean.getClass();
                do
                {
                    Field[] fields = beanClass.getDeclaredFields();
                    for (Field field : fields)
                    {
                        if (!field.isAnnotationPresent(Inject.class))
                            continue;
                        if (field.getType().equals(ClusterSimulation.Builder.class))
                        {
                            field.setAccessible(true);
                            field.set(bean, builder);
                        }
                        else
                        {
                            throw new RuntimeException("Unsupported injectable field type: " + field.getType() +
                                    " in class " + beanClass.getName() + ". " +
                                    "Only INodeProbeFactory and Output are supported.");
                        }
                    }
                }
                while ((beanClass = beanClass.getSuperclass()) != null);
                return bean;
            }
            catch (Exception e)
            {
                throw new CommandLine.InitializationException("Failed to create instance of " + cls, e);
            }
        }
    }

    private static CommandLine.IFactory simulationFactory()
    {
        return new InjectPaxosClusterSimulationFactory(new PaxosClusterSimulation.Builder()
                                                       .unique(uniqueNum.getAndIncrement()));
    }

    public static void executeWithExceptionThrowing(String[] args)
    {
        SimulatorUtils.executeWithExceptionThrowing(PaxosSimulationRunner.class, simulationFactory(), args);
    }

    /**
     * See {@link org.apache.cassandra.simulator} package info for execution tips
     */
    public static void main(String[] args) throws IOException
    {
        System.exit(SimulatorUtils.prepareRunner(PaxosSimulationRunner.class, simulationFactory(), null).execute(args));
    }
}
