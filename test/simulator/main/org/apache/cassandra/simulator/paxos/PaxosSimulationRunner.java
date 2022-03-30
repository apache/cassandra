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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import io.airlift.airline.Cli;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.simulator.SimulationRunner;

public class PaxosSimulationRunner extends SimulationRunner
{
    @Command(name = "run")
    public static class Run extends SimulationRunner.Run<PaxosClusterSimulation.Builder>
    {
        @Option(name = "--consistency", description = "Specify the consistency level to perform paxos operations at")
        String consistency;

        @Option(name = "--with-paxos-state-cache", arity = 0, description = "Run with the paxos state cache always enabled")
        boolean withStateCache;

        @Option(name = "--without-paxos-state-cache", arity = 0, description = "Run with the paxos state cache always disabled")
        boolean withoutStateCache;

        @Option(name = "--variant", title = "paxos variant", description = "Specify the initial paxos variant to use")
        String variant;

        @Option(name = "--to-variant",  title = "paxos variant", description = "Specify the paxos variant to change to at some point during the simulation")
        String toVariant;

        public Run() {}

        @Override
        protected void propagate(PaxosClusterSimulation.Builder builder)
        {
            super.propagate(builder);
            propagateTo(consistency, withStateCache, withoutStateCache, variant, toVariant, builder);
        }
    }

    @Command(name = "record")
    public static class Record extends SimulationRunner.Record<PaxosClusterSimulation.Builder>
    {
        @Option(name = "--consistency")
        String consistency;

        @Option(name = "--with-paxos-state-cache", arity = 0)
        boolean withStateCache;

        @Option(name = "--without-paxos-state-cache", arity = 0)
        boolean withoutStateCache;

        @Option(name = "--variant", description = "paxos variant to use")
        String variant;

        @Option(name = "--to-variant", description = "paxos variant to change to during the simulation")
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
    public static class Reconcile extends SimulationRunner.Reconcile<PaxosClusterSimulation.Builder>
    {
        @Option(name = "--consistency")
        String consistency;

        @Option(name = "--with-paxos-state-cache", arity = 0)
        boolean withStateCache;

        @Option(name = "--without-paxos-state-cache", arity = 0)
        boolean withoutStateCache;

        @Option(name = "--variant", description = "paxos variant to use")
        String variant;

        @Option(name = "--to-variant", description = "paxos variant to change to during the simulation")
        String toVariant;
        
        public Reconcile() {}

        @Override
        protected void propagate(PaxosClusterSimulation.Builder builder)
        {
            super.propagate(builder);
            propagateTo(consistency, withStateCache, withoutStateCache, variant, toVariant, builder);
        }
    }

    public static class Help extends HelpCommand<PaxosClusterSimulation.Builder> {}

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

    /**
     * See {@link org.apache.cassandra.simulator} package info for execution tips
     */
    public static void main(String[] args) throws IOException
    {
        PaxosClusterSimulation.Builder builder = new PaxosClusterSimulation.Builder();
        builder.unique(uniqueNum.getAndIncrement());

        Cli.<SimulationRunner.ICommand<PaxosClusterSimulation.Builder>>builder("paxos")
           .withCommand(Run.class)
           .withCommand(Reconcile.class)
           .withCommand(Record.class)
           .withCommand(Help.class)
           .withDefaultCommand(Help.class)
           .build()
           .parse(args)
           .run(builder);
    }
}
