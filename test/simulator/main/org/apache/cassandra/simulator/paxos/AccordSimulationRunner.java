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
import java.util.concurrent.atomic.AtomicInteger;

import io.airlift.airline.Cli;
import io.airlift.airline.Command;
import org.apache.cassandra.simulator.SimulationRunner;

public class AccordSimulationRunner extends SimulationRunner
{
    @Command(name = "run")
    public static class Run extends SimulationRunner.Run<AccordClusterSimulation.Builder>
    {
        public Run() {}

        @Override
        protected void run(long seed, AccordClusterSimulation.Builder builder) throws IOException
        {
            builder.applyHandicaps();
            super.run(seed, builder);
        }
    }

    @Command(name = "record")
    public static class Record extends SimulationRunner.Record<AccordClusterSimulation.Builder>
    {
        public Record() {}
    }

    @Command(name = "reconcile")
    public static class Reconcile extends SimulationRunner.Reconcile<AccordClusterSimulation.Builder>
    {
        public Reconcile() {}
    }

    public static class Help extends HelpCommand<AccordClusterSimulation.Builder> {}

    // for simple unit tests so we can simply invoke main()
    private static final AtomicInteger uniqueNum = new AtomicInteger();

    /**
     * See {@link org.apache.cassandra.simulator} package info for execution tips
     */
    public static void main(String[] args) throws IOException
    {
        AccordClusterSimulation.Builder builder = new AccordClusterSimulation.Builder();
        builder.unique(uniqueNum.getAndIncrement());

        Cli.<ICommand<AccordClusterSimulation.Builder>>builder("accord")
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
