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

package org.apache.cassandra.harry.runner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.visitors.Visitor;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Daemon.NON_DAEMON;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Interrupts.UNSYNCHRONIZED;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.SAFE;

public class FlaggedRunner extends Runner
{
    private final List<Configuration.VisitorPoolConfiguration> poolConfigurations;
    private final CountDownLatch stopLatch;

    public FlaggedRunner(Run run, Configuration config, List<Configuration.VisitorPoolConfiguration> poolConfigurations, CountDownLatch stopLatch)
    {
        super(run, config);
        this.poolConfigurations = poolConfigurations;
        this.stopLatch = stopLatch;
    }

    @Override
    protected void runInternal() throws Throwable
    {
        List<Interruptible> threads = new ArrayList<>();
        Map<String, Integer> counters = new HashMap<>();
        for (Configuration.VisitorPoolConfiguration poolConfiguration : poolConfigurations)
        {
            for (int i = 0; i < poolConfiguration.concurrency; i++)
            {
                Visitor visitor = poolConfiguration.visitor.make(run);
                String name = String.format("%s-%d", poolConfiguration.prefix, i + 1);
                counters.put(name, 0);
                Interruptible thread = ExecutorFactory.Global.executorFactory().infiniteLoop(name, wrapInterrupt((state) -> {
                    if (state == Interruptible.State.NORMAL)
                    {
                        visitor.visit();
                        counters.compute(name, (n, old) -> old + 1);
                    }
                }, stopLatch::decrement, errors::add), SAFE, NON_DAEMON, UNSYNCHRONIZED);
                threads.add(thread);
            }
        }

        stopLatch.await();
        shutdown(threads::stream);
        System.out.println("counters = " + counters);
        if (!errors.isEmpty())
            mergeAndThrow(errors);
    }

    @Override
    public String type()
    {
        return "concurrent_flagged";
    }
}
