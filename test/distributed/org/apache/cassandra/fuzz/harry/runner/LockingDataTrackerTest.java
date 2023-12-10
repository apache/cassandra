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

package org.apache.cassandra.fuzz.harry.runner;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.harry.ddl.SchemaGenerators;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.runner.Runner;
import org.apache.cassandra.harry.tracker.LockingDataTracker;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Daemon.NON_DAEMON;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Interrupts.UNSYNCHRONIZED;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.SAFE;

public class LockingDataTrackerTest
{
    @Test
    public void testDataTracker() throws Throwable
    {
        SchemaSpec schemaSpec = SchemaGenerators.defaultSchemaSpecGen("test").inflate(1L);
        OpSelectors.PureRng rng = new OpSelectors.PCGFast(1L);
        OpSelectors.PdSelector pdSelector = new OpSelectors.DefaultPdSelector(rng, 5, 2);
        LockingDataTracker tracker = new LockingDataTracker(pdSelector, schemaSpec);

        AtomicReference<State> excluded = new AtomicReference<>(State.UNLOCKED);
        AtomicInteger readers = new AtomicInteger(0);
        AtomicInteger writers = new AtomicInteger(0);

        WaitQueue queue = WaitQueue.newWaitQueue();
        WaitQueue.Signal interrupt = queue.register();
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        long lts = 1;
        long pd = pdSelector.pd(lts, schemaSpec);
        int parallelism = 2;
        for (int i = 0; i < parallelism; i++)
        {
            ExecutorFactory.Global.executorFactory().infiniteLoop("write-" + i, Runner.wrapInterrupt(state -> {
                try
                {
                    if (state == Interruptible.State.NORMAL)
                    {
                        tracker.beginModification(lts);
                        Assert.assertEquals(0, readers.get());
                        writers.incrementAndGet();
                        excluded.updateAndGet((prev) -> {
                            assert (prev == State.UNLOCKED || prev == State.LOCKED_FOR_WRITE) : prev;
                            return State.LOCKED_FOR_WRITE;
                        });
                        Assert.assertEquals(0, readers.get());
                        excluded.updateAndGet((prev) -> {
                            assert (prev == State.UNLOCKED || prev == State.LOCKED_FOR_WRITE) : prev;
                            return State.UNLOCKED;
                        });
                        Assert.assertEquals(0, readers.get());
                        writers.decrementAndGet();
                        tracker.endModification(lts);
                    }
                }
                catch (Throwable t)
                {
                    t.printStackTrace();
                    throw t;
                }
            }, interrupt::signal, errors::add), SAFE, NON_DAEMON, UNSYNCHRONIZED);
        }

        for (int i = 0; i < parallelism; i++)
        {
            ExecutorFactory.Global.executorFactory().infiniteLoop("read-" + i, Runner.wrapInterrupt(state -> {
                try
                {
                    if (state == Interruptible.State.NORMAL)
                    {
                        tracker.beginValidation(pd);
                        Assert.assertEquals(0, writers.get());
                        readers.incrementAndGet();
                        excluded.updateAndGet((prev) -> {
                            assert (prev == State.UNLOCKED || prev == State.LOCKED_FOR_READ) : prev;
                            return State.LOCKED_FOR_READ;
                        });
                        Assert.assertEquals(0, writers.get());
                        excluded.updateAndGet((prev) -> {
                            assert (prev == State.UNLOCKED || prev == State.LOCKED_FOR_READ) : prev;
                            return State.UNLOCKED;
                        });
                        Assert.assertEquals(0, writers.get());
                        readers.decrementAndGet();
                        tracker.endValidation(pd);
                    }
                }
                catch (Throwable t)
                {
                    t.printStackTrace();
                    throw t;
                }
            }, interrupt::signal, errors::add), SAFE, NON_DAEMON, UNSYNCHRONIZED);
        }

        interrupt.await(1, TimeUnit.MINUTES);
        if (!errors.isEmpty())
            Runner.mergeAndThrow(errors);
    }
    enum State { UNLOCKED, LOCKED_FOR_READ, LOCKED_FOR_WRITE }
}
