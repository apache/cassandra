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

package org.apache.cassandra.service.accord;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.Test;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.utils.concurrent.Semaphore;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public class AccordExecutorBurnTest
{
    static class State
    {
        final AccordExecutor executor;
        final AccordCommandStore[] commandStores;
        final ExecutorPlus loadGen;
        final int generators;
        final int targetCount;
        final Semaphore permits;
        final AtomicInteger submitted = new AtomicInteger();
        final AtomicInteger completed = new AtomicInteger();

        State(AccordExecutor executor, Function<AccordExecutor, AccordCommandStore> storeFactory,
              int taskCount, int concurrency, int generators, int commandStores)
        {
            this.executor = executor;
            this.targetCount = taskCount;
            this.permits = Semaphore.newSemaphore(concurrency);
            this.generators = generators;
            this.loadGen = executorFactory().pooled("loadgen", generators);
            this.commandStores = new AccordCommandStore[commandStores];
            for (int i = 0 ; i < commandStores ; ++i)
                this.commandStores[i] = storeFactory.apply(executor);
        }

        void start()
        {
            for (int i = 0 ; i < generators ; ++i)
                loadGen.execute(this::run);
        }

        void run()
        {
            while (true)
            {
                int slot = submitted.get();
                if (slot >= targetCount)
                    return;
                if (!submitted.compareAndSet(slot, slot + 1))
                    continue;

                try { permits.acquire(1); }
                catch (InterruptedException e) { throw new UncheckedInterruptedException(e); }
                submitSomething();
            }
        }

        private void submitSomething()
        {
        }
    }

    @Test
    public void test()
    {

    }

}
