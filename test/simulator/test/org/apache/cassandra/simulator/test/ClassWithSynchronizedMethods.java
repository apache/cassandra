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

package org.apache.cassandra.simulator.test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.utils.Nemesis;
import org.apache.cassandra.utils.Simulate;

import static org.apache.cassandra.utils.Simulate.With.MONITORS;

@Simulate(with = MONITORS)
public class ClassWithSynchronizedMethods
{
    @Nemesis
    private static final AtomicInteger staticCounter1 = new AtomicInteger();
    @Nemesis
    private static final AtomicInteger staticCounter2 = new AtomicInteger();

    public static class Execution
    {
        public final int thread;
        public final int sequenceNumber;

        public Execution(int thread, int sequenceNumber)
        {
            this.thread = thread;
            this.sequenceNumber = sequenceNumber;
        }

        public String toString()
        {
            return "Execution{" +
                   "thread=" + thread +
                   ", sequenceNumber=" + sequenceNumber +
                   '}';
        }
    }

    public static final List<Execution> executions = new ArrayList<>();

    public static synchronized void synchronizedMethodWithParams(int thread, int sequenceNumber)
    {
        int before1 = staticCounter1.get();
        int before2 = staticCounter2.get();

        Execution execution = new Execution(thread, sequenceNumber);
        executions.add(execution);

        // Despite interleavings of synchronized method calls, two threads can not enter the synchronized block together,
        // even in presence of nemesis
        boolean res1 = staticCounter1.compareAndSet(before1, before1 + 1);
        assert res1;
        boolean res2 = staticCounter2.compareAndSet(before2, before2 + 1);
        assert res2;
    }
}
