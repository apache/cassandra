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

package org.apache.cassandra.simulator.systems;

import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * An abstraction for the simulation to permit the main scheduling thread
 * to trigger a simulation thread and then wait synchronously for the end
 * of this part of its simulation, i.e. until its next wait is intercepted.
 *
 * This permits more efficient scheduling, by (often) ensuring the simulated
 * thread and the scheduling thread do not run simultaneously.
 */
@Shared(scope = SIMULATION)
public interface NotifyThreadPaused
{
    void notifyThreadPaused();

    class AwaitPaused implements NotifyThreadPaused
    {
        final Object monitor;
        boolean isDone;
        AwaitPaused(Object monitor) { this.monitor = monitor; }
        AwaitPaused() { this.monitor = this; }

        @Override
        public void notifyThreadPaused()
        {
            synchronized (monitor)
            {
                isDone = true;
                monitor.notifyAll();
            }
        }

        public void awaitPause()
        {
            try
            {
                synchronized (monitor)
                {
                    while (!isDone)
                        monitor.wait();
                }
            }
            catch (InterruptedException ie)
            {
                throw new UncheckedInterruptedException(ie);
            }
        }
    }

}
