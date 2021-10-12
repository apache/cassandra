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

package org.apache.cassandra.concurrent;

import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Recursive.INTERFACES;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * An {@link ExecutorPlus} that guarantees the order of execution matches the order of task submission,
 * and provides a simple mechanism for the recurring pattern of ensuring a job is executed at least once
 * after some point in time (i.e. ensures that at most one copy of the task is queued, with up to one
 * copy running as well)
 */
@Shared(scope = SIMULATION, inner = INTERFACES)
public interface SequentialExecutorPlus extends ExecutorPlus
{
    public interface AtLeastOnceTrigger
    {
        /**
         * Ensure the job is run at least once in its entirety after this method is invoked (including any already queued)
         */
        public boolean trigger();

        /**
         * Run the provided task after all queued and executing jobs have completed
         */
        public void runAfter(Runnable run);

        /**
         * Wait until all queued and executing jobs have completed
         */
        public void sync();
    }

    /**
     * Return an object for orchestrating the execution of this task at least once (in its entirety) after
     * the trigger is invoked, i.e. saturating the number of pending tasks at 1 (2 including any possibly executing
     * at the time of invocation)
     */
    public AtLeastOnceTrigger atLeastOnceTrigger(Runnable runnable);
}
