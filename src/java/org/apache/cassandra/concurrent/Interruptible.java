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

import static org.apache.cassandra.concurrent.Interruptible.State.*;
import static org.apache.cassandra.utils.Shared.Recursive.INTERFACES;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@Shared(scope = SIMULATION, inner = INTERFACES)
public interface Interruptible extends Shutdownable
{
    public enum State { NORMAL, INTERRUPTED, SHUTTING_DOWN }

    public static class TerminateException extends InterruptedException {}

    public interface Task
    {
        void run(State state) throws InterruptedException;

        static Task from(SimpleTask simpleTask)
        {
            return state -> { if (state == NORMAL) simpleTask.run(); };
        }
    }

    /**
     * A Task that only runs on NORMAL states
     */
    public interface SimpleTask
    {
        void run() throws InterruptedException;
    }

    void interrupt();
}

