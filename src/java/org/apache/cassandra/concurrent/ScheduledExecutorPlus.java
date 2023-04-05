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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@Shared(scope = SIMULATION)
public interface ScheduledExecutorPlus extends ExecutorPlus, ScheduledExecutorService
{
    /**
     * Schedule an action that is recurring but self-administered.
     */
    ScheduledFuture<?> scheduleSelfRecurring(Runnable run, long delay, TimeUnit units);

    /**
     * Schedule a timeout action. This method is primarily used by the Simulator to modify its
     * scheduling behaviour with respect to this operation.
     */
    ScheduledFuture<?> scheduleAt(Runnable run, long deadline);

    /**
     * Schedule a timeout action. This method is primarily used by the Simulator to modify its
     * scheduling behaviour with respect to this operation.
     */
    ScheduledFuture<?> scheduleTimeoutAt(Runnable run, long deadline);

    /**
     * Schedule a timeout action. This method is primarily used by the Simulator to modify its
     * scheduling behaviour with respect to this operation.
     */
    ScheduledFuture<?> scheduleTimeoutWithDelay(Runnable run, long delay, TimeUnit units);
}
