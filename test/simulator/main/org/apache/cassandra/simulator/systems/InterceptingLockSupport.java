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

import java.util.concurrent.locks.LockSupport;

import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.simulator.systems.InterceptedWait.Kind.TIMED_WAIT;
import static org.apache.cassandra.simulator.systems.InterceptedWait.Kind.UNBOUNDED_WAIT;

// This class is either used as the target for LockSupport calls, or as the template for
// overwriting LockSupport itself (for wider system coverage) if used with the javaagent
@SuppressWarnings("unused")
@Shared
public class InterceptingLockSupport
{
    public static void park()
    {
        Thread thread = Thread.currentThread();
        if (!(thread instanceof InterceptibleThread) || !((InterceptibleThread) thread).park(UNBOUNDED_WAIT))
            realPark();
    }

    public static void parkNanos(long nanos)
    {
        Thread thread = Thread.currentThread();
        if (!(thread instanceof InterceptibleThread) || !((InterceptibleThread) thread).park(TIMED_WAIT))
            realParkNanos(nanos);
    }

    public static void parkUntil(long millis)
    {
        Thread thread = Thread.currentThread();
        if (!(thread instanceof InterceptibleThread) || !((InterceptibleThread) thread).park(TIMED_WAIT))
            realParkUntil(millis);
    }

    public static void park(Object blocker)
    {
        Thread thread = Thread.currentThread();
        if (!(thread instanceof InterceptibleThread) || !((InterceptibleThread) thread).park(UNBOUNDED_WAIT))
            realPark(blocker);
    }

    public static void parkNanos(Object blocker, long nanos)
    {
        Thread thread = Thread.currentThread();
        if (!(thread instanceof InterceptibleThread) || !((InterceptibleThread) thread).park(TIMED_WAIT))
            realParkNanos(blocker, nanos);
    }

    public static void parkUntil(Object blocker, long millis)
    {
        Thread thread = Thread.currentThread();
        if (!(thread instanceof InterceptibleThread) || !((InterceptibleThread) thread).park(TIMED_WAIT))
            realParkUntil(blocker, millis);
    }

    public static void unpark(Thread thread)
    {
        Thread currentThread = Thread.currentThread();
        if (!(thread instanceof InterceptibleThread) || !(currentThread instanceof InterceptibleThread)
            || !((InterceptibleThread) thread).unpark((InterceptibleThread) currentThread))
                realUnpark(thread);
    }

    private static void realPark()
    {
        LockSupport.park();
    }

    private static void realParkNanos(long nanos)
    {
        LockSupport.parkNanos(nanos);
    }

    private static void realParkUntil(long millis)
    {
        LockSupport.parkUntil(millis);
    }

    private static void realPark(Object blocker)
    {
        LockSupport.park(blocker);
    }

    private static void realParkNanos(Object blocker, long nanos)
    {
        LockSupport.parkNanos(blocker, nanos);
    }

    private static void realParkUntil(Object blocker, long millis)
    {
        LockSupport.parkUntil(blocker, millis);
    }

    private static void realUnpark(Thread thread)
    {
        LockSupport.unpark(thread);
    }
}
