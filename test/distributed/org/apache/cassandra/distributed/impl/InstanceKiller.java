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

package org.apache.cassandra.distributed.impl;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.cassandra.utils.JVMStabilityInspector;

public class InstanceKiller extends JVMStabilityInspector.Killer
{
    private static final AtomicLong KILL_ATTEMPTS = new AtomicLong(0);

    private final Consumer<Boolean> onKill;

    public InstanceKiller(Consumer<Boolean> onKill)
    {
        this.onKill = onKill != null ? onKill : ignore -> {};
    }

    public static long getKillAttempts()
    {
        return KILL_ATTEMPTS.get();
    }

    public static void clear()
    {
        KILL_ATTEMPTS.set(0);
    }

    @Override
    protected void killCurrentJVM(Throwable t, boolean quiet)
    {
        KILL_ATTEMPTS.incrementAndGet();
        onKill.accept(quiet);
        // the bad part is that System.exit kills the JVM, so all code which calls kill won't hit the
        // next line; yet in in-JVM dtests System.exit is not desirable, so need to rely on a runtime exception
        // as a means to try to stop execution
        throw new InstanceShutdown();
    }

    public static final class InstanceShutdown extends RuntimeException { }
}
