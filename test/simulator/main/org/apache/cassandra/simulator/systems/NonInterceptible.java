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

import java.util.concurrent.Callable;
import java.util.function.Supplier;

import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@Shared(scope = SIMULATION)
public class NonInterceptible
{
    @Shared(scope = SIMULATION)
    public enum Permit { REQUIRED, OPTIONAL }

    private static final ThreadLocal<Permit> PERMIT = new ThreadLocal<>();

    public static boolean isPermitted(Permit permit)
    {
        Permit current = PERMIT.get();
        return current != null && current.compareTo(permit) >= 0;
    }

    public static boolean isPermitted()
    {
        return PERMIT.get() != null;
    }

    public static void execute(Permit permit, Runnable runnable)
    {
        if (isPermitted())
        {
            runnable.run();
        }
        else
        {
            PERMIT.set(permit);
            try
            {
                runnable.run();
            }
            finally
            {
                PERMIT.set(null);
            }
        }
    }

    public static <V> V apply(Permit permit, Supplier<V> supplier)
    {
        if (isPermitted())
        {
            return supplier.get();
        }
        else
        {
            PERMIT.set(permit);
            try
            {
                return supplier.get();
            }
            finally
            {
                PERMIT.set(null);
            }
        }
    }

    public static <V> V call(Permit permit, Callable<V> call) throws Exception
    {
        if (isPermitted())
        {
            return call.call();
        }
        else
        {
            PERMIT.set(permit);
            try
            {
                return call.call();
            }
            finally
            {
                PERMIT.set(null);
            }
        }
    }
}
