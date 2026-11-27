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

package org.apache.cassandra.security;

import java.util.Set;

import io.netty.util.concurrent.FastThreadLocal;

/**
 * Thread group of the threads that execute user-defined functions. Used by {@link ThreadAwareSecurityManager} to
 * determine whether access-control checks need to be performed.
 */
public final class SecurityThreadGroup extends ThreadGroup
{
    private static final FastThreadLocal<Boolean> initializedThread = new FastThreadLocal<>();

    private final Set<String> allowedPackages;
    private final ThreadInitializer threadInitializer;

    public SecurityThreadGroup(String name, Set<String> allowedPackages, ThreadInitializer threadInitializer)
    {
        super(name);
        this.allowedPackages = allowedPackages;
        this.threadInitializer = threadInitializer;
    }

    public void initializeThread()
    {
        threadInitializer.initializeThread();
    }

    /**
     * @return true if the current thread is an initialized thread of a {@code SecurityThreadGroup}. The first call on
     * such a thread runs the group's {@link ThreadInitializer}. Calls made while it runs return false.
     */
    public static boolean isSecuredThread()
    {
        ThreadGroup tg = Thread.currentThread().getThreadGroup();
        if (!(tg instanceof SecurityThreadGroup))
            return false;
        Boolean threadInitialized = initializedThread.get();
        if (threadInitialized == null)
        {
            initializedThread.set(false);
            ((SecurityThreadGroup) tg).initializeThread();
            initializedThread.set(true);
            threadInitialized = true;
        }
        return threadInitialized;
    }

    public boolean isPackageAllowed(String pkg)
    {
        return allowedPackages == null || allowedPackages.contains(pkg);
    }

    @FunctionalInterface
    public interface ThreadInitializer
    {
        void initializeThread();
    }
}
