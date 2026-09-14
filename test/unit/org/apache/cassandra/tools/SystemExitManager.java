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

package org.apache.cassandra.tools;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.inject.BytemanAgentSupport;
import org.apache.cassandra.utils.Shared;

/**
 * Intercepts Runtime exit and halt calls in tests with a Byteman rule. The rule throws
 * {@link SystemExitException} from the application class loader. Exit blocking is reference-counted and
 * {@link Shared} across in-JVM dtest class loaders.
 */
@Shared
public final class SystemExitManager
{
    private static final AtomicInteger blockedCount = new AtomicInteger(0);
    private static volatile boolean ruleInstalled = false;

    private SystemExitManager()
    {
    }

    /** Starts blocking Runtime exit and halt calls. */
    public static void blockExit()
    {
        ensureRuleInstalled();
        blockedCount.incrementAndGet();
    }

    /** Ends one {@link #blockExit()} scope. */
    public static void unblockExit()
    {
        blockedCount.updateAndGet(n -> n > 0 ? n - 1 : 0);
    }

    /** Clears all exit blocks. */
    public static void reset()
    {
        blockedCount.set(0);
    }

    /** Returns whether the Byteman rule should block an exit. */
    public static boolean isExitBlocked()
    {
        return blockedCount.get() > 0;
    }

    /** Installs the interception rule without blocking exits. */
    public static void ensureInstalled()
    {
        ensureRuleInstalled();
    }

    private static synchronized void ensureRuleInstalled()
    {
        if (ruleInstalled)
            return;

        // A direct Byteman throw reaches the caller without an ExecuteException wrapper.
        // The shared agent enables bootstrap-class transformation for Runtime.
        String rule =
        "RULE cassandra-test intercept Runtime.exit\n" +
        "CLASS java.lang.Runtime\n" +
        "METHOD exit\n" +
        "AT ENTRY\n" +
        "IF org.apache.cassandra.tools.SystemExitManager.isExitBlocked()\n" +
        "DO throw new org.apache.cassandra.tools.SystemExitException($1)\n" +
        "ENDRULE\n" +
        "RULE cassandra-test intercept Runtime.halt\n" +
        "CLASS java.lang.Runtime\n" +
        "METHOD halt\n" +
        "AT ENTRY\n" +
        "IF org.apache.cassandra.tools.SystemExitManager.isExitBlocked()\n" +
        "DO throw new org.apache.cassandra.tools.SystemExitException($1)\n" +
        "ENDRULE\n";

        try
        {
            BytemanAgentSupport.submitRules(rule);
            ruleInstalled = true;
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Failed to install Byteman System.exit interception rule", t);
        }
    }
}
