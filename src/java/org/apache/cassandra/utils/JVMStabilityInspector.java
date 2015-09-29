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
package org.apache.cassandra.utils;

import java.io.FileNotFoundException;
import java.net.SocketException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.Cassandra;

/**
 * Responsible for deciding whether to kill the JVM if it gets in an "unstable" state (think OOM).
 */
public final class JVMStabilityInspector
{
    private static final Logger logger = LoggerFactory.getLogger(JVMStabilityInspector.class);
    private static Killer killer = new Killer();


    private JVMStabilityInspector() {}

    /**
     * Certain Throwables and Exceptions represent "Die" conditions for the server.
     * This recursively checks the input Throwable's cause hierarchy until null.
     * @param t
     *      The Throwable to check for server-stop conditions
     */
    public static void inspectThrowable(Throwable t)
    {
        boolean isUnstable = false;
        if (t instanceof OutOfMemoryError)
            isUnstable = true;

        if (DatabaseDescriptor.getDiskFailurePolicy() == Config.DiskFailurePolicy.die)
            if (t instanceof FSError || t instanceof CorruptSSTableException)
            isUnstable = true;

        // Check for file handle exhaustion
        if (t instanceof FileNotFoundException || t instanceof SocketException)
            if (t.getMessage().contains("Too many open files"))
                isUnstable = true;

        if (isUnstable)
            killer.killCurrentJVM(t);

        if (t.getCause() != null)
            inspectThrowable(t.getCause());
    }

    public static void inspectCommitLogThrowable(Throwable t)
    {
        if (!StorageService.instance.isSetupCompleted())
        {
            logger.error("Exiting due to error while processing commit log during initialization.", t);
            killer.killCurrentJVM(t, true);
        } else if (DatabaseDescriptor.getCommitFailurePolicy() == Config.CommitFailurePolicy.die)
            killer.killCurrentJVM(t);
        else
            inspectThrowable(t);
    }

    public static void killCurrentJVM(Throwable t, boolean quiet)
    {
        killer.killCurrentJVM(t, quiet);
    }

    public static void userFunctionTimeout(Throwable t)
    {
        switch (DatabaseDescriptor.getUserFunctionTimeoutPolicy())
        {
            case die:
                // policy to give 250ms grace time to
                ScheduledExecutors.nonPeriodicTasks.schedule(() -> killer.killCurrentJVM(t), 250, TimeUnit.MILLISECONDS);
                break;
            case die_immediate:
                killer.killCurrentJVM(t);
                break;
            case ignore:
                logger.error(t.getMessage());
                break;
        }
    }

    @VisibleForTesting
    public static Killer replaceKiller(Killer newKiller) {
        Killer oldKiller = JVMStabilityInspector.killer;
        JVMStabilityInspector.killer = newKiller;
        return oldKiller;
    }

    @VisibleForTesting
    public static class Killer
    {
        private final AtomicBoolean killing = new AtomicBoolean();

        /**
        * Certain situations represent "Die" conditions for the server, and if so, the reason is logged and the current JVM is killed.
        *
        * @param t
        *      The Throwable to log before killing the current JVM
        */
        protected void killCurrentJVM(Throwable t)
        {
            killCurrentJVM(t, false);
        }

        protected void killCurrentJVM(Throwable t, boolean quiet)
        {
            if (!quiet)
            {
                t.printStackTrace(System.err);
                logger.error("JVM state determined to be unstable.  Exiting forcefully due to:", t);
            }
            if (killing.compareAndSet(false, true))
            {
                StorageService.instance.removeShutdownHook();
                System.exit(100);
            }
        }
    }
}
