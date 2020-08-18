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
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;

/**
 * Responsible for deciding whether to kill the JVM if it gets in an "unstable" state (think OOM).
 */
public final class JVMStabilityInspector
{
    private static final Logger logger = LoggerFactory.getLogger(JVMStabilityInspector.class);
    private static Killer killer = new Killer();

    private static Object lock = new Object();
    private static boolean printingHeapHistogram;

    // It is used for unit test
    public static OnKillHook killerHook;

    private JVMStabilityInspector() {}

    /**
     * Certain Throwables and Exceptions represent "Die" conditions for the server.
     * This recursively checks the input Throwable's cause hierarchy until null.
     * @param t
     *      The Throwable to check for server-stop conditions
     */
    public static void inspectThrowable(Throwable t) throws OutOfMemoryError
    {
        inspectThrowable(t, true);
    }

    public static void inspectThrowable(Throwable t, boolean propagateOutOfMemory) throws OutOfMemoryError
    {
        inspectThrowable(t, propagateOutOfMemory, JVMStabilityInspector::inspectDiskError);
    }

    public static void inspectCommitLogThrowable(Throwable t)
    {
        inspectThrowable(t, true, JVMStabilityInspector::inspectCommitLogError);
    }

    private static void inspectDiskError(Throwable t)
    {
        if (t instanceof CorruptSSTableException)
            FileUtils.handleCorruptSSTable((CorruptSSTableException) t);
        else if (t instanceof FSError)
            FileUtils.handleFSError((FSError) t);
    }

    public static void inspectThrowable(Throwable t, boolean propagateOutOfMemory, Consumer<Throwable> fn) throws OutOfMemoryError
    {
        boolean isUnstable = false;
        if (t instanceof OutOfMemoryError)
        {
            if (Boolean.getBoolean("cassandra.printHeapHistogramOnOutOfMemoryError"))
            {
                // We want to avoid printing multiple time the heap histogram if multiple OOM errors happen in a short
                // time span.
                synchronized(lock)
                {
                    if (printingHeapHistogram)
                        return;
                    printingHeapHistogram = true;
                }
                HeapUtils.logHeapHistogram();
            }

            logger.error("OutOfMemory error letting the JVM handle the error:", t);

            StorageService.instance.removeShutdownHook();
            // We let the JVM handle the error. The startup checks should have warned the user if it did not configure
            // the JVM behavior in case of OOM (CASSANDRA-13006).
            if (!propagateOutOfMemory)
                return;

            throw (OutOfMemoryError) t;
        }

        if (DatabaseDescriptor.getDiskFailurePolicy() == Config.DiskFailurePolicy.die)
            if (t instanceof FSError || t instanceof CorruptSSTableException)
                isUnstable = true;

        fn.accept(t);

        // Check for file handle exhaustion
        if (t instanceof FileNotFoundException || t instanceof SocketException)
            if (t.getMessage().contains("Too many open files"))
                isUnstable = true;

        if (isUnstable)
            killer.killCurrentJVM(t);

        if (t.getCause() != null)
            inspectThrowable(t.getCause(), propagateOutOfMemory, fn);
    }

    private static void inspectCommitLogError(Throwable t)
    {
        if (!StorageService.instance.isDaemonSetupCompleted())
        {
            logger.error("Exiting due to error while processing commit log during initialization.", t);
            killer.killCurrentJVM(t, true);
        }
        else if (DatabaseDescriptor.getCommitFailurePolicy() == Config.CommitFailurePolicy.die)
            killer.killCurrentJVM(t);
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
    public static Killer replaceKiller(Killer newKiller)
    {
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

            boolean doExit = killerHook != null ? killerHook.execute(t) : true;

            if (doExit && killing.compareAndSet(false, true))
            {
                StorageService.instance.removeShutdownHook();
                System.exit(100);
            }
        }
    }

    /**
     * This class is usually used to avoid JVM exit when running junit tests.
     */
    public interface OnKillHook
    {
        /**
         *
         * @return False will skip exit
         */
        boolean execute(Throwable t);
    }
}
