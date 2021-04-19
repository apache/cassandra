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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.exceptions.UnrecoverableIllegalStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.nicoulaj.compilecommand.annotations.Exclude;
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
        inspectThrowable(t, JVMStabilityInspector::inspectDiskError);
    }

    public static void inspectCommitLogThrowable(Throwable t)
    {
        inspectThrowable(t, JVMStabilityInspector::inspectCommitLogError);
    }

    private static void inspectDiskError(Throwable t)
    {
        if (t instanceof CorruptSSTableException)
            FileUtils.handleCorruptSSTable((CorruptSSTableException) t);
        else if (t instanceof FSError)
            FileUtils.handleFSError((FSError) t);
    }

    public static void inspectThrowable(Throwable t, Consumer<Throwable> fn) throws OutOfMemoryError
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

            forceHeapSpaceOomMaybe((OutOfMemoryError) t);

            // We let the JVM handle the error. The startup checks should have warned the user if it did not configure
            // the JVM behavior in case of OOM (CASSANDRA-13006).
            throw (OutOfMemoryError) t;
        }
        else if (t instanceof UnrecoverableIllegalStateException)
        {
            isUnstable = true;
        }

        if (DatabaseDescriptor.getDiskFailurePolicy() == Config.DiskFailurePolicy.die)
            if (t instanceof FSError || t instanceof CorruptSSTableException)
                isUnstable = true;

        fn.accept(t);

        // Check for file handle exhaustion
        if (t instanceof FileNotFoundException || t instanceof SocketException)
            if (t.getMessage() != null && t.getMessage().contains("Too many open files"))
                isUnstable = true;

        if (isUnstable)
            killer.killCurrentJVM(t);

        if (t.getSuppressed() != null)
            for (Throwable suppressed : t.getSuppressed())
                inspectThrowable(suppressed, fn);

        if (t.getCause() != null)
            inspectThrowable(t.getCause(), fn);
    }

    /**
     * Intentionally produce a heap space OOM upon seeing a Direct buffer memory OOM.
     * Direct buffer OOM cannot trigger JVM OOM error related options,
     * e.g. OnOutOfMemoryError, HeapDumpOnOutOfMemoryError, etc.
     * See CASSANDRA-15214 for more details
     */
    @Exclude // Exclude from just in time compilation.
    private static void forceHeapSpaceOomMaybe(OutOfMemoryError oom)
    {
        // See the oom thrown from java.nio.Bits.reserveMemory.
        // In jdk 13 and up, the message is "Cannot reserve XX bytes of direct buffer memory (...)"
        // In jdk 11 and below, the message is "Direct buffer memory"
        if ((oom.getMessage() != null && oom.getMessage().toLowerCase().contains("direct buffer memory")) ||
            Arrays.stream(oom.getStackTrace()).anyMatch(x -> x.getClassName().equals("java.nio.Bits")
                                                             && x.getMethodName().equals("reserveMemory")))
        {
            logger.error("Force heap space OutOfMemoryError in the presence of", oom);
            // Start to produce heap space OOM forcibly.
            List<long[]> ignored = new ArrayList<>();
            while (true)
            {
                // java.util.AbstractCollection.MAX_ARRAY_SIZE is defined as Integer.MAX_VALUE - 8
                // so Integer.MAX_VALUE / 2 should be a large enough and safe size to request.
                ignored.add(new long[Integer.MAX_VALUE / 2]);
            }
        }
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
