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

package org.apache.cassandra.service;

import java.nio.file.FileStore;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DisallowedDirectories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.FSDiskFullWriteError;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSNoDiskAvailableForWriteError;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.utils.JVMStabilityInspector;

public class DefaultDiskErrorsHandler implements DiskErrorsHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DefaultDiskErrorsHandler.class);

    private static final Set<Class<?>> exceptionsSkippingDataRemoval = ImmutableSet.of(OutOfMemoryError.class);

    @Override
    public void init()
    {
        // intentionally empty
    }

    @Override
    public void close() throws Exception
    {
        // intentionally empty
    }

    @Override
    public void handleCorruptSSTable(CorruptSSTableException e)
    {
        if (!StorageService.instance.isDaemonSetupCompleted())
            handleStartupFSError(e);

        switch (DatabaseDescriptor.getDiskFailurePolicy())
        {
            case die:
            case stop_paranoid:
                // exception not logged here on purpose as it is already logged
                logger.error("Stopping transports as disk_failure_policy is " + DatabaseDescriptor.getDiskFailurePolicy());
                StorageService.instance.stopTransports();
                break;
        }
    }

    @Override
    public void handleFSError(FSError e)
    {
        if (!StorageService.instance.isDaemonSetupCompleted())
            handleStartupFSError(e);

        switch (DatabaseDescriptor.getDiskFailurePolicy())
        {
            case die:
            case stop_paranoid:
            case stop:
                // exception not logged here on purpose as it is already logged
                logger.error("Stopping transports as disk_failure_policy is " + DatabaseDescriptor.getDiskFailurePolicy());
                StorageService.instance.stopTransports();
                break;
            case best_effort:

                // There are a few scenarios where we know that the node will not be able to operate properly.
                // For those scenarios we want to stop the transports and let the administrators handle the problem.
                // Those scenarios are:
                // * All the disks are full
                // * All the disks for a given keyspace have been marked as unwriteable
                if (e instanceof FSDiskFullWriteError || e instanceof FSNoDiskAvailableForWriteError)
                {
                    logger.error("Stopping transports: " + e.getCause().getMessage());
                    StorageService.instance.stopTransports();
                }

                // for both read and write errors mark the path as unwritable.
                DisallowedDirectories.maybeMarkUnwritable(new File(e.path));
                if (e instanceof FSReadError && shouldMaybeRemoveData(e))
                {
                    File directory = DisallowedDirectories.maybeMarkUnreadable(new File(e.path));
                    if (directory != null)
                        Keyspace.removeUnreadableSSTables(directory);
                }
                break;
            case ignore:
                // already logged, so left nothing to do
                break;
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public void handleStartupFSError(Throwable t)
    {
        switch (DatabaseDescriptor.getDiskFailurePolicy())
        {
            case stop_paranoid:
            case stop:
            case die:
                logger.error("Exiting forcefully due to file system exception on startup, disk failure policy \"{}\"",
                             DatabaseDescriptor.getDiskFailurePolicy(),
                             t);
                JVMStabilityInspector.killCurrentJVM(t, true);
                break;
            default:
                break;
        }
    }

    @Override
    public boolean handleCommitError(String message, Throwable t)
    {
        JVMStabilityInspector.inspectCommitLogThrowable(t);
        switch (DatabaseDescriptor.getCommitFailurePolicy())
        {
            // Needed here for unit tests to not fail on default assertion
            case die:
            case stop:
                StorageService.instance.stopTransports();
                //$FALL-THROUGH$
            case stop_commit:
                String errorMsg = String.format("%s. Commit disk failure policy is %s; terminating thread.", message, DatabaseDescriptor.getCommitFailurePolicy());
                logger.error(addAdditionalInformationIfPossible(errorMsg), t);
                return false;
            case ignore:
                logger.error(addAdditionalInformationIfPossible(message), t);
                return true;
            default:
                throw new AssertionError(DatabaseDescriptor.getCommitFailurePolicy());
        }
    }

    @Override
    public void inspectDiskError(Throwable t)
    {
        if (t instanceof CorruptSSTableException)
            handleCorruptSSTable((CorruptSSTableException) t);
        else if (t instanceof FSError)
            handleFSError((FSError) t);
    }

    @Override
    public void inspectCommitLogError(Throwable t)
    {
        if (!StorageService.instance.isDaemonSetupCompleted())
        {
            logger.error("Exiting due to error while processing commit log during initialization.", t);
            JVMStabilityInspector.killCurrentJVM(t, true);
        }
        else if (DatabaseDescriptor.getCommitFailurePolicy() == Config.CommitFailurePolicy.die)
            JVMStabilityInspector.killCurrentJVM(t, false);
    }

    private boolean shouldMaybeRemoveData(Throwable error)
    {
        for (Throwable t = error; t != null; t = t.getCause())
        {
            for (Class<?> c : exceptionsSkippingDataRemoval)
                if (c.isAssignableFrom(t.getClass()))
                    return false;
            for (Throwable s : t.getSuppressed())
                for (Class<?> c : exceptionsSkippingDataRemoval)
                    if (c.isAssignableFrom(s.getClass()))
                        return false;
        }

        return true;
    }

    /**
     * Add additional information to the error message if the commit directory does not have enough free space.
     *
     * @param msg the original error message
     * @return the message with additional information if possible
     */
    private static String addAdditionalInformationIfPossible(String msg)
    {
        long unallocatedSpace = freeDiskSpace();
        int segmentSize = DatabaseDescriptor.getCommitLogSegmentSize();

        if (unallocatedSpace < segmentSize)
        {
            return String.format("%s. %d bytes required for next commitlog segment but only %d bytes available. Check %s to see if not enough free space is the reason for this error.",
                                 msg, segmentSize, unallocatedSpace, DatabaseDescriptor.getCommitLogLocation());
        }
        return msg;
    }

    private static long freeDiskSpace()
    {
        return PathUtils.tryGetSpace(new File(DatabaseDescriptor.getCommitLogLocation()).toPath(), FileStore::getTotalSpace);
    }
}
