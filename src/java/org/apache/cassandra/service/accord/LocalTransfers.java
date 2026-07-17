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

package org.apache.cassandra.service.accord;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public class LocalTransfers
{
    private static final Logger logger = LoggerFactory.getLogger(LocalTransfers.class);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    // SSTable imports that we are coordinating
    private final Map<Long, CoordinatedTransfer> coordinating = new ConcurrentHashMap<>();

    // Added when we have a streamed SSTable in our pending directory
    private final Map<TimeUUID, PendingLocalTransfer> local = new ConcurrentHashMap<>();

    final ExecutorPlus executor = executorFactory().pooled("LocalTrackedTransfers", Integer.MAX_VALUE);

    public static final LocalTransfers instance = new LocalTransfers();
    static LocalTransfers instance()
    {
        return instance;
    }

    void save(CoordinatedTransfer transfer)
    {
        lock.writeLock().lock();
        try
        {
            CoordinatedTransfer existing = coordinating.put(transfer.id(), transfer);
            Preconditions.checkState(existing == null);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    void received(PendingLocalTransfer transfer)
    {
        lock.writeLock().lock();
        try
        {
            logger.debug("received: {}", transfer);
            Preconditions.checkState(!transfer.sstables.isEmpty());

            PendingLocalTransfer existing = local.put(transfer.planId, transfer);
            Preconditions.checkState(existing == null);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    private void cleanupCoordinatedTransfer(CoordinatedTransfer transfer)
    {
        lock.writeLock().lock();
        try
        {
            purge(transfer);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    private void cleanupPendingLocalTransfer(TimeUUID timeUUID)
    {
        lock.writeLock().lock();
        try
        {
            purge(local.get(timeUUID));
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    private void purge(TransferFailed failed)
    {
        lock.writeLock().lock();
        try
        {
            PendingLocalTransfer pending = local.get(failed.planId);
            if (pending == null)
            {
                logger.warn("Cannot purge unknown local pending transfer {}", failed);
                return;
            }
            purge(pending);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    private void purge(PendingLocalTransfer transfer)
    {
        logger.info("Cleaning up pending transfer {}", transfer);

        lock.writeLock().lock();
        try
        {
            // Delete the entire pending transfer directory /pending/<planId>/
            if (!transfer.sstables.isEmpty())
            {
                SSTableReader sstable = transfer.sstables.iterator().next();
                File pendingDir = sstable.descriptor.directory;

                if (pendingDir.exists())
                {
                    Preconditions.checkState(pendingDir.absolutePath().contains(transfer.planId.toString()));
                    logger.debug("Deleting pending transfer directory: {}", pendingDir);
                    pendingDir.deleteRecursive();
                }
                local.remove(transfer.planId);
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    private void purge(CoordinatedTransfer transfer)
    {
        logger.info("Cleaning up completed coordinated transfer: {}", transfer);

        lock.writeLock().lock();
        try
        {
            coordinating.remove(transfer.id());

            CoordinatedTransfer.SingleTransferResult localPending = transfer.streamResults.get(FBUtilities.getBroadcastAddressAndPort());
            PendingLocalTransfer localTransfer;
            TimeUUID planId;
            if (localPending != null && (planId = localPending.planId()) != null && (localTransfer = local.get(planId)) != null)
                purge(localTransfer);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    void scheduleCoordinatedTransferCleanup(CoordinatedTransfer transfer)
    {
        executor.submit(() -> {
            try
            {
                cleanupCoordinatedTransfer(transfer);
            }
            catch (Throwable t)
            {
                logger.error("Cleanup failed", t);
            }
        });
    }

    void schedulePendingLocalTransferCleanup(TimeUUID timeUUID)
    {
        executor.submit(() -> {
            try
            {
                cleanupPendingLocalTransfer(timeUUID);
            }
            catch (Throwable t)
            {
                logger.error("Cleanup failed", t);
            }
        });
    }

    public void activatePendingTransfers(TxnRead.ImportMetadata metadata)
    {
        lock.readLock().lock();
        try
        {
            int activatedTransfer = 0;
            for (TimeUUID planId : metadata.getPlanIds())
            {
                PendingLocalTransfer pendingLocalTransfer = local.get(planId);
                if (pendingLocalTransfer != null)
                {
                    activatedTransfer += 1;
                    pendingLocalTransfer.activate();
                }
            }

            Invariants.require(activatedTransfer == 1);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public void shutdownNowAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownNowAndWait(timeout, unit, executor);
    }

    public static IVerbHandler<TransferFailed> verbHandler = message -> {
        LocalTransfers.instance().purge(message.payload);
        MessagingService.instance().respond(NoPayload.noPayload, message);
    };
}
