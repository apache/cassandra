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

package org.apache.cassandra.replication;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.repair.RepairJob;
import org.apache.cassandra.repair.SyncStat;
import org.apache.cassandra.repair.SyncTask;
import org.apache.cassandra.repair.SyncTasks;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

/**
 * Singleton registry maintaining state for bulk data transfers on the local node.
 * <p>
 * This includes {@link CoordinatedTransfer} instances that the current node is coordinating, and
 * {@link PendingLocalTransfer} instances that are coordinated by other nodes. Pending transfers are inactive until
 * activated by the coordinator.
 * <p>
 * TODO: Make changes to pending set durable with SystemKeyspace.savePendingLocalTransfer(transfer)?
 * TODO: Add vtable for visibility into local and coordinated transfers
 */
public class TransferTrackingService
{
    private static final Logger logger = LoggerFactory.getLogger(TransferTrackingService.class);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<ShortMutationId, CoordinatedTransfer> coordinating = new ConcurrentHashMap<>();
    private final Map<TimeUUID, PendingLocalTransfer> local = new ConcurrentHashMap<>();

    final ExecutorPlus executor = executorFactory().pooled("LocalTrackedTransfers", Integer.MAX_VALUE);

    private static final TransferTrackingService instance = new TransferTrackingService();
    public static TransferTrackingService instance()
    {
        return instance;
    }

    void save(TrackedImportTransfer transfer)
    {
        lock.writeLock().lock();
        try
        {
            saveInternal(transfer);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    private void saveInternal(CoordinatedTransfer transfer)
    {
        Preconditions.checkNotNull(transfer.id(), "Cannot coordinate a transfar with no ID");
        logger.debug("{} Saving {}", transfer.logPrefix(), transfer);
        CoordinatedTransfer existing = coordinating.put(transfer.id(), transfer);
        Preconditions.checkState(existing == null, "Attempted to save transfer multiple times");
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

    /**
     * Track a repair as a set of {@link TrackedRepairTransfer} instances corresponding to sync tasks prior to task 
     * execution so when the syncs are done, we can activate them via {@link ActivationRequest} or fail by 
     * sending {@link TransferFailed} to all replicas. In other words, one {@link RepairJob} will have as many
     * transfers as sync tasks.
     */
    public void onRepairSyncExecution(SyncTasks tasks)
    {
        lock.writeLock().lock();
        try
        {
            tasks.apply((SyncTasks.ShardedSyncTask shardedTask) -> {
                TrackedRepairTransfer transfer = new TrackedRepairTransfer(shardedTask);
                saveInternal(transfer);
            });
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Begin activation for transfers whose repair sync tasks have completed.
     */
    public Future<List<SyncStat>> onRepairSyncCompletion(RepairJob job, Future<List<SyncStat>> syncCompletion, Executor executor)
    {
        AsyncPromise<List<SyncStat>> activationFuture = new AsyncPromise<>();

        syncCompletion.addCallback(new FutureCallback<>()
        {
            @Override
            public void onSuccess(List<SyncStat> syncs)
            {
                logger.info("Completed syncs {}. Coordinating: {} Local: {}", syncs, coordinating, local);

                List<Pair<SyncStat, TrackedRepairTransfer>> transfersToActivate = new ArrayList<>();

                lock.writeLock().lock();
                try
                {
                    // Look up transfers while holding the lock
                    for (SyncStat sync : syncs)
                    {
                        TrackedRepairTransfer transfer = (TrackedRepairTransfer) coordinating.get(sync.transferId);
                        transfersToActivate.add(Pair.create(sync, transfer));
                    }
                }
                finally
                {
                    lock.writeLock().unlock();
                }

                // Activate transfers WITHOUT holding the lock (activate() acquires its own locks and can block)
                for (Pair<SyncStat, TrackedRepairTransfer> pair : transfersToActivate)
                {
                    TrackedRepairTransfer transfer = pair.right;

                    try
                    {
                        logger.info("{} Activating transfer...", transfer.logPrefix());
                        transfer.activate(pair.left);
                    }
                    catch (Throwable t)
                    {
                        // Note: cleanup will be triggered automatically when the async COMMIT responses complete
                        logger.error("{} Failed to activate transfer", transfer.logPrefix(), t);
                        activationFuture.tryFailure(t);
                    }
                }

                // Activation succeeded, complete the future with the sync stats
                activationFuture.trySuccess(syncs);
            }

            @Override
            public void onFailure(Throwable t)
            {
                logger.error("Failed to complete sync tasks. Cleaning up pending transfers... ", t);

                lock.writeLock().lock();
                try
                {
                    Set<ShortMutationId> transferIds = new HashSet<>();
                    for (SyncTask task : job.getSyncTasks())
                    {
                        ShortMutationId transferId = task.getTransferId();
                        Preconditions.checkNotNull(transferId);
                        transferIds.add(transferId);

                        TimeUUID planId = task.getPlanId();
                        if (planId == null)
                            continue;

                        CoordinatedTransfer transfer = coordinating.get(transferId);
                        Pair<InetAddressAndPort, InetAddressAndPort> pair = Pair.create(task.nodePair().coordinator, task.nodePair().peer);
                        transfer.streamResults.put(pair, CoordinatedTransfer.SingleTransferResult.Init().streamFailed(planId));
                        transfer.streamResults.put(pair.reverse(), CoordinatedTransfer.SingleTransferResult.Init().streamFailed(planId));
                    }

                    for (ShortMutationId transferId : transferIds)
                    {
                        CoordinatedTransfer transfer = coordinating.get(transferId);
                        try
                        {
                            transfer.notifyFailure();
                        }
                        catch (Throwable t0)
                        {
                            logger.error("{} Failed to notify peers of repair failure", transfer.logPrefix(), t0);
                        }
                    }

                    scheduleCleanup();
                    activationFuture.tryFailure(t);
                }
                finally
                {
                    lock.writeLock().unlock();
                }
            }
        }, executor);

        return activationFuture;
    }

    Purger purger = new Purger();

    static class Purger
    {
        /**
         * It's safe to purge a transfer if it failed either before it was activated anywhere, or after all activation
         * has completed everywhere. If a transfer is partially activated (on some replicas but not others), it's going
         * to be included in future reconciliations and needs to be preserved until reconciliation is complete.
         */
        boolean test(CoordinatedTransfer transfer)
        {
            logger.debug("Checking whether we can purge {}", transfer);
            boolean failedBeforeActivation = false;
            boolean noneActivated = true;
            boolean allComplete = true;
            for (TrackedImportTransfer.SingleTransferResult result : transfer.streamResults.values())
            {
                switch (result.state)
                {
                    case STREAM_FAILED:
                    case PREPARE_FAILED:
                        failedBeforeActivation = true;
                        break;
                    case COMMITTED:
                        noneActivated = false;
                        break;
                    case INIT:
                    case STREAM_COMPLETE:
                    case PREPARING:
                    case COMMITTING:
                        allComplete = false;
                }
            }

            return (failedBeforeActivation && noneActivated) || allComplete;
        }

        boolean test(PendingLocalTransfer transfer)
        {
            return transfer.activated;
        }
    }

    private void cleanup()
    {
        lock.writeLock().lock();
        try
        {
            for (PendingLocalTransfer transfer : local.values())
                if (purger.test(transfer))
                    purge(transfer);

            for (CoordinatedTransfer transfer : coordinating.values())
                if (purger.test(transfer))
                    purge(transfer);
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

            if (transfer.id() != null)
                coordinating.remove(transfer.id());
            
            for (Map.Entry<Pair<InetAddressAndPort, InetAddressAndPort>, CoordinatedTransfer.SingleTransferResult> result : transfer.streamResults.entrySet())
            {
                if (result.getKey().right.equals(FBUtilities.getBroadcastAddressAndPort()))
                {
                    CoordinatedTransfer.SingleTransferResult localPending = transfer.streamResults.get(result.getKey());
                    PendingLocalTransfer localTransfer;
                    TimeUUID planId;
                    if (localPending != null && (planId = localPending.planId()) != null && (localTransfer = local.get(planId)) != null)
                        purge(localTransfer);
                }
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    void scheduleCleanup()
    {
        executor.submit(() -> {
            try
            {
                cleanup();
            }
            catch (Throwable t)
            {
                logger.error("Cleanup failed", t);
            }
        });
    }

    @Nullable PendingLocalTransfer getPendingTransfer(TimeUUID planId)
    {
        lock.readLock().lock();
        try
        {
            return local.get(planId);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Nullable
    CoordinatedTransfer getActivatedTransfer(ShortMutationId transferId)
    {
        lock.readLock().lock();
        try
        {
            return coordinating.get(transferId);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public static IVerbHandler<TransferFailed> verbHandler = message -> {
        TransferTrackingService.instance().purge(message.payload);
        MessagingService.instance().respond(NoPayload.noPayload, message);
    };
}
