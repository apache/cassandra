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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.journal.RecordPointer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.concurrent.Semaphore;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.ExecutorFactory.SystemThreadTag.NON_DAEMON;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Interrupts.SYNCHRONIZED;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.SAFE;
import static org.apache.cassandra.utils.concurrent.Semaphore.newSemaphore;

// TODO (expected): handle temporarily down nodes
public final class ActiveLogReconciler implements Shutdownable
{
    private static final Logger logger = LoggerFactory.getLogger(ActiveLogReconciler.class);

    public enum Priority { HIGH, REGULAR }

    // prioritised delivery of mutations that are needed by reads;
    private final ManyToOneConcurrentLinkedQueue<Task> highPriorityTasks;

    // regular write retries
    private final ManyToOneConcurrentLinkedQueue<Task> regularPriorityTasks;

    private final Interruptible executor;
    private final Semaphore haveWork;

    ActiveLogReconciler()
    {
        highPriorityTasks = new ManyToOneConcurrentLinkedQueue<>();
        regularPriorityTasks = new ManyToOneConcurrentLinkedQueue<>();
        haveWork = newSemaphore(1);
        executor = executorFactory().infiniteLoop("Active-Log-Reconciler", new SendRunnable(), SAFE, NON_DAEMON, SYNCHRONIZED);
    }

    /**
     * Schedule delivery of a mutation to the specified host
     * TODO (expected): deduplicate via OutgoingMutations
     */
    void schedule(ShortMutationId mutationId, InetAddressAndPort toHost, Priority priority)
    {
        queue(priority).offer(Task.from(mutationId, toHost));
        haveWork.release(1);
    }

    /**
     * Schedule delivery of mutations to the specified host
     * TODO (expected): deduplicate via OutgoingMutations
     */
    void schedule(Offsets offsets, InetAddressAndPort toHost, Priority priority)
    {
        ManyToOneConcurrentLinkedQueue<Task> queue = queue(priority);
        offsets.forEach(id -> queue.offer(Task.from(id, toHost)));
        haveWork.release(1);
    }

    private ManyToOneConcurrentLinkedQueue<Task> queue(Priority priority)
    {
        switch (priority)
        {
            case HIGH: return highPriorityTasks;
            case REGULAR: return regularPriorityTasks;
            default: throw new IllegalStateException();
        }
    }

    private class SendRunnable implements Interruptible.Task
    {
        @Override
        public void run(Interruptible.State state) throws InterruptedException
        {
            if (isPaused || isShutdown) return;

            // TODO (expected): backoff, rate limits, per host and total
            Task task;
            while ((task = highPriorityTasks.poll()) != null)
                task.send();
            while ((task = regularPriorityTasks.poll()) != null)
                task.send();

            haveWork.acquire(1);
        }
    }

    private static abstract class Task implements RequestCallback<NoPayload>
    {
        private static Task from(ShortMutationId id, InetAddressAndPort toHost)
        {
            CoordinatedTransfer transfer = TransferTrackingService.instance().getActivatedTransfer(id);
            if (transfer != null)
                return new TransferTask(transfer, toHost);
            else
                return new MutationTask(id, toHost);
        }

        abstract void send();
    }

    private static final class MutationTask extends Task
    {
        private final ShortMutationId mutationId;
        private final InetAddressAndPort toHost;

        MutationTask(ShortMutationId mutationId, InetAddressAndPort toHost)
        {
            this.mutationId = mutationId;
            this.toHost = toHost;
        }

        @Override
        public boolean invokeOnFailure()
        {
            return true;
        }

        @Override
        public void onResponse(Message<NoPayload> msg)
        {
            MutationTrackingService.instance().receivedWriteResponse(mutationId, toHost);
        }

        @Override
        public void onFailure(InetAddressAndPort from, RequestFailure failureReason)
        {
            MutationTrackingService.instance().retryFailedWrite(mutationId, toHost, failureReason);
        }

        void send()
        {
            RecordPointer pointer = MutationJournal.instance().lookUp(mutationId);
            Preconditions.checkNotNull(pointer, "Mutation %s not found in the journal", mutationId);

            MutationJournal.instance().read(pointer, (segment, position, key, buffer, version) -> {

                // don't send mutations to nodes that have migrated to, or are in the process of migrating to untracked replication
                try (DataInputBuffer in = new DataInputBuffer(buffer, true))
                {
                    ClusterMetadata metadata = ClusterMetadata.current();
                    TableId tableId = Mutation.serializer.deserializeTableId(in, version, DeserializationHelper.Flag.LOCAL);

                    TableMetadata tableMetadata = metadata.schema.getTableMetadata(tableId);
                    if (tableMetadata == null)
                        return;

                    KeyspaceMetadata ksm = metadata.schema.getKeyspaceMetadata(tableMetadata.keyspace);
                    if (ksm == null || !ksm.useMutationTracking())
                        return;

                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }

                Message<PushMutationRequest> message =
                Message.outWithFlag(Verb.PUSH_MUTATION_REQ,
                                    new PushMutationRequest.Buffer(version, buffer),
                                    MessageFlag.CALL_BACK_ON_FAILURE);
                MessagingService.instance().sendWithCallback(message, toHost, this);
            });
        }
    }

    private static final class TransferTask extends Task
    {
        private final CoordinatedTransfer transfer;
        private final InetAddressAndPort toHost;

        TransferTask(CoordinatedTransfer transfer, InetAddressAndPort toHost)
        {
            this.transfer = transfer;
            this.toHost = toHost;
        }

        @Override
        public boolean invokeOnFailure()
        {
            return true;
        }

        @Override
        public void onResponse(Message<NoPayload> msg)
        {
            logger.debug("Received activation ack for TransferTask from {}", toHost);
            MutationTrackingService.instance().receivedActivationResponse(transfer, toHost);
        }

        @Override
        public void onFailure(InetAddressAndPort from, RequestFailure failureReason)
        {
            onFailure(failureReason.failure);
        }

        public void onFailure(Throwable cause)
        {
            logger.debug("Received activation failure for TransferTask from {} due to", toHost, cause);
            MutationTrackingService.instance().retryFailedTransfer(transfer, toHost, cause);
        }

        void send()
        {
            logger.debug("Sending activation to {}", toHost);
            TransferTrackingService.instance().executor.submit(() -> {
                try
                {
                    transfer.activate(toHost);
                    onResponse(null);
                }
                catch (Throwable t)
                {
                    onFailure(t);
                }
            });
        }
    }

    private volatile boolean isShutdown = false;
    private volatile boolean isPaused = false;

    @Override
    public boolean isTerminated()
    {
        return executor.isTerminated();
    }

    @Override
    public void shutdown()
    {
        executor.shutdown();
    }

    @Override
    public Object shutdownNow()
    {
        return executor.shutdownNow();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        return executor.awaitTermination(timeout, units);
    }

    public void shutdownBlocking() throws InterruptedException
    {
        isShutdown = true;
        if (!executor.isTerminated())
        {
            executor.shutdown();
            executor.awaitTermination(1, MINUTES);
        }
    }

    @VisibleForTesting
    void pauseForTesting()
    {
        isPaused = true;
    }

    @VisibleForTesting
    void resumeForTesting()
    {
        isPaused = false;
    }
}
