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
package org.apache.cassandra.repair;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * RepairJob runs repair on given ColumnFamily.
 */
public class RepairJob extends AbstractFuture<RepairResult> implements Runnable
{
    private static Logger logger = LoggerFactory.getLogger(RepairJob.class);

    private final RepairSession session;
    private final RepairJobDesc desc;
    private final boolean isSequential;
    private final long repairedAt;
    private final ListeningExecutorService taskExecutor;

    /**
     * Create repair job to run on specific columnfamily
     *
     * @param session RepairSession that this RepairJob belongs
     * @param columnFamily name of the ColumnFamily to repair
     * @param isSequential when true, validation runs sequentially among replica
     * @param repairedAt when the repair occurred (millis)
     * @param taskExecutor Executor to run various repair tasks
     */
    public RepairJob(RepairSession session,
                     String columnFamily,
                     boolean isSequential,
                     long repairedAt,
                     ListeningExecutorService taskExecutor)
    {
        this.session = session;
        this.desc = new RepairJobDesc(session.parentRepairSession, session.getId(), session.keyspace, columnFamily, session.getRange());
        this.isSequential = isSequential;
        this.repairedAt = repairedAt;
        this.taskExecutor = taskExecutor;
    }

    /**
     * Runs repair job.
     *
     * This sets up necessary task and runs them on given {@code taskExecutor}.
     * After submitting all tasks, waits until validation with replica completes.
     */
    public void run()
    {
        List<InetAddress> allEndpoints = new ArrayList<>(session.endpoints);
        allEndpoints.add(FBUtilities.getBroadcastAddress());

        ListenableFuture<List<TreeResponse>> validations;
        if (isSequential)
        {
            // Request snapshot to all replica
            List<ListenableFuture<InetAddress>> snapshotTasks = new ArrayList<>(allEndpoints.size());
            for (InetAddress endpoint : allEndpoints)
            {
                SnapshotTask snapshotTask = new SnapshotTask(desc, endpoint);
                snapshotTasks.add(snapshotTask);
                taskExecutor.execute(snapshotTask);
            }
            // When all snapshot complete, send validation requests
            ListenableFuture<List<InetAddress>> allSnapshotTasks = Futures.allAsList(snapshotTasks);
            validations = Futures.transform(allSnapshotTasks, new AsyncFunction<List<InetAddress>, List<TreeResponse>>()
            {
                public ListenableFuture<List<TreeResponse>> apply(List<InetAddress> endpoints) throws Exception
                {
                    return sendValidationRequest(endpoints);
                }
            }, taskExecutor);
        }
        else
        {
            // If not sequential, just send validation request to all replica
            validations = sendValidationRequest(allEndpoints);
        }

        // When all validations complete, submit sync tasks
        ListenableFuture<List<SyncStat>> syncResults = Futures.transform(validations, new AsyncFunction<List<TreeResponse>, List<SyncStat>>()
        {
            public ListenableFuture<List<SyncStat>> apply(List<TreeResponse> trees) throws Exception
            {
                // Unregister from FailureDetector once we've completed synchronizing Merkle trees.
                // After this point, we rely on tcp_keepalive for individual sockets to notify us when a connection is down.
                // See CASSANDRA-3569
                FailureDetector.instance.unregisterFailureDetectionEventListener(session);

                InetAddress local = FBUtilities.getLocalAddress();

                List<SyncTask> syncTasks = new ArrayList<>();
                // We need to difference all trees one against another
                for (int i = 0; i < trees.size() - 1; ++i)
                {
                    TreeResponse r1 = trees.get(i);
                    for (int j = i + 1; j < trees.size(); ++j)
                    {
                        TreeResponse r2 = trees.get(j);
                        SyncTask task;
                        if (r1.endpoint.equals(local) || r2.endpoint.equals(local))
                        {
                            task = new LocalSyncTask(desc, r1, r2, repairedAt);
                        }
                        else
                        {
                            task = new RemoteSyncTask(desc, r1, r2);
                            // RemoteSyncTask expects SyncComplete message sent back.
                            // Register task to RepairSession to receive response.
                            session.waitForSync(Pair.create(desc, new NodePair(r1.endpoint, r2.endpoint)), (RemoteSyncTask) task);
                        }
                        syncTasks.add(task);
                        taskExecutor.submit(task);
                    }
                }
                return Futures.allAsList(syncTasks);
            }
        }, taskExecutor);

        // When all sync complete, set the final result
        Futures.addCallback(syncResults, new FutureCallback<List<SyncStat>>()
        {
            public void onSuccess(List<SyncStat> stats)
            {
                logger.info(String.format("[repair #%s] %s is fully synced", session.getId(), desc.columnFamily));
                set(new RepairResult(desc, stats));
            }

            /**
             * Snapshot, validation and sync failures are all handled here
             */
            public void onFailure(Throwable t)
            {
                logger.warn(String.format("[repair #%s] %s sync failed", session.getId(), desc.columnFamily));
                setException(t);
            }
        }, taskExecutor);

        // Wait for validation to complete
        Futures.getUnchecked(validations);
    }

    /**
     * Creates {@link ValidationTask} and submit them to task executor.
     * If isSequential flag is true, wait previous ValidationTask to complete before submitting the next.
     *
     * @param endpoints Endpoint addresses to send validation request
     * @return Future that can get all {@link TreeResponse} from replica, if all validation succeed.
     */
    private ListenableFuture<List<TreeResponse>> sendValidationRequest(Collection<InetAddress> endpoints)
    {
        logger.info(String.format("[repair #%s] requesting merkle trees for %s (to %s)", desc.sessionId, desc.columnFamily, endpoints));
        int gcBefore = Keyspace.open(desc.keyspace).getColumnFamilyStore(desc.columnFamily).gcBefore(System.currentTimeMillis());
        List<ListenableFuture<TreeResponse>> tasks = new ArrayList<>(endpoints.size());
        for (InetAddress endpoint : endpoints)
        {
            ValidationTask task = new ValidationTask(desc, endpoint, gcBefore);
            tasks.add(task);
            session.waitForValidation(Pair.create(desc, endpoint), task);
            taskExecutor.execute(task);
            if (isSequential)
            {
                // tasks are sequentially sent so wait until current validation is done.
                // NOTE: Wait happens on taskExecutor thread
                Futures.getUnchecked(task);
            }
        }
        return Futures.allAsList(tasks);
    }
}
