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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;

import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.ValidationRequest;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

/**
 * RepairJob runs repair on given ColumnFamily.
 */
public class RepairJob
{
    private static Logger logger = LoggerFactory.getLogger(RepairJob.class);

    public final RepairJobDesc desc;
    private final RepairParallelism parallelismDegree;
    // first we send tree requests. this tracks the endpoints remaining to hear from
    private final IRequestCoordinator<InetAddress> treeRequests;
    // tree responses are then tracked here
    private final List<TreeResponse> trees = new ArrayList<>();
    // once all responses are received, each tree is compared with each other, and differencer tasks
    // are submitted. the job is done when all differencers are complete.
    private final ListeningExecutorService taskExecutor;
    private final Condition requestsSent = new SimpleCondition();
    private int gcBefore = -1;

    private volatile boolean failed = false;
    /* Count down as sync completes */
    private AtomicInteger waitForSync;

    private final IRepairJobEventListener listener;

    /**
     * Create repair job to run on specific columnfamily
     */
    public RepairJob(IRepairJobEventListener listener,
                     UUID parentSessionId,
                     UUID sessionId,
                     String keyspace,
                     String columnFamily,
                     Range<Token> range,
                     RepairParallelism parallelismDegree,
                     ListeningExecutorService taskExecutor)
    {
        this.listener = listener;
        this.desc = new RepairJobDesc(parentSessionId, sessionId, keyspace, columnFamily, range);
        this.parallelismDegree = parallelismDegree;
        this.taskExecutor = taskExecutor;

        IRequestProcessor<InetAddress> processor = new IRequestProcessor<InetAddress>()
        {
            @Override
            public void process(InetAddress endpoint)
            {
                ValidationRequest request = new ValidationRequest(desc, gcBefore);
                MessagingService.instance().sendOneWay(request.createMessage(), endpoint);
            }
        };

        switch (parallelismDegree)
        {
            case SEQUENTIAL:
                this.treeRequests = new SequentialRequestCoordinator<>(processor);
                break;
            case PARALLEL:
                this.treeRequests = new ParallelRequestCoordinator<>(processor);
                break;
            case DATACENTER_AWARE:
                this.treeRequests = new DatacenterAwareRequestCoordinator(processor);
                break;
            default:
                throw new AssertionError("Unknown degree of parallelism specified");
        }
    }

    /**
     * @return true if this job failed
     */
    public boolean isFailed()
    {
        return failed;
    }

    /**
     * Send merkle tree request to every involved neighbor.
     */
    public void sendTreeRequests(Collection<InetAddress> endpoints)
    {
        // send requests to all nodes
        List<InetAddress> allEndpoints = new ArrayList<>(endpoints);
        allEndpoints.add(FBUtilities.getBroadcastAddress());

        // Create a snapshot at all nodes unless we're using pure parallel repairs
        if (parallelismDegree != RepairParallelism.PARALLEL)
        {
            List<ListenableFuture<InetAddress>> snapshotTasks = new ArrayList<>(allEndpoints.size());
            for (InetAddress endpoint : allEndpoints)
            {
                SnapshotTask snapshotTask = new SnapshotTask(desc, endpoint);
                snapshotTasks.add(snapshotTask);
                taskExecutor.execute(snapshotTask);
            }
            ListenableFuture<List<InetAddress>> allSnapshotTasks = Futures.allAsList(snapshotTasks);
            // Execute send tree request after all snapshot complete
            Futures.addCallback(allSnapshotTasks, new FutureCallback<List<InetAddress>>()
            {
                public void onSuccess(List<InetAddress> endpoints)
                {
                    sendTreeRequestsInternal(endpoints);
                }

                public void onFailure(Throwable throwable)
                {
                    // TODO need to propagate error to RepairSession
                    logger.error("Error occurred during snapshot phase", throwable);
                    listener.failedSnapshot();
                    failed = true;
                }
            }, taskExecutor);
        }
        else
        {
            sendTreeRequestsInternal(allEndpoints);
        }
    }

    private void sendTreeRequestsInternal(Collection<InetAddress> endpoints)
    {
        this.gcBefore = Keyspace.open(desc.keyspace).getColumnFamilyStore(desc.columnFamily).gcBefore(System.currentTimeMillis());
        for (InetAddress endpoint : endpoints)
            treeRequests.add(endpoint);

        logger.info(String.format("[repair #%s] requesting merkle trees for %s (to %s)", desc.sessionId, desc.columnFamily, endpoints));
        treeRequests.start();
        requestsSent.signalAll();
    }

    /**
     * Add a new received tree and return the number of remaining tree to
     * be received for the job to be complete.
     *
     * Callers may assume exactly one addTree call will result in zero remaining endpoints.
     *
     * @param endpoint address of the endpoint that sent response
     * @param tree sent Merkle tree or null if validation failed on endpoint
     * @return the number of responses waiting to receive
     */
    public synchronized int addTree(InetAddress endpoint, MerkleTree tree)
    {
        // Wait for all request to have been performed (see #3400)
        try
        {
            requestsSent.await();
        }
        catch (InterruptedException e)
        {
            throw new AssertionError("Interrupted while waiting for requests to be sent");
        }

        if (tree == null)
            failed = true;
        else
            trees.add(new TreeResponse(endpoint, tree));
        return treeRequests.completed(endpoint);
    }

    /**
     * Submit differencers for running.
     * All tree *must* have been received before this is called.
     */
    public void submitDifferencers()
    {
        assert !failed;
        List<Differencer> differencers = new ArrayList<>();
        // We need to difference all trees one against another
        for (int i = 0; i < trees.size() - 1; ++i)
        {
            TreeResponse r1 = trees.get(i);
            for (int j = i + 1; j < trees.size(); ++j)
            {
                TreeResponse r2 = trees.get(j);
                Differencer differencer = new Differencer(desc, r1, r2);
                differencers.add(differencer);
                logger.debug("Queueing comparison {}", differencer);
            }
        }
        waitForSync = new AtomicInteger(differencers.size());
        for (Differencer differencer : differencers)
            taskExecutor.submit(differencer);

        trees.clear(); // allows gc to do its thing
    }

    /**
     * @return true if the given node pair was the last remaining
     */
    boolean completedSynchronization()
    {
        return waitForSync.decrementAndGet() == 0;
    }
}
