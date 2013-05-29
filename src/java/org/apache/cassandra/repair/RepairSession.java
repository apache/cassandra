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

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RepairException;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.*;

/**
 * Triggers repairs with all neighbors for the given table, cfs and range.
 */
public class RepairSession extends WrappedRunnable implements IEndpointStateChangeSubscriber, IFailureDetectionEventListener
{
    private static Logger logger = LoggerFactory.getLogger(ActiveRepairService.class);

    /** Repair session ID */
    private final UUID id;
    public final String keyspace;
    private final String[] cfnames;
    public final boolean isSequential;
    /** Range to repair */
    public final Range<Token> range;
    public final Set<InetAddress> endpoints;

    private volatile Exception exception;
    private final AtomicBoolean isFailed = new AtomicBoolean(false);

    // First, all RepairJobs are added to this queue,
    final Queue<RepairJob> jobs = new ConcurrentLinkedQueue<>();
    // and after receiving all validation, the job is moved to
    // this map, keyed by CF name.
    final Map<String, RepairJob> syncingJobs = new ConcurrentHashMap<>();

    private final SimpleCondition completed = new SimpleCondition();
    public final Condition differencingDone = new SimpleCondition();

    private volatile boolean terminated = false;

    /**
     * Create new repair session.
     *
     * @param range range to repair
     * @param keyspace name of keyspace
     * @param isSequential true if performing repair on snapshots sequentially
     * @param isLocal true if you want to perform repair only inside the data center
     * @param cfnames names of columnfamilies
     */
    public RepairSession(Range<Token> range, String keyspace, boolean isSequential, boolean isLocal, String... cfnames)
    {
        this(UUIDGen.getTimeUUID(), range, keyspace, isSequential, isLocal, cfnames);
    }

    public RepairSession(UUID id, Range<Token> range, String keyspace, boolean isSequential, boolean isLocal, String[] cfnames)
    {
        this.id = id;
        this.isSequential = isSequential;
        this.keyspace = keyspace;
        this.cfnames = cfnames;
        assert cfnames.length > 0 : "Repairing no column families seems pointless, doesn't it";
        this.range = range;
        this.endpoints = ActiveRepairService.getNeighbors(keyspace, range, isLocal);
    }

    public UUID getId()
    {
        return id;
    }

    public Range<Token> getRange()
    {
        return range;
    }

    /**
     * Receive merkle tree response or failed response from {@code endpoint} for current repair job.
     *
     * @param desc repair job description
     * @param endpoint endpoint that sent merkle tree
     * @param tree calculated merkle tree, or null if validation failed
     */
    public void validationComplete(RepairJobDesc desc, InetAddress endpoint, MerkleTree tree)
    {
        RepairJob job = jobs.peek();
        if (job == null)
        {
            assert terminated;
            return;
        }

        if (tree == null)
        {
            exception = new RepairException(desc, "Validation failed in " + endpoint);
            forceShutdown();
            return;
        }

        logger.info(String.format("[repair #%s] Received merkle tree for %s from %s", getId(), desc.columnFamily, endpoint));

        assert job.desc.equals(desc);
        if (job.addTree(endpoint, tree) == 0)
        {
            logger.debug("All response received for " + getId() + "/" + desc.columnFamily);
            if (!job.isFailed())
            {
                syncingJobs.put(job.desc.columnFamily, job);
                job.submitDifferencers();
            }

            // This job is complete, switching to next in line (note that only
            // one thread will can ever do this)
            jobs.poll();
            RepairJob nextJob = jobs.peek();
            if (nextJob == null)
                // We are done with this repair session as far as differencing
                // is considered. Just inform the session
                differencingDone.signalAll();
            else
                nextJob.sendTreeRequests(endpoints);
        }
    }

    /**
     * Notify this session that sync completed/failed with given {@code NodePair}.
     *
     * @param desc synced repair job
     * @param nodes nodes that completed sync
     * @param success true if sync succeeded
     */
    public void syncComplete(RepairJobDesc desc, NodePair nodes, boolean success)
    {
        RepairJob job = syncingJobs.get(desc.columnFamily);
        if (job == null)
        {
            assert terminated;
            return;
        }

        if (!success)
        {
            exception = new RepairException(desc, String.format("Sync failed between %s and %s", nodes.endpoint1, nodes.endpoint2));
            forceShutdown();
            return;
        }

        logger.debug(String.format("[repair #%s] Repair completed between %s and %s on %s", getId(), nodes.endpoint1, nodes.endpoint2, desc.columnFamily));

        if (job.completedSynchronization(nodes, success))
        {
            RepairJob completedJob = syncingJobs.remove(job.desc.columnFamily);
            String remaining = syncingJobs.size() == 0 ? "" : String.format(" (%d remaining column family to sync for this session)", syncingJobs.size());
            if (completedJob != null && completedJob.isFailed())
                logger.warn(String.format("[repair #%s] %s sync failed%s", getId(), desc.columnFamily, remaining));
            else
                logger.info(String.format("[repair #%s] %s is fully synced%s", getId(), desc.columnFamily, remaining));

            if (jobs.isEmpty() && syncingJobs.isEmpty())
            {
                // this repair session is completed
                completed.signalAll();
            }
        }
    }

    private String repairedNodes()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(FBUtilities.getBroadcastAddress());
        for (InetAddress ep : endpoints)
            sb.append(", ").append(ep);
        return sb.toString();
    }

    // we don't care about the return value but care about it throwing exception
    public void runMayThrow() throws Exception
    {
        logger.info(String.format("[repair #%s] new session: will sync %s on range %s for %s.%s", getId(), repairedNodes(), range, keyspace, Arrays.toString(cfnames)));

        if (endpoints.isEmpty())
        {
            differencingDone.signalAll();
            logger.info(String.format("[repair #%s] No neighbors to repair with on range %s: session completed", getId(), range));
            return;
        }

        // Checking all nodes are live
        for (InetAddress endpoint : endpoints)
        {
            if (!FailureDetector.instance.isAlive(endpoint))
            {
                String message = String.format("Cannot proceed on repair because a neighbor (%s) is dead: session failed", endpoint);
                differencingDone.signalAll();
                logger.error(String.format("[repair #%s] ", getId()) + message);
                throw new IOException(message);
            }
        }
        ActiveRepairService.instance.addToActiveSessions(this);
        try
        {
            // Create and queue a RepairJob for each column family
            for (String cfname : cfnames)
            {
                RepairJob job = new RepairJob(id, keyspace, cfname, range, isSequential);
                jobs.offer(job);
            }

            jobs.peek().sendTreeRequests(endpoints);

            // block whatever thread started this session until all requests have been returned:
            // if this thread dies, the session will still complete in the background
            completed.await();
            if (exception == null)
            {
                logger.info(String.format("[repair #%s] session completed successfully", getId()));
            }
            else
            {
                logger.error(String.format("[repair #%s] session completed with the following error", getId()), exception);
                throw exception;
            }
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException("Interrupted while waiting for repair.");
        }
        finally
        {
            // mark this session as terminated
            terminate();
            ActiveRepairService.instance.removeFromActiveSessions(this);
        }
    }

    public void terminate()
    {
        terminated = true;
        for (RepairJob job : jobs)
            job.terminate();
        jobs.clear();
        syncingJobs.clear();
    }

    /**
     * clear all RepairJobs and terminate this session.
     */
    public void forceShutdown()
    {
        differencingDone.signalAll();
        completed.signalAll();
    }

    void failedNode(InetAddress remote)
    {
        String errorMsg = String.format("Endpoint %s died", remote);
        exception = new IOException(errorMsg);
        // If a node failed, we stop everything (though there could still be some activity in the background)
        forceShutdown();
    }

    public void onJoin(InetAddress endpoint, EndpointState epState) {}
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {}
    public void onAlive(InetAddress endpoint, EndpointState state) {}
    public void onDead(InetAddress endpoint, EndpointState state) {}

    public void onRemove(InetAddress endpoint)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    public void onRestart(InetAddress endpoint, EndpointState epState)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    public void convict(InetAddress endpoint, double phi)
    {
        if (!endpoints.contains(endpoint))
            return;

        // We want a higher confidence in the failure detection than usual because failing a repair wrongly has a high cost.
        if (phi < 2 * DatabaseDescriptor.getPhiConvictThreshold())
            return;

        // Though unlikely, it is possible to arrive here multiple time and we
        // want to avoid print an error message twice
        if (!isFailed.compareAndSet(false, true))
            return;

        failedNode(endpoint);
    }
}
