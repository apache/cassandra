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

import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.ReadRepairDecision;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.SpeculativeRetryParam;
import org.apache.cassandra.service.StorageProxy.LocalReadRunnable;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;

/**
 * Sends a read request to the replicas needed to satisfy a given ConsistencyLevel.
 *
 * Optionally, may perform additional requests to provide redundancy against replica failure:
 * AlwaysSpeculatingReadExecutor will always send a request to one extra replica, while
 * SpeculatingReadExecutor will wait until it looks like the original request is in danger
 * of timing out before performing extra reads.
 */
public abstract class AbstractReadExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractReadExecutor.class);

    protected final ReadCommand command;
    protected final List<InetAddress> targetReplicas;
    protected final ReadCallback handler;
    protected final TraceState traceState;

    AbstractReadExecutor(Keyspace keyspace, ReadCommand command, ConsistencyLevel consistencyLevel, List<InetAddress> targetReplicas)
    {
        this.command = command;
        this.targetReplicas = targetReplicas;
        this.handler = new ReadCallback(new DigestResolver(keyspace, command, consistencyLevel, targetReplicas.size()), consistencyLevel, command, targetReplicas);
        this.traceState = Tracing.instance.get();

        // Set the digest version (if we request some digests). This is the smallest version amongst all our target replicas since new nodes
        // knows how to produce older digest but the reverse is not true.
        // TODO: we need this when talking with pre-3.0 nodes. So if we preserve the digest format moving forward, we can get rid of this once
        // we stop being compatible with pre-3.0 nodes.
        int digestVersion = MessagingService.current_version;
        for (InetAddress replica : targetReplicas)
            digestVersion = Math.min(digestVersion, MessagingService.instance().getVersion(replica));
        command.setDigestVersion(digestVersion);
    }

    protected void makeDataRequests(Iterable<InetAddress> endpoints)
    {
        makeRequests(command, endpoints);

    }

    protected void makeDigestRequests(Iterable<InetAddress> endpoints)
    {
        makeRequests(command.copy().setIsDigestQuery(true), endpoints);
    }

    private void makeRequests(ReadCommand readCommand, Iterable<InetAddress> endpoints)
    {
        boolean hasLocalEndpoint = false;

        for (InetAddress endpoint : endpoints)
        {
            if (StorageProxy.canDoLocalRequest(endpoint))
            {
                hasLocalEndpoint = true;
                continue;
            }

            if (traceState != null)
                traceState.trace("reading {} from {}", readCommand.isDigestQuery() ? "digest" : "data", endpoint);
            logger.trace("reading {} from {}", readCommand.isDigestQuery() ? "digest" : "data", endpoint);
            MessageOut<ReadCommand> message = readCommand.createMessage(MessagingService.instance().getVersion(endpoint));
            MessagingService.instance().sendRRWithFailure(message, endpoint, handler);
        }

        // We delay the local (potentially blocking) read till the end to avoid stalling remote requests.
        if (hasLocalEndpoint)
        {
            logger.trace("reading {} locally", readCommand.isDigestQuery() ? "digest" : "data");
            StageManager.getStage(Stage.READ).maybeExecuteImmediately(new LocalReadRunnable(command, handler));
        }
    }

    /**
     * Perform additional requests if it looks like the original will time out.  May block while it waits
     * to see if the original requests are answered first.
     */
    public abstract void maybeTryAdditionalReplicas();

    /**
     * Get the replicas involved in the [finished] request.
     *
     * @return target replicas + the extra replica, *IF* we speculated.
     */
    public abstract Collection<InetAddress> getContactedReplicas();

    /**
     * send the initial set of requests
     */
    public abstract void executeAsync();

    /**
     * wait for an answer.  Blocks until success or timeout, so it is caller's
     * responsibility to call maybeTryAdditionalReplicas first.
     */
    public PartitionIterator get() throws ReadFailureException, ReadTimeoutException, DigestMismatchException
    {
        return handler.get();
    }

    /**
     * @return an executor appropriate for the configured speculative read policy
     */
    public static AbstractReadExecutor getReadExecutor(SinglePartitionReadCommand command, ConsistencyLevel consistencyLevel) throws UnavailableException
    {
        Keyspace keyspace = Keyspace.open(command.metadata().ksName);
        List<InetAddress> allReplicas = StorageProxy.getLiveSortedEndpoints(keyspace, command.partitionKey());
        ReadRepairDecision repairDecision = command.metadata().newReadRepairDecision();
        List<InetAddress> targetReplicas = consistencyLevel.filterForQuery(keyspace, allReplicas, repairDecision);

        // Throw UAE early if we don't have enough replicas.
        consistencyLevel.assureSufficientLiveNodes(keyspace, targetReplicas);

        if (repairDecision != ReadRepairDecision.NONE)
        {
            Tracing.trace("Read-repair {}", repairDecision);
            ReadRepairMetrics.attempted.mark();
        }

        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().cfId);
        SpeculativeRetryParam retry = cfs.metadata.params.speculativeRetry;

        // Speculative retry is disabled *OR* there are simply no extra replicas to speculate.
        if (retry.equals(SpeculativeRetryParam.NONE) || consistencyLevel.blockFor(keyspace) == allReplicas.size())
            return new NeverSpeculatingReadExecutor(keyspace, command, consistencyLevel, targetReplicas);

        if (targetReplicas.size() == allReplicas.size())
        {
            // CL.ALL, RRD.GLOBAL or RRD.DC_LOCAL and a single-DC.
            // We are going to contact every node anyway, so ask for 2 full data requests instead of 1, for redundancy
            // (same amount of requests in total, but we turn 1 digest request into a full blown data request).
            return new AlwaysSpeculatingReadExecutor(keyspace, cfs, command, consistencyLevel, targetReplicas);
        }

        // RRD.NONE or RRD.DC_LOCAL w/ multiple DCs.
        InetAddress extraReplica = allReplicas.get(targetReplicas.size());
        // With repair decision DC_LOCAL all replicas/target replicas may be in different order, so
        // we might have to find a replacement that's not already in targetReplicas.
        if (repairDecision == ReadRepairDecision.DC_LOCAL && targetReplicas.contains(extraReplica))
        {
            for (InetAddress address : allReplicas)
            {
                if (!targetReplicas.contains(address))
                {
                    extraReplica = address;
                    break;
                }
            }
        }
        targetReplicas.add(extraReplica);

        if (retry.equals(SpeculativeRetryParam.ALWAYS))
            return new AlwaysSpeculatingReadExecutor(keyspace, cfs, command, consistencyLevel, targetReplicas);
        else // PERCENTILE or CUSTOM.
            return new SpeculatingReadExecutor(keyspace, cfs, command, consistencyLevel, targetReplicas);
    }

    public static class NeverSpeculatingReadExecutor extends AbstractReadExecutor
    {
        public NeverSpeculatingReadExecutor(Keyspace keyspace, ReadCommand command, ConsistencyLevel consistencyLevel, List<InetAddress> targetReplicas)
        {
            super(keyspace, command, consistencyLevel, targetReplicas);
        }

        public void executeAsync()
        {
            makeDataRequests(targetReplicas.subList(0, 1));
            if (targetReplicas.size() > 1)
                makeDigestRequests(targetReplicas.subList(1, targetReplicas.size()));
        }

        public void maybeTryAdditionalReplicas()
        {
            // no-op
        }

        public Collection<InetAddress> getContactedReplicas()
        {
            return targetReplicas;
        }
    }

    private static class SpeculatingReadExecutor extends AbstractReadExecutor
    {
        private final ColumnFamilyStore cfs;
        private volatile boolean speculated = false;

        public SpeculatingReadExecutor(Keyspace keyspace,
                                       ColumnFamilyStore cfs,
                                       ReadCommand command,
                                       ConsistencyLevel consistencyLevel,
                                       List<InetAddress> targetReplicas)
        {
            super(keyspace, command, consistencyLevel, targetReplicas);
            this.cfs = cfs;
        }

        public void executeAsync()
        {
            // if CL + RR result in covering all replicas, getReadExecutor forces AlwaysSpeculating.  So we know
            // that the last replica in our list is "extra."
            List<InetAddress> initialReplicas = targetReplicas.subList(0, targetReplicas.size() - 1);

            if (handler.blockfor < initialReplicas.size())
            {
                // We're hitting additional targets for read repair.  Since our "extra" replica is the least-
                // preferred by the snitch, we do an extra data read to start with against a replica more
                // likely to reply; better to let RR fail than the entire query.
                makeDataRequests(initialReplicas.subList(0, 2));
                if (initialReplicas.size() > 2)
                    makeDigestRequests(initialReplicas.subList(2, initialReplicas.size()));
            }
            else
            {
                // not doing read repair; all replies are important, so it doesn't matter which nodes we
                // perform data reads against vs digest.
                makeDataRequests(initialReplicas.subList(0, 1));
                if (initialReplicas.size() > 1)
                    makeDigestRequests(initialReplicas.subList(1, initialReplicas.size()));
            }
        }

        public void maybeTryAdditionalReplicas()
        {
            // no latency information, or we're overloaded
            if (cfs.sampleLatencyNanos > TimeUnit.MILLISECONDS.toNanos(command.getTimeout()))
                return;

            if (!handler.await(cfs.sampleLatencyNanos, TimeUnit.NANOSECONDS))
            {
                // Could be waiting on the data, or on enough digests.
                ReadCommand retryCommand = command;
                if (handler.resolver.isDataPresent())
                    retryCommand = command.copy().setIsDigestQuery(true);

                InetAddress extraReplica = Iterables.getLast(targetReplicas);
                if (traceState != null)
                    traceState.trace("speculating read retry on {}", extraReplica);
                logger.trace("speculating read retry on {}", extraReplica);
                int version = MessagingService.instance().getVersion(extraReplica);
                MessagingService.instance().sendRRWithFailure(retryCommand.createMessage(version), extraReplica, handler);
                speculated = true;

                cfs.metric.speculativeRetries.inc();
            }
        }

        public Collection<InetAddress> getContactedReplicas()
        {
            return speculated
                 ? targetReplicas
                 : targetReplicas.subList(0, targetReplicas.size() - 1);
        }
    }

    private static class AlwaysSpeculatingReadExecutor extends AbstractReadExecutor
    {
        private final ColumnFamilyStore cfs;

        public AlwaysSpeculatingReadExecutor(Keyspace keyspace,
                                             ColumnFamilyStore cfs,
                                             ReadCommand command,
                                             ConsistencyLevel consistencyLevel,
                                             List<InetAddress> targetReplicas)
        {
            super(keyspace, command, consistencyLevel, targetReplicas);
            this.cfs = cfs;
        }

        public void maybeTryAdditionalReplicas()
        {
            // no-op
        }

        public Collection<InetAddress> getContactedReplicas()
        {
            return targetReplicas;
        }

        @Override
        public void executeAsync()
        {
            makeDataRequests(targetReplicas.subList(0, targetReplicas.size() > 1 ? 2 : 1));
            if (targetReplicas.size() > 2)
                makeDigestRequests(targetReplicas.subList(2, targetReplicas.size()));
            cfs.metric.speculativeRetries.inc();
        }
    }
}
