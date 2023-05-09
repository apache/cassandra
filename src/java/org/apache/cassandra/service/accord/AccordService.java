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

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.BarrierType;
import accord.api.Result;
import accord.coordinate.Preempted;
import accord.coordinate.Timeout;
import accord.impl.SimpleProgressLog;
import accord.impl.SizeOfIntersectionSorter;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.ShardDistributor.EvenSplit;
import accord.messages.Request;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import accord.topology.TopologyManager;
import accord.utils.DefaultRandom;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.metrics.AccordClientRequestMetrics;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.KeyspaceSplitter;
import org.apache.cassandra.service.accord.api.AccordScheduler;
import org.apache.cassandra.service.accord.exceptions.ReadPreemptedException;
import org.apache.cassandra.service.accord.exceptions.WritePreemptedException;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.getConcurrentAccordOps;
import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.accordReadMetrics;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.accordWriteMetrics;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class AccordService implements IAccordService, Shutdownable
{
    private static final Logger logger = LoggerFactory.getLogger(AccordService.class);

    private final Node node;
    private final Shutdownable nodeShutdown;
    private final AccordMessageSink messageSink;
    private final AccordConfigurationService configService;
    private final AccordScheduler scheduler;
    private final AccordVerbHandler<? extends Request> verbHandler;

    private static final IAccordService NOOP_SERVICE = new IAccordService()
    {
        @Override
        public IVerbHandler<? extends Request> verbHandler()
        {
            return null;
        }

        @Override
        public void createEpochFromConfigUnsafe() { }

        @Override
        public long barrier(@Nonnull Seekables keysOrRanges, long minEpoch, long queryStartNanos, BarrierType barrierType, boolean isForWrite)
        {
            throw new UnsupportedOperationException("No accord barriers should be executed when accord_transactions_enabled = false in cassandra.yaml");
        }

        @Override
        public @Nonnull TxnResult coordinate(@Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, @Nonnull long queryStartNanos)
        {
            throw new UnsupportedOperationException("No accord transaction should be executed when accord_transactions_enabled = false in cassandra.yaml");
        }

        @Override
        public long currentEpoch()
        {
            throw new UnsupportedOperationException("Cannot return epoch when accord_transactions_enabled = false in cassandra.yaml");
        }

        @Override
        public void setCacheSize(long kb) { }

        @Override
        public TopologyManager topology()
        {
            throw new UnsupportedOperationException("Cannot return topology when accord_transactions_enabled = false in cassandra.yaml");
        }

        @Override
        public void shutdownAndWait(long timeout, TimeUnit unit) { }
    };

    private static class Handle
    {
        public static final AccordService instance = new AccordService();
    }

    public static IAccordService instance()
    {
        return DatabaseDescriptor.getAccordTransactionsEnabled() ? Handle.instance : NOOP_SERVICE;
    }

    public static long uniqueNow()
    {
        return TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis());
    }

    private AccordService()
    {
        Node.Id localId = EndpointMapping.endpointToId(FBUtilities.getBroadcastAddressAndPort());
        logger.info("Starting accord with nodeId {}", localId);
        this.messageSink = new AccordMessageSink();
        this.configService = new AccordConfigurationService(localId);
        this.scheduler = new AccordScheduler();
        this.node = new Node(localId,
                             messageSink,
                             configService,
                             AccordService::uniqueNow,
                             () -> null,
                             new KeyspaceSplitter(new EvenSplit<>(getConcurrentAccordOps(), getPartitioner().accordSplitter())),
                             new AccordAgent(),
                             new DefaultRandom(),
                             scheduler,
                             SizeOfIntersectionSorter.SUPPLIER,
                             SimpleProgressLog::new,
                             AccordCommandStores::new);
        this.nodeShutdown = toShutdownable(node);
        this.verbHandler = new AccordVerbHandler<>(this.node);
    }

    @Override
    public IVerbHandler<? extends Request> verbHandler()
    {
        return verbHandler;
    }

    @Override
    @VisibleForTesting
    public void createEpochFromConfigUnsafe()
    {
        configService.createEpochFromConfig();
    }

    @Override
    public long barrier(@Nonnull Seekables keysOrRanges, long epoch, long queryStartNanos, BarrierType barrierType, boolean isForWrite)
    {
        AccordClientRequestMetrics metrics = isForWrite ? accordWriteMetrics : accordReadMetrics;
        TxnId txnId = null;
        try
        {
            // Synchronization is a temporary solution to make sure whatever epoch we find has the config created in Accord
            long currentEpoch;
            synchronized (ClusterMetadataService.instance)
            {
                currentEpoch = ClusterMetadataService.instance.metadata.epoch.getEpoch();
            }
            if (node.topology().localForEpoch(currentEpoch).ranges().isEmpty())
            {
                metrics.unavailables.mark();
                throw new UnavailableException("There are no local shards to run the transaction on", ConsistencyLevel.ANY, 0, 0);
            }

            logger.debug("Starting barrier key: {} epoch: {} barrierType: {} isForWrite {}", keysOrRanges, epoch, barrierType, isForWrite);
            txnId = node.nextTxnId(Kind.SyncPoint, keysOrRanges.domain());
            AsyncResult<Timestamp> asyncResult = node.barrier(keysOrRanges, epoch, barrierType);
            long deadlineNanos = queryStartNanos + DatabaseDescriptor.getTransactionTimeout(NANOSECONDS);
            Timestamp barrierExecuteAt = AsyncChains.getBlocking(asyncResult, deadlineNanos - nanoTime(), NANOSECONDS);
            logger.debug("Completed in {}ms barrier key: {} epoch: {} barrierType: {} isForWrite {}",
                         NANOSECONDS.toMillis(nanoTime() - queryStartNanos),
                         keysOrRanges, epoch, barrierType, isForWrite);
            return barrierExecuteAt.epoch();
        }
        catch (ExecutionException e)
        {
            Throwable cause = e.getCause();
            if (cause instanceof Timeout)
            {
                metrics.timeouts.mark();
                throw newBarrierTimeout(txnId, barrierType.global);
            }
            if (cause instanceof Preempted)
            {
                metrics.preempts.mark();
                //TODO need to improve
                // Coordinator "could" query the accord state to see whats going on but that doesn't exist yet.
                // Protocol also doesn't have a way to denote "unknown" outcome, so using a timeout as the closest match
                throw newBarrierPreempted(txnId, barrierType.global);
            }
            metrics.failures.mark();
            throw new RuntimeException(cause);
        }
        catch (InterruptedException e)
        {
            metrics.failures.mark();
            throw new UncheckedInterruptedException(e);
        }
        catch (TimeoutException e)
        {
            metrics.timeouts.mark();
            throw newBarrierTimeout(txnId, barrierType.global);
        }
        finally
        {
            // TODO Should barriers have a dedicated latency metric? Should it be a read/write metric?
            // What about counts for timeouts/failures/preempts?
            metrics.addNano(nanoTime() - queryStartNanos);
        }
    }

    @Override
    public long currentEpoch()
    {
        return configService.currentEpoch();
    }

    @Override
    public TopologyManager topology()
    {
        return node.topology();
    }

    /**
     * Consistency level is just echoed back in timeouts, in the future it may be used for interoperability
     * with non-Accord operations.
     */
    @Override
    public @Nonnull TxnResult coordinate(@Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, long queryStartNanos)
    {
        AccordClientRequestMetrics metrics = txn.isWrite() ? accordWriteMetrics : accordReadMetrics;
        TxnId txnId = null;
        try
        {
            // Synchronziation is a temporary solution to make sure whatever epoch we find has the config created in Accord
            long epoch;
            synchronized (ClusterMetadataService.instance)
            {
                epoch = ClusterMetadataService.instance.metadata.epoch.getEpoch();
            }
            if (node.topology().localForEpoch(epoch).ranges().isEmpty())
            {
                metrics.unavailables.mark();
                throw new UnavailableException("There are no local shards to run the transaction on", consistencyLevel, 0, 0);
            }

            metrics.keySize.update(txn.keys().size());
            txnId = node.nextTxnId(txn.kind(), txn.keys().domain());
            long deadlineNanos = queryStartNanos + DatabaseDescriptor.getTransactionTimeout(NANOSECONDS);
            AsyncResult<Result> asyncResult = node.coordinate(txnId, txn);
            Result result = AsyncChains.getBlocking(asyncResult, deadlineNanos - nanoTime(), NANOSECONDS);
            return (TxnResult) result;
        }
        catch (ExecutionException e)
        {
            Throwable cause = e.getCause();
            if (cause instanceof Timeout)
            {
                metrics.timeouts.mark();
                throw newTimeout(txnId, txn, consistencyLevel);
            }
            if (cause instanceof Preempted)
            {
                metrics.preempts.mark();
                //TODO need to improve
                // Coordinator "could" query the accord state to see whats going on but that doesn't exist yet.
                // Protocol also doesn't have a way to denote "unknown" outcome, so using a timeout as the closest match
                throw newPreempted(txnId, txn, consistencyLevel);
            }
            metrics.failures.mark();
            throw new RuntimeException(cause);
        }
        catch (InterruptedException e)
        {
            metrics.failures.mark();
            throw new UncheckedInterruptedException(e);
        }
        catch (TimeoutException e)
        {
            metrics.timeouts.mark();
            throw newTimeout(txnId, txn, consistencyLevel);
        }
        finally
        {
            metrics.addNano(nanoTime() - queryStartNanos);
        }
    }

    private static RequestTimeoutException newTimeout(TxnId txnId, Txn txn, ConsistencyLevel consistencyLevel)
    {
        throw txn.isWrite() ? new WriteTimeoutException(WriteType.CAS, consistencyLevel, 0, 0, txnId.toString())
                            : new ReadTimeoutException(consistencyLevel, 0, 0, false, txnId.toString());
    }

    private static RuntimeException newPreempted(TxnId txnId, Txn txn, ConsistencyLevel consistencyLevel)
    {
        throw txn.isWrite() ? new WritePreemptedException(WriteType.CAS, consistencyLevel, 0, 0, txnId.toString())
                            : new ReadPreemptedException(consistencyLevel, 0, 0, false, txnId.toString());
    }

    private static ReadTimeoutException newBarrierTimeout(TxnId txnId, boolean global)
    {
        return new ReadTimeoutException(global ? ConsistencyLevel.ANY : ConsistencyLevel.QUORUM, 0, 0, false, txnId.toString());
    }

    private static ReadTimeoutException newBarrierPreempted(TxnId txnId, boolean global)
    {
        return new ReadPreemptedException(global ? ConsistencyLevel.ANY : ConsistencyLevel.QUORUM, 0, 0, false, txnId.toString());
    }


    @VisibleForTesting
    AccordMessageSink messageSink()
    {
        return messageSink;
    }

    @Override
    public void setCacheSize(long kb)
    {
        long bytes = kb << 10;
        AccordCommandStores commandStores = (AccordCommandStores) node.commandStores();
        commandStores.setCacheSize(bytes);
    }

    @Override
    public boolean isTerminated()
    {
        return scheduler.isTerminated();
    }

    @Override
    public void shutdown()
    {
        ExecutorUtils.shutdown(Arrays.asList(scheduler, nodeShutdown));
    }

    @Override
    public Object shutdownNow()
    {
        ExecutorUtils.shutdownNow(Arrays.asList(scheduler, nodeShutdown));
        return null;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        try
        {
            ExecutorUtils.awaitTermination(timeout, units, Arrays.asList(scheduler, nodeShutdown));
            return true;
        }
        catch (TimeoutException e)
        {
            return false;
        }
    }

    @VisibleForTesting
    @Override
    public void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownAndWait(timeout, unit, this);
    }

    public Id nodeId()
    {
        return node.id();
    }

    private static Shutdownable toShutdownable(Node node)
    {
        return new Shutdownable() {
            private volatile boolean isShutdown = false;

            @Override
            public boolean isTerminated()
            {
                // we don't know about terminiated... so settle for shutdown!
                return isShutdown;
            }

            @Override
            public void shutdown()
            {
                isShutdown = true;
                node.shutdown();
            }

            @Override
            public Object shutdownNow()
            {
                // node doesn't offer shutdownNow
                shutdown();
                return null;
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit units)
            {
                // node doesn't offer
                return true;
            }
        };
    }
}
