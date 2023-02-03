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
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Result;
import accord.coordinate.Preempted;
import accord.coordinate.Timeout;
import accord.impl.SimpleProgressLog;
import accord.impl.SizeOfIntersectionSorter;
import accord.local.Node;
import accord.local.ShardDistributor.EvenSplit;
import accord.messages.Request;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.TopologyManager;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.concurrent.Shutdownable;
import accord.utils.async.AsyncResult;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.metrics.AccordClientRequestMetrics;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.KeyspaceSplitter;
import org.apache.cassandra.service.accord.api.AccordScheduler;
import org.apache.cassandra.service.accord.exceptions.ReadPreemptedException;
import org.apache.cassandra.service.accord.exceptions.WritePreemptedException;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.config.DatabaseDescriptor.getConcurrentAccordOps;
import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class AccordService implements IAccordService, Shutdownable
{
    private static final Logger logger = LoggerFactory.getLogger(AccordService.class);

    public static final AccordClientRequestMetrics readMetrics = new AccordClientRequestMetrics("AccordRead");
    public static final AccordClientRequestMetrics writeMetrics = new AccordClientRequestMetrics("AccordWrite");

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
        public TxnData coordinate(Txn txn, ConsistencyLevel consistencyLevel)
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
                             new Random(),
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

    public static long nowInMicros()
    {
        return TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis());
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
    public TxnData coordinate(Txn txn, ConsistencyLevel consistencyLevel)
    {
        AccordClientRequestMetrics metrics = txn.isWrite() ? writeMetrics : readMetrics;
        TxnId txnId = null;
        final long startNanos = nanoTime();
        try
        {
            metrics.keySize.update(txn.keys().size());
            txnId = node.nextTxnId(txn.kind(), txn.keys().domain());
            AsyncResult<Result> asyncResult = node.coordinate(txnId, txn);
            Result result = AsyncChains.getBlocking(asyncResult, DatabaseDescriptor.getTransactionTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            return (TxnData) result;
        }
        catch (ExecutionException e)
        {
            Throwable cause = e.getCause();
            if (cause instanceof Timeout)
            {
                metrics.timeouts.mark();
                throw throwTimeout(txnId, txn, consistencyLevel);
            }
            if (cause instanceof Preempted)
            {
                metrics.preempts.mark();
                //TODO need to improve
                // Coordinator "could" query the accord state to see whats going on but that doesn't exist yet.
                // Protocol also doesn't have a way to denote "unknown" outcome, so using a timeout as the closest match
                throw throwPreempted(txnId, txn, consistencyLevel);
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
            throw throwTimeout(txnId, txn, consistencyLevel);
        }
        finally
        {
            metrics.addNano(nanoTime() - startNanos);
        }
    }

    private static RuntimeException throwTimeout(TxnId txnId, Txn txn, ConsistencyLevel consistencyLevel)
    {
        throw txn.isWrite() ? new WriteTimeoutException(WriteType.CAS, consistencyLevel, 0, 0, txnId.toString())
                            : new ReadTimeoutException(consistencyLevel, 0, 0, false, txnId.toString());
    }

    private static RuntimeException throwPreempted(TxnId txnId, Txn txn, ConsistencyLevel consistencyLevel)
    {
        throw txn.isWrite() ? new WritePreemptedException(WriteType.CAS, consistencyLevel, 0, 0, txnId.toString())
                            : new ReadPreemptedException(consistencyLevel, 0, 0, false, txnId.toString());
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
