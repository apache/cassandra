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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.coordinate.CoordinationFailed;
import accord.coordinate.Exhausted;
import accord.coordinate.Preempted;
import accord.coordinate.Timeout;
import accord.coordinate.TopologyMismatch;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.TxnId;
import accord.utils.UnhandledEnum;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailureException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.txn.RetryWithNewProtocolResult;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class AccordResult<V> extends AsyncFuture<V> implements BiConsumer<V, Throwable>, IAccordService.IAccordResult<V>
{
    private static final Logger logger = LoggerFactory.getLogger(AccordResult.class);

    final @Nullable TxnId txnId;
    final Seekables<?, ?> keysOrRanges;
    final RequestBookkeeping bookkeeping;
    final long startedAtNanos, deadlineAtNanos;
    final boolean isTxnRequest;

    public AccordResult(@Nullable TxnId txnId, Seekables<?, ?> keysOrRanges, RequestBookkeeping bookkeeping, long startedAtNanos, long deadlineAtNanos, boolean isTxnRequest)
    {
        this.txnId = txnId;
        this.keysOrRanges = keysOrRanges;
        this.bookkeeping = bookkeeping;
        this.startedAtNanos = startedAtNanos;
        this.deadlineAtNanos = deadlineAtNanos;
        this.isTxnRequest = isTxnRequest;
    }

    @Override
    public V awaitAndGet() throws RequestExecutionException
    {
        try
        {
            if (!awaitUntil(deadlineAtNanos))
                accept(null, new Timeout(txnId, null));
        }
        catch (InterruptedException e)
        {
            accept(null, e);
        }

        Throwable fail = fail();
        if (fail != null)
            throw (RequestExecutionException) fail;
        return success();
    }

    /*
     * Interop execution with AccordInteropExecution can throw a bunch of C* read not Accord errors
     * that we want to preserve and make the top level exception here.
     */
    private static Throwable maybeWrappedInRequestFailureException(Timeout timeout)
    {
        Throwable toCheck = timeout;
        do
        {
            if (toCheck instanceof ReadTimeoutException)
            {
                ReadTimeoutException rte = (ReadTimeoutException) toCheck;
                return new ReadTimeoutException(rte.consistency, rte.received, rte.blockFor, rte.dataPresent, timeout);
            }
            else if (toCheck instanceof ReadFailureException)
            {
                ReadFailureException rfe = (ReadFailureException) toCheck;
                return new ReadFailureException(rfe.getMessage(), rfe.consistency, rfe.received, rfe.blockFor, rfe.dataPresent, rfe.failureReasonByEndpoint, timeout);
            }
        } while ((toCheck = toCheck.getCause()) != null);
        return timeout;
    }

    @Override
    public void accept(V success, Throwable fail)
    {
        if (fail != null)
        {
            RequestExecutionException report;
            CoordinationFailed coordinationFailed = findCoordinationFailed(fail);
            TxnId txnId = this.txnId;
            if (coordinationFailed != null)
            {
                if (txnId == null && coordinationFailed.txnId() != null)
                    txnId = coordinationFailed.txnId();

                if (coordinationFailed instanceof Timeout)
                {
                    // Preserve the interop execution created exception if there is one
                    Throwable maybeWrappedInRequestFailureException = maybeWrappedInRequestFailureException((Timeout)coordinationFailed);
                    if (maybeWrappedInRequestFailureException instanceof RequestFailureException)
                    {
                        bookkeeping.newTimeout(txnId, keysOrRanges);
                        report = (RequestFailureException)maybeWrappedInRequestFailureException;
                    }
                    else
                    {
                        report = bookkeeping.newTimeout(txnId, keysOrRanges);
                    }
                }
                else if (coordinationFailed instanceof Preempted)
                {
                    report = bookkeeping.newPreempted(txnId, keysOrRanges);
                }
                else if (coordinationFailed instanceof Exhausted)
                {
                    report = bookkeeping.newExhausted(txnId, keysOrRanges);
                }
                else if (isTxnRequest && coordinationFailed instanceof TopologyMismatch)
                {
                    // Excluding bugs topology mismatch can occur because a table was dropped in between creating the txn
                    // and executing it.
                    // It could also race with the table stopping/starting being managed by Accord.
                    // The caller can retry if the table indeed exists and is managed by Accord.
                    Set<TableId> txnDroppedTables = txnDroppedTables(keysOrRanges);
                    Tracing.trace("Accord returned topology mismatch: " + coordinationFailed.getMessage());
                    logger.debug("Accord returned topology mismatch", coordinationFailed);
                    bookkeeping.markTopologyMismatch();
                    // Throw IRE in case the caller fails to check if the table still exists
                    if (!txnDroppedTables.isEmpty())
                    {
                        Tracing.trace("Accord txn uses dropped tables {}", txnDroppedTables);
                        logger.debug("Accord txn uses dropped tables {}", txnDroppedTables);
                        throw new InvalidRequestException("Accord transaction uses dropped tables");
                    }
                    trySuccess((V) RetryWithNewProtocolResult.instance);
                    return;
                }
                else
                {
                    report = bookkeeping.newFailed(txnId, keysOrRanges);
                }
                    // this case happens when a non-timeout exception is seen, and we are unable to move forward
                if (txnId != null && txnId.isSyncPoint())
                    AccordAgent.onFailedBarrier(txnId, fail);
            }
            else
            {
                logger.error("Unexpected exception", fail);
                JVMStabilityInspector.inspectThrowable(fail);
                report = bookkeeping.newFailed(txnId, keysOrRanges);
            }
            report.addSuppressed(fail);
            tryFailure(report);
        }
        else
        {
            if (success == RetryWithNewProtocolResult.instance)
            {
                bookkeeping.markRetryDifferentSystem();
                Tracing.trace("Got retry different system error from Accord, will retry");
            }
            trySuccess(success);
        }
        bookkeeping.markElapsedNanos(nanoTime() - startedAtNanos);
    }

    @Override
    public boolean awaitUntil(long nanoTimeDeadline) throws InterruptedException
    {
        if (super.awaitUntil(nanoTimeDeadline))
            return true;

        accept(null, new Timeout(null, null));
        return false;
    }

    public Throwable fail()
    {
        return cause();
    }

    public V success()
    {
        return getNow();
    }

    @Override
    public AccordResult<V> addCallback(BiConsumer<? super V, Throwable> callback)
    {
        super.addCallback(callback);
        return this;
    }

    private static CoordinationFailed findCoordinationFailed(Throwable fail)
    {
        while (fail != null)
        {
            if (fail instanceof CoordinationFailed)
                return (CoordinationFailed) fail;
            Throwable next = fail.getCause();
            if (next == fail)
                return null;
            fail = next;
        }
        return null;
    }

    private static Set<TableId> txnDroppedTables(Seekables<?,?> keys)
    {
        Set<TableId> tables = new HashSet<>();
        for (Seekable seekable : keys)
        {
            switch (seekable.domain())
            {
                default: UnhandledEnum.unknown(seekable.domain());
                case Key:
                    tables.add(((PartitionKey) seekable).table());
                    break;
                case Range:
                    tables.add(((TokenRange) seekable).table());
                    break;
            }
        }

        Iterator<TableId> tablesIterator = tables.iterator();
        while (tablesIterator.hasNext())
        {
            TableId table = tablesIterator.next();
            if (Schema.instance.getTableMetadata(table) != null)
                tablesIterator.remove();
        }
        return tables;
    }
}
