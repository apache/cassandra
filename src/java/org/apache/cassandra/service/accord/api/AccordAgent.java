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

package org.apache.cassandra.service.accord.api;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.EventsListener;
import accord.api.Result;
import accord.local.Command;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.metrics.AccordMetrics;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.getReadRpcTimeout;

// TODO (expected): merge with AccordService
public class AccordAgent implements Agent
{
    private static final Logger logger = LoggerFactory.getLogger(AccordAgent.class);

    // TODO (required): this should be configurable and have exponential back-off, escaping to operator input past a certain number of retries
    private long retryBootstrapDelayMicros = SECONDS.toMicros(1L);

    public void setRetryBootstrapDelay(long delay, TimeUnit units)
    {
        retryBootstrapDelayMicros = units.toMicros(delay);
    }

    @Override
    public void onRecover(Node node, Result success, Throwable fail)
    {
        // TODO: this
    }

    @Override
    public void onInconsistentTimestamp(Command command, Timestamp prev, Timestamp next)
    {
        // TODO: this
        AssertionError error = new AssertionError("Inconsistent execution timestamp detected for txnId " + command.txnId() + ": " + prev + " != " + next);
        onUncaughtException(error);
        throw error;
    }

    @Override
    public void onFailedBootstrap(String phase, Ranges ranges, Runnable retry, Throwable failure)
    {
        logger.error("Failed bootstrap at {} for {}", phase, ranges, failure);
        AccordService.instance().scheduler().once(retry, retryBootstrapDelayMicros, MICROSECONDS);
    }

    @Override
    public void onUncaughtException(Throwable t)
    {
        // TODO: this
        JVMStabilityInspector.uncaughtException(Thread.currentThread(), t);
    }

    @Override
    public void onHandledException(Throwable t)
    {
        // TODO: this
    }

    @Override
    public boolean isExpired(TxnId initiated, long now)
    {
        // TODO: should distinguish between reads and writes
        return now - initiated.hlc() > getReadRpcTimeout(MICROSECONDS);
    }

    @Override
    public Txn emptyTxn(Txn.Kind kind, Seekables<?, ?> keysOrRanges)
    {
        return new Txn.InMemory(kind, keysOrRanges, TxnRead.EMPTY, TxnQuery.ALL, null);
    }

    @Override
    public EventsListener metricsEventsListener()
    {
        return AccordMetrics.Listener.instance;
    }
}
