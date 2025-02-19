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

import javax.annotation.Nullable;

import accord.primitives.TxnId;
import org.apache.cassandra.config.AccordSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.StringRetryStrategy;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.RetryStrategy;
import org.apache.cassandra.service.TimeoutStrategy;

import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.accordReadMetrics;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.accordWriteMetrics;
import static org.apache.cassandra.service.TimeoutStrategy.LatencySourceFactory.none;
import static org.apache.cassandra.service.TimeoutStrategy.LatencySourceFactory.of;
import static org.apache.cassandra.service.TimeoutStrategy.LatencySourceFactory.rw;

public class AccordWaitStrategies
{
    static TimeoutStrategy slowTxnPreaccept, slowSyncPointPreaccept, slowRead;
    static TimeoutStrategy expireTxn, expireSyncPoint, expireDurability, expireEpochWait;
    static TimeoutStrategy fetchTxn, fetchSyncPoint;
    static RetryStrategy recoverTxn, recoverSyncPoint, retrySyncPoint, retryDurability, retryBootstrap;
    static RetryStrategy retryFetchMinEpoch, retryFetchTopology;

    public static @Nullable TimeoutStrategy slowRead(@Nullable TxnId txnId)
    {
        if (txnId != null && txnId.isSyncPoint())
            return null;
        return slowRead;
    }

    public static TimeoutStrategy expire(@Nullable TxnId txnId, Verb verb)
    {
        if (txnId == null || !txnId.isSyncPoint())
            return expireTxn;
        return verb == Verb.ACCORD_WAIT_UNTIL_APPLIED_REQ ? expireDurability : expireSyncPoint;
    }

    public static TimeoutStrategy fetch(@Nullable TxnId txnId)
    {
        if (txnId == null || !txnId.isSyncPoint())
            return fetchTxn;
        return fetchSyncPoint;
    }

    public static TimeoutStrategy slowPreaccept(TxnId txnId)
    {
        if (txnId.isSyncPoint())
            return expireSyncPoint;
        return slowTxnPreaccept;
    }

    public static RetryStrategy retryFetchWatermarks()
    {
        return retryFetchMinEpoch;
    }

    public static RetryStrategy retryFetchTopology()
    {
        return retryFetchTopology;
    }

    static
    {
        AccordSpec config = DatabaseDescriptor.getAccord();
        setSlowRead(config.slow_read);
        setSlowTxnPreaccept(config.slow_txn_preaccept);
        setSlowSyncPointPreaccept(config.slow_syncpoint_preaccept);
        setExpireTxn(config.expire_txn);
        setExpireSyncPoint(config.expire_syncpoint);
        setExpireDurability(config.expire_durability);
        setExpireEpochWait(config.expire_epoch_wait);
        setFetchTxn(config.fetch_txn);
        setFetchSyncPoint(config.fetch_syncpoint);
        setRecoverTxn(config.recover_txn);
        setRecoverSyncPoint(config.recover_syncpoint);
        setRetrySyncPoint(config.retry_syncpoint);
        setRetryDurability(config.retry_durability);
        setRetryBootstrap(config.retry_bootstrap);
        setRetryFetchMinEpoch(config.retry_fetch_min_epoch);
        setRetryFetchTopology(config.retry_fetch_topology);
    }

    public static void setSlowRead(String spec)
    {
        slowRead = TimeoutStrategy.parse(spec, of(accordReadMetrics));
    }

    public static void setSlowTxnPreaccept(String spec)
    {
        slowTxnPreaccept = TimeoutStrategy.parse(spec, rw(accordReadMetrics, accordWriteMetrics));
    }

    public static void setSlowSyncPointPreaccept(String spec)
    {
        slowSyncPointPreaccept = TimeoutStrategy.parse(spec, none());
    }

    public static void setExpireTxn(String spec)
    {
        expireTxn = TimeoutStrategy.parse(spec, rw(accordReadMetrics, accordWriteMetrics));
    }

    public static void setExpireSyncPoint(String spec)
    {
        expireSyncPoint = TimeoutStrategy.parse(spec, none());
    }

    public static void setExpireDurability(String spec)
    {
        expireDurability = TimeoutStrategy.parse(spec, none());
    }

    public static void setExpireEpochWait(String spec)
    {
        expireEpochWait = TimeoutStrategy.parse(spec, none());
    }

    public static void setFetchTxn(String spec)
    {
        fetchTxn = TimeoutStrategy.parse(spec, rw(accordReadMetrics, accordWriteMetrics));
    }

    public static void setFetchSyncPoint(String spec)
    {
        fetchSyncPoint = TimeoutStrategy.parse(spec, none());
    }

    public static void setRecoverTxn(String spec)
    {
        recoverTxn = RetryStrategy.parse(spec, rw(accordReadMetrics, accordWriteMetrics));
    }

    public static void setRecoverSyncPoint(StringRetryStrategy spec)
    {
        recoverSyncPoint = spec.retry();
    }

    public static void setRetrySyncPoint(StringRetryStrategy spec)
    {
        retrySyncPoint = spec.retry();
    }

    public static void setRetryDurability(StringRetryStrategy spec)
    {
        retryDurability = spec.retry();
    }

    public static void setRetryBootstrap(StringRetryStrategy spec)
    {
        retryBootstrap = spec.retry();
    }

    public static void setRetryFetchMinEpoch(StringRetryStrategy spec)
    {
        retryFetchMinEpoch = spec.retry();
    }

    public static void setRetryFetchTopology(StringRetryStrategy spec)
    {
        retryFetchTopology = spec.retry();
    }

    static RetryStrategy recover(TxnId txnId)
    {
        if (txnId.is(ExclusiveSyncPoint))
            return recoverSyncPoint;
        return recoverTxn;
    }
}
