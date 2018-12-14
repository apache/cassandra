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

package org.apache.cassandra.service.throttler;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestThrottledException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.metrics.KeyspaceMetrics;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

/**
 * KeyspaceBasedRequestThrottler throttles different read/write requests based on limits defined in a
 * Cassandra table. See {code CassandraKeyspaceLimitProvider} for details.
 *
 * In order to use this request throttler you need to add the following configuration in cassandra.yaml:
 *
 * request_throttler:
 *   - class_name: org.apache.cassandra.service.throttler.KeyspaceBasedRequestThrottler
 *     parameters:
 *       - fetch_limits_period_in_sec: "10"
 *         replenish_limits_period_in_sec: "1"
 */
public class KeyspaceBasedRequestThrottler implements IRequestThrottler
{

    private static final Logger logger = LoggerFactory.getLogger(KeyspaceBasedRequestThrottler.class);

    // This string contains hyphens so that it cannot conflict with a valid keyspace name.
    public static final String DEFAULT_PER_KEYSPACE_LIMIT_KEY = "default-per-keyspace-limit";

    private Map<String, String> params;
    private Map<String, KeyspaceLimits> fetchedKeyspaceLimits;
    private Map<String, KeyspaceLimits> currentKeyspaceLimits;
    private ScheduledExecutorPlus replenishExecutor;
    private ScheduledExecutorPlus fetchExecutor;
    private CassandraKeyspaceLimitProvider limitProvider;
    private ReadWriteLock fetchedKeyspaceLimitsLock;

    public KeyspaceBasedRequestThrottler(Map<String, String> params)
    {
        this.params = params;
        currentKeyspaceLimits = new NonBlockingHashMap<>();
        fetchedKeyspaceLimits = new NonBlockingHashMap<>();
        replenishExecutor = executorFactory().scheduled(false, "RequestThrottler-Replenish", Thread.NORM_PRIORITY);
        fetchExecutor = executorFactory().scheduled(false, "RequestThrottler-Fetch", Thread.NORM_PRIORITY);
        limitProvider = new CassandraKeyspaceLimitProvider();
        fetchedKeyspaceLimitsLock = new ReentrantReadWriteLock();
    }

    /**
     * Performs set up for the request throttler.
     *
     * 1. Setups up the limit provider
     * 2. Parses configuration parameters
     * 3. Starts thread1 which periodically fetches the limits from the provider
     * 4. Starts thread2 which periodically replenishes the limits for the current epoch.
     */
    public void setup()
    {
        limitProvider.setup();

        String fetchLimitsPeriod = params.get("fetch_limits_period_in_sec");
        if (fetchLimitsPeriod == null)
        {
            throw new ConfigurationException("Parameter 'fetch_limits_period_in_sec' cannot be blank");
        }
        int paramFetchLimitsPeriodInSec = Integer.parseInt(fetchLimitsPeriod);

        String replenishLimitsPeriod = params.get("replenish_limits_period_in_sec");
        if (replenishLimitsPeriod == null)
        {
            throw new ConfigurationException("Parameter 'replenish_limits_period_in_sec' cannot be blank");
        }
        int paramReplenishLimitsPeriodInSec = Integer.parseInt(replenishLimitsPeriod);

        replenishExecutor.scheduleAtFixedRate(() -> {
            replenishLocalLimits();
        }, 0, paramReplenishLimitsPeriodInSec, TimeUnit.SECONDS);
        fetchExecutor.scheduleAtFixedRate(() -> {
            fetchLimitsFromProvider();
        }, 0, paramFetchLimitsPeriodInSec, TimeUnit.SECONDS);
    }

    public CassandraKeyspaceLimitProvider getLimitProvider()
    {
        return limitProvider;
    }

    @VisibleForTesting
    Map<String, KeyspaceLimits> getCurrentKeyspaceLimits()
    {
        return currentKeyspaceLimits;
    }

    @VisibleForTesting
    Map<String, KeyspaceLimits> getFetchedKeyspaceLimits()
    {
        return fetchedKeyspaceLimits;
    }

    /**
     * Fetches the limits from the provider and updates the fetchedKeyspaceLimits.
     * Note: this function is called periodically and modifies the fetchedKeyspaceLimits, while another thread reads it.
     * So we acquire a write lock to avoid race conditions.
     */
    @VisibleForTesting
    void fetchLimitsFromProvider()
    {
        try
        {
            Map<String, KeyspaceLimits> map = limitProvider.getKeyspaceLimits();
            fetchedKeyspaceLimitsLock.writeLock().lock();
            // Update the existing limits only if there were no problems fetching the limits.
            // This makes the throttler resilient, in case there are problems upstream.
            if (map != null)
            {
                fetchedKeyspaceLimits.clear();
                // First, apply default per keyspace limits if they are set, for non system keyspaces.
                if (map.containsKey(DEFAULT_PER_KEYSPACE_LIMIT_KEY))
                {
                    for (String keyspace : StorageService.instance.getNonSystemKeyspaces())
                    {
                        if (!keyspace.startsWith("system_"))
                        {
                            fetchedKeyspaceLimits.put(keyspace, map.get(DEFAULT_PER_KEYSPACE_LIMIT_KEY));
                        }
                    }
                }
                // Then apply keyspace specific limits.
                fetchedKeyspaceLimits.putAll(map);

                logger.info("Fetched keyspace limits from provider: " + fetchedKeyspaceLimits);
            }
            else
            {
                logger.error("Got null limits from provider, not updating existing limits");
            }
        }
        catch (RequestValidationException | RequestExecutionException t)
        {
            logger.error("Caught exception while fetching limits: " + t.getMessage(), t);
        }
        finally
        {
            fetchedKeyspaceLimitsLock.writeLock().unlock();
        }
    }

    /**
     * Replaces the currentKeyspaceLimits with the ones fetched from the provider.
     * Note: this function is called periodically and reads the fetchedKeyspaceLimits, while another thread modifies it.
     * So we acquire a read lock to avoid race conditions.
     */
    @VisibleForTesting
    void replenishLocalLimits()
    {
        try {
            fetchedKeyspaceLimitsLock.readLock().lock();
            for (Map.Entry<String, KeyspaceLimits> entry : fetchedKeyspaceLimits.entrySet())
            {
                KeyspaceLimits limit = currentKeyspaceLimits.get(entry.getKey());
                if (limit == null)
                {
                    // New entry appeared in the fetched limits, add it to current.
                    currentKeyspaceLimits.put(entry.getKey(), entry.getValue());
                }
                else
                {
                    // Existing entry changed, update it.
                    limit.set(entry.getValue());
                }
            }
            // Existing entry has been deleted in the fetched limits, remove it.
            for (String keyspaceLimit : currentKeyspaceLimits.keySet()) {
                if (!fetchedKeyspaceLimits.containsKey(keyspaceLimit)) {
                    currentKeyspaceLimits.remove(keyspaceLimit);
                }
            }

            logger.debug("Replenished keyspace limits from provider: " + currentKeyspaceLimits);
        }
        finally
        {
            fetchedKeyspaceLimitsLock.readLock().unlock();
        }
    }

    @Override
    public void maybeThrottleRead(ReadCommand command, ConsistencyLevel consistencyLevel) throws RequestThrottledException
    {
        final String keyspaceName = command.metadata().keyspace;
        KeyspaceLimits keyspaceLimits = currentKeyspaceLimits.get(keyspaceName);
        if (keyspaceLimits == null)
        {
            return;
        }
        logger.debug("Maybe throttle read, keyspace name: " + keyspaceName);

        KeyspaceMetrics metrics = Keyspace.open(keyspaceName).metric;
        if (consistencyLevel != null && consistencyLevel.isSerialConsistency())
        {
            int limit = keyspaceLimits.serialReadLimit.decrementAndGet();
            if (limit < 0)
            {
                final String msg = "Throttling serial read for keyspace " + keyspaceName + ": over limit by " + (-limit);
                metrics.serialReadThrottles.inc();
                logger.info(msg);
                throw new RequestThrottledException(msg);
            }
        }

        if (command instanceof SinglePartitionReadCommand)
        {
            int limit = keyspaceLimits.singleReadLimit.decrementAndGet();
            if (limit < 0)
            {
                final String msg = "Throttling single read for keyspace " + keyspaceName + ": over limit by " + (-limit);
                logger.info(msg);
                metrics.singleReadThrottles.inc();
                throw new RequestThrottledException(msg);
            }
        }
        else if (command instanceof PartitionRangeReadCommand)
        {
            int limit = keyspaceLimits.rangeReadLimit.decrementAndGet();
            if (limit < 0)
            {
                final String msg = "Throttling range read for keyspace " + keyspaceName + ": over limit by " + (-limit);
                logger.info(msg);
                metrics.rangeReadThrottles.inc();
                throw new RequestThrottledException(msg);
            }
        }
        else
        {
            logger.warn("Unknown command type: " + command);
        }
    }

    @Override
    public void maybeThrottleMutation(IMutation mutation, ConsistencyLevel consistencyLevel) throws RequestThrottledException
    {
        final String keyspaceName = mutation.getKeyspaceName();
        KeyspaceLimits keyspaceLimits = currentKeyspaceLimits.get(keyspaceName);
        if (keyspaceLimits == null)
        {
            return;
        }

        KeyspaceMetrics metrics = Keyspace.open(keyspaceName).metric;
        logger.debug("Maybe throttle mutation, Keyspace name :" + keyspaceName);

        if (consistencyLevel.isSerialConsistency())
        {
            int limit = keyspaceLimits.serialMutationLimit.decrementAndGet();
            if (limit < 0)
            {
                final String msg = "Throttling serial mutation for keyspace " + keyspaceName + ": over limit by " + (-limit);
                logger.info(msg);
                metrics.serialMutationThrottles.inc();
                throw new RequestThrottledException(msg);
            }
        }

        int limit = keyspaceLimits.singleMutationLimit.decrementAndGet();
        if (limit < 0)
        {
            final String msg = "Throttling single mutation for keyspace " + keyspaceName + ": over limit by " + (-limit);
            logger.info(msg);
            metrics.serialMutationThrottles.inc();
            throw new RequestThrottledException(msg);
        }
    }
}
