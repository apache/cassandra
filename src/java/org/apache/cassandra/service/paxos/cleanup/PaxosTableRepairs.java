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

package org.apache.cassandra.service.paxos.cleanup;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.AbstractPaxosRepair;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.PaxosRepair;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.service.paxos.Commit.isAfter;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Coordinates repairs on a given key to prevent multiple repairs being scheduled for a single key
 */
public class PaxosTableRepairs implements AbstractPaxosRepair.Listener
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosTableRepairs.class);

    static class KeyRepair
    {
        private final DecoratedKey key;

        private final ArrayDeque<AbstractPaxosRepair> queued = new ArrayDeque<>();

        private KeyRepair(DecoratedKey key)
        {
            this.key = key;
        }

        void onFirst(Predicate<AbstractPaxosRepair> predicate, Consumer<AbstractPaxosRepair> consumer, boolean removeBeforeAction)
        {
            while (!queued.isEmpty())
            {
                AbstractPaxosRepair repair = queued.peek();
                if (repair.isComplete())
                {
                    queued.remove();
                    continue;
                }

                if (predicate.test(repair))
                {
                    if (removeBeforeAction)
                        queued.remove();
                    consumer.accept(repair);
                }
                return;
            }
        }

        void clear()
        {
            while (!queued.isEmpty())
                queued.remove().cancelUnexceptionally();
        }

        AbstractPaxosRepair startOrGetOrQueue(PaxosTableRepairs tableRepairs, DecoratedKey key, Ballot incompleteBallot, ConsistencyLevel consistency, TableMetadata table, Consumer<PaxosRepair.Result> onComplete)
        {
            Preconditions.checkArgument(this.key.equals(key));

            if (!queued.isEmpty() && !isAfter(incompleteBallot, queued.peekLast().incompleteBallot()))
            {
                queued.peekLast().addListener(onComplete);
                return queued.peekLast();
            }

            AbstractPaxosRepair repair = tableRepairs.createRepair(key, incompleteBallot, consistency, table);

            repair.addListener(tableRepairs);
            repair.addListener(onComplete);

            queued.add(repair);
            maybeScheduleNext();
            return repair;
        }

        @VisibleForTesting
        AbstractPaxosRepair activeRepair()
        {
            return queued.peek();
        }

        @VisibleForTesting
        boolean queueContains(AbstractPaxosRepair repair)
        {
            return queued.contains(repair);
        }

        void maybeScheduleNext()
        {
            onFirst(repair -> !repair.isStarted(), AbstractPaxosRepair::start, false);
        }

        void complete(AbstractPaxosRepair repair)
        {
            queued.remove(repair);
            maybeScheduleNext();
        }

        int pending()
        {
            return queued.size();
        }

        boolean isEmpty()
        {
            return queued.isEmpty();
        }
    }

    private final Map<DecoratedKey, KeyRepair> keyRepairs = new ConcurrentHashMap<>();

    @VisibleForTesting
    KeyRepair getKeyRepairUnsafe(DecoratedKey key)
    {
        return keyRepairs.get(key);
    }

    synchronized AbstractPaxosRepair startOrGetOrQueue(DecoratedKey key, Ballot incompleteBallot, ConsistencyLevel consistency, TableMetadata table, Consumer<PaxosRepair.Result> onComplete)
    {
        KeyRepair keyRepair = keyRepairs.computeIfAbsent(key, KeyRepair::new);
        return keyRepair.startOrGetOrQueue(this, key, incompleteBallot, consistency, table, onComplete);
    }

    public synchronized void onComplete(AbstractPaxosRepair repair, AbstractPaxosRepair.Result result)
    {
        KeyRepair keyRepair = keyRepairs.get(repair.partitionKey());
        if (keyRepair == null)
        {
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES,
                             "onComplete callback fired for nonexistant KeyRepair");
            return;
        }

        keyRepair.complete(repair);
        if (keyRepair.queueContains(repair))
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES,
                             "repair not removed after call to onComplete");

        if (keyRepair.isEmpty())
            keyRepairs.remove(repair.partitionKey());
    }

    synchronized void evictHungRepairs(long activeSinceNanos)
    {
        Predicate<AbstractPaxosRepair> timeoutPredicate = repair -> repair.startedNanos() - activeSinceNanos < 0;
        for (KeyRepair repair : keyRepairs.values())
        {
            if (repair.isEmpty())
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES,
                                 "inactive KeyRepair found, this means post-repair cleanup/schedule isn't working properly");
            repair.onFirst(timeoutPredicate, r -> {
                logger.warn("cancelling timed out paxos repair: {}", r);
                r.cancelUnexceptionally();
            }, true);
            repair.maybeScheduleNext();
            if (repair.isEmpty())
                keyRepairs.remove(repair.key);
        }
    }

    synchronized void clear()
    {
        for (KeyRepair repair : keyRepairs.values())
            repair.clear();
        keyRepairs.clear();
    }

    @VisibleForTesting
    synchronized boolean hasActiveRepairs(DecoratedKey key)
    {
        return keyRepairs.containsKey(key);
    }

    AbstractPaxosRepair createRepair(DecoratedKey key, Ballot incompleteBallot, ConsistencyLevel consistency, TableMetadata table)
    {
        return PaxosRepair.create(consistency, key, incompleteBallot, table);
    }

    private static final ConcurrentMap<TableId, PaxosTableRepairs> tableRepairsMap = new ConcurrentHashMap<>();

    static PaxosTableRepairs getForTable(TableId tableId)
    {
        return tableRepairsMap.computeIfAbsent(tableId, k -> new PaxosTableRepairs());
    }

    public static void evictHungRepairs()
    {
        long deadline = nanoTime() - TimeUnit.MINUTES.toNanos(5);
        for (PaxosTableRepairs repairs : tableRepairsMap.values())
            repairs.evictHungRepairs(deadline);
    }

    public static void clearRepairs()
    {
        for (PaxosTableRepairs repairs : tableRepairsMap.values())
            repairs.clear();
    }

}
