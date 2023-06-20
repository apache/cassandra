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

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.AbstractPaxosRepair;
import org.apache.cassandra.service.paxos.PaxosRepair;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.service.paxos.uncommitted.UncommittedPaxosKey;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

import static org.apache.cassandra.service.paxos.cleanup.PaxosCleanupSession.TIMEOUT_NANOS;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class PaxosCleanupLocalCoordinator extends AsyncFuture<PaxosCleanupResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosCleanupLocalCoordinator.class);
    private static final UUID INTERNAL_SESSION = new UUID(0, 0);

    private final UUID session;
    private final TableId tableId;
    private final TableMetadata table;
    private final Collection<Range<Token>> ranges;
    private final CloseableIterator<UncommittedPaxosKey> uncommittedIter;
    private int count = 0;
    private final long deadline;

    private final Map<DecoratedKey, AbstractPaxosRepair> inflight = new ConcurrentHashMap<>();
    private final PaxosTableRepairs tableRepairs;

    private PaxosCleanupLocalCoordinator(UUID session, TableId tableId, Collection<Range<Token>> ranges, CloseableIterator<UncommittedPaxosKey> uncommittedIter)
    {
        this.session = session;
        this.tableId = tableId;
        this.table = Schema.instance.getTableMetadata(tableId);
        this.ranges = ranges;
        this.uncommittedIter = uncommittedIter;
        this.tableRepairs = PaxosTableRepairs.getForTable(tableId);
        this.deadline = TIMEOUT_NANOS + nanoTime();
    }

    public synchronized void start()
    {
        if (table == null)
        {
            fail("Unknown tableId: " + tableId);
            return;
        }

        if (!PaxosRepair.validatePeerCompatibility(table, ranges))
        {
            fail("Unsupported peer versions for " + tableId + ' ' + ranges.toString());
            return;
        }

        logger.info("Completing uncommitted paxos instances for {} on ranges {} for session {}", table, ranges, session);

        scheduleKeyRepairsOrFinish();
    }

    public static PaxosCleanupLocalCoordinator create(PaxosCleanupRequest request)
    {
        CloseableIterator<UncommittedPaxosKey> iterator = PaxosState.uncommittedTracker().uncommittedKeyIterator(request.tableId, request.ranges);
        return new PaxosCleanupLocalCoordinator(request.session, request.tableId, request.ranges, iterator);
    }

    public static PaxosCleanupLocalCoordinator createForAutoRepair(TableId tableId, Collection<Range<Token>> ranges)
    {
        CloseableIterator<UncommittedPaxosKey> iterator = PaxosState.uncommittedTracker().uncommittedKeyIterator(tableId, ranges);
        return new PaxosCleanupLocalCoordinator(INTERNAL_SESSION, tableId, ranges, iterator);
    }

    /**
     * Schedule as many key repairs as we can, up to the paralellism limit. If no repairs are scheduled and
     * none are in flight when the iterator is exhausted, the session will be finished
     */
    private void scheduleKeyRepairsOrFinish()
    {
        int parallelism = DatabaseDescriptor.getPaxosRepairParallelism();
        Preconditions.checkArgument(parallelism > 0);
        if (inflight.size() < parallelism)
        {
            if (nanoTime() - deadline >= 0)
            {
                fail("timeout");
                return;
            }

            while (inflight.size() < parallelism && uncommittedIter.hasNext())
                repairKey(uncommittedIter.next());

        }

        if (inflight.isEmpty())
            finish();
    }

    private boolean repairKey(UncommittedPaxosKey uncommitted)
    {
        logger.trace("repairing {}", uncommitted);
        Preconditions.checkState(!inflight.containsKey(uncommitted.getKey()));
        ConsistencyLevel consistency = uncommitted.getConsistencyLevel();

        // we don't know the consistency of this operation, presumably because it originated
        // before we started tracking paxos cl, so we don't attempt to repair it
        if (consistency == null)
            return false;

        inflight.put(uncommitted.getKey(), tableRepairs.startOrGetOrQueue(uncommitted.getKey(), uncommitted.ballot(), uncommitted.getConsistencyLevel(), table, result -> {
            if (result.wasSuccessful())
                onKeyFinish(uncommitted.getKey());
            else
                onKeyFailure(result.toString());
        }));
        return true;
    }

    private synchronized void onKeyFinish(DecoratedKey key)
    {
        if (!inflight.containsKey(key))
            return;
        logger.trace("finished repairing {}", key);
        inflight.remove(key);
        count++;

        scheduleKeyRepairsOrFinish();
    }

    private void complete(PaxosCleanupResponse response)
    {
        uncommittedIter.close();
        trySuccess(response);
    }

    private void onKeyFailure(String reason)
    {
        // not synchronized to avoid deadlock with callback we register on start
        inflight.values().forEach(AbstractPaxosRepair::cancel);
        fail(reason);
    }

    private synchronized void fail(String reason)
    {
        logger.info("Failing paxos cleanup session {} for {} on ranges {}. Reason: {}", session, table, ranges, reason);
        complete(PaxosCleanupResponse.failed(session, reason));
    }

    private void finish()
    {
        logger.info("Completed {} uncommitted paxos instances for {} on ranges {} for session {}", count, table, ranges, session);
        complete(PaxosCleanupResponse.success(session));
    }
}
