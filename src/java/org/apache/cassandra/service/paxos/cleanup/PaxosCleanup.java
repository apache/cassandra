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
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.Future;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.getCasContentionTimeout;
import static org.apache.cassandra.config.DatabaseDescriptor.getWriteRpcTimeout;

public class PaxosCleanup extends AsyncFuture<Void> implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosCleanup.class);

    private final SharedContext ctx;
    private final Collection<InetAddressAndPort> endpoints;
    private final TableMetadata table;
    private final Collection<Range<Token>> ranges;
    private final boolean skippedReplicas;
    private final Executor executor;

    // references kept for debugging
    private PaxosStartPrepareCleanup startPrepare;
    private PaxosFinishPrepareCleanup finishPrepare;
    private PaxosCleanupSession session;
    private PaxosCleanupComplete complete;

    public PaxosCleanup(SharedContext ctx, Collection<InetAddressAndPort> endpoints, TableMetadata table, Collection<Range<Token>> ranges, boolean skippedReplicas, Executor executor)
    {
        this.ctx = ctx;
        this.endpoints = endpoints;
        this.table = table;
        this.ranges = ranges;
        this.skippedReplicas = skippedReplicas;
        this.executor = executor;
    }

    private <T> void addCallback(Future<T> future, Consumer<T> onComplete)
    {
        future.addCallback(onComplete, this::tryFailure);
    }

    public static PaxosCleanup cleanup(SharedContext ctx, Collection<InetAddressAndPort> endpoints, TableMetadata table, Collection<Range<Token>> ranges, boolean skippedReplicas, Executor executor)
    {
        PaxosCleanup cleanup = new PaxosCleanup(ctx, endpoints, table, ranges, skippedReplicas, executor);
        executor.execute(cleanup);
        return cleanup;
    }

    public void run()
    {
        EndpointState localEpState = ctx.gossiper().getEndpointStateForEndpoint(ctx.broadcastAddressAndPort());
        startPrepare = PaxosStartPrepareCleanup.prepare(ctx, table, endpoints, localEpState, ranges);
        addCallback(startPrepare, this::finishPrepare);
    }

    private void finishPrepare(PaxosCleanupHistory result)
    {
        ctx.nonPeriodicTasks().schedule(() -> {
            boolean isUrgent = Schema.instance.getKeyspaceMetadata(table.keyspace).params.replication.isMeta();
            finishPrepare = PaxosFinishPrepareCleanup.finish(ctx, endpoints, isUrgent, result);
            addCallback(finishPrepare, (v) -> startSession(result.highBound));
        }, Math.min(getCasContentionTimeout(MILLISECONDS), getWriteRpcTimeout(MILLISECONDS)), MILLISECONDS);
    }

    private void startSession(Ballot lowBound)
    {
        session = new PaxosCleanupSession(ctx, endpoints, table.id, ranges);
        addCallback(session, (v) -> finish(lowBound));
        executor.execute(session);
    }

    private void finish(Ballot lowBound)
    {
        complete = new PaxosCleanupComplete(ctx, endpoints, table.id, ranges, lowBound, skippedReplicas);
        addCallback(complete, this::trySuccess);
        executor.execute(complete);
    }

    private static boolean isOutOfRange(SharedContext ctx, String ksName, Collection<Range<Token>> repairRanges)
    {
        Keyspace keyspace = Keyspace.open(ksName);
        Collection<Range<Token>> localRanges = Range.normalize(ClusterMetadata.current().writeRanges(keyspace.getMetadata(), ctx.broadcastAddressAndPort()));

        for (Range<Token> repairRange : Range.normalize(repairRanges))
        {
            if (!Iterables.any(localRanges, localRange -> localRange.contains(repairRange)))
                return true;
        }
        return false;
    }

    static boolean isInRangeAndShouldProcess(SharedContext ctx, Collection<Range<Token>> ranges, TableId tableId)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(tableId);

        Keyspace keyspace = Keyspace.open(metadata.keyspace);
        Preconditions.checkNotNull(keyspace);

        if (!isOutOfRange(ctx, metadata.keyspace, ranges))
            return true;

        logger.warn("Out of range PaxosCleanup request for {}: {}", metadata, ranges);
        return false;
    }
}
