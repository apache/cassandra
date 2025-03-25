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
package org.apache.cassandra.replication;

import java.util.function.IntSupplier;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.replication.CoordinatorLog.CoordinatorLogPrimary;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.jctools.maps.NonBlockingHashMapLong;

public class Shard
{
    private final String keyspace;
    private final Range<Token> tokenRange;
    private final int localHostId;
    private final Participants participants;
    private final Epoch sinceEpoch;
    private final NonBlockingHashMapLong<CoordinatorLog> logs;
    // TODO (expected): add support for log rotation
    private final CoordinatorLogPrimary currentLocalLog;

    Shard(String keyspace, Range<Token> tokenRange, int localHostId, Participants participants, Epoch sinceEpoch, IntSupplier logIdProvider)
    {
        this.keyspace = keyspace;
        this.tokenRange = tokenRange;
        this.localHostId = localHostId;
        this.participants = participants;
        this.sinceEpoch = sinceEpoch;
        this.logs = new NonBlockingHashMapLong<>();
        this.currentLocalLog = startNewLog(localHostId, logIdProvider.getAsInt(), participants);
        CoordinatorLogId logId = currentLocalLog.logId;
        Preconditions.checkArgument(!logId.isNone());
        logs.put(logId.asLong(), currentLocalLog);
    }

    MutationId nextId()
    {
        return currentLocalLog.nextId();
    }

    void witnessedRemoteMutation(MutationId mutationId, InetAddressAndPort onHost)
    {
        int onHostId = ClusterMetadata.current().directory.peerId(onHost).id();
        get(mutationId).witnessedRemoteMutation(mutationId, onHostId);
    }

    void startWriting(Mutation mutation)
    {
        get(mutation.id()).startWriting(mutation);
    }

    void finishWriting(Mutation mutation)
    {
        get(mutation.id()).finishWriting(mutation);
    }

    void addSummaryForKey(Token token, boolean includePending, MutationSummary.Builder builder)
    {
        logs.forEach((id, log) -> {
            MutationSummary.CoordinatorSummary.Builder summaryBuilder = builder.builderForLog(log.logId);
            log.collectOffsetsFor(token, builder.tableId, includePending, summaryBuilder.unreconciled, summaryBuilder.reconciled);
        });
    }

    void addSummaryForRange(AbstractBounds<PartitionPosition> range, boolean includePending, MutationSummary.Builder builder)
    {
        logs.forEach((id, log) -> {
            MutationSummary.CoordinatorSummary.Builder summaryBuilder = builder.builderForLog(log.logId);
            log.collectOffsetsFor(range, builder.tableId, includePending, summaryBuilder.unreconciled, summaryBuilder.reconciled);
        });
    }

    /**
     * Creates a new coordinator log for this host. Primarily on Shard init (node startup or topology change).
     * Also on keyspace creation.
     */
    private static CoordinatorLog.CoordinatorLogPrimary startNewLog(int localHostId, int hostLogId, Participants participants)
    {
        CoordinatorLogId logId = new CoordinatorLogId(localHostId, hostLogId);
        return new CoordinatorLog.CoordinatorLogPrimary(localHostId, logId, participants);
    }

    private CoordinatorLog get(MutationId mutationId)
    {
        Preconditions.checkArgument(!mutationId.isNone());
        return get(mutationId.logId());
    }

    private CoordinatorLog get(long logId)
    {
        CoordinatorLog log = logs.get(logId);
        return log != null
             ? log : logs.computeIfAbsent(logId, ignore -> CoordinatorLog.create(localHostId, new CoordinatorLogId(logId), participants));
    }
}
