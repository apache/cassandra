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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;

// TODO (consider): instrument for leaks / failing to close detection
public class ListeningPendingRead implements MutationTrackingService.PendingRead
{
    private final ReadCommand command;
    private final Set<ListeningPendingRead> pendingReads;

    // TODO: do not hold onto mutation objects - extract the relevant fields, or fetch from journal if needed
    private final Map<MutationId, Mutation> pendingWrites = new ConcurrentHashMap<>();

     ListeningPendingRead(ReadCommand command, Set<ListeningPendingRead> pendingReads)
    {
        this.command = command;
        this.pendingReads = pendingReads;
    }

    public void onNewWrite(Mutation mutation)
    {
        if (command.readsMutationContents(mutation))
            pendingWrites.put(mutation.id(), mutation);
    }

    @Override
    public void close()
    {
        pendingReads.remove(this);
    }

    public Set<MutationId> mutationIds()
    {
        return pendingWrites.keySet();
    }

    @Override
    public UnfilteredPartitionIterator augmentResponseWithPendingWrites(UnfilteredPartitionIterator iterator, MutationSummary summary)
    {
        if (pendingWrites.isEmpty() || summary.isEmpty())
            return iterator;

        List<Mutation> augmentingMutations = new ArrayList<>(pendingWrites.size());
        for (Mutation mutation : pendingWrites.values())
            if (summary.contains(mutation.id()))
                augmentingMutations.add(mutation);
        return command.augmentResultWithMutations(iterator, augmentingMutations);
    }
}
