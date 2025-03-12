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

import com.google.common.collect.Sets;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.replication.simple.SimpleMutationSummary;
import org.apache.cassandra.schema.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Hook for preventing races between reads and log write / memtable write.
 *
 * The write path applies mutations to the log, then applies them to the memtable, and this is not synchronized
 * with the read path. This means it's possible for a logged data read to get a mutation summary from the log
 * and have it include a mutation id that hasn't yet been applied to the memtable, and return a data response
 * without that mutation. This would cause a read to return with missing data.
 *
 * To prevent that, we keep track of writes that may have raced with a read, and add them to the data response
 * if they're included in the mutation summary
 */
public class PendingWrites
{

    public class ListeningPendingRead implements PendingRead
    {
        private final ReadCommand command;
        private final Map<MutationId, Mutation> pendingWrites = new ConcurrentHashMap<>();

        public ListeningPendingRead(ReadCommand command)
        {
            this.command = command;
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
            SimpleMutationSummary mutationSummary = (SimpleMutationSummary) summary;
            if (pendingWrites.isEmpty() || mutationSummary.isEmpty())
                return iterator;

            List<Mutation> augmentingMutations = new ArrayList<>(pendingWrites.size());
            for (Mutation mutation : pendingWrites.values())
            {
                if (mutationSummary.allIds.contains(mutation.id()))
                    augmentingMutations.add(mutation);
            }
            return command.augmentResultWithMutations(iterator, augmentingMutations);
        }
    }

    private final Map<MutationId, Mutation> pendingMutations = new ConcurrentHashMap<MutationId, Mutation>();
    private final Set<ListeningPendingRead> pendingReads = Sets.newConcurrentHashSet();

    public PendingWrite startWrite(Mutation mutation)
    {
        if (mutation.id().isNone())
            return PendingWrite.NOOP;

        pendingMutations.put(mutation.id(), mutation);
        pendingReads.forEach(read -> read.onNewWrite(mutation));

        return new PendingWrite()
        {
            @Override
            public void close()
            {
                pendingMutations.remove(mutation.id());
            }
        };
    }

    public PendingRead startRead(ReadCommand command)
    {
        if (!Schema.instance.getKeyspaceMetadata(command.metadata().keyspace).useMutationTracking())
            return PendingRead.NOOP;

        ListeningPendingRead pendingRead = new ListeningPendingRead(command);
        pendingReads.add(pendingRead);
        pendingMutations.values().forEach(pendingRead::onNewWrite);

        return pendingRead;
    }
}
