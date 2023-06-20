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

package org.apache.cassandra.db;

import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class CassandraKeyspaceWriteHandler implements KeyspaceWriteHandler
{
    private final Keyspace keyspace;

    public CassandraKeyspaceWriteHandler(Keyspace keyspace)
    {
        this.keyspace = keyspace;
    }

    @Override
    public WriteContext beginWrite(Mutation mutation, boolean makeDurable) throws RequestExecutionException
    {
        OpOrder.Group group = null;
        try
        {
            group = Keyspace.writeOrder.start();

            // write the mutation to the commitlog and memtables
            CommitLogPosition position = null;
            if (makeDurable)
            {
                position = addToCommitLog(mutation);
            }
            return new CassandraWriteContext(group, position);
        }
        catch (Throwable t)
        {
            if (group != null)
            {
                group.close();
            }
            throw t;
        }
    }

    private CommitLogPosition addToCommitLog(Mutation mutation)
    {
        // Usually one of these will be true, so first check if that's the case.
        boolean allSkipCommitlog = true;
        boolean noneSkipCommitlog = true;
        for (PartitionUpdate update : mutation.getPartitionUpdates())
        {
            if (update.metadata().params.memtable.factory().writesShouldSkipCommitLog())
                noneSkipCommitlog = false;
            else
                allSkipCommitlog = false;
        }

        if (!noneSkipCommitlog)
        {
            if (allSkipCommitlog)
                return null;
            else
            {
                Set<TableId> ids = new HashSet<>();
                for (PartitionUpdate update : mutation.getPartitionUpdates())
                {
                    if (update.metadata().params.memtable.factory().writesShouldSkipCommitLog())
                        ids.add(update.metadata().id);
                }
                mutation = mutation.without(ids);
            }
        }
        // Note: It may be a good idea to precalculate none/all for the set of all tables in the keyspace,
        // or memoize the mutation.getTableIds()->ids map (needs invalidation on schema version change).

        Tracing.trace("Appending to commitlog");
        return CommitLog.instance.add(mutation);
    }

    private WriteContext createEmptyContext()
    {
        OpOrder.Group group = null;
        try
        {
            group = Keyspace.writeOrder.start();
            return new CassandraWriteContext(group, null);
        }
        catch (Throwable t)
        {
            if (group != null)
            {
                group.close();
            }
            throw t;
        }
    }

    @Override
    public WriteContext createContextForIndexing()
    {
        return createEmptyContext();
    }

    @Override
    public WriteContext createContextForRead()
    {
        return createEmptyContext();
    }
}
