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

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.PendingWrite;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class TrackedKeyspaceWriteHandler implements KeyspaceWriteHandler
{
    private final Keyspace keyspace;

    public TrackedKeyspaceWriteHandler(Keyspace keyspace)
    {
        this.keyspace = keyspace;
    }

    @Override
    public WriteContext beginWrite(Mutation mutation, boolean makeDurable) throws RequestExecutionException
    {
        OpOrder.Group group = null;
        PendingWrite pendingWrite = null;
        try
        {
            group = Keyspace.writeOrder.start();
            pendingWrite = MutationTrackingService.instance().startWrite(mutation);

            // write the mutation to the commitlog and memtables
            Tracing.trace("Appending to mutation journal");
            CommitLogPosition position = MutationJournal.instance.write(mutation.id(), mutation);
            return new CassandraWriteContext(group, position, pendingWrite);
        }
        catch (Throwable t)
        {
            if (group != null)
                group.close();
            if (pendingWrite != null)
                pendingWrite.close();
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

    private WriteContext createEmptyContext()
    {
        OpOrder.Group group = Keyspace.writeOrder.start();
        try
        {
            return new CassandraWriteContext(group, null, PendingWrite.NOOP);
        }
        catch (Throwable t)
        {
            if (group != null)
                group.close();
            throw t;
        }
    }
}
