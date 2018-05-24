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

import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.exceptions.RequestExecutionException;
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
    @SuppressWarnings("resource") // group is closed when CassandraWriteContext is closed
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
                Tracing.trace("Appending to commitlog");
                position = CommitLog.instance.add(mutation);
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

    @SuppressWarnings("resource") // group is closed when CassandraWriteContext is closed
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
