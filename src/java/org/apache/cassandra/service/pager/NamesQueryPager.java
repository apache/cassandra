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
package org.apache.cassandra.service.pager;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnCounter;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;

/**
 * Pager over a SliceByNamesReadCommand.
 */
public class NamesQueryPager implements SinglePartitionPager
{
    private final SliceByNamesReadCommand command;
    private final ConsistencyLevel consistencyLevel;
    private final ClientState state;
    private final boolean localQuery;

    private volatile boolean queried;

    /**
     * For now, we'll only use this in CQL3. In there, as name query can never
     * yield more than one CQL3 row, there is no need for paging and so this is straight-forward.
     *
     * For thrift, we could imagine needing to page, though even then it's very
     * unlikely unless the pageSize is very small.
     *
     * In any case we currently assert in fetchPage if it's a "thrift" query (i.e. a query that
     * count every cell individually) and the names filter asks for more than pageSize columns.
     */
    // Don't use directly, use QueryPagers method instead
    NamesQueryPager(SliceByNamesReadCommand command, ConsistencyLevel consistencyLevel, ClientState state, boolean localQuery)
    {
        this.command = command;
        this.consistencyLevel = consistencyLevel;
        this.state = state;
        this.localQuery = localQuery;
    }

    public ByteBuffer key()
    {
        return command.key;
    }

    public ColumnCounter columnCounter()
    {
        // We know NamesQueryFilter.columnCounter don't care about his argument
        return command.filter.columnCounter(null, command.timestamp);
    }

    public PagingState state()
    {
        return null;
    }

    public boolean isExhausted()
    {
        return queried;
    }

    public List<Row> fetchPage(int pageSize) throws RequestValidationException, RequestExecutionException
    {
        assert command.filter.countCQL3Rows() || command.filter.columns.size() <= pageSize;

        if (isExhausted())
            return Collections.<Row>emptyList();

        queried = true;
        return localQuery
             ? Collections.singletonList(command.getRow(Keyspace.open(command.ksName)))
             : StorageProxy.read(Collections.<ReadCommand>singletonList(command), consistencyLevel, state);
    }

    public int maxRemaining()
    {
        if (queried)
            return 0;

        return command.filter.countCQL3Rows() ? 1 : command.filter.columns.size();
    }

    public long timestamp()
    {
        return command.timestamp;
    }
}
