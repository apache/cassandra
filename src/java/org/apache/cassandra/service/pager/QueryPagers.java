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
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnCounter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;

/**
 * Static utility methods to create query pagers.
 */
public class QueryPagers
{
    private QueryPagers() {};

    private static int maxQueried(ReadCommand command)
    {
        if (command instanceof SliceByNamesReadCommand)
        {
            NamesQueryFilter filter = ((SliceByNamesReadCommand)command).filter;
            return filter.countCQL3Rows() ? 1 : filter.columns.size();
        }
        else
        {
            SliceQueryFilter filter = ((SliceFromReadCommand)command).filter;
            return filter.count;
        }
    }

    public static boolean mayNeedPaging(Pageable command, int pageSize)
    {
        if (command instanceof Pageable.ReadCommands)
        {
            List<ReadCommand> commands = ((Pageable.ReadCommands)command).commands;

            // Using long on purpose, as we could overflow otherwise
            long maxQueried = 0;
            for (ReadCommand readCmd : commands)
                maxQueried += maxQueried(readCmd);

            return maxQueried > pageSize;
        }
        else if (command instanceof ReadCommand)
        {
            return maxQueried((ReadCommand)command) > pageSize;
        }
        else
        {
            assert command instanceof RangeSliceCommand;
            RangeSliceCommand rsc = (RangeSliceCommand)command;
            // We don't support paging for thrift in general because the way thrift RangeSliceCommand count rows
            // independently of cells makes things harder (see RangeSliceQueryPager). The one case where we do
            // get a RangeSliceCommand from CQL3 without the countCQL3Rows flag set is for DISTINCT. In that case
            // however, the underlying sliceQueryFilter count is 1, so that the RSC limit is still a limit on the
            // number of CQL3 rows returned.
            assert rsc.countCQL3Rows || (rsc.predicate instanceof SliceQueryFilter && ((SliceQueryFilter)rsc.predicate).count == 1);
            return rsc.maxResults > pageSize;
        }
    }

    private static QueryPager pager(ReadCommand command, ConsistencyLevel consistencyLevel, ClientState cState, boolean local, PagingState state)
    {
        if (command instanceof SliceByNamesReadCommand)
            return new NamesQueryPager((SliceByNamesReadCommand)command, consistencyLevel, cState, local);
        else
            return new SliceQueryPager((SliceFromReadCommand)command, consistencyLevel, cState, local, state);
    }

    private static QueryPager pager(Pageable command, ConsistencyLevel consistencyLevel, ClientState cState, boolean local, PagingState state)
    {
        if (command instanceof Pageable.ReadCommands)
        {
            List<ReadCommand> commands = ((Pageable.ReadCommands)command).commands;
            if (commands.size() == 1)
                return pager(commands.get(0), consistencyLevel, cState, local, state);

            return new MultiPartitionPager(commands, consistencyLevel, cState, local, state, ((Pageable.ReadCommands) command).limitForQuery);
        }
        else if (command instanceof ReadCommand)
        {
            return pager((ReadCommand)command, consistencyLevel, cState, local, state);
        }
        else
        {
            assert command instanceof RangeSliceCommand;
            RangeSliceCommand rangeCommand = (RangeSliceCommand)command;
            if (rangeCommand.predicate instanceof NamesQueryFilter)
                return new RangeNamesQueryPager(rangeCommand, consistencyLevel, local, state);
            else
                return new RangeSliceQueryPager(rangeCommand, consistencyLevel, local, state);
        }
    }

    public static QueryPager pager(Pageable command, ConsistencyLevel consistencyLevel, ClientState cState)
    {
        return pager(command, consistencyLevel, cState, false, null);
    }

    public static QueryPager pager(Pageable command, ConsistencyLevel consistencyLevel, ClientState cState, PagingState state)
    {
        return pager(command, consistencyLevel, cState, false, state);
    }

    public static QueryPager localPager(Pageable command)
    {
        return pager(command, null, null, true, null);
    }

    /**
     * Convenience method to (locally) page an internal row.
     * Used to 2ndary index a wide row without dying.
     */
    public static Iterator<ColumnFamily> pageRowLocally(final ColumnFamilyStore cfs, ByteBuffer key, final int pageSize)
    {
        SliceFromReadCommand command = new SliceFromReadCommand(cfs.metadata.ksName, key, cfs.name, System.currentTimeMillis(), new IdentityQueryFilter());
        final SliceQueryPager pager = new SliceQueryPager(command, null, null, true);

        return new Iterator<ColumnFamily>()
        {
            // We don't use AbstractIterator because we don't want hasNext() to do an actual query
            public boolean hasNext()
            {
                return !pager.isExhausted();
            }

            public ColumnFamily next()
            {
                try
                {
                    List<Row> rows = pager.fetchPage(pageSize);
                    ColumnFamily cf = rows.isEmpty() ? null : rows.get(0).cf;
                    return cf == null ? ArrayBackedSortedColumns.factory.create(cfs.metadata) : cf;
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Convenience method that count (live) cells/rows for a given slice of a row, but page underneath.
     */
    public static int countPaged(String keyspace,
                                 String columnFamily,
                                 ByteBuffer key,
                                 SliceQueryFilter filter,
                                 ConsistencyLevel consistencyLevel,
                                 ClientState cState,
                                 final int pageSize,
                                 long now) throws RequestValidationException, RequestExecutionException
    {
        SliceFromReadCommand command = new SliceFromReadCommand(keyspace, key, columnFamily, now, filter);
        final SliceQueryPager pager = new SliceQueryPager(command, consistencyLevel, cState, false);

        ColumnCounter counter = filter.columnCounter(Schema.instance.getCFMetaData(keyspace, columnFamily).comparator, now);
        while (!pager.isExhausted())
        {
            List<Row> next = pager.fetchPage(pageSize);
            if (!next.isEmpty())
                counter.countAll(next.get(0).cf);
        }
        return counter.live();
    }
}
