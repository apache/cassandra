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

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.ClientState;

/**
 * Pager over a list of ReadCommand.
 *
 * Note that this is not easy to make efficient. Indeed, we need to page the first command fully before
 * returning results from the next one, but if the result returned by each command is small (compared to pageSize),
 * paging the commands one at a time under-performs compared to parallelizing. On the other, if we parallelize
 * and each command raised pageSize results, we'll end up with commands.size() * pageSize results in memory, which
 * defeats the purpose of paging.
 *
 * For now, we keep it simple (somewhat) and just do one command at a time. Provided that we make sure to not
 * create a pager unless we need to, this is probably fine. Though if we later want to get fancy, we could use the
 * cfs meanRowSize to decide if parallelizing some of the command might be worth it while being confident we don't
 * blow out memory.
 */
class MultiPartitionPager implements QueryPager
{
    private final SinglePartitionPager[] pagers;
    private final long timestamp;

    private int remaining;
    private int current;

    MultiPartitionPager(List<ReadCommand> commands, ConsistencyLevel consistencyLevel, ClientState cState, boolean localQuery, PagingState state, int limitForQuery)
    {
        int i = 0;
        // If it's not the beginning (state != null), we need to find where we were and skip previous commands
        // since they are done.
        if (state != null)
            for (; i < commands.size(); i++)
                if (commands.get(i).key.equals(state.partitionKey))
                    break;

        if (i >= commands.size())
        {
            pagers = null;
            timestamp = -1;
            return;
        }

        pagers = new SinglePartitionPager[commands.size() - i];
        // 'i' is on the first non exhausted pager for the previous page (or the first one)
        pagers[0] = makePager(commands.get(i), consistencyLevel, cState, localQuery, state);
        timestamp = commands.get(i).timestamp;

        // Following ones haven't been started yet
        for (int j = i + 1; j < commands.size(); j++)
        {
            ReadCommand command = commands.get(j);
            if (command.timestamp != timestamp)
                throw new IllegalArgumentException("All commands must have the same timestamp or weird results may happen.");
            pagers[j - i] = makePager(command, consistencyLevel, cState, localQuery, null);
        }

        remaining = state == null ? limitForQuery : state.remaining;
    }

    private static SinglePartitionPager makePager(ReadCommand command, ConsistencyLevel consistencyLevel, ClientState cState, boolean localQuery, PagingState state)
    {
        return command instanceof SliceFromReadCommand
             ? new SliceQueryPager((SliceFromReadCommand)command, consistencyLevel, cState, localQuery, state)
             : new NamesQueryPager((SliceByNamesReadCommand)command, consistencyLevel, cState, localQuery);
    }

    public PagingState state()
    {
        // Sets current to the first non-exhausted pager
        if (isExhausted())
            return null;

        PagingState state = pagers[current].state();
        return new PagingState(pagers[current].key(), state == null ? null : state.cellName, remaining);
    }

    public boolean isExhausted()
    {
        if (remaining <= 0 || pagers == null)
            return true;

        while (current < pagers.length)
        {
            if (!pagers[current].isExhausted())
                return false;

            current++;
        }
        return true;
    }

    public List<Row> fetchPage(int pageSize) throws RequestValidationException, RequestExecutionException
    {
        List<Row> result = new ArrayList<Row>();

        int remainingThisQuery = Math.min(remaining, pageSize);
        while (remainingThisQuery > 0 && !isExhausted())
        {
            // isExhausted has set us on the first non-exhausted pager
            List<Row> page = pagers[current].fetchPage(remainingThisQuery);
            if (page.isEmpty())
                continue;

            Row row = page.get(0);
            int fetched = pagers[current].columnCounter().countAll(row.cf).live();
            remaining -= fetched;
            remainingThisQuery -= fetched;
            result.add(row);
        }

        return result;
    }

    public int maxRemaining()
    {
        return remaining;
    }

    public long timestamp()
    {
        return timestamp;
    }
}
