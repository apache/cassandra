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

    private volatile int current;

    MultiPartitionPager(List<ReadCommand> commands, ConsistencyLevel consistencyLevel, boolean localQuery)
    {
        this(commands, consistencyLevel, localQuery, null);
    }

    MultiPartitionPager(List<ReadCommand> commands, ConsistencyLevel consistencyLevel, boolean localQuery, PagingState state)
    {
        this.pagers = new SinglePartitionPager[commands.size()];

        long tstamp = -1;
        for (int i = 0; i < commands.size(); i++)
        {
            ReadCommand command = commands.get(i);
            if (tstamp == -1)
                tstamp = command.timestamp;
            else if (tstamp != command.timestamp)
                throw new IllegalArgumentException("All commands must have the same timestamp or weird results may happen.");

            PagingState tmpState = state != null && command.key.equals(state.partitionKey) ? state : null;
            pagers[i] = command instanceof SliceFromReadCommand
                      ? new SliceQueryPager((SliceFromReadCommand)command, consistencyLevel, localQuery, tmpState)
                      : new NamesQueryPager((SliceByNamesReadCommand)command, consistencyLevel, localQuery, tmpState);
        }
        timestamp = tstamp;
    }

    public PagingState state()
    {
        PagingState state = pagers[current].state();
        return state == null
             ? null
             : new PagingState(state.partitionKey, state.cellName, maxRemaining());
    }

    public boolean isExhausted()
    {
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
        int remaining = pageSize;
        List<Row> result = new ArrayList<Row>();

        while (!isExhausted() && remaining > 0)
        {
            // Exhausted also sets us on the first non-exhausted pager
            List<Row> page = pagers[current].fetchPage(remaining);
            if (page.isEmpty())
                continue;

            Row row = page.get(0);
            remaining -= pagers[current].columnCounter().countAll(row.cf).live();
            result.add(row);
        }

        return result;
    }

    public int maxRemaining()
    {
        int max = 0;
        for (int i = current; i < pagers.length; i++)
            max += pagers[i].maxRemaining();
        return max;
    }

    public long timestamp()
    {
        return timestamp;
    }
}
