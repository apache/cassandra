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

package org.apache.cassandra.stress.operations.userdefined;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.naming.OperationNotSupportedException;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.stress.Operation;
import org.apache.cassandra.stress.StressYaml;
import org.apache.cassandra.stress.WorkManager;
import org.apache.cassandra.stress.generate.TokenRangeIterator;
import org.apache.cassandra.stress.report.Timer;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.ThriftClient;

public class TokenRangeQuery extends Operation
{
    private final FastThreadLocal<State> currentState = new FastThreadLocal<>();

    private final TableMetadata tableMetadata;
    private final TokenRangeIterator tokenRangeIterator;
    private final String columns;
    private final int pageSize;
    private final boolean isWarmup;

    public TokenRangeQuery(Timer timer,
                           StressSettings settings,
                           TableMetadata tableMetadata,
                           TokenRangeIterator tokenRangeIterator,
                           StressYaml.TokenRangeQueryDef def,
                           boolean isWarmup)
    {
        super(timer, settings);
        this.tableMetadata = tableMetadata;
        this.tokenRangeIterator = tokenRangeIterator;
        this.columns = sanitizeColumns(def.columns, tableMetadata);
        this.pageSize = isWarmup ? Math.min(100, def.page_size) : def.page_size;
        this.isWarmup = isWarmup;
    }

    /**
     * We need to specify the columns by name because we need to add token(partition_keys) in order to count
     * partitions. So if the user specifies '*' then replace it with a list of all columns.
     */
    private static String sanitizeColumns(String columns, TableMetadata tableMetadata)
    {
        if (!columns.equals("*"))
            return columns;

        return String.join(", ", tableMetadata.getColumns().stream().map(ColumnMetadata::getName).collect(Collectors.toList()));
    }

    /**
     * The state of a token range currently being retrieved.
     * Here we store the paging state to retrieve more pages
     * and we keep track of which partitions have already been retrieved,
     */
    private final static class State
    {
        public final TokenRange tokenRange;
        public final String query;
        public PagingState pagingState;
        public Set<Token> partitions = new HashSet<>();

        public State(TokenRange tokenRange, String query)
        {
            this.tokenRange = tokenRange;
            this.query = query;
        }

        @Override
        public String toString()
        {
            return String.format("[%s, %s]", tokenRange.getStart(), tokenRange.getEnd());
        }
    }

    abstract static class Runner implements RunOp
    {
        int partitionCount;
        int rowCount;

        @Override
        public int partitionCount()
        {
            return partitionCount;
        }

        @Override
        public int rowCount()
        {
            return rowCount;
        }
    }

    private class JavaDriverRun extends Runner
    {
        final JavaDriverClient client;

        private JavaDriverRun(JavaDriverClient client)
        {
            this.client = client;
        }

        public boolean run() throws Exception
        {
            State state = currentState.get();
            if (state == null)
            { // start processing a new token range
                TokenRange range = tokenRangeIterator.next();
                if (range == null)
                    return true; // no more token ranges to process

                state = new State(range, buildQuery(range));
                currentState.set(state);
            }

            ResultSet results;
            Statement statement = new SimpleStatement(state.query);
            statement.setFetchSize(pageSize);

            if (state.pagingState != null)
                statement.setPagingState(state.pagingState);

            results = client.getSession().execute(statement);
            state.pagingState = results.getExecutionInfo().getPagingState();

            int remaining = results.getAvailableWithoutFetching();
            rowCount += remaining;

            for (Row row : results)
            {
                // this call will only succeed if we've added token(partition keys) to the query
                Token partition = row.getPartitionKeyToken();
                if (!state.partitions.contains(partition))
                {
                    partitionCount += 1;
                    state.partitions.add(partition);
                }

                if (--remaining == 0)
                    break;
            }

            if (results.isExhausted() || isWarmup)
            { // no more pages to fetch or just warming up, ready to move on to another token range
                currentState.set(null);
            }

            return true;
        }
    }

    private String buildQuery(TokenRange tokenRange)
    {
        Token start = tokenRange.getStart();
        Token end = tokenRange.getEnd();
        List<String> pkColumns = tableMetadata.getPartitionKey().stream().map(ColumnMetadata::getName).collect(Collectors.toList());
        String tokenStatement = String.format("token(%s)", String.join(", ", pkColumns));

        StringBuilder ret = new StringBuilder();
        ret.append("SELECT ");
        ret.append(tokenStatement); // add the token(pk) statement so that we can count partitions
        ret.append(", ");
        ret.append(columns);
        ret.append(" FROM ");
        ret.append(tableMetadata.getName());
        if (start != null || end != null)
            ret.append(" WHERE ");
        if (start != null)
        {
            ret.append(tokenStatement);
            ret.append(" > ");
            ret.append(start.toString());
        }

        if (start != null && end != null)
            ret.append(" AND ");

        if (end != null)
        {
            ret.append(tokenStatement);
            ret.append(" <= ");
            ret.append(end.toString());
        }

        return ret.toString();
    }

    private static class ThriftRun extends Runner
    {
        final ThriftClient client;

        private ThriftRun(ThriftClient client)
        {
            this.client = client;
        }

        public boolean run() throws Exception
        {
            throw new OperationNotSupportedException("Bulk read over thrift not supported");
        }
    }


    @Override
    public void run(JavaDriverClient client) throws IOException
    {
        timeWithRetry(new JavaDriverRun(client));
    }

    @Override
    public void run(ThriftClient client) throws IOException
    {
        timeWithRetry(new ThriftRun(client));
    }

    public int ready(WorkManager workManager)
    {
        tokenRangeIterator.update();

        if (tokenRangeIterator.exhausted() && currentState.get() == null)
            return 0;

        int numLeft = workManager.takePermits(1);

        return numLeft > 0 ? 1 : 0;
    }

    public String key()
    {
        State state = currentState.get();
        return state == null ? "-" : state.toString();
    }
}
