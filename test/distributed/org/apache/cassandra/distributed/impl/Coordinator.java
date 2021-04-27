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

package org.apache.cassandra.distributed.impl;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;

import com.google.common.collect.Iterators;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.QueryResult;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.ClientStat;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class Coordinator implements ICoordinator
{
    final Instance instance;
    public Coordinator(Instance instance)
    {
        this.instance = instance;
    }

    @Override
    public SimpleQueryResult executeWithResult(String query, ConsistencyLevel consistencyLevel, Object... boundValues)
    {
        return instance().sync(() -> executeInternal(query, consistencyLevel, boundValues)).call();
    }

    public Future<SimpleQueryResult> asyncExecuteWithTracingWithResult(UUID sessionId, String query, ConsistencyLevel consistencyLevelOrigin, Object... boundValues)
    {
        return instance.async(() -> {
            try
            {
                Tracing.instance.newSession(sessionId, Collections.emptyMap());
                return executeInternal(query, consistencyLevelOrigin, boundValues);
            }
            finally
            {
                Tracing.instance.stopSession();
            }
        }).call();
    }

    protected org.apache.cassandra.db.ConsistencyLevel toCassandraCL(ConsistencyLevel cl)
    {
        return org.apache.cassandra.db.ConsistencyLevel.fromCode(cl.ordinal());
    }

    private SimpleQueryResult executeInternal(String query, ConsistencyLevel consistencyLevelOrigin, Object[] boundValues)
    {
        ClientState clientState = makeFakeClientState();
        CQLStatement prepared = QueryProcessor.getStatement(query, clientState);
        List<ByteBuffer> boundBBValues = new ArrayList<>();
        ConsistencyLevel consistencyLevel = ConsistencyLevel.valueOf(consistencyLevelOrigin.name());
        for (Object boundValue : boundValues)
            boundBBValues.add(ByteBufferUtil.objectToBytes(boundValue));

        prepared.validate(QueryState.forInternalCalls());

        // Start capturing warnings on this thread. Note that this will implicitly clear out any previous 
        // warnings as it sets a new State instance on the ThreadLocal.
        ClientWarn.instance.captureWarnings();
        
        ResultMessage res = prepared.execute(QueryState.forInternalCalls(),
                                             QueryOptions.create(toCassandraCL(consistencyLevel),
                                                                 boundBBValues,
                                                                 false,
                                                                 Integer.MAX_VALUE,
                                                                 null,
                                                                 null,
                                                                 ProtocolVersion.CURRENT,
                                                                 null),
                                             System.nanoTime());

        // Collect warnings reported during the query.
        if (res != null)
            res.setWarnings(ClientWarn.instance.getWarnings());

        return RowUtil.toQueryResult(res);
    }

    public Object[][] executeWithTracing(UUID sessionId, String query, ConsistencyLevel consistencyLevelOrigin, Object... boundValues)
    {
        return IsolatedExecutor.waitOn(asyncExecuteWithTracing(sessionId, query, consistencyLevelOrigin, boundValues));
    }

    public IInstance instance()
    {
        return instance;
    }

    @Override
    public QueryResult executeWithPagingWithResult(String query, ConsistencyLevel consistencyLevelOrigin, int pageSize, Object... boundValues)
    {
        if (pageSize <= 0)
            throw new IllegalArgumentException("Page size should be strictly positive but was " + pageSize);

        return instance.sync(() -> {
            QueryState state = new QueryState(makeFakeClientState());
            ConsistencyLevel consistencyLevel = ConsistencyLevel.valueOf(consistencyLevelOrigin.name());
            CQLStatement prepared = QueryProcessor.getStatement(query, state.getClientState());
            final List<ByteBuffer> boundBBValues = new ArrayList<>();
            for (Object boundValue : boundValues)
                boundBBValues.add(ByteBufferUtil.objectToBytes(boundValue));

            prepared.validate(state);
            assert prepared instanceof SelectStatement : "Only SELECT statements can be executed with paging";

            long nanoTime = System.nanoTime();
            SelectStatement selectStatement = (SelectStatement) prepared;

            QueryOptions initialOptions = QueryOptions.create(toCassandraCL(consistencyLevel),
                                                              boundBBValues,
                                                              false,
                                                              pageSize,
                                                              null,
                                                              null,
                                                              ProtocolVersion.CURRENT,
                                                              selectStatement.keyspace());


            ResultMessage.Rows initialRows = selectStatement.execute(state, initialOptions, nanoTime);
            Iterator<Object[]> iter = new Iterator<Object[]>() {
                ResultMessage.Rows rows = selectStatement.execute(state, initialOptions, nanoTime);
                Iterator<Object[]> iter = RowUtil.toIter(rows);

                public boolean hasNext()
                {
                    if (iter.hasNext())
                        return true;

                    if (rows.result.metadata.getPagingState() == null)
                        return false;

                    QueryOptions nextOptions = QueryOptions.create(toCassandraCL(consistencyLevel),
                                                                   boundBBValues,
                                                                   true,
                                                                   pageSize,
                                                                   rows.result.metadata.getPagingState(),
                                                                   null,
                                                                   ProtocolVersion.CURRENT,
                                                                   selectStatement.keyspace());

                    rows = selectStatement.execute(state, nextOptions, nanoTime);
                    iter = Iterators.forArray(RowUtil.toObjects(initialRows.result.metadata.names, rows.result.rows));

                    return hasNext();
                }

                public Object[] next()
                {
                    return iter.next();
                }
            };

            return QueryResults.fromObjectArrayIterator(RowUtil.getColumnNames(initialRows.result.metadata.names), iter);
        }).call();
    }

    private static final ClientState makeFakeClientState()
    {
        return ClientState.forExternalCalls(new InetSocketAddress(FBUtilities.getJustLocalAddress(), 9042));
    }
}
