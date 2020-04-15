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
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.Pageable;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.service.pager.QueryPagers;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.tracing.Tracing;
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
                Tracing.instance.newSession(sessionId);
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
        ClientState clientState = ClientState.forInternalCalls();
        CQLStatement prepared = QueryProcessor.getStatement(query, clientState).statement;
        List<ByteBuffer> boundBBValues = new ArrayList<>();
        ConsistencyLevel consistencyLevel = ConsistencyLevel.valueOf(consistencyLevelOrigin.name());
        for (Object boundValue : boundValues)
            boundBBValues.add(ByteBufferUtil.objectToBytes(boundValue));

        prepared.validate(QueryState.forInternalCalls().getClientState());
        ResultMessage res = prepared.execute(QueryState.forInternalCalls(),
                                             QueryOptions.create(toCassandraCL(consistencyLevel),
                                                                 boundBBValues,
                                                                 false,
                                                                 Integer.MAX_VALUE,
                                                                 null,
                                                                 null,
                                                                 Server.CURRENT_VERSION));

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
            ConsistencyLevel consistencyLevel = ConsistencyLevel.valueOf(consistencyLevelOrigin.name());
            CQLStatement prepared = QueryProcessor.getStatement(query, ClientState.forInternalCalls()).statement;
            List<ByteBuffer> boundBBValues = new ArrayList<>();
            for (Object boundValue : boundValues)
            {
                boundBBValues.add(ByteBufferUtil.objectToBytes(boundValue));
            }

            prepared.validate(QueryState.forInternalCalls().getClientState());
            assert prepared instanceof SelectStatement : "Only SELECT statements can be executed with paging";

            ClientState clientState = QueryState.forInternalCalls().getClientState();
            SelectStatement selectStatement = (SelectStatement) prepared;
            QueryOptions queryOptions = QueryOptions.create(toCassandraCL(consistencyLevel),
                                                            boundBBValues,
                                                            false,
                                                            pageSize,
                                                            null,
                                                            null,
                                                            Server.CURRENT_VERSION);
            Pageable pageable = selectStatement.getPageableCommand(queryOptions);

            // Usually pager fetches a single page (see SelectStatement#execute). We need to iterate over all
            // of the results lazily.
            QueryPager pager = QueryPagers.pager(pageable, toCassandraCL(consistencyLevel), clientState, null);
            Iterator<Object[]> iter = RowUtil.toObjects(selectStatement.getResultMetadata().names,
                                                        UntypedResultSet.create(selectStatement,
                                                                                pager,
                                                                                pageSize).iterator());

            // We have to make sure iterator is not running on main thread.
            Iterator<Object[]> it =  new Iterator<Object[]>() {
                public boolean hasNext()
                {
                    return instance.sync(() -> iter.hasNext()).call();
                }

                public Object[] next()
                {
                    return instance.sync(() -> iter.next()).call();
                }
            };
            return QueryResults.fromObjectArrayIterator(RowUtil.getColumnNames(selectStatement.getResultMetadata().names), it);
        }).call();
    }

    private static ClientState makeFakeClientState()
    {
        return ClientState.forExternalCalls(new InetSocketAddress(FBUtilities.getLocalAddress(), 9042));
    }
}
