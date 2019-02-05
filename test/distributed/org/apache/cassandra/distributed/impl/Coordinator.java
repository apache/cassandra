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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.Pageable;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.service.pager.QueryPagers;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

public class Coordinator implements ICoordinator
{
    final Instance instance;
    public Coordinator(Instance instance)
    {
        this.instance = instance;
    }

    @Override
    public Object[][] execute(String query, Enum<?> consistencyLevelOrigin, Object... boundValues)
    {
        return instance.sync(() -> {
            ConsistencyLevel consistencyLevel = ConsistencyLevel.valueOf(consistencyLevelOrigin.name());
            CQLStatement prepared = QueryProcessor.getStatement(query, ClientState.forInternalCalls()).statement;
            List<ByteBuffer> boundBBValues = new ArrayList<>();
            for (Object boundValue : boundValues)
            {
                boundBBValues.add(ByteBufferUtil.objectToBytes(boundValue));
            }

            prepared.validate(QueryState.forInternalCalls().getClientState());
            ResultMessage res = prepared.execute(QueryState.forInternalCalls(),
                                                 QueryOptions.create(consistencyLevel,
                                                                     boundBBValues,
                                                                     false,
                                                                     Integer.MAX_VALUE,
                                                                     null,
                                                                     null,
                                                                     Server.CURRENT_VERSION));

            if (res != null && res.kind == ResultMessage.Kind.ROWS)
            {
                return RowUtil.toObjects((ResultMessage.Rows) res);
            }
            else
            {
                return new Object[][]{};
            }
        }).call();
    }

    @Override
    public Iterator<Object[]> executeWithPaging(String query, Enum<?> consistencyLevelOrigin, int pageSize, Object... boundValues)
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
            QueryOptions queryOptions = QueryOptions.create(consistencyLevel,
                                                            boundBBValues,
                                                            false,
                                                            pageSize,
                                                            null,
                                                            null,
                                                            Server.CURRENT_VERSION);
            Pageable pageable = selectStatement.getPageableCommand(queryOptions);

            // Usually pager fetches a single page (see SelectStatement#execute). We need to iterate over all
            // of the results lazily.
            QueryPager pager = QueryPagers.pager(pageable, consistencyLevel, clientState, null);
            Iterator<Object[]> iter = RowUtil.toObjects(selectStatement.getResultMetadata().names,
                                                        UntypedResultSet.create(selectStatement,
                                                                                pager,
                                                                                pageSize).iterator());

            // We have to make sure iterator is not running on main thread.
            return new Iterator<Object[]>() {
                public boolean hasNext()
                {
                    return instance.sync(() -> iter.hasNext()).call();
                }

                public Object[] next()
                {
                    return instance.sync(() -> iter.next()).call();
                }
            };
        }).call();
    }
}
