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
package org.apache.cassandra.service;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Represents the state related to a given query.
 */
public class QueryState
{
    private final ClientState clientState;
    private volatile long clock;
    private volatile UUID preparedTracingSession;
    private volatile Pager pager;

    public QueryState(ClientState clientState)
    {
        this.clientState = clientState;
    }

    public ClientState getClientState()
    {
        return clientState;
    }

    /**
     * This clock guarantees that updates for the same QueryState will be ordered
     * in the sequence seen, even if multiple updates happen in the same millisecond.
     */
    public long getTimestamp()
    {
        long current = System.currentTimeMillis() * 1000;
        clock = clock >= current ? clock + 1 : current;
        return clock;
    }

    public boolean traceNextQuery()
    {
        if (preparedTracingSession != null)
        {
            return true;
        }

        double tracingProbability = StorageService.instance.getTracingProbability();
        return tracingProbability != 0 && FBUtilities.threadLocalRandom().nextDouble() < tracingProbability;
    }

    public void prepareTracingSession(UUID sessionId)
    {
        this.preparedTracingSession = sessionId;
    }

    public UUID getAndResetCurrentTracingSession()
    {
        UUID previous = preparedTracingSession;
        preparedTracingSession = null;
        return previous;
    }

    public void createTracingSession()
    {
        if (this.preparedTracingSession == null)
        {
            Tracing.instance.newSession();
        }
        else
        {
            UUID session = this.preparedTracingSession;
            Tracing.instance.newSession(session);
        }
    }

    public void attachPager(QueryPager queryPager, SelectStatement statement, List<ByteBuffer> variables)
    {
        pager = new Pager(queryPager, statement, variables);
    }

    public boolean hasPager()
    {
        return pager != null;
    }

    public void dropPager()
    {
        pager = null;
    }

    public ResultMessage.Rows getNextPage(int pageSize) throws RequestValidationException, RequestExecutionException
    {
        assert pager != null; // We've already validated (in ServerConnection) that this should not be null

        int currentLimit = pager.queryPager.maxRemaining();
        List<Row> page = pager.queryPager.fetchPage(pageSize);
        ResultMessage.Rows msg = pager.statement.processResults(page, pager.variables, currentLimit, pager.queryPager.timestamp());

        if (pager.queryPager.isExhausted())
            dropPager();
        else
            msg.result.metadata.setHasMorePages();

        return msg;
    }

    // Groups the actual query pager with the Select Query
    private static class Pager
    {
        private final QueryPager queryPager;
        private final SelectStatement statement;
        private final List<ByteBuffer> variables;

        private Pager(QueryPager queryPager, SelectStatement statement, List<ByteBuffer> variables)
        {
            this.queryPager = queryPager;
            this.statement = statement;
            this.variables = variables;
        }
    }
}
