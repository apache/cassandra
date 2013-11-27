package org.apache.cassandra.stress.operations;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class CqlCounterAdder extends CqlOperation<Integer>
{
    public CqlCounterAdder(State state, long idx)
    {
        super(state, idx);
    }

    @Override
    protected String buildQuery()
    {
        String counterCF = state.isCql2() ? "Counter1" : "Counter3";

        StringBuilder query = new StringBuilder("UPDATE ").append(wrapInQuotesIfRequired(counterCF));

        if (state.isCql2())
            query.append(" USING CONSISTENCY ").append(state.settings.command.consistencyLevel);

        query.append(" SET ");

        // TODO : increment distribution subset of columns
        for (int i = 0; i < state.settings.columns.maxColumnsPerKey; i++)
        {
            if (i > 0)
                query.append(",");

            query.append('C').append(i).append("=C").append(i).append("+1");
        }
        query.append(" WHERE KEY=?");
        return query.toString();
    }

    @Override
    protected List<ByteBuffer> getQueryParameters(byte[] key)
    {
        return Collections.singletonList(ByteBuffer.wrap(key));
    }

    @Override
    protected CqlRunOp buildRunOp(ClientWrapper client, String query, Object queryId, List<ByteBuffer> params, String keyid, ByteBuffer key)
    {
        return new CqlRunOpAlwaysSucceed(client, query, queryId, params, keyid, key, 1);
    }
}
