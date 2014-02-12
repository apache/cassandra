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
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.utils.UUIDGen;

public class CqlInserter extends CqlOperation<Integer>
{

    public CqlInserter(State state, long idx)
    {
        super(state, idx);
    }

    @Override
    protected String buildQuery()
    {
        StringBuilder query = new StringBuilder("UPDATE ").append(wrapInQuotesIfRequired(state.settings.schema.columnFamily));

        if (state.isCql2())
            query.append(" USING CONSISTENCY ").append(state.settings.command.consistencyLevel);

        query.append(" SET ");

        for (int i = 0 ; i < state.settings.columns.maxColumnsPerKey; i++)
        {
            if (i > 0)
                query.append(',');

            if (state.settings.columns.useTimeUUIDComparator)
            {
                if (state.isCql3())
                    throw new UnsupportedOperationException("Cannot use UUIDs in column names with CQL3");

                query.append(wrapInQuotesIfRequired(UUIDGen.getTimeUUID().toString()))
                        .append(" = ?");
            }
            else
            {
                query.append(wrapInQuotesIfRequired("C" + i)).append(" = ?");
            }
        }

        query.append(" WHERE KEY=?");
        return query.toString();
    }

    @Override
    protected List<ByteBuffer> getQueryParameters(byte[] key)
    {
        final ArrayList<ByteBuffer> queryParams = new ArrayList<>();
        final List<ByteBuffer> values = generateColumnValues(ByteBuffer.wrap(key));
        queryParams.addAll(values);
        queryParams.add(ByteBuffer.wrap(key));
        return queryParams;
    }

    @Override
    protected CqlRunOp<Integer> buildRunOp(ClientWrapper client, String query, Object queryId, List<ByteBuffer> params, String keyid, ByteBuffer key)
    {
        return new CqlRunOpAlwaysSucceed(client, query, queryId, params, keyid, key, 1);
    }
}
