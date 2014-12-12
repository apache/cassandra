package org.apache.cassandra.stress.operations.predefined;
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

import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.cassandra.stress.generate.SeedManager;
import org.apache.cassandra.stress.settings.Command;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.Timer;

public class CqlInserter extends CqlOperation<Integer>
{

    public CqlInserter(Timer timer, PartitionGenerator generator, SeedManager seedManager, StressSettings settings)
    {
        super(Command.WRITE, timer, generator, seedManager, settings);
    }

    @Override
    protected String buildQuery()
    {
        StringBuilder query = new StringBuilder("UPDATE ").append(wrapInQuotesIfRequired(type.table));

        if (isCql2())
            query.append(" USING CONSISTENCY ").append(settings.command.consistencyLevel);

        query.append(" SET ");

        for (int i = 0 ; i < settings.columns.maxColumnsPerKey ; i++)
        {
            if (i > 0)
                query.append(',');

            query.append(wrapInQuotesIfRequired(settings.columns.namestrs.get(i))).append(" = ?");
        }

        query.append(" WHERE KEY=?");
        return query.toString();
    }

    @Override
    protected List<Object> getQueryParameters(byte[] key)
    {
        final ArrayList<Object> queryParams = new ArrayList<>();
        List<ByteBuffer> values = getColumnValues();
        queryParams.addAll(values);
        queryParams.add(ByteBuffer.wrap(key));
        return queryParams;
    }

    @Override
    protected CqlRunOp<Integer> buildRunOp(ClientWrapper client, String query, Object queryId, List<Object> params, ByteBuffer key)
    {
        return new CqlRunOpAlwaysSucceed(client, query, queryId, params, key, 1);
    }

    public boolean isWrite()
    {
        return true;
    }
}
