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
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.cassandra.stress.generate.SeedManager;
import org.apache.cassandra.stress.settings.Command;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.Timer;

public class CqlCounterGetter extends CqlOperation<Integer>
{

    public CqlCounterGetter(Timer timer, PartitionGenerator generator, SeedManager seedManager, StressSettings settings)
    {
        super(Command.COUNTER_READ, timer, generator, seedManager, settings);
    }

    @Override
    protected List<Object> getQueryParameters(byte[] key)
    {
        return Collections.<Object>singletonList(ByteBuffer.wrap(key));
    }

    @Override
    protected String buildQuery()
    {
        StringBuilder query = new StringBuilder("SELECT ");

        // TODO: obey slice/noslice option (instead of always slicing)
        if (isCql2())
            query.append("FIRST ").append(settings.columns.maxColumnsPerKey).append(" ''..''");
        else
            query.append("*");

        String counterCF = isCql2() ? type.table : "Counter3";

        query.append(" FROM ").append(wrapInQuotesIfRequired(counterCF));

        if (isCql2())
            query.append(" USING CONSISTENCY ").append(settings.command.consistencyLevel);

        return query.append(" WHERE KEY=?").toString();
    }

    @Override
    protected CqlRunOp<Integer> buildRunOp(ClientWrapper client, String query, Object queryId, List<Object> params, ByteBuffer key)
    {
        return new CqlRunOpTestNonEmpty(client, query, queryId, params, key);
    }

}
