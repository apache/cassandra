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

import org.apache.cassandra.stress.generate.Distribution;
import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.cassandra.stress.generate.SeedManager;
import org.apache.cassandra.stress.report.Timer;
import org.apache.cassandra.stress.settings.Command;
import org.apache.cassandra.stress.settings.StressSettings;

public class CqlCounterAdder extends CqlOperation<Integer>
{

    final Distribution counteradd;
    public CqlCounterAdder(DistributionFactory counteradd, Timer timer, PartitionGenerator generator, SeedManager seedManager, StressSettings settings)
    {
        super(Command.COUNTER_WRITE, timer, generator, seedManager, settings);
        this.counteradd = counteradd.get();
    }

    @Override
    protected String buildQuery()
    {
        StringBuilder query = new StringBuilder("UPDATE counter1 SET ");

        // TODO : increment distribution subset of columns
        for (int i = 0; i < settings.columns.maxColumnsPerKey; i++)
        {
            if (i > 0)
                query.append(",");

            String name = wrapInQuotes(settings.columns.namestrs.get(i));
            query.append(name).append("=").append(name).append("+?");
        }
        query.append(" WHERE KEY=?");
        return query.toString();
    }

    @Override
    protected List<Object> getQueryParameters(byte[] key)
    {
        final List<Object> list = new ArrayList<>();
        for (int i = 0; i < settings.columns.maxColumnsPerKey; i++)
            list.add(counteradd.next());
        list.add(ByteBuffer.wrap(key));
        return list;
    }

    @Override
    protected CqlRunOp<Integer> buildRunOp(QueryExecutor<?> queryExecutor, List<Object> params, ByteBuffer key)
    {
        return new CqlRunOpAlwaysSucceed(queryExecutor, params, key, 1);
    }

    public boolean isWrite()
    {
        return true;
    }
}
