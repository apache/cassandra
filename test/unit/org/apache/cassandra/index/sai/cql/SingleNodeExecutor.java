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

package org.apache.cassandra.index.sai.cql;

import java.util.List;
import java.util.stream.Collectors;

import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.inject.Injections;

public class SingleNodeExecutor implements BaseDataModel.Executor
{
    private final SAITester tester;
    private final Injections.Counter counter;

    public SingleNodeExecutor(SAITester tester, Injections.Counter counter)
    {
        this.tester = tester;
        this.counter = counter;
    }

    @Override
    public void createTable(String statement)
    {
        tester.createTable(statement);
    }

    @Override
    public void flush(String keyspace, String table)
    {
        tester.flush(keyspace, table);
    }

    @Override
    public void compact(String keyspace, String table)
    {
        tester.compact(keyspace, table);
    }

    @Override
    public void disableCompaction(String keyspace, String table)
    {
        tester.disableCompaction(keyspace, table);
    }

    @Override
    public void waitForIndexQueryable(String keyspace, String index)
    {
        tester.waitForIndexQueryable(keyspace, index);
    }

    @Override
    public void executeLocal(String query, Object... values) throws Throwable
    {
        tester.executeFormattedQuery(query, values);
    }

    @Override
    public List<Object> executeRemote(String query, int fetchSize, Object... values)
    {
        SimpleStatement statement = new SimpleStatement(query, values);
        statement.setFetchSize(fetchSize);
        return tester.sessionNet().execute(statement).all().stream().map(r -> r.getObject(0)).collect(Collectors.toList());
    }

    @Override
    public void counterReset()
    {
        counter.reset();
    }

    @Override
    public long getCounter()
    {
        return counter.get();
    }
}
