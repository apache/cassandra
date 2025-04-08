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

package org.apache.cassandra.harry.test;


import org.junit.BeforeClass;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.shared.DistributedTestBase;
import org.apache.cassandra.distributed.test.log.FuzzTestBase;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.execution.CQLVisitExecutor;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;

public class HistoryBuilderInJvmDTest extends HistoryBuilderTest
{
    private static final FuzzTestBase tester = new FuzzTestBase();

    private final Cluster cluster;

    public HistoryBuilderInJvmDTest() throws Throwable
    {
        cluster = tester.builder().withNodes(1).start();
        cluster.schemaChange(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};", keyspace()));
    }

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        FuzzTestBase.beforeClass();
    }

    @Override
    protected String keyspace()
    {
        return DistributedTestBase.KEYSPACE;
    }

    @Override
    protected void createTable(String schema)
    {
        cluster.schemaChange(schema);
    }

    @Override
    protected void flush(String keyspace, String table)
    {
        cluster.get(1).flush(keyspace);
    }

    @Override
    public CQLVisitExecutor create(SchemaSpec schema, HistoryBuilder historyBuilder)
    {
        return InJvmDTestVisitExecutor.builder().build(schema, historyBuilder.valueGenerators(), cluster);
    }
}
