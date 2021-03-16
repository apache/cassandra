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

import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher;
import org.apache.cassandra.inject.Injections;

import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;

@RunWith(Parameterized.class)
public class AbstractQueryTester extends SAITester
{
    protected static final Injections.Counter INDEX_QUERY_COUNTER = Injections.newCounter("IndexQueryCounter")
                                                                              .add(newInvokePoint().onClass(StorageAttachedIndexSearcher.class).onMethod("search"))
                                                                              .build();

    @Parameterized.Parameter(0)
    public DataModel dataModel;
    @Parameterized.Parameter(1)
    public List<IndexQuerySupport.BaseQuerySet> sets;

    protected DataModel.Executor executor;

    @Before
    public void setup() throws Throwable
    {
        requireNetwork();

        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", DataModel.KEYSPACE));

        Injections.inject(INDEX_QUERY_COUNTER);

        executor = new SingleNodeExecutor(this, INDEX_QUERY_COUNTER);
    }

    @SuppressWarnings("unused")
    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> params() throws Throwable
    {
        List<Object[]> scenarios = new LinkedList<>();

        scenarios.add(new Object[]{ new DataModel.BaseDataModel(DataModel.NORMAL_COLUMNS, DataModel.NORMAL_COLUMN_DATA), IndexQuerySupport.BASE_QUERY_SETS });

        scenarios.add(new Object[]{ new DataModel.CompoundKeyDataModel(DataModel.NORMAL_COLUMNS, DataModel.NORMAL_COLUMN_DATA), IndexQuerySupport.BASE_QUERY_SETS });

        scenarios.add(new Object[]{ new DataModel.CompoundKeyWithStaticsDataModel(DataModel.STATIC_COLUMNS, DataModel.STATIC_COLUMN_DATA), IndexQuerySupport.STATIC_QUERY_SETS });

        scenarios.add(new Object[]{ new DataModel.CompositePartitionKeyDataModel(DataModel.NORMAL_COLUMNS, DataModel.NORMAL_COLUMN_DATA),
                                    ImmutableList.builder().addAll(IndexQuerySupport.BASE_QUERY_SETS).addAll(IndexQuerySupport.COMPOSITE_PARTITION_QUERY_SETS).build()});

        return scenarios;
    }
}
