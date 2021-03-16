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

package org.apache.cassandra.distributed.test.sai;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.shared.Byteman;
import org.apache.cassandra.distributed.shared.Shared;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.sai.cql.DataModel;
import org.apache.cassandra.index.sai.cql.IndexQuerySupport;

@RunWith(Parameterized.class)
public class AbstractQueryTester extends TestBaseImpl
{
    protected static final String INJECTION_SCRIPT = "RULE count searches\n" +
                                                     "CLASS org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher\n" +
                                                     "METHOD search\n" +
                                                     "AT ENTRY\n" +
                                                     "IF TRUE\n" +
                                                     "DO\n" +
                                                     "   org.apache.cassandra.distributed.test.sai.AbstractQueryTester$Counter.increment()\n" +
                                                     "ENDRULE\n";

    @Parameterized.Parameter(0)
    public String name;
    @Parameterized.Parameter(1)
    public Supplier<DataModel> dataModel;
    @Parameterized.Parameter(2)
    public List<IndexQuerySupport.BaseQuerySet> sets;

    protected static DataModel.Executor executor;

    protected static Cluster cluster;

    @BeforeClass
    public static void setupCluster() throws Exception
    {
        cluster = Cluster.build(3)
                         .withConfig(config -> config.set("hinted_handoff_enabled", false))
                         .withInstanceInitializer((cl, nodeNumber) -> {
                             Byteman.createFromText(INJECTION_SCRIPT).install(cl);
                         })
                         .start();

        cluster.schemaChange("CREATE KEYSPACE " + DataModel.KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}");

        executor = new MultiNodeExecutor(cluster);
    }

    @AfterClass
    public static void closeCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @SuppressWarnings("unused")
    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> params() throws Throwable
    {
        List<Object[]> scenarios = new LinkedList<>();

        scenarios.add(new Object[]{ "BaseDataModel",
                                    (Supplier<DataModel>) () -> new DataModel.BaseDataModel(DataModel.NORMAL_COLUMNS, DataModel.NORMAL_COLUMN_DATA),
                                    IndexQuerySupport.BASE_QUERY_SETS });

        scenarios.add(new Object[]{ "CompoundKeyDataModel",
                                    (Supplier<DataModel>) () -> new DataModel.CompoundKeyDataModel(DataModel.NORMAL_COLUMNS, DataModel.NORMAL_COLUMN_DATA),
                                    IndexQuerySupport.BASE_QUERY_SETS });

        scenarios.add(new Object[]{ "CompoundKeyWithStaticsDataModel",
                                    (Supplier<DataModel>) () -> new DataModel.CompoundKeyWithStaticsDataModel(DataModel.STATIC_COLUMNS, DataModel.STATIC_COLUMN_DATA),
                                    IndexQuerySupport.STATIC_QUERY_SETS });

        scenarios.add(new Object[]{ "CompositePartitionKeyDataModel",
                                    (Supplier<DataModel>) () -> new DataModel.CompositePartitionKeyDataModel(DataModel.NORMAL_COLUMNS, DataModel.NORMAL_COLUMN_DATA),
                                    ImmutableList.builder().addAll(IndexQuerySupport.BASE_QUERY_SETS).addAll(IndexQuerySupport.COMPOSITE_PARTITION_QUERY_SETS).build() });

        return scenarios;
    }

    @Shared
    protected static final class Counter
    {
        protected static AtomicLong counter = new AtomicLong(0);

        public static void increment()
        {
            counter.incrementAndGet();
        }

        public static void reset()
        {
            counter.set(0);
        }

        public static long get()
        {
            return counter.get();
        }
    }
}
