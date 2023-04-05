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

package org.apache.cassandra.distributed.fuzz.test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.Iterators;
import org.junit.Test;

import harry.core.Configuration;
import harry.core.Run;
import harry.ddl.ColumnSpec;
import harry.ddl.SchemaSpec;
import harry.model.Model;
import harry.model.QuiescentChecker;
import harry.model.clock.OffsetClock;
import harry.model.sut.SystemUnderTest;
import harry.operations.Query;
import harry.visitors.GeneratingVisitor;
import harry.visitors.LtsVisitor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.distributed.fuzz.FixedSchemaProviderConfiguration;
import org.apache.cassandra.distributed.fuzz.HarryHelper;
import org.apache.cassandra.distributed.fuzz.SSTableLoadingVisitor;
import org.apache.cassandra.distributed.impl.RowUtil;

public class SSTableGeneratorTest extends CQLTester
{
    private static final Configuration configuration;

    static
    {
        try
        {
            HarryHelper.init();
            configuration = HarryHelper.defaultConfiguration()
                                       .setClock(() -> new OffsetClock(10000L))
                                       .build();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static SchemaSpec schemaSpec = new SchemaSpec(KEYSPACE, "tbl1",
                                                          Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.asciiType),
                                                                        ColumnSpec.pk("pk2", ColumnSpec.int64Type)),
                                                          Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.asciiType, false),
                                                                        ColumnSpec.ck("ck2", ColumnSpec.int64Type, false)),
                                                          Arrays.asList(ColumnSpec.regularColumn("v1", ColumnSpec.int32Type),
                                                                        ColumnSpec.regularColumn("v2", ColumnSpec.int64Type),
                                                                        ColumnSpec.regularColumn("v3", ColumnSpec.int32Type),
                                                                        ColumnSpec.regularColumn("v4", ColumnSpec.asciiType)),
                                                          Arrays.asList(ColumnSpec.staticColumn("s1", ColumnSpec.asciiType),
                                                                        ColumnSpec.staticColumn("s2", ColumnSpec.int64Type)));

    @Test
    public void testSSTableGenerator()
    {
        createTable(schemaSpec.compile().cql());
        Run run = configuration.unbuild()
                               .setSchemaProvider(new FixedSchemaProviderConfiguration(schemaSpec))
                               .setSUT(CqlTesterSUT::new)
                               .build()
                               .createRun();


        SSTableLoadingVisitor sstableVisitor = new SSTableLoadingVisitor(run, 1000);
        LtsVisitor visitor = new GeneratingVisitor(run, sstableVisitor);
        Set<Long> pds = new HashSet<>();
        run.tracker.onLtsStarted((lts) -> pds.add(run.pdSelector.pd(lts, run.schemaSpec)));
        for (int i = 0; i < 1000; i++)
            visitor.visit();

        sstableVisitor.forceFlush();

        Model checker = new QuiescentChecker(run);
        for (Long pd : pds)
            checker.validate(Query.selectPartition(run.schemaSpec, pd, false));
    }

    public class CqlTesterSUT implements SystemUnderTest
    {
        public boolean isShutdown()
        {
            return false;
        }

        public void shutdown()
        {
        }

        public CompletableFuture<Object[][]> executeAsync(String s, ConsistencyLevel consistencyLevel, Object... objects)
        {
            throw new RuntimeException("Not implemented");
        }

        public Object[][] execute(String s, ConsistencyLevel consistencyLevel, Object... objects)
        {
            try
            {
                return Iterators.toArray(RowUtil.toIter(SSTableGeneratorTest.this.execute(s, objects)),
                                         Object[].class);
            }
            catch (Throwable throwable)
            {
                throw new RuntimeException();
            }
        }
    }
}
