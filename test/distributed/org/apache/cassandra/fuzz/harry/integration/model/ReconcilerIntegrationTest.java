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

package org.apache.cassandra.fuzz.harry.integration.model;

import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.dsl.ValueDescriptorIndexGenerator;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.harry.tracker.DefaultDataTracker;

public class ReconcilerIntegrationTest extends IntegrationTestBase
{
    private static final long SEED = 1;

    @Test
    public void testTrackingWithStatics() throws Throwable
    {
        SchemaSpec schema = new SchemaSpec(KEYSPACE, "tbl1",
                                           Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.int64Type)),
                                           Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.int64Type)),
                                           Arrays.asList(ColumnSpec.regularColumn("v1", ColumnSpec.int64Type),
                                                         ColumnSpec.regularColumn("v2", ColumnSpec.int64Type)),
                                           Arrays.asList(ColumnSpec.staticColumn("static1", ColumnSpec.int64Type),
                                                         ColumnSpec.staticColumn("static2", ColumnSpec.int64Type)))
                            .trackLts();

        beforeEach();
        sut.schemaChange(schema.compile().cql());

        TokenPlacementModel.ReplicationFactor rf = new TokenPlacementModel.SimpleReplicationFactor(1);
        ReplayingHistoryBuilder historyBuilder = new ReplayingHistoryBuilder(SEED, 100, 1, new DefaultDataTracker(), sut, schema, rf, SystemUnderTest.ConsistencyLevel.QUORUM);
        historyBuilder.visitPartition(1).insert(1,
                                                new long[]{ ValueDescriptorIndexGenerator.UNSET, ValueDescriptorIndexGenerator.UNSET },
                                                new long[]{ 1, 1 });
        historyBuilder.validate(1);

        historyBuilder.visitPartition(2).insert(2,
                                                new long[]{ 1, 1 },
                                                new long[]{ 1, 1 });
        historyBuilder.visitPartition(2).deleteRowRange(1, 3, true, true);
        historyBuilder.validate(2);
    }

    @Test
    public void testTrackingWithoutStatics() throws Throwable
    {
        SchemaSpec schema = new SchemaSpec(KEYSPACE, "tbl1",
                                           Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.int64Type)),
                                           Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.int64Type)),
                                           Arrays.asList(ColumnSpec.regularColumn("v1", ColumnSpec.int64Type),
                                                         ColumnSpec.regularColumn("v2", ColumnSpec.int64Type)),
                                           Arrays.asList())
                            .trackLts();

        beforeEach();
        sut.schemaChange(schema.compile().cql());

        TokenPlacementModel.ReplicationFactor rf = new TokenPlacementModel.SimpleReplicationFactor(1);
        ReplayingHistoryBuilder historyBuilder = new ReplayingHistoryBuilder(SEED, 100, 1, new DefaultDataTracker(), sut, schema, rf, SystemUnderTest.ConsistencyLevel.QUORUM);
                                                                             historyBuilder.visitPartition(2).insert(2,
                                                new long[]{ 1, 1 });
        historyBuilder.visitPartition(2).deleteRowRange(1, 3, true, true);
        historyBuilder.validate(2);

        historyBuilder = new ReplayingHistoryBuilder(SEED, 100, 1, new DefaultDataTracker(), sut, schema, rf, SystemUnderTest.ConsistencyLevel.QUORUM);
        historyBuilder.visitPartition(2).insert(2,
                                                new long[]{ 1, 1 });
        historyBuilder.visitPartition(2).deleteRowRange(1, 3, true, true);
        historyBuilder.validate(2);
    }
}
