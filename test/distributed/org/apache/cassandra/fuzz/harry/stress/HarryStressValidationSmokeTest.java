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

package org.apache.cassandra.fuzz.harry.stress;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.stress.HarryStress;
import org.apache.cassandra.harry.stress.RotationStrategy;
import org.apache.cassandra.harry.stress.VisitGenerator;
import org.apache.cassandra.harry.stress.distribution.Distributions;

import static java.util.Arrays.asList;
import static org.apache.cassandra.harry.ColumnSpec.asciiType;
import static org.apache.cassandra.harry.ColumnSpec.ck;
import static org.apache.cassandra.harry.ColumnSpec.int64Type;
import static org.apache.cassandra.harry.ColumnSpec.pk;
import static org.apache.cassandra.harry.ColumnSpec.regularColumn;

public class HarryStressValidationSmokeTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(HarryStressValidationSmokeTest.class);

    private static final String TABLE = "stress_smoke";

    private static final long ITERATIONS = 100_000;
    private static final int NUM_PARTITIONS = 100;
    private static final int REPLACE_WITH_NEW = 10;
    private static final int ROW_POPULATION = 100;
    private static final int COLUMN_POPULATION = 100;
    private static final int CONCURRENCY = 2;
    private static final int RATE_PER_SECOND = 20_000;

    @Test
    public void validatingStressSmokeTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).start(), 1))
        {
            SchemaSpec schema = schema();
            cluster.schemaChange(schema.compile());

            HarryStress stress =
                new HarryStress(schema,
                                Distributions.fixed(ROW_POPULATION),
                                column -> Distributions.fixed(COLUMN_POPULATION),
                                Generators.pick(VisitGenerator.VisitType.values()),
                                Distributions.fixed(1),
                                new VisitGenerator.RandomOpKindGenFactory(),
                                new RotationStrategy.FixedRotationStrategy(NUM_PARTITIONS, REPLACE_WITH_NEW, 0),
                                null,
                                30,
                                () -> (statement, onComplete) -> {
                                    Object[][] rows = cluster.coordinator(1).execute(statement.cql(),
                                                                                     ConsistencyLevel.QUORUM,
                                                                                     statement.bindings());
                                    if (onComplete != null)
                                        onComplete.run();
                                    return rows;
                                },
                                CONCURRENCY,
                                RATE_PER_SECOND,
                                0,
                                Long.MAX_VALUE,
                                0);

            stress.start(ITERATIONS, Long.MAX_VALUE);
            logger.info("Validating stress smoke test ran ~{} visits across {} partitions with no validation failures",
                        ITERATIONS, NUM_PARTITIONS);
        }
    }

    private static SchemaSpec schema()
    {
        return new SchemaSpec(KEYSPACE, TABLE,
                              asList(pk("pk1", asciiType), pk("pk2", int64Type)),
                              asList(ck("ck1", asciiType, false), ck("ck2", int64Type, false)),
                              asList(regularColumn("v1", asciiType), regularColumn("v2", int64Type), regularColumn("v3", asciiType)),
                              asList(),
                              SchemaSpec.optionsBuilder().ifNotExists(true).addWriteTimestamps(true));
    }
}
