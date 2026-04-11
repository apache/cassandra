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

package org.apache.cassandra.fuzz.sai;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.test.IntegrationTestBase;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.MagicConstants;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;
import org.apache.cassandra.harry.util.BitSet;

import static org.apache.cassandra.harry.dsl.HistoryBuilderHelper.generateClusteringRelations;
import static org.apache.cassandra.harry.dsl.HistoryBuilderHelper.generateValueRelations;
import static org.apache.cassandra.harry.dsl.SingleOperationBuilder.IdxRelation;

public class StaticsTortureTest extends IntegrationTestBase
{
    private static final int MAX_PARTITION_SIZE = 10_000;
    private static final int NUM_PARTITIONS = 100;
    private static final int UNIQUE_CELL_VALUES = 5;

    @Test
    public void staticsTortureTest()
    {
        CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.setInt(6);
        int idx = 0;
        for (boolean b1 : new boolean[]{ false, true })
            for (boolean b2 : new boolean[]{ false, true })
                for (boolean b3 : new boolean[]{ false, true })
                {
                    staticsTortureTest(Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.asciiType, Generators.ascii(4, 1000), b1),
                                                     ColumnSpec.ck("ck2", ColumnSpec.asciiType, Generators.ascii(4, 1000), b2),
                                                     ColumnSpec.ck("ck3", ColumnSpec.int64Type, Generators.int64(), b3)),
                                       idx++);
                }
    }

    public void staticsTortureTest(List<ColumnSpec<?>> cks, int idx)
    {
        SchemaSpec schema = new SchemaSpec(idx,
                                           10_000,
                                           KEYSPACE,
                                           "tbl" + idx,
                                           Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.int64Type, Generators.int64()),
                                                         ColumnSpec.pk("pk2", ColumnSpec.asciiType, Generators.ascii(4, 1000)),
                                                         ColumnSpec.pk("pk3", ColumnSpec.int64Type, Generators.int64())),
                                           cks,
                                           Arrays.asList(ColumnSpec.regularColumn("v1", ColumnSpec.asciiType, Generators.ascii(4, 1000)),
                                                         ColumnSpec.regularColumn("v2", ColumnSpec.int64Type),
                                                         ColumnSpec.regularColumn("v3", ColumnSpec.int64Type)),
                                           Arrays.asList(ColumnSpec.staticColumn("s1", ColumnSpec.asciiType, Generators.ascii(4, 1000)),
                                                         ColumnSpec.staticColumn("s2", ColumnSpec.int64Type),
                                                         ColumnSpec.staticColumn("s3", ColumnSpec.asciiType, Generators.ascii(4, 1000))));


        cluster.schemaChange(schema.compile());
        cluster.get(1).nodetool("disableautocompaction");
        cluster.schemaChange(String.format("CREATE INDEX %s_%s_sai_idx ON %s.%s (%s) USING 'sai' " +
                                       "WITH OPTIONS = {'case_sensitive': 'false', 'normalize': 'true', 'ascii': 'true'};",
                                       schema.table,
                                       schema.regularColumns.get(0).name,
                                       schema.keyspace,
                                       schema.table,
                                       schema.regularColumns.get(0).name));
        cluster.schemaChange(String.format("CREATE INDEX %s_%s_sai_idx ON %s.%s (%s) USING 'sai';",
                                       schema.table,
                                       schema.regularColumns.get(1).name,
                                       schema.keyspace,
                                       schema.table,
                                       schema.regularColumns.get(1).name));
        cluster.schemaChange(String.format("CREATE INDEX %s_%s_sai_idx ON %s.%s (%s) USING 'sai';",
                                       schema.table,
                                       schema.regularColumns.get(2).name,
                                       schema.keyspace,
                                       schema.table,
                                       schema.regularColumns.get(2).name));
        cluster.schemaChange(String.format("CREATE INDEX %s_%s_sai_idx ON %s.%s (%s) USING 'sai';",
                                       schema.table,
                                       schema.staticColumns.get(0).name,
                                       schema.keyspace,
                                       schema.table,
                                       schema.staticColumns.get(0).name));
        cluster.schemaChange(String.format("CREATE INDEX %s_%s_sai_idx ON %s.%s (%s) USING 'sai';",
                                       schema.table,
                                       schema.staticColumns.get(1).name,
                                       schema.keyspace,
                                       schema.table,
                                       schema.staticColumns.get(1).name));
        cluster.schemaChange(String.format("CREATE INDEX %s_%s_sai_idx ON %s.%s (%s) USING 'sai';",
                                       schema.table,
                                       schema.staticColumns.get(2).name,
                                       schema.keyspace,
                                       schema.table,
                                       schema.staticColumns.get(2).name));

        Generator<BitSet> regularColumnBitSet = Generators.bitSet(schema.regularColumns.size());
        Generator<BitSet> staticColumnBitSet = Generators.bitSet(schema.staticColumns.size());
        EntropySource rng = new JdkRandomEntropySource(1l);

        ReplayingHistoryBuilder history = new ReplayingHistoryBuilder(schema.valueGenerators, hb -> {
            return InJvmDTestVisitExecutor.builder().pageSizeSelector(i -> rng.nextInt(1, 10)).build(schema, hb, cluster);
        });


        for (int i = 0; i < NUM_PARTITIONS; i++)
        {
            history.insert(i, rng.nextInt(5),
                           new int[]{ rng.nextBoolean() ? MagicConstants.UNSET_IDX : rng.nextInt(UNIQUE_CELL_VALUES),
                                      rng.nextBoolean() ? MagicConstants.UNSET_IDX : rng.nextInt(UNIQUE_CELL_VALUES),
                                      rng.nextBoolean() ? MagicConstants.UNSET_IDX : rng.nextInt(UNIQUE_CELL_VALUES)
                           },
                           new int[]{ rng.nextBoolean() ? MagicConstants.UNSET_IDX : rng.nextInt(UNIQUE_CELL_VALUES),
                                      rng.nextBoolean() ? MagicConstants.UNSET_IDX : rng.nextInt(UNIQUE_CELL_VALUES),
                                      rng.nextBoolean() ? MagicConstants.UNSET_IDX : rng.nextInt(UNIQUE_CELL_VALUES)
                           });
            history.insert(i,rng.nextInt(5),
                           new int[]{ rng.nextBoolean() ? MagicConstants.UNSET_IDX : rng.nextInt(UNIQUE_CELL_VALUES),
                                      rng.nextBoolean() ? MagicConstants.UNSET_IDX : rng.nextInt(UNIQUE_CELL_VALUES),
                                      rng.nextBoolean() ? MagicConstants.UNSET_IDX : rng.nextInt(UNIQUE_CELL_VALUES)
                           },
                           new int[]{ rng.nextBoolean() ? MagicConstants.UNSET_IDX : rng.nextInt(UNIQUE_CELL_VALUES),
                                      rng.nextBoolean() ? MagicConstants.UNSET_IDX : rng.nextInt(UNIQUE_CELL_VALUES),
                                      rng.nextBoolean() ? MagicConstants.UNSET_IDX : rng.nextInt(UNIQUE_CELL_VALUES)
                           });

            if (rng.nextFloat() > 0.9f)
            {
                history.deleteRowRange(i, rng.nextInt(5), rng.nextInt(5),
                                       rng.nextInt(1, schema.clusteringKeys.size()),
                                       rng.nextBoolean(), rng.nextBoolean());
            }

            if (rng.nextFloat() > 0.9f)
            {
                history.deleteColumns(i, rng.nextInt(5), regularColumnBitSet.generate(rng), staticColumnBitSet.generate(rng));
            }

            if (i % 50 == 0)
                cluster.get(1).nodetool("flush", schema.keyspace, schema.table);

            if (i % 100 == 0)
                cluster.get(1).nodetool("compact", schema.keyspace, schema.table);
        }

        Generator<Integer> ckIdxGen = Generators.int32(0, MAX_PARTITION_SIZE);
        for (int pdx = 0; pdx < NUM_PARTITIONS; pdx++)
        {
            for (int i = 0; i < 10; i++)
            {
                List<IdxRelation> ckRelations = generateClusteringRelations(rng, schema.clusteringKeys.size(), ckIdxGen);
                List<IdxRelation> regularRelations = generateValueRelations(rng, schema.regularColumns.size(),
                                                                            column -> Math.min(schema.valueGenerators.regularPopulation(column), MAX_PARTITION_SIZE));
                List<IdxRelation> staticRelations = generateValueRelations(rng, schema.staticColumns.size(),
                                                                           column -> Math.min(schema.valueGenerators.staticPopulation(column), MAX_PARTITION_SIZE));
                history.select(pdx,
                               ckRelations.toArray(new IdxRelation[0]),
                               regularRelations.toArray(new IdxRelation[0]),
                               staticRelations.toArray(new IdxRelation[0]));
            }
        }
    }
}