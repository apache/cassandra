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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.fuzz.harry.integration.model.IntegrationTestBase;
import org.apache.cassandra.harry.data.ResultSetRow;
import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.gen.DataGenerators;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;
import org.apache.cassandra.harry.model.AgainstSutChecker;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.model.SelectHelper;
import org.apache.cassandra.harry.operations.CompiledStatement;
import org.apache.cassandra.harry.operations.FilteringQuery;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.operations.Relation;
import org.apache.cassandra.harry.sut.DoubleWritingSut;
import org.apache.cassandra.harry.sut.QueryModifyingSut;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.harry.tracker.DataTracker;
import org.apache.cassandra.harry.tracker.DefaultDataTracker;

public class StaticsTortureTest extends IntegrationTestBase
{
    private static final int MAX_PARTITION_SIZE = 10_000;
    private static final int NUM_PARTITIONS = 100;
    private static final int UNIQUE_CELL_VALUES = 5;

    long seed = 1;

    @Test
    public void staticsTortureTest()
    {
        CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.setInt(6);
        staticsTortureTest(Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.asciiType(4, 100)),
                                         ColumnSpec.ck("ck2", ColumnSpec.asciiType),
                                         ColumnSpec.ck("ck3", ColumnSpec.int64Type)));

        for (boolean b1 : new boolean[]{ true, false })
            for (boolean b2 : new boolean[]{ true, false })
                for (boolean b3 : new boolean[]{ true, false })
                {
                    staticsTortureTest(Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.asciiType(4, 100), b1),
                                                     ColumnSpec.ck("ck2", ColumnSpec.asciiType, b2),
                                                     ColumnSpec.ck("ck3", ColumnSpec.int64Type, b3)));
                }
    }

    public void staticsTortureTest(List<ColumnSpec<?>> cks)
    {
        SchemaSpec schema = new SchemaSpec(KEYSPACE, "tbl" + (seed++),
                                           Arrays.asList(ColumnSpec.ck("pk1", ColumnSpec.int64Type),
                                                         ColumnSpec.ck("pk2", ColumnSpec.asciiType(4, 100)),
                                                         ColumnSpec.ck("pk3", ColumnSpec.int64Type)),
                                           cks,
                                           Arrays.asList(ColumnSpec.regularColumn("v1", ColumnSpec.asciiType(40, 100)),
                                                         ColumnSpec.regularColumn("v2", ColumnSpec.int64Type),
                                                         ColumnSpec.regularColumn("v3", ColumnSpec.int64Type)),
                                           List.of(ColumnSpec.staticColumn("s1", ColumnSpec.asciiType(40, 100)),
                                                   ColumnSpec.staticColumn("s2", ColumnSpec.int64Type),
                                                   ColumnSpec.staticColumn("s3", ColumnSpec.asciiType(40, 100))
                                           ));

        sut.schemaChange(schema.compile().cql());
        SchemaSpec debugSchema = schema.cloneWithName(schema.keyspace, schema.table + "_debug");
        sut.schemaChange(schema.cloneWithName(schema.keyspace, schema.table + "_debug").compile().cql());
        sut.schemaChange(String.format("CREATE INDEX %s_%s_sai_idx ON %s.%s (%s) USING 'sai' " +
                                       "WITH OPTIONS = {'case_sensitive': 'false', 'normalize': 'true', 'ascii': 'true'};",
                                       schema.table,
                                       schema.regularColumns.get(0).name,
                                       schema.keyspace,
                                       schema.table,
                                       schema.regularColumns.get(0).name));
        sut.schemaChange(String.format("CREATE INDEX %s_%s_sai_idx ON %s.%s (%s) USING 'sai';",
                                       schema.table,
                                       schema.regularColumns.get(1).name,
                                       schema.keyspace,
                                       schema.table,
                                       schema.regularColumns.get(1).name));
        sut.schemaChange(String.format("CREATE INDEX %s_%s_sai_idx ON %s.%s (%s) USING 'sai';",
                                       schema.table,
                                       schema.regularColumns.get(2).name,
                                       schema.keyspace,
                                       schema.table,
                                       schema.regularColumns.get(2).name));
        sut.schemaChange(String.format("CREATE INDEX %s_%s_sai_idx ON %s.%s (%s) USING 'sai';",
                                       schema.table,
                                       schema.staticColumns.get(0).name,
                                       schema.keyspace,
                                       schema.table,
                                       schema.staticColumns.get(0).name));
        sut.schemaChange(String.format("CREATE INDEX %s_%s_sai_idx ON %s.%s (%s) USING 'sai';",
                                       schema.table,
                                       schema.staticColumns.get(1).name,
                                       schema.keyspace,
                                       schema.table,
                                       schema.staticColumns.get(1).name));
        sut.schemaChange(String.format("CREATE INDEX %s_%s_sai_idx ON %s.%s (%s) USING 'sai';",
                                       schema.table,
                                       schema.staticColumns.get(2).name,
                                       schema.keyspace,
                                       schema.table,
                                       schema.staticColumns.get(2).name));
        DataTracker tracker = new DefaultDataTracker();
        TokenPlacementModel.ReplicationFactor rf = new TokenPlacementModel.SimpleReplicationFactor(cluster.size());
        ReplayingHistoryBuilder history = new ReplayingHistoryBuilder(seed,
                                                                      MAX_PARTITION_SIZE,
                                                                      100,
                                                                      tracker,
                                                                      new DoubleWritingSut(sut,
                                                                                           new QueryModifyingSut(sut,
                                                                                                                 schema.keyspace + "." + schema.table,
                                                                                                                 debugSchema.keyspace + "." + debugSchema.table)),
                                                                      schema,
                                                                      rf,
                                                                      SystemUnderTest.ConsistencyLevel.QUORUM);

        EntropySource rng = new JdkRandomEntropySource(1l);
        long[] values = new long[UNIQUE_CELL_VALUES];
        for (int i = 0; i < values.length; i++)
            values[i] = rng.next();

        for (int i = 0; i < NUM_PARTITIONS; i++)
        {
            history.visitPartition(i)
                   .insert(1,
                           new long[]{ rng.nextBoolean() ? DataGenerators.UNSET_DESCR : values[rng.nextInt(values.length)],
                                       rng.nextBoolean() ? DataGenerators.UNSET_DESCR : values[rng.nextInt(values.length)],
                                       rng.nextBoolean() ? DataGenerators.UNSET_DESCR : values[rng.nextInt(values.length)]
                           },
                           new long[]{ rng.nextBoolean() ? DataGenerators.UNSET_DESCR : values[rng.nextInt(values.length)],
                                       rng.nextBoolean() ? DataGenerators.UNSET_DESCR : values[rng.nextInt(values.length)],
                                       rng.nextBoolean() ? DataGenerators.UNSET_DESCR : values[rng.nextInt(values.length)]
                   });
            history.visitPartition(i)
                   .insert(5,
                           new long[]{ rng.nextBoolean() ? DataGenerators.UNSET_DESCR : values[rng.nextInt(values.length)],
                                       rng.nextBoolean() ? DataGenerators.UNSET_DESCR : values[rng.nextInt(values.length)],
                                       rng.nextBoolean() ? DataGenerators.UNSET_DESCR : values[rng.nextInt(values.length)]
                           },
                           new long[]{ rng.nextBoolean() ? DataGenerators.UNSET_DESCR : values[rng.nextInt(values.length)],
                                       rng.nextBoolean() ? DataGenerators.UNSET_DESCR : values[rng.nextInt(values.length)],
                                       rng.nextBoolean() ? DataGenerators.UNSET_DESCR : values[rng.nextInt(values.length)]
                   });

            if (rng.nextFloat() > 0.9f)
            {
                history.visitPartition(i)
                       .deleteRowRange(rng.nextInt(5), rng.nextInt(5), rng.nextBoolean(), rng.nextBoolean());
            }

            if (rng.nextFloat() > 0.9f)
            {
                history.visitPartition(i)
                       .deleteColumns();
            }

            if (i % 50 == 0)
                cluster.get(1).nodetool("flush", schema.keyspace, schema.table);
        }

        Model model = new AgainstSutChecker(tracker, history.clock(), sut, schema, schema.cloneWithName(schema.keyspace, debugSchema.table)) {
            @Override
            protected List<ResultSetRow> executeOnDebugSchema(Query query)
            {
                CompiledStatement s2 = query.toSelectStatement(doubleWriteTable.allColumnsSet, true)
                                            .withSchema(schema.keyspace, schema.table, doubleWriteTable.keyspace, doubleWriteTable.table)
                                            .withFiltering();
                return SelectHelper.execute(sut, clock, s2, schema);
            }
        };

        for (int pdx = 0; pdx < NUM_PARTITIONS; pdx++)
        {
            long pd = history.presetSelector.pdAtPosition(pdx);
            history.presetSelector.pdAtPosition(1);
            for (int i1 = 0; i1 < values.length; i1++)
                for (int i2 = 0; i2 < values.length; i2++)
                    for (int i3 = 0; i3 < values.length; i3++)
                    {
                        long[] descriptors = new long[]{ values[i1], values[i2], values[i3],
                                                         values[i1], values[i2], values[i3] };
                        List<Relation> relations = new ArrayList<>();
                        Stream.concat(schema.regularColumns.stream(),
                                      schema.staticColumns.stream())
                              .forEach(new Consumer<>()
                              {
                                  int counter = 0;

                                  @Override
                                  public void accept(ColumnSpec<?> column)
                                  {
                                      if (rng.nextBoolean())
                                          return;

                                      if (column.type.toString().equals(ColumnSpec.int64Type.toString()))
                                      {
                                          if (rng.nextBoolean())
                                          {
                                              relations.add(Relation.relation(Relation.RelationKind.EQ,
                                                                              column,
                                                                              descriptors[counter]));
                                          }
                                          else
                                          {
                                              Relation.relation(rng.nextBoolean() ? Relation.RelationKind.LT : Relation.RelationKind.GT,
                                                                column,
                                                                descriptors[counter]);
                                          }
                                      }
                                      else
                                      {
                                          Relation.relation(Relation.RelationKind.EQ,
                                                            column,
                                                            descriptors[counter]);
                                      }

                                      counter++;
                                  }
                              });

                        // Without partition key
                        model.validate(new FilteringQuery(-1, false, relations, schema)
                        {
                            @Override
                            public CompiledStatement toSelectStatement()
                            {
                                return SelectHelper.select(schemaSpec, null, schemaSpec.allColumnsSet, relations, reverse, true);
                            }
                        });
                        model.validate(new FilteringQuery(pd, false, relations, schema));
                    }
        }
    }
}