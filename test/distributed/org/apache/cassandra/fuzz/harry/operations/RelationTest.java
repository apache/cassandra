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

package org.apache.cassandra.fuzz.harry.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.fuzz.harry.gen.DataGeneratorsTest;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.operations.Query;
import org.apache.cassandra.harry.operations.QueryGenerator;
import org.apache.cassandra.harry.util.BitSet;

public class RelationTest
{
    private static int RUNS = 50;

    @Test
    public void testKeyGenerators()
    {
        for (int size = 1; size < 5; size++)
        {
            Iterator<ColumnSpec.DataType[]> iter = DataGeneratorsTest.permutations(size,
                                                                                   ColumnSpec.DataType.class,
                                                                                   ColumnSpec.int8Type,
                                                                                   ColumnSpec.asciiType,
                                                                                   ColumnSpec.int16Type,
                                                                                   ColumnSpec.int32Type,
                                                                                   ColumnSpec.int64Type,
                                                                                   ColumnSpec.floatType,
                                                                                   ColumnSpec.doubleType
            );
            while (iter.hasNext())
            {
                ColumnSpec.DataType[] types = iter.next();
                List<ColumnSpec<?>> spec = new ArrayList<>(types.length);
                for (int i = 0; i < types.length; i++)
                    spec.add(ColumnSpec.ck("r" + i, types[i], false));

                SchemaSpec schemaSpec = new SchemaSpec("ks",
                                                       "tbl",
                                                       Collections.singletonList(ColumnSpec.pk("pk", ColumnSpec.int64Type)),
                                                       spec,
                                                       Collections.emptyList(),
                                                       Collections.emptyList());

                long[] cds = new long[RUNS];

                int[] fractions = new int[schemaSpec.clusteringKeys.size()];
                int last = cds.length;
                for (int i = fractions.length - 1; i >= 0; i--)
                {
                    fractions[i] = last;
                    last = last / 2;
                }

                for (int i = 0; i < cds.length; i++)
                {
                    long cd = OpSelectors.HierarchicalDescriptorSelector.cd(i, fractions, schemaSpec, new OpSelectors.PCGFast(1L), 1L);
                    cds[i] = schemaSpec.adjustPdEntropy(cd);
                }
                Arrays.sort(cds);

                OpSelectors.PureRng rng = new OpSelectors.PCGFast(1L);

                // TODO: replace with mocks?
                QueryGenerator querySelector = new QueryGenerator(schemaSpec,
                                                                  new OpSelectors.PdSelector()
                                                                  {
                                                                      protected long pd(long lts)
                                                                      {
                                                                          return lts;
                                                                      }

                                                                      public long nextLts(long lts)
                                                                      {
                                                                          throw new RuntimeException("not implemented");
                                                                      }

                                                                      public long prevLts(long lts)
                                                                      {
                                                                          throw new RuntimeException("not implemented");
                                                                      }

                                                                      public long maxLtsFor(long pd)
                                                                      {
                                                                          throw new RuntimeException("not implemented");
                                                                      }

                                                                      public long minLtsAt(long position)
                                                                      {
                                                                          throw new RuntimeException("not implemented");
                                                                      }


                                                                      public long minLtsFor(long pd)
                                                                      {
                                                                          throw new RuntimeException("not implemented");
                                                                      }

                                                                      public long maxPosition(long maxLts)
                                                                      {
                                                                          throw new RuntimeException("not implemented");
                                                                      }
                                                                  },
                                                                  new OpSelectors.DescriptorSelector()
                                                                  {
                                                                      public int operationsPerLts(long lts)
                                                                      {
                                                                          throw new RuntimeException("not implemented");
                                                                      }

                                                                      public int maxPartitionSize()
                                                                      {
                                                                          throw new RuntimeException("not implemented");
                                                                      }

                                                                      public boolean isCdVisitedBy(long pd, long lts, long cd)
                                                                      {
                                                                          throw new RuntimeException("not implemented");
                                                                      }

                                                                      protected long cd(long pd, long lts, long opId)
                                                                      {
                                                                          throw new RuntimeException("not implemented");
                                                                      }

                                                                      public long randomCd(long pd, long entropy)
                                                                      {
                                                                          return Math.abs(rng.prev(entropy)) % cds.length;
                                                                      }

                                                                      protected long vd(long pd, long cd, long lts, long opId, int col)
                                                                      {
                                                                          throw new RuntimeException("not implemented");
                                                                      }

                                                                      public OpSelectors.OperationKind operationType(long pd, long lts, long opId)
                                                                      {
                                                                          throw new RuntimeException("not implemented");
                                                                      }

                                                                      public BitSet columnMask(long pd, long lts, long opId, OpSelectors.OperationKind opType)
                                                                      {
                                                                          throw new RuntimeException("not implemented");
                                                                      }

                                                                      public long opId(long pd, long lts, long cd)
                                                                      {
                                                                          return 0;
                                                                      }
                                                                  },
                                                                  rng);

                QueryGenerator.TypedQueryGenerator gen = new QueryGenerator.TypedQueryGenerator(rng, querySelector);

                try
                {
                    for (int i = 0; i < RUNS; i++)
                    {
                        Query query = gen.inflate(i, 0);
                        for (int j = 0; j < cds.length; j++)
                        {
                            long cd = schemaSpec.ckGenerator.adjustEntropyDomain(cds[i]);
                            // the only thing we care about here is that query
                            Assert.assertEquals(String.format("Error caught while running a query %s with cd %d",
                                                              query, cd),
                                                Query.simpleMatch(query, cd),
                                                query.matchCd(cd));
                        }
                    }
                }
                catch (Throwable t)
                {
                    throw new AssertionError("Caught error for the type combination " + Arrays.toString(types), t);
                }
            }
        }
    }
}
