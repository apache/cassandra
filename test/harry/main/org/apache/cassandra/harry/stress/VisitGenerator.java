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

package org.apache.cassandra.harry.stress;

import accord.utils.UnhandledEnum;
import org.apache.cassandra.harry.MagicConstants;
import org.apache.cassandra.harry.Relations;
import org.apache.cassandra.harry.dsl.SingleOperationVisitBuilder;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.Surjections;
import org.apache.cassandra.harry.gen.rng.SeedableEntropySource;
import org.apache.cassandra.harry.op.ClusteringOrderBy;
import org.apache.cassandra.harry.op.Kind;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.harry.stress.distribution.Distribution;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.cassandra.harry.op.Operations.Operation;
import static org.apache.cassandra.harry.op.Operations.PartitionOperation;

/**
 * A stateful generator for operation steps.
 *
 * Generator maintains a list of active partitions. On each logical timestamp, a subset of these
 * partitions is chosen for visit.
 */
public class VisitGenerator implements Surjections.Surjection<Visit>, Supplier<Visit>
{
    final ActivePartition.Partitions partitions;
    final Generator<VisitType> visitTypeGen;

    final Distribution visitSizeDistribution;
    final OpKindGenFactory operationKindGen;

    // The target number of inserts to split a partition into; if more than one, the partition will be placed in the revisit set
    private final AtomicLong lts = new AtomicLong();

    public VisitGenerator(ActivePartition.Partitions partitions,
                          Generator<VisitType> visitTypeGen,
                          Distribution visitSizeDistribution,
                          OpKindGenFactory operationKindGen)
    {
        this(partitions, visitTypeGen, visitSizeDistribution, operationKindGen, 0);
    }

    public VisitGenerator(ActivePartition.Partitions partitions,
                          Generator<VisitType> visitTypeGen,
                          Distribution visitSizeDistribution,
                          OpKindGenFactory operationKindGen,
                          long initialLts)
    {
        this.partitions = partitions;
        this.visitTypeGen = visitTypeGen;
        this.visitSizeDistribution = visitSizeDistribution;
        this.operationKindGen = operationKindGen;
        this.lts.set(initialLts);
    }

    public interface ColumnPopulation
    {
        Distribution distribution(String column);
    }

    public interface OpKindGenFactory
    {
        Generator<Kind> forType(VisitType type);
    }

    @Override
    public Visit inflate(long lts)
    {
        Visit visit;
        VisitType visitType = SeedableEntropySource.computeWithSeed(lts, visitTypeGen::generate);
        switch (visitType)
        {
            case MUTATE:
                visit = mutatingVisit(lts);
                break;
            case VALIDATE:
                visit = validatingVisit(lts);
                break;
            default:
                throw UnhandledEnum.unknown(visitType);
        }
        return visit;
    }

    @Override
    public Visit get()
    {
        return inflate(this.lts.getAndIncrement());
    }

    private Visit mutatingVisit(long lts)
    {
        return mutatingVisit(lts, visitSizeDistribution, operationKindGen, partitions::pick);
    }

    public static Visit mutatingVisit(long lts, Distribution visitSizeDistribution, OpKindGenFactory operationKindGen, Function<EntropySource, ActivePartition> partitionPicker)
    {
        Operation[] operations = new Operation[Math.toIntExact(visitSizeDistribution.next(lts))];
        for (int op = 0; op < operations.length; op++)
        {
            // If you are changing choosing here, make sure to also change TokenIndexGenerator#generate
            ActivePartition partition = SeedableEntropySource.computeWithSeed(lts, ~op, partitionPicker);
            operations[op] = SeedableEntropySource.computeWithSeed(lts, op, rng -> {
                Kind kind = operationKindGen.forType(VisitType.MUTATE).generate(rng);
                return mutatingOperation(partition, kind, lts, rng);
            });
        }
        return new Visit(lts, operations);
    }

    static PartitionOperation mutatingOperation(ActivePartition partition, Kind kind, long lts, EntropySource rng)
    {
        // TODO: more sophisticated picking
        int cdIdx = rng.nextInt(partition.ckPopulation());

        int[] valueIdxs = new int[partition.regularColumnCount()];
        for (int i = 0; i < valueIdxs.length; i++)
            valueIdxs[i] = rng.nextInt(partition.regularPopulation(i));
        int[] sValueIdxs = new int[partition.staticColumnCount()];
        for (int i = 0; i < sValueIdxs.length; i++)
            sValueIdxs[i] = rng.nextInt(partition.staticColumnCount());
        return SingleOperationVisitBuilder.write(partition.pkGen(),
                                                 partition,
                                                 lts,
                                                 partition.idx,
                                                 cdIdx,
                                                 valueIdxs,
                                                 sValueIdxs,
                                                 kind);
    }

    private Visit validatingVisit(long lts)
    {
        ActivePartition partition = SeedableEntropySource.computeWithSeed(lts, ~0, partitions::pick);
        return SeedableEntropySource.computeWithSeed(lts, rng -> {
            Kind kind = operationKindGen.forType(VisitType.VALIDATE).generate(rng);
            return new Visit(lts, new Operation[] { validatingOperation(partition, kind, lts, rng) });
        });
    }

    private static PartitionOperation validatingOperation(ActivePartition partition, Kind kind, long lts, EntropySource rng)
    {
        switch (kind)
        {
            case CUSTOM:
            case UPDATE:
            case INSERT:
            case DELETE_PARTITION:
            case DELETE_ROW:
            case DELETE_COLUMNS:
            case DELETE_RANGE:
                throw new IllegalArgumentException("Validating operation can only be SELECT");
            case SELECT_PARTITION:
                return new Operations.SelectPartition(lts, partition.pd, ClusteringOrderBy.ASC);
            case SELECT_ROW:
                // ckIdxGen yields a clustering index; convert it to the clustering descriptor (cd) the SelectRow
                // expects, exactly as SELECT_RANGE does for its bound below.
                return new Operations.SelectRow(lts, partition.pd, partition.ckGen().descriptorAt(partition.ckIdxGen.generate(rng)));
            case SELECT_RANGE:
                long lowerBoundIdx = partition.ckIdxGen.generate(rng);
                long lowerBoundCd = partition.ckGen().descriptorAt(lowerBoundIdx);

                int nonEqFrom = rng.nextInt(partition.ckColumnCount());
                boolean includeBound = rng.nextBoolean();

                Relations.RelationKind[] lowerBoundRelations = new Relations.RelationKind[partition.ckColumnCount()];
                for (int i = 0; i < Math.min(nonEqFrom + 1, partition.ckColumnCount()); i++)
                {
                    if (i < nonEqFrom)
                        lowerBoundRelations[i] = Relations.RelationKind.EQ;
                    else
                        lowerBoundRelations[i] = includeBound ? Relations.RelationKind.GTE : Relations.RelationKind.GT;
                }

                return new Operations.SelectRange(lts, partition.pd,
                                                  lowerBoundCd, MagicConstants.UNSET_DESCR,
                                                  lowerBoundRelations, null);
            case SELECT_CUSTOM:
            default:
                throw new UnsupportedOperationException(kind + " is not supported");
        }
    }

    // TODO (required) The percent of a given rows columns to populate
    public enum VisitType
    {
        MUTATE,
        VALIDATE
        // TODO: custom actions: repair, expand/shrink cluster
        // TODO: TCM actions, such as ALTER TABLE, etc, too?
    }

    public static class RandomOpKindGenFactory implements OpKindGenFactory
    {
        public static Kind[] VALIDATE_KINDS = new Kind[] { Kind.SELECT_PARTITION, Kind.SELECT_ROW, Kind.SELECT_RANGE };
        public static Kind[] MUTATE_KINDS = new Kind[] { Kind.INSERT, Kind.UPDATE };

        private final Generator<Kind> mutateGen = Generators.pick(MUTATE_KINDS);
        private final Generator<Kind> validateGen = Generators.pick(VALIDATE_KINDS);

        @Override
        public Generator<Kind> forType(VisitGenerator.VisitType type)
        {
            switch (type)
            {
                case MUTATE: return mutateGen;
                case VALIDATE: return validateGen;
                default: throw new AssertionError();
            }
        }
    }

    public static class WeightedOpKindGenFactory implements OpKindGenFactory
    {
        private final Generator<Kind> mutateGen;
        private final Generator<Kind> validateGen;

        public WeightedOpKindGenFactory(Map<Kind, Integer> mutateWeights, Map<Kind, Integer> validateWeights)
        {
            this.mutateGen = Generators.weighted(mutateWeights);
            this.validateGen = Generators.weighted(validateWeights);
        }

        @Override
        public Generator<Kind> forType(VisitType type)
        {
            switch (type)
            {
                case MUTATE: return mutateGen;
                case VALIDATE: return validateGen;
                default: throw new AssertionError();
            }
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public static class Builder
        {
            private final Map<Kind, Integer> mutateWeights = new LinkedHashMap<>();
            private final Map<Kind, Integer> validateWeights = new LinkedHashMap<>();

            public Builder()
            {
                mutateWeights.put(Kind.INSERT, 1);
                mutateWeights.put(Kind.UPDATE, 1);
                validateWeights.put(Kind.SELECT_PARTITION, 1);
                validateWeights.put(Kind.SELECT_ROW, 1);
                validateWeights.put(Kind.SELECT_RANGE, 1);
            }

            public Builder mutate(Kind kind, int weight)
            {
                mutateWeights.put(kind, weight);
                return this;
            }

            public Builder validate(Kind kind, int weight)
            {
                validateWeights.put(kind, weight);
                return this;
            }

            public WeightedOpKindGenFactory build()
            {
                return new WeightedOpKindGenFactory(mutateWeights, validateWeights);
            }
        }
    }
}
