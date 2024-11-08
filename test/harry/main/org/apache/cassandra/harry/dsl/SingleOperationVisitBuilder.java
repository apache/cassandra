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

package org.apache.cassandra.harry.dsl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;
import org.apache.cassandra.harry.gen.IndexGenerators;
import org.apache.cassandra.harry.gen.rng.PCGFastPure;
import org.apache.cassandra.harry.gen.rng.PureRng;
import org.apache.cassandra.harry.gen.rng.SeedableEntropySource;
import org.apache.cassandra.harry.MagicConstants;
import org.apache.cassandra.harry.Relations;
import org.apache.cassandra.harry.gen.ValueGenerators;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.harry.util.BitSet;

import static org.apache.cassandra.harry.op.Operations.Kind;
import static org.apache.cassandra.harry.op.Operations.Operation;
import static org.apache.cassandra.harry.op.Operations.WriteOp;

class SingleOperationVisitBuilder implements SingleOperationBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(SingleOperationVisitBuilder.class);

    // TODO: singleton collection for this op class
    protected final List<Operation> operations;

    protected final long lts;

    protected final Consumer<Visit> appendToLog;
    protected final SeedableEntropySource rngSupplier = new SeedableEntropySource();
    protected final PureRng seedSelector;

    protected int opIdCounter;

    protected final ValueGenerators valueGenerators;
    protected final IndexGenerators indexGenerators;

    SingleOperationVisitBuilder(long lts,
                                ValueGenerators valueGenerators,
                                IndexGenerators indexGenerators,
                                Consumer<Visit> appendToLog)
    {
        this.operations = new ArrayList<>();
        this.lts = lts;

        this.appendToLog = appendToLog;
        this.opIdCounter = 0;

        this.valueGenerators = valueGenerators;
        this.indexGenerators = indexGenerators;

        this.seedSelector = new PureRng.PCGFast(lts);
    }

    @Override
    public SingleOperationBuilder insert()
    {
        int opId = opIdCounter++;
        long seed = PCGFastPure.shuffle(PCGFastPure.advanceState(lts, opId, lts));
        return rngSupplier.computeWithSeed(seed, rng -> {
            int pdIdx = indexGenerators.pkIdxGen.generate(rng);
            int cdIdx = indexGenerators.ckIdxGen.generate(rng);

            int[] valueIdxs = new int[indexGenerators.regularIdxGens.length];
            for (int i = 0; i < valueIdxs.length; i++)
                valueIdxs[i] = indexGenerators.regularIdxGens[i].generate(rng);
            int[] sValueIdxs = new int[indexGenerators.staticIdxGens.length];
            for (int i = 0; i < sValueIdxs.length; i++)
                sValueIdxs[i] = indexGenerators.staticIdxGens[i].generate(rng);
            return insert(pdIdx, cdIdx, valueIdxs, sValueIdxs);
        });
    }

    @Override
    public SingleOperationBuilder insert(int pdIdx)
    {
        int opId = opIdCounter++;
        long seed = PCGFastPure.shuffle(PCGFastPure.advanceState(lts, opId, lts));
        return rngSupplier.computeWithSeed(seed, rng -> {
            int cdIdx = indexGenerators.ckIdxGen.generate(rng);

            int[] valueIdxs = new int[indexGenerators.regularIdxGens.length];
            for (int i = 0; i < valueIdxs.length; i++)
                valueIdxs[i] = indexGenerators.regularIdxGens[i].generate(rng);
            int[] sValueIdxs = new int[indexGenerators.staticIdxGens.length];
            for (int i = 0; i < sValueIdxs.length; i++)
                sValueIdxs[i] = indexGenerators.staticIdxGens[i].generate(rng);
            return insert(pdIdx, cdIdx, valueIdxs, sValueIdxs);
        });
    }

    @Override
    public SingleOperationBuilder insert(int pdIdx, int cdIdx)
    {
        int opId = opIdCounter++;
        long seed = PCGFastPure.shuffle(PCGFastPure.advanceState(lts, opId, lts));
        return rngSupplier.computeWithSeed(seed, rng -> {
            int[] valueIdxs = new int[indexGenerators.regularIdxGens.length];
            for (int i = 0; i < valueIdxs.length; i++)
                valueIdxs[i] = indexGenerators.regularIdxGens[i].generate(rng);
            int[] sValueIdxs = new int[indexGenerators.staticIdxGens.length];
            for (int i = 0; i < sValueIdxs.length; i++)
                sValueIdxs[i] = indexGenerators.staticIdxGens[i].generate(rng);
            return insert(pdIdx, cdIdx, valueIdxs, sValueIdxs);
        });
    }

    @Override
    public SingleOperationBuilder insert(int pdIdx, int cdIdx, int[] valueIdxs, int[] sValueIdxs)
    {
        return write(pdIdx, cdIdx, valueIdxs, sValueIdxs, Kind.INSERT);
    }

    @Override
    public SingleOperationBuilder custom(Runnable runnable, String tag)
    {
        // TODO: assert that custom op is always alone in visit
        operations.add(new Operations.CustomRunnableOperation(lts, opIdCounter, runnable) {
            @Override
            public String toString()
            {
                return String.format("%s (%s)", Kind.CUSTOM, tag);
            }
        });
        build();
        return this;
    }

    @Override
    public SingleOperationBuilder custom(OperationFactory factory)
    {
        operations.add(factory.make(lts, opIdCounter++));
        build();
        return this;
    }

    @Override
    public SingleOperationBuilder update()
    {
        int opId = opIdCounter++;
        long seed = PCGFastPure.shuffle(PCGFastPure.advanceState(lts, opId, lts));
        return rngSupplier.computeWithSeed(seed, rng -> {
            int pdIdx = indexGenerators.pkIdxGen.generate(rng);
            int cdIdx = indexGenerators.ckIdxGen.generate(rng);

            int[] valueIdxs = new int[indexGenerators.regularIdxGens.length];
            for (int i = 0; i < valueIdxs.length; i++)
                valueIdxs[i] = indexGenerators.regularIdxGens[i].generate(rng);
            int[] sValueIdxs = new int[indexGenerators.staticIdxGens.length];
            for (int i = 0; i < sValueIdxs.length; i++)
                sValueIdxs[i] = indexGenerators.staticIdxGens[i].generate(rng);
            return update(pdIdx, cdIdx, valueIdxs, sValueIdxs);
        });
    }

    @Override
    public SingleOperationBuilder update(int pdIdx)
    {
        int opId = opIdCounter++;
        long seed = PCGFastPure.shuffle(PCGFastPure.advanceState(lts, opId, lts));
        return rngSupplier.computeWithSeed(seed, rng -> {
            int cdIdx = indexGenerators.ckIdxGen.generate(rng);

            int[] valueIdxs = new int[indexGenerators.regularIdxGens.length];
            for (int i = 0; i < valueIdxs.length; i++)
                valueIdxs[i] = indexGenerators.regularIdxGens[i].generate(rng);
            int[] sValueIdxs = new int[indexGenerators.staticIdxGens.length];
            for (int i = 0; i < sValueIdxs.length; i++)
                sValueIdxs[i] = indexGenerators.staticIdxGens[i].generate(rng);
            return update(pdIdx, cdIdx, valueIdxs, sValueIdxs);
        });
    }

    @Override
    public SingleOperationBuilder update(int pdIdx, int cdIdx)
    {
        int opId = opIdCounter++;
        long seed = PCGFastPure.shuffle(PCGFastPure.advanceState(lts, opId, lts));
        return rngSupplier.computeWithSeed(seed, rng -> {
            int[] valueIdxs = new int[indexGenerators.regularIdxGens.length];
            for (int i = 0; i < valueIdxs.length; i++)
                valueIdxs[i] = indexGenerators.regularIdxGens[i].generate(rng);
            int[] sValueIdxs = new int[indexGenerators.staticIdxGens.length];
            for (int i = 0; i < sValueIdxs.length; i++)
                sValueIdxs[i] = indexGenerators.staticIdxGens[i].generate(rng);
            return update(pdIdx, cdIdx, valueIdxs, sValueIdxs);
        });
    }

    @Override
    public SingleOperationBuilder update(int pdIdx, int cdIdx, int[] valueIdxs, int[] sValueIdxs)
    {
        return write(pdIdx, cdIdx, valueIdxs, sValueIdxs, Kind.UPDATE);
    }

    private SingleOperationBuilder write(int pdIdx, int cdIdx, int[] valueIdxs, int[] sValueIdxs, Kind kind)
    {
        assert valueIdxs.length == valueGenerators.regularColumnGens.size();
        assert sValueIdxs.length == valueGenerators.staticColumnGens.size();

        long pd = valueGenerators.pkGen.descriptorAt(pdIdx);
        long cd = valueGenerators.ckGen.descriptorAt(cdIdx);

        opIdCounter++;
        long[] vds = new long[valueIdxs.length];
        for (int i = 0; i < valueGenerators.regularColumnGens.size(); i++)
        {
            int valueIdx = valueIdxs[i];
            if (valueIdx == MagicConstants.UNSET_IDX)
                vds[i] = MagicConstants.UNSET_DESCR;
            else
                vds[i] = valueGenerators.regularColumnGens.get(i).descriptorAt(valueIdx);
        }

        long[] sds = new long[sValueIdxs.length];
        for (int i = 0; i < sValueIdxs.length; i++)
        {
            int valueIdx = sValueIdxs[i];
            if (valueIdx == MagicConstants.UNSET_IDX)
                sds[i] = MagicConstants.UNSET_DESCR;
            else
                sds[i] = valueGenerators.staticColumnGens.get(i).descriptorAt(valueIdx);
        }

        operations.add(new WriteOp(lts, pd, cd, vds, sds, kind) {
            @Override
            public String toString()
            {
                return String.format("%s (%d, %d, %s, %s)",
                                     kind, pdIdx, cdIdx, Arrays.toString(valueIdxs), Arrays.toString(sValueIdxs));
            }
        });
        build();
        return this;
    }

    @Override
    public SingleOperationVisitBuilder deleteRowRange(int pdIdx, int lowerBoundRowIdx, int upperBoundRowIdx,
                                                      int nonEqFrom, boolean includeLowerBound, boolean includeUpperBound)
    {
        long pd = valueGenerators.pkGen.descriptorAt(pdIdx);

        long lowerBoundCd = valueGenerators.ckGen.descriptorAt(lowerBoundRowIdx);
        long upperBoundCd = valueGenerators.ckGen.descriptorAt(upperBoundRowIdx);

        Relations.RelationKind[] lowerBoundRelations = new Relations.RelationKind[valueGenerators.ckComparators.size()];
        Relations.RelationKind[] upperBoundRelations = new Relations.RelationKind[valueGenerators.ckComparators.size()];

        int opId = opIdCounter++;
        rngSupplier.doWithSeed(opId, rng -> {
            for (int i = 0; i < Math.min(nonEqFrom + 1, valueGenerators.ckComparators.size()); i++)
            {
                if (i < nonEqFrom)
                    lowerBoundRelations[i] = Relations.RelationKind.EQ;
                else
                {
                    lowerBoundRelations[i] = includeLowerBound ? Relations.RelationKind.GTE : Relations.RelationKind.GT;
                    upperBoundRelations[i] = includeUpperBound ? Relations.RelationKind.LTE : Relations.RelationKind.LT;
                }
            }
        });

        operations.add(new Operations.DeleteRange(lts, pd,
                                                  lowerBoundCd, upperBoundCd,
                                                  lowerBoundRelations, upperBoundRelations) {
            @Override
            public String toString()
            {
                return String.format("DELETE (%d, >%s%d, <%s%d) - (%d)",
                                     pdIdx,
                                     includeLowerBound ? "=" : "", lowerBoundRowIdx,
                                     includeUpperBound ? "=" : "", upperBoundRowIdx,
                                     nonEqFrom);
            }
        });
        build();
        return this;
    }

    @Override
    public SingleOperationBuilder deletePartition(int pdIdx)
    {
        long pd = valueGenerators.pkGen.descriptorAt(pdIdx);
        opIdCounter++;

        operations.add(new Operations.DeletePartition(lts, pd) {
            @Override
            public String toString()
            {
                return String.format("DELETE_PARTITION (%d)", pdIdx);
            }
        });
        build();
        return this;
    }

    @Override
    public SingleOperationBuilder deleteRow(int pdIdx, int rowIdx)
    {
        long pd = valueGenerators.pkGen.descriptorAt(pdIdx);
        opIdCounter++;
        long cd = valueGenerators.ckGen.descriptorAt(rowIdx);
        operations.add(new Operations.DeleteRow(lts, pd, cd) {
            @Override
            public String toString()
            {
                return String.format("DELETE_ROW (%d, %s)", pdIdx, rowIdx);
            }
        });
        build();
        return this;
    }

    @Override
    public SingleOperationBuilder deleteColumns(int pdIdx, int rowIdx, BitSet regularSelection, BitSet staticSelection)
    {
        long pd = valueGenerators.pkGen.descriptorAt(pdIdx);
        opIdCounter++;
        long cd = valueGenerators.ckGen.descriptorAt(rowIdx);
        operations.add(new Operations.DeleteColumns(lts, pd, cd, regularSelection, staticSelection)  {
            @Override
            public String toString()
            {
                return String.format("DELETE_COLUMNS (%d, %d, %s, %s)", pdIdx, rowIdx, regularSelection, staticSelection);
            }
        });
        build();
        return this;
    }

    @Override
    public SingleOperationBuilder deleteRowSliceByLowerBound(int pdIdx, int lowerBoundRowIdx, int nonEqFrom, boolean includeBound)
    {
        long pd = valueGenerators.pkGen.descriptorAt(pdIdx);
        long lowerBoundCd = valueGenerators.ckGen.descriptorAt(lowerBoundRowIdx);

        Relations.RelationKind[] lowerBoundRelations = new Relations.RelationKind[valueGenerators.ckComparators.size()];

        int opId = opIdCounter++;
        rngSupplier.doWithSeed(opId, rng -> {
            for (int i = 0; i < Math.min(nonEqFrom + 1, valueGenerators.ckComparators.size()); i++)
            {
                if (i < nonEqFrom)
                    lowerBoundRelations[i] = Relations.RelationKind.EQ;
                else
                    lowerBoundRelations[i] = includeBound ? Relations.RelationKind.GTE : Relations.RelationKind.GT;
            }
        });

        operations.add(new Operations.DeleteRange(lts, pd,
                                                  lowerBoundCd, MagicConstants.UNSET_DESCR,
                                                  lowerBoundRelations, null)  {
            @Override
            public String toString()
            {
                return String.format("DELETE (%d, >%s%d) - (%d)",
                                     pdIdx, includeBound ? "=" : "", lowerBoundRowIdx, nonEqFrom);
            }
        });
        build();
        return this;
    }

    @Override
    public SingleOperationBuilder deleteRowSliceByUpperBound(int pdIdx, int upperBoundRowIdx, int nonEqFrom, boolean includeBound)
    {
        long pd = valueGenerators.pkGen.descriptorAt(pdIdx);
        long upperBoundCd = valueGenerators.ckGen.descriptorAt(upperBoundRowIdx);

        Relations.RelationKind[] upperBoundRelations = new Relations.RelationKind[valueGenerators.ckComparators.size()];

        int opId = opIdCounter++;
        rngSupplier.doWithSeed(opId, rng -> {
            for (int i = 0; i < Math.min(nonEqFrom + 1, valueGenerators.ckComparators.size()); i++)
            {
                if (i < nonEqFrom)
                    upperBoundRelations[i] = Relations.RelationKind.EQ;
                else
                    upperBoundRelations[i] = includeBound ? Relations.RelationKind.LTE : Relations.RelationKind.LT;
            }
        });

        operations.add(new Operations.DeleteRange(lts, pd,
                                                  MagicConstants.UNSET_DESCR, upperBoundCd,
                                                  null, upperBoundRelations) {
            @Override
            public String toString()
            {
                return String.format("DELETE (%d, <%s%d) - (%d)",
                                     pdIdx, includeBound ? "=" : "", upperBoundRowIdx, nonEqFrom);
            }
        });
        build();
        return this;
    }


    @Override
    public SingleOperationVisitBuilder selectRowRange(int pdIdx, int lowerBoundRowIdx, int upperBoundRowIdx,
                                                      int nonEqFrom, boolean includeLowerBound, boolean includeUpperBound)
    {
        long pd = valueGenerators.pkGen.descriptorAt(pdIdx);
        long lowerBoundCd = valueGenerators.ckGen.descriptorAt(lowerBoundRowIdx);
        long upperBoundCd = valueGenerators.ckGen.descriptorAt(upperBoundRowIdx);

        Relations.RelationKind[] lowerBoundRelations = new Relations.RelationKind[valueGenerators.ckComparators.size()];
        Relations.RelationKind[] upperBoundRelations = new Relations.RelationKind[valueGenerators.ckComparators.size()];

        int opId = opIdCounter++;
        rngSupplier.doWithSeed(opId, rng -> {
            for (int i = 0; i < Math.min(nonEqFrom + 1, valueGenerators.ckComparators.size()); i++)
            {
                if (i < nonEqFrom)
                    lowerBoundRelations[i] = Relations.RelationKind.EQ;
                else
                {
                    lowerBoundRelations[i] = includeLowerBound ? Relations.RelationKind.GTE : Relations.RelationKind.GT;
                    upperBoundRelations[i] = includeUpperBound ? Relations.RelationKind.LTE : Relations.RelationKind.LT;
                }
            }
        });

        operations.add(new Operations.SelectRange(lts, pd,
                                                  lowerBoundCd, upperBoundCd,
                                                  lowerBoundRelations, upperBoundRelations));
        build();
        return this;
    }

    @Override
    public SingleOperationBuilder select(int pdIdx, IdxRelation[] ckIdxRelations, IdxRelation[] regularIdxRelations, IdxRelation[] staticIdxRelations)
    {
        long pd = valueGenerators.pkGen.descriptorAt(pdIdx);
        opIdCounter++;

        Relations.Relation[] ckRelations = new Relations.Relation[ckIdxRelations.length];
        for (int i = 0; i < ckRelations.length; i++)
        {
            Invariants.checkState(ckIdxRelations[i].column < valueGenerators.ckComparators.size());
            ckRelations[i] = new Relations.Relation(ckIdxRelations[i].kind,
                                                    valueGenerators.ckGen.descriptorAt(ckIdxRelations[i].idx),
                                                    ckIdxRelations[i].column);
        }

        Relations.Relation[] regularRelations = new Relations.Relation[regularIdxRelations.length];
        for (int i = 0; i < regularRelations.length; i++)
        {
            Invariants.checkState(regularIdxRelations[i].column < valueGenerators.regularComparators.size());
            regularRelations[i] = new Relations.Relation(regularIdxRelations[i].kind,
                                                         valueGenerators.regularColumnGens.get(regularIdxRelations[i].column).descriptorAt(regularIdxRelations[i].idx),
                                                         regularIdxRelations[i].column);
        }

        Relations.Relation[] staticRelations = new Relations.Relation[staticIdxRelations.length];
        for (int i = 0; i < staticRelations.length; i++)
        {
            Invariants.checkState(staticIdxRelations[i].column < valueGenerators.staticComparators.size());
            staticRelations[i] = new Relations.Relation(staticIdxRelations[i].kind,
                                                        valueGenerators.staticColumnGens.get(staticIdxRelations[i].column).descriptorAt(staticIdxRelations[i].idx),
                                                        staticIdxRelations[i].column);
        }

        operations.add(new Operations.SelectCustom(lts, pd, ckRelations, regularRelations, staticRelations));
        build();
        return this;
    }

    @Override
    public SingleOperationBuilder selectPartition(int pdIdx)
    {
        long pd = valueGenerators.pkGen.descriptorAt(pdIdx);
        opIdCounter++;

        operations.add(new Operations.SelectPartition(lts, pd));
        build();
        return this;
    }

    @Override
    public SingleOperationBuilder selectPartition(int pdIdx, Operations.ClusteringOrderBy orderBy)
    {
        long pd = valueGenerators.pkGen.descriptorAt(pdIdx);
        opIdCounter++;

        operations.add(new Operations.SelectPartition(lts, pd, orderBy));
        build();
        return this;
    }

    @Override
    public SingleOperationBuilder selectRow(int pdIdx, int rowIdx)
    {
        long pd = valueGenerators.pkGen.descriptorAt(pdIdx);
        opIdCounter++;
        long cd = valueGenerators.ckGen.descriptorAt(rowIdx);
        operations.add(new Operations.SelectRow(lts, pd, cd));
        build();
        return this;
    }

    @Override
    public SingleOperationBuilder selectRowSliceByLowerBound(int pdIdx, int lowerBoundRowIdx, int nonEqFrom, boolean includeBound)
    {
        long pd = valueGenerators.pkGen.descriptorAt(pdIdx);
        long lowerBoundCd = valueGenerators.ckGen.descriptorAt(lowerBoundRowIdx);

        Relations.RelationKind[] lowerBoundRelations = new Relations.RelationKind[valueGenerators.ckComparators.size()];

        int opId = opIdCounter++;
        rngSupplier.doWithSeed(opId, rng -> {
            for (int i = 0; i < Math.min(nonEqFrom + 1, valueGenerators.ckComparators.size()); i++)
            {
                if (i < nonEqFrom)
                    lowerBoundRelations[i] = Relations.RelationKind.EQ;
                else
                    lowerBoundRelations[i] = includeBound ? Relations.RelationKind.GTE : Relations.RelationKind.GT;
            }
        });

        operations.add(new Operations.SelectRange(lts, pd,
                                                  lowerBoundCd, MagicConstants.UNSET_DESCR,
                                                  lowerBoundRelations, null));
        build();
        return this;
    }

    @Override
    public SingleOperationBuilder selectRowSliceByUpperBound(int pdIdx, int upperBoundRowIdx, int nonEqFrom, boolean includeBound)
    {
        long pd = valueGenerators.pkGen.descriptorAt(pdIdx);
        long upperBoundCd = valueGenerators.ckGen.descriptorAt(upperBoundRowIdx);

        Relations.RelationKind[] upperBoundRelations = new Relations.RelationKind[valueGenerators.ckComparators.size()];

        int opId = opIdCounter++;
        rngSupplier.doWithSeed(opId, rng -> {
            for (int i = 0; i < Math.min(nonEqFrom + 1, valueGenerators.ckComparators.size()); i++)
            {
                if (i < nonEqFrom)
                    upperBoundRelations[i] = Relations.RelationKind.EQ;
                else
                    upperBoundRelations[i] = includeBound ? Relations.RelationKind.LTE : Relations.RelationKind.LT;
            }
        });

        operations.add(new Operations.SelectRange(lts, pd,
                                                  MagicConstants.UNSET_DESCR, upperBoundCd,
                                                  null, upperBoundRelations));
        build();
        return this;
    }

    int size()
    {
        return this.operations.size();
    }

    Visit build()
    {
        Operation[] ops = new Operation[operations.size()];
        operations.toArray(ops);
        Visit visit = new Visit(lts, ops);
        appendToLog.accept(visit);
        return visit;
    }
}
