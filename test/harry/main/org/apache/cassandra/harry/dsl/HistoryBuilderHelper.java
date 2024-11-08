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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.MagicConstants;
import org.apache.cassandra.harry.Relations;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.util.BitSet;

import static org.apache.cassandra.harry.Relations.RelationKind.EQ;
import static org.apache.cassandra.harry.Relations.RelationKind.GT;
import static org.apache.cassandra.harry.Relations.RelationKind.GTE;
import static org.apache.cassandra.harry.Relations.RelationKind.LT;

/**
 * Things that seemed like a good idea, but ultimately were not a good fit for the HistoryBuilder API
 */
public class HistoryBuilderHelper
{
    /**
     * Perform a random insert to any row
     */
    public static void insertRandomData(SchemaSpec schema, Generator<Integer> pkGen, Generator<Integer> ckGen, EntropySource rng, SingleOperationBuilder history)
    {
        insertRandomData(schema, pkGen.generate(rng), ckGen.generate(rng), rng, history);
    }

    public static void insertRandomData(SchemaSpec schema, int partitionIdx, int rowIdx, EntropySource rng, SingleOperationBuilder history)
    {
        int[] vIdxs = new int[schema.regularColumns.size()];
        for (int i = 0; i < schema.regularColumns.size(); i++)
            vIdxs[i] = rng.nextInt(schema.valueGenerators.regularPopulation(i));
        int[] sIdxs = new int[schema.staticColumns.size()];
        for (int i = 0; i < schema.staticColumns.size(); i++)
            sIdxs[i] = rng.nextInt(schema.valueGenerators.staticPopulation(i));
        history.insert(partitionIdx, rowIdx, vIdxs, sIdxs);
    }

    public static void insertRandomData(SchemaSpec schema, int pkIdx, EntropySource rng, SingleOperationBuilder history)
    {
        insertRandomData(schema,
                         pkIdx,
                         rng.nextInt(0, schema.valueGenerators.ckPopulation()),
                         rng,
                         0,
                         history);
    }

    public static void insertRandomData(SchemaSpec schema, int partitionIdx, int rowIdx, EntropySource rng, double chanceOfUnset, SingleOperationBuilder history)
    {
        int[] vIdxs = new int[schema.regularColumns.size()];
        for (int i = 0; i < schema.regularColumns.size(); i++)
            vIdxs[i] = rng.nextDouble() <= chanceOfUnset ? MagicConstants.UNSET_IDX : rng.nextInt(schema.valueGenerators.regularPopulation(i));
        int[] sIdxs = new int[schema.staticColumns.size()];
        for (int i = 0; i < schema.staticColumns.size(); i++)
            sIdxs[i] = rng.nextDouble() <= chanceOfUnset ? MagicConstants.UNSET_IDX : rng.nextInt(schema.valueGenerators.staticPopulation(i));
        history.insert(partitionIdx, rowIdx, vIdxs, sIdxs);
    }


    public static void deleteRandomColumns(SchemaSpec schema, int partitionIdx, int rowIdx, EntropySource rng, SingleOperationBuilder history)
    {
        Generator<BitSet> regularMask = Generators.bitSet(schema.regularColumns.size());
        Generator<BitSet> staticMask = Generators.bitSet(schema.staticColumns.size());

        history.deleteColumns(partitionIdx,
                              rowIdx,
                              regularMask.generate(rng),
                              staticMask.generate(rng));
    }

    private static final Generator<Relations.RelationKind> relationKindGen = Generators.pick(LT, GT, EQ);
    private static final Set<Relations.RelationKind> lowBoundRelations = Set.of(GT, GTE, EQ);

    /**
     * Generates random relations for regular and static columns for FILTERING and SAI queries.
     *
     * Will generate at most 2 relations per column:
     *   * generates a random relation
     *     * if this relation is EQ, that's the only relation that will lock this column
     *     * if relation is GT, next bound, if generated, will be LT
     *     * if relation is LT, next bound, if generated, will be GT
     *
     * @param rng - random number generator
     * @param numColumns - number of columns in the generated set of relationships
     * @param population - expected population / number of possible values for a given column
     * @return a list of relations
     */
    public static List<SingleOperationBuilder.IdxRelation> generateValueRelations(EntropySource rng, int numColumns, Function<Integer, Integer> population)
    {
        List<SingleOperationBuilder.IdxRelation> relations = new ArrayList<>();
        Map<Integer, Set<Relations.RelationKind>> kindsMap = new HashMap<>();
        int remainingColumns = numColumns;
        while (remainingColumns > 0)
        {
            int column = rng.nextInt(numColumns);
            Set<Relations.RelationKind> kinds = kindsMap.computeIfAbsent(column, c -> new HashSet<>());
            if (kinds.size() > 1 || kinds.contains(EQ))
                continue;
            Relations.RelationKind kind;
            if (kinds.size() == 1)
            {
                if (kinds.contains(LT)) kind = GT;
                else kind = LT;
                remainingColumns--;
            }
            else
                // TODO: weights per relation?
                kind = relationKindGen.generate(rng);

            if (kind == EQ)
                remainingColumns--;

            kinds.add(kind);

            int regularIdx = rng.nextInt(population.apply(column));
            relations.add(new SingleOperationBuilder.IdxRelation(kind, regularIdx, column));
            if (rng.nextBoolean())
                break;
        }
        return relations;
    }

    /**
     * Generates random relations for regular and static columns for FILTERING and SAI queries.
     *
     * Will generate at most 2 relations per column. Low bound will always use values from low bound clustering,
     * high bound will always use values from high bound.
     *
     * @param rng - random number generator
     * @param numColumns - number of columns in the generated set of relationships
     * @return a list of relations
     */
    public static List<SingleOperationBuilder.IdxRelation> generateClusteringRelations(EntropySource rng, int numColumns, Generator<Integer> ckGen)
    {
        List<SingleOperationBuilder.IdxRelation> relations = new ArrayList<>();
        Map<Integer, Set<Relations.RelationKind>> kindsMap = new HashMap<>();
        int remainingColumns = numColumns;
        int lowBoundIdx = ckGen.generate(rng);
        int highBoundIdx = ckGen.generate(rng);
        while (remainingColumns > 0)
        {
            int column = rng.nextInt(numColumns);
            Set<Relations.RelationKind> kinds = kindsMap.computeIfAbsent(column, c -> new HashSet<>());
            if (kinds.size() > 1 || kinds.contains(EQ))
                continue;
            Relations.RelationKind kind;
            if (kinds.size() == 1)
            {
                if (kinds.contains(LT)) kind = GT;
                else kind = LT;
                remainingColumns--;
            }
            else
                kind = relationKindGen.generate(rng);

            if (kind == EQ)
                remainingColumns--;

            kinds.add(kind);

            relations.add(new SingleOperationBuilder.IdxRelation(kind, lowBoundRelations.contains(kind) ? lowBoundIdx : highBoundIdx, column));
            if (rng.nextBoolean())
                break;
        }
        return relations;
    }
}
