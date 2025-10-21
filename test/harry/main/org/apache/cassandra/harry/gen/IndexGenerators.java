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

package org.apache.cassandra.harry.gen;

import org.apache.cassandra.harry.MagicConstants;

public class IndexGenerators
{
    private final ValueGenerators valueGenerators;
    public final Generator<Integer> pkIdxGen;
    public final Generator<Integer> ckIdxGen;
    public final Generator<Integer>[] regularIdxGens;
    public final Generator<Integer>[] staticIdxGens;

    public static IndexGenerators withDefaults(ValueGenerators valueGenerators)
    {
        Generator<Integer>[] regularIdxGens = new Generator[valueGenerators.regularColumnGens.size()];
        Generator<Integer>[] staticIdxGens = new Generator[valueGenerators.staticColumnGens.size()];

        for (int i = 0; i < regularIdxGens.length; i++)
        {
            int column = i;
            regularIdxGens[i] = (rng) -> rng.nextInt(valueGenerators.regularPopulation(column));
        }

        for (int i = 0; i < staticIdxGens.length; i++)
        {
            int column = i;
            staticIdxGens[i] = (rng) -> rng.nextInt(valueGenerators.staticPopulation(column));
        }

        return new IndexGenerators(valueGenerators,
                                   // TODO: distribution for visits
                                   Generators.int32(0, valueGenerators.pkPopulation()),
                                   Generators.int32(0, valueGenerators.ckPopulation()),
                                   regularIdxGens,
                                   staticIdxGens);
    }

    public IndexGenerators(ValueGenerators valueGenerators,
                           Generator<Integer> pkIdxGen,
                           Generator<Integer> ckIdxGen,
                           Generator<Integer>[] regularIdxGens,
                           Generator<Integer>[] staticIdxGens)
    {
        this.valueGenerators = valueGenerators;
        this.pkIdxGen = pkIdxGen;
        this.ckIdxGen = ckIdxGen;
        this.regularIdxGens = regularIdxGens;
        this.staticIdxGens = staticIdxGens;

    }

    public IndexGenerators roundRobinPk()
    {
        Generator<Integer> pkIdxGen = new Generator<Integer>()
        {
            int offset = 0;
            @Override
            public Integer generate(EntropySource rng)
            {
                int next = offset++;

                if (offset < 0)
                    offset = 0;

                return next % valueGenerators.pkPopulation();
            }
        };

        return new IndexGenerators(valueGenerators,
                                   pkIdxGen,
                                   ckIdxGen,
                                   regularIdxGens,
                                   staticIdxGens);
    }

    public IndexGenerators trackPk()
    {
        if (pkIdxGen instanceof Generators.TrackingGenerator<?>)
            return this;

        return new IndexGenerators(valueGenerators,
                                   Generators.tracking(pkIdxGen),
                                   ckIdxGen,
                                   regularIdxGens,
                                   staticIdxGens);
    }

    public IndexGenerators withChanceOfUnset(double chanceOfUnset)
    {
        Generator<Integer>[] regularIdxGens = new Generator[valueGenerators.regularColumnGens.size()];
        Generator<Integer>[] staticIdxGens = new Generator[valueGenerators.staticColumnGens.size()];

        for (int i = 0; i < regularIdxGens.length; i++)
        {
            int column = i;
            regularIdxGens[i] = (rng) -> rng.nextDouble() <= chanceOfUnset ? MagicConstants.UNSET_IDX : rng.nextInt(valueGenerators.regularPopulation(column));
        }

        for (int i = 0; i < staticIdxGens.length; i++)
        {
            int column = i;
            staticIdxGens[i] = (rng) -> rng.nextDouble() <= chanceOfUnset ? MagicConstants.UNSET_IDX : rng.nextInt(valueGenerators.staticPopulation(column));
        }

        return new IndexGenerators(valueGenerators,
                                   // TODO: distribution for visits
                                   Generators.int32(0, valueGenerators.pkPopulation()),
                                   Generators.int32(0, valueGenerators.ckPopulation()),
                                   regularIdxGens,
                                   staticIdxGens);
    }
}
