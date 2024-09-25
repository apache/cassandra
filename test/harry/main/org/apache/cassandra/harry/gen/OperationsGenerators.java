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

import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.op.Operations;

public class OperationsGenerators
{
    public static Generator<Long> lts()
    {
        return new Generator<>()
        {
            long counter = 0;

            @Override
            public Long generate(EntropySource rng)
            {
                return counter++;
            }
        };
    }

    // TODO: distributions
    public static Generator<Long> sequentialPd(SchemaSpec schema)
    {
        // TODO: switch away from Indexed generators here
        HistoryBuilder.IndexedValueGenerators valueGenerators = (HistoryBuilder.IndexedValueGenerators) schema.valueGenerators;
        int population = valueGenerators.pkPopulation();

        return new Generator<>()
        {
            int counter = 0;

            @Override
            public Long generate(EntropySource rng)
            {
                return valueGenerators.pkGen().descriptorAt(counter++ % population);
            }
        };
    }

    public static Generator<Long> sequentialCd(SchemaSpec schema)
    {
        // TODO: switch away from Indexed generators here
        HistoryBuilder.IndexedValueGenerators valueGenerators = (HistoryBuilder.IndexedValueGenerators) schema.valueGenerators;
        int population = valueGenerators.ckPopulation();

        return new Generator<>()
        {
            int counter = 0;

            @Override
            public Long generate(EntropySource rng)
            {
                return valueGenerators.ckGen().descriptorAt(counter++ % population);
            }
        };
    }

    public static Generator<ToOp> writeOp(SchemaSpec schema)
    {
        return writeOp(schema, sequentialPd(schema), sequentialCd(schema));
    }

    // TODO: chance of unset
    public static Generator<ToOp> writeOp(SchemaSpec schema,
                                          Generator<Long> pdGen,
                                          Generator<Long> cdGen)
    {
        // TODO: switch away from Indexed generators here
        HistoryBuilder.IndexedValueGenerators valueGenerators = (HistoryBuilder.IndexedValueGenerators) schema.valueGenerators;

        return (rng) -> {
            long pd = pdGen.generate(rng);
            long cd = cdGen.generate(rng);
            long[] vds = new long[schema.regularColumns.size()];
            for (int i = 0; i < schema.regularColumns.size(); i++)
            {
                int idx = rng.nextInt(valueGenerators.regularPopulation(i));
                vds[i] = valueGenerators.regularColumnGen(i).descriptorAt(idx);
            }
            long[] sds = new long[schema.staticColumns.size()];
            for (int i = 0; i < schema.staticColumns.size(); i++)
            {
                int idx = rng.nextInt(valueGenerators.staticPopulation(i));
                sds[i] = valueGenerators.staticColumnGen(i).descriptorAt(idx);
            }
            return lts -> new Operations.WriteOp(lts, pd, cd, vds, sds, Operations.Kind.INSERT);
        };
    }

    public interface ToOp
    {
        Operations.Operation toOp(long lts);
    }
}
