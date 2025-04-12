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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.SchemaSpec;

public class SchemaGenerators
{
    public static Generator<SchemaSpec> schemaSpecGen(String ks, String prefix, int expectedValues)
    {
        return schemaSpecGen(ks, prefix, expectedValues, SchemaSpec.optionsBuilder());
    }

    public static Generator<SchemaSpec> schemaSpecGen(String ks, String prefix, int expectedValues, SchemaSpec.Options options)
    {
        return new Generator<>()
        {
            final Generator<List<ColumnSpec<?>>> regularGen = Generators.list(1, 10, regularColumnSpecGen());
            final Generator<List<ColumnSpec<?>>> staticGen = Generators.list(1, 10, staticColumnSpecGen());
            final Generator<List<ColumnSpec<?>>> ckGen = Generators.list(1, 10, ckColumnSpecGen());
            final Generator<List<ColumnSpec<?>>> pkGen = Generators.list(1, 10, pkColumnSpecGen());

            int counter = 0;
            public SchemaSpec generate(EntropySource rng)
            {
                int idx = counter++;
                return new SchemaSpec(rng.next(),
                                      expectedValues,
                                      ks,
                                      prefix + idx,
                                      pkGen.generate(rng),
                                      ckGen.generate(rng),
                                      regularGen.generate(rng),
                                      staticGen.generate(rng),
                                      options);
            };
        };
    }

    public static Generator<ColumnSpec<?>> regularColumnSpecGen()
    {
        return columnSpecGen("regular", ColumnSpec.Kind.REGULAR, Generators.pick(ColumnSpec.TYPES));
    }

    public static Generator<ColumnSpec<?>> staticColumnSpecGen()
    {
        return columnSpecGen("static", ColumnSpec.Kind.STATIC, Generators.pick(ColumnSpec.TYPES));
    }

    public static Generator<ColumnSpec<?>> pkColumnSpecGen()
    {
        return columnSpecGen("pk", ColumnSpec.Kind.PARTITION_KEY, Generators.pick(ColumnSpec.TYPES));
    }

    public static Generator<ColumnSpec<?>> ckColumnSpecGen()
    {
        List<ColumnSpec.DataType<?>> forwardAndReverse = new ArrayList<>();
        forwardAndReverse.addAll(ColumnSpec.TYPES);
        forwardAndReverse.addAll(ColumnSpec.ReversedType.cache.values());
        return columnSpecGen("ck", ColumnSpec.Kind.CLUSTERING, Generators.pick(forwardAndReverse));
    }

    public static Generator<ColumnSpec<?>> columnSpecGen(String prefix, ColumnSpec.Kind kind, Generator<ColumnSpec.DataType<?>> typeGen)
    {
        return new Generator<ColumnSpec<?>>()
        {
            int counter = 0;
            public ColumnSpec<?> generate(EntropySource rng)
            {
                ColumnSpec.DataType<?> type = typeGen.generate(rng);
                Generator<?> gen = TypeAdapters.defaults.get(type.asServerType());
                int idx = counter++;
                return new ColumnSpec(prefix + idx, type, gen, kind);
            }
        };
    }

    public static Generator<SchemaSpec> trivialSchema(String ks, String table, int population)
    {
        return (rng) -> {
            return new SchemaSpec(rng.next(),
                                  population,
                                  ks, table,
                                  Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.int64Type, Generators.int64())),
                                  Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.int64Type, Generators.int64(), false)),
                                  Arrays.asList(ColumnSpec.regularColumn("v1", ColumnSpec.int64Type)),
                                  List.of(ColumnSpec.staticColumn("s1", ColumnSpec.int64Type)));
        };
    }
}
