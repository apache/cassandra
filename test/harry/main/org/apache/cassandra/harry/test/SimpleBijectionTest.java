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

package org.apache.cassandra.harry.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import accord.utils.Invariants;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.gen.InvertibleGenerator;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.gen.ValueGenerators;

import static org.apache.cassandra.harry.checker.TestHelper.withRandom;
import static org.apache.cassandra.harry.gen.InvertibleGenerator.MAX_ENTROPY;

public class SimpleBijectionTest
{
    @Test
    public void testOrder()
    {
        withRandom(rng -> {
            for (ColumnSpec.DataType<?> t : ColumnSpec.TYPES)
            {
                for (ColumnSpec.DataType type : new ColumnSpec.DataType[]{ t, ColumnSpec.ReversedType.cache.get(t) })
                {
                    ColumnSpec<Object> column = (ColumnSpec<Object>) ColumnSpec.regularColumn("regular", type);
                    InvertibleGenerator<Object> generator = InvertibleGenerator.fromType(rng,100, column);


                    Object previous = null;
                    for (int i = 0; i < generator.population(); i++)
                    {
                        Object next = generator.inflate(generator.descriptorAt(i));
                        if (previous != null)
                        {
                            Invariants.checkState(column.type.comparator().compare(next, previous) > 0,
                                                  "%s should be > %s", next, previous);
                        }
                        previous = next;
                    }
                }
            }
        });
    }

    @Test
    public void testArrayOrder()
    {
        withRandom(rng -> {
            for (boolean[] order : new boolean[][]{ { false, false }, { true, false }, { false, true }, { true, true } })
            {
                List<ColumnSpec<?>> columns = new ArrayList<>();
                for (int i = 0; i < order.length; i++)
                    columns.add(ColumnSpec.ck("test", ColumnSpec.asciiType, order[i]));
                InvertibleGenerator<Object[]> generator = new InvertibleGenerator<>(rng,
                                                                                    MAX_ENTROPY,
                                                                                    100,
                                                                                    SchemaSpec.forKeys(columns),
                                                                                    (Object[] a, Object[] b) -> ValueGenerators.compareKeys(columns, a, b));
                Object[] previous = null;
                for (int i = 0; i < 100; i++)
                {
                    long descr = generator.descriptorAt(i);
                    Object[] next = generator.inflate(descr);
                    if (previous != null)
                        Assert.assertTrue(ValueGenerators.compareKeys(columns, next, previous) > 0);
                    Assert.assertEquals(descr, generator.deflate(next));
                    previous = next;
                }
            }
        });
    }

    // TODO (now): negative tests
}
