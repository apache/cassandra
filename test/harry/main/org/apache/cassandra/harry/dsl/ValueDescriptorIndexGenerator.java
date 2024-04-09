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

import java.util.function.LongSupplier;

import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.gen.Bytes;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.DataGenerators;
import org.apache.cassandra.harry.gen.Surjections;
import org.apache.cassandra.harry.model.OpSelectors;

/**
 * By default, descriptors in Harry are chosen as items in the stream of `pd ^ cd ^ lts ^ col`,
 * but for purpose of some tests one may want to have more fine-grained control over values,
 * which is particularly useful for 2i testing.
 *
 * Imagine all possible value descriptors as items in some array (each column would have its own
 * array with different descriptors). This generator will pick an item from this array by given index.
 */
public class ValueDescriptorIndexGenerator implements Surjections.Surjection<Long>
{
    public static int UNSET = Integer.MIN_VALUE;

    private final OpSelectors.PureRng rng;
    private final long columnHash;
    private final long mask;

    public ValueDescriptorIndexGenerator(ColumnSpec<?> columnSpec, long seed)
    {
        this(columnSpec, new OpSelectors.PCGFast(seed));
    }

    public ValueDescriptorIndexGenerator(ColumnSpec<?> columnSpec, OpSelectors.PureRng rng)
    {
        this.rng = rng;
        this.columnHash = columnSpec.hashCode();
        this.mask = Bytes.bytePatternFor(columnSpec.type.maxSize());
    }

    @Override
    public Long inflate(long idx)
    {
        if (idx == UNSET)
            return DataGenerators.UNSET_DESCR;

        return rng.randomNumber(idx, columnHash) & mask;
    }

    /**
     * Returns a supplier that would uniformly pick from at most {@param values} values.
     *
     * @param values number of possible values
     */
    public LongSupplier toSupplier(EntropySource orig, int values, float chanceOfUnset)
    {
        EntropySource derived = orig.derive();
        if (chanceOfUnset > 0)
        {
            assert chanceOfUnset < 1.0;
            return () -> {
                if (orig.nextFloat() < chanceOfUnset)
                    return DataGenerators.UNSET_DESCR;
                return inflate(derived.nextInt(values));
            };
        }

        return () -> inflate(derived.nextInt(values));
    }
}
