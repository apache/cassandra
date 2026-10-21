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

package org.apache.cassandra.utils;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.LargeBitSet;
import accord.utils.SimpleBitSet;
import accord.utils.SmallBitSet;

import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;

import static accord.utils.Property.qt;

public class SimpleBitSetSerializersTest
{
    @Test
    public void small()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(smallGen()).check(bits -> Serializers.testSerde(output, SimpleBitSetSerializers.small, bits));
    }

    @Test
    public void large()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(largeGen()).check(bits -> Serializers.testSerde(output, SimpleBitSetSerializers.large, bits));
    }

    @Test
    public void any()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(anyGen()).check(bits -> {
            Serializers.testSerde(output, SimpleBitSetSerializers.any, bits, (expected, actual) -> {
                if (actual.getClass() == expected.getClass())
                {
                    Assertions.assertThat(actual)
                              .describedAs("The deserialized output does not match the serialized input")
                              .isEqualTo(expected);
                }
                else
                {
                    // large can become small when deserialize
                    Assertions.assertThat(expected.getClass()).isEqualTo(LargeBitSet.class);
                    Assertions.assertThat(actual.getClass()).isEqualTo(SmallBitSet.class);

                    Assertions.assertThat(actual.nextSetBit(0)).isEqualTo(expected.nextSetBit(0));

                    for (int i = actual.nextSetBit(0); i >= 0;)
                    {
                        Assertions.assertThat(actual.nextSetBit(i + 1))
                                  .describedAs("Difference searching for next bit from %s", (i + 1))
                                  .isEqualTo(expected.nextSetBit(i + 1));
                        i = actual.nextSetBit(i + 1);
                    }
                }
            });
        });
    }

    private static Gen<SimpleBitSet> anyGen()
    {
        return rs -> rs.nextBoolean() ? smallGen().next(rs) : largeGen().next(rs);
    }

    private static Gen<SmallBitSet> smallGen()
    {
        return Gens.longs().all().map(SmallBitSet::new);
    }

    private static Gen<LargeBitSet> largeGen()
    {
        return rs -> {
            int size = rs.nextInt(0, 1 << 10);
            LargeBitSet bitSet = new LargeBitSet(size);
            if (size == 0 || rs.decide(0.2))
                return bitSet; // empty
            if (rs.decide(0.2))
            {
                // set 1 bit randomly
                bitSet.set(rs.nextInt(0, size));
                return bitSet;
            }
            for (int i = 0; i < size; i++)
            {
                if (rs.nextBoolean())
                    bitSet.set(i);
            }
            return bitSet;
        };
    }
}
