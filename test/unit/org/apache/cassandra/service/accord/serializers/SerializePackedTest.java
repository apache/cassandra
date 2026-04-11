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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;
import java.util.Arrays;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;

import static accord.utils.Property.qt;

public class SerializePackedTest
{
    private static final Gen<int[]> zeros = rs -> new int[rs.nextInt(0, 10)];
    private static final Gen<int[]> randomZeroAndPositive = array(rs -> rs.nextInt(-1, Integer.MAX_VALUE) + 1);
    private static final Gen<int[]> randomZeroAndPositiveSmall = array(rs -> rs.nextInt(0, 1 << 8));
    private static final Gen<int[]> negatives = array(Gens.ints().between(Integer.MIN_VALUE, -1), rs -> rs.nextInt(1, 10));

    private static final Gen<int[]> monotonic = rs -> {
        int[] array = new int[rs.nextInt(0, 10)];
        for (int i = 0; i < array.length; i++)
            array[i] = i;
        return array;
    };

    private static final Gen<int[]> zeroAndPositive = rs -> {
        if (rs.decide(0.2f)) return monotonic.next(rs);
        return randomZeroAndPositive.next(rs);
    };

    private static final Gen<int[]> zeroAndPositiveSmall = rs -> {
        if (rs.decide(0.2f)) return monotonic.next(rs);
        return randomZeroAndPositiveSmall.next(rs);
    };

    private static Gen<int[]> array(Gen.IntGen valueGen)
    {
        return array(valueGen, rs -> rs.nextInt(0, 10));
    }

    private static Gen<int[]> array(Gen.IntGen valueGen, Gen.IntGen size)
    {
        return rs -> {
            int[] array = new int[size.nextInt(rs)];
            for (int i = 0; i < array.length; i++)
                array[i] = valueGen.nextInt(rs);
            Arrays.sort(array);
            return array;
        };
    }

    @Test
    public void serde()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(zeroAndPositive).check(array -> Serializers.testSerde(output, PackedSortedSerializer.instance, array));
    }

    @Test
    public void serdeNegative()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(negatives).check(array -> {
            output.clear();
            Assertions.assertThatThrownBy(() -> PackedSortedSerializer.instance.serialize(array, output))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Found a negative value at offset");
        });
    }

    @Test
    public void serdeZeros()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(zeros).check(array -> Serializers.testSerde(output, PackedSortedSerializer.instance, array));
    }

    @Test
    public void serializerIsSmallerThanSimpleList()
    {
        qt().forAll(zeroAndPositiveSmall).check(array -> {
            var list = SimpleListSerializer.instance.serialize(array);
            var packed = PackedSortedSerializer.instance.serialize(array);

            Assertions.assertThat(packed.remaining()).isLessThanOrEqualTo(list.remaining());
        });
    }

    public static class PackedSortedSerializer implements UnversionedSerializer<int[]>
    {
        public static final PackedSortedSerializer instance = new PackedSortedSerializer();

        @Override
        public void serialize(int[] t, DataOutputPlus out) throws IOException
        {
            SerializePacked.serializePackedSortedIntsAndLength(t, out);
        }

        @Override
        public int[] deserialize(DataInputPlus in) throws IOException
        {
            return SerializePacked.deserializePackedSortedIntsAndLength(in);
        }

        @Override
        public long serializedSize(int[] t)
        {
            return SerializePacked.serializedSizeOfPackedSortedIntsAndLength(t);
        }
    }

    public static class SimpleListSerializer implements UnversionedSerializer<int[]>
    {
        public static final SimpleListSerializer instance = new SimpleListSerializer();

        @Override
        public void serialize(int[] t, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(t.length);
            for (int i : t)
                out.writeVInt32(i);
        }

        @Override
        public int[] deserialize(DataInputPlus in) throws IOException
        {
            int size = in.readUnsignedVInt32();
            int[] array = new int[size];
            for (int i = 0; i < size; i++)
                array[i] = in.readVInt32();
            return array;
        }

        @Override
        public long serializedSize(int[] t)
        {
            long size = TypeSizes.sizeofUnsignedVInt(t.length);
            for (int i = 0; i < t.length; i++)
                size += TypeSizes.sizeofVInt(t[i]);
            return size;
        }
    }
}