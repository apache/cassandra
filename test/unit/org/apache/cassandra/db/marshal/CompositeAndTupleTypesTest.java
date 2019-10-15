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

package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.marshal.ValueAccessors.ACCESSORS;

public class CompositeAndTupleTypesTest
{
    interface TypeFactory<T extends AbstractType> { T createType(List<AbstractType<?>> types); }
    interface ValueCombiner<V> { V combine(ValueAccessor<V> accessor, V[] values); }

    public static Object[] createValues(ValueGenerator[] generators, Random random)
    {
        Object[] values = new Object[generators.length];
        for (int i=0; i<generators.length; i++)
            values[i] = generators[i].nextValue(random);
        return values;
    }

    public static ByteBuffer[] decompose(ValueGenerator[] generators, Object[] values)
    {
        assert generators.length == values.length;
        ByteBuffer[] decomposed = new ByteBuffer[generators.length];
        for (int i=0; i<generators.length; i++)
        {
            decomposed[i] = ((AbstractType<Object>) generators[i].getType()).decompose(values[i]);
        }
        return decomposed;
    }

    public static <S, D> D[] convert(S[] src, ValueAccessor<S> srcAccessor, ValueAccessor<D> dstAccessor)
    {
        D[] dst = dstAccessor.createArray(src.length);
        for (int i=0; i<src.length; i++)
            dst[i] = dstAccessor.convert(src[i], srcAccessor);
        return dst;
    }

    public static <V> Object[] compose(ValueGenerator[] generators, V[] values, ValueAccessor<V> accessor)
    {
        assert generators.length == values.length;

        Object[] composed = new Object[generators.length];
        for (int i=0; i<generators.length; i++)
        {
            composed[i] = ((AbstractType<Object>) generators[i].getType()).compose(values[i], accessor);
        }
        return composed;
    }

    public <AT extends AbstractType> void testSerializationDeserialization(TypeFactory<AT> typeFactory, ValueCombiner combiner)
    {
        for (int i=0; i<100; i++)
        {
            Random random = new Random(i);
            ValueGenerator[] generators = ValueGenerator.randomGenerators(random, 100);
            AT type = typeFactory.createType(ValueGenerator.toTypes(generators));

            for (int j=0; j<100; j++)
            {
                Object[] expected = createValues(generators, random);
                for (ValueAccessor<Object> srcAccessor : ACCESSORS)
                {
                    ByteBuffer[] srcBuffers = decompose(generators, expected);
                    Object[] srcValues = convert(srcBuffers, ByteBufferAccessor.instance, srcAccessor);
                    Object srcJoined = combiner.combine(srcAccessor, srcValues);
                    String srcString  = type.getString(srcJoined, srcAccessor);

                    for (ValueAccessor<Object> dstAccessor : ACCESSORS)
                    {
                        // convert data types and deserialize with
                        Object[] dstValues = convert(srcValues, srcAccessor, dstAccessor);
                        Object dstJoined = combiner.combine(dstAccessor, dstValues);
                        String dstString = type.getString(dstJoined, dstAccessor);

                        Object[] composed = compose(generators, dstValues, dstAccessor);
                        Assert.assertArrayEquals(expected, composed);
                        ValueAccessors.assertDataEquals(srcJoined, srcAccessor, dstJoined, dstAccessor);
                        Assert.assertEquals(srcString, dstString);
                    }
                }
            }
        }
    }

    @Test
    public void tuple()
    {
        testSerializationDeserialization(TupleType::new, TupleType::buildValue);
    }

    @Test
    public void userType()
    {
        TypeFactory<UserType> factory = types -> {
            List<FieldIdentifier> names = new ArrayList<>(types.size());
            for (int i=0; i<types.size(); i++)
            {
                names.add(FieldIdentifier.forUnquoted("t" + i));
            }
            return new UserType("ks", ByteBufferUtil.bytes("user_type"), names, types, false);
        };
        testSerializationDeserialization(factory, TupleType::buildValue);
    }

    @Test
    public void composite()
    {
        testSerializationDeserialization(CompositeType::new, CompositeType::build);
    }
}
