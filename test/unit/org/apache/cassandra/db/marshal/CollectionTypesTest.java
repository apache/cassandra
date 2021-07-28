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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.CQL3Type;

import static org.apache.cassandra.db.marshal.ValueAccessors.ACCESSORS;

public class CollectionTypesTest
{
    interface TypeFactory<T extends CollectionType> { T createType(AbstractType<?> keyType, AbstractType<?> valType); }
    interface ValueFactory<T> { T createValue(ValueGenerator keyGen, ValueGenerator valGen, int size, Random random); }

    static <CT extends CollectionType, T> void testSerializationDeserialization(TypeFactory<CT> typeFactory, ValueFactory<T> valueFactory, ValueGenerator keyType)
    {
        for (ValueGenerator valueType : ValueGenerator.GENERATORS)
        {
            CT type = typeFactory.createType(keyType != null ? keyType.getType() : null, valueType.getType());
            CQL3Type.Collection cql3Type = new CQL3Type.Collection(type);

            for (int i=0; i<500; i++)
            {
                Random random = new Random(i);
                int size = random.nextInt(1000);
                T expected = valueFactory.createValue(keyType, valueType, size, random);

                for (ValueAccessor<Object> srcAccessor : ACCESSORS)
                {
                    ByteBuffer srcBuffer = type.decompose(expected);
                    Object srcBytes = srcAccessor.convert(srcBuffer, ByteBufferAccessor.instance);
                    String srcString = type.getString(srcBytes, srcAccessor);

                    for (ValueAccessor<Object> dstAccessor : ACCESSORS)
                    {
                        Object dstBytes = dstAccessor.convert(srcBytes, srcAccessor);
                        String dstString = type.getString(dstBytes, dstAccessor);
                        T composed = (T) type.compose(dstBytes, dstAccessor);
                        Assert.assertEquals(expected, composed);
                        ValueAccessors.assertDataEquals(srcBytes, srcAccessor, dstBytes, dstAccessor);
                        Assert.assertEquals(srcString, dstString);
                    }
                }
            }
        }
    }

    static <CT extends CollectionType, T> void testSerializationDeserialization(TypeFactory<CT> typeFactory, ValueFactory<T> valueFactory)
    {
        for (ValueGenerator keyType : ValueGenerator.GENERATORS)
            testSerializationDeserialization(typeFactory, valueFactory, keyType);

    }

    private static List<Object> randomList(ValueGenerator keyGen, ValueGenerator valGen, int size, Random random)
    {
        List<Object> list = new ArrayList<>();
        for (int k=0; k<size; k++)
            list.add(valGen.nextValue(random));
        return list;
    }

    @Test
    public void list()
    {
        testSerializationDeserialization((k, v) -> ListType.getInstance(v, false), CollectionTypesTest::randomList, null);
    }

    private static Map<Object, Object> randomMap(ValueGenerator keyGen, ValueGenerator valGen, int size, Random random)
    {
        Map<Object, Object> map = new HashMap<>();
        for (int k=0; k<size; k++)
            map.put(keyGen.nextValue(random), valGen.nextValue(random));
        return map;
    }

    @Test
    public void map()
    {
        testSerializationDeserialization((k, v) -> MapType.getInstance(k, v, false), CollectionTypesTest::randomMap);
    }

    private static Set<Object> randomSet(ValueGenerator keyGen, ValueGenerator valGen, int size, Random random)
    {
        Set<Object> set = new HashSet<>();
        for (int k=0; k<size; k++)
            set.add(valGen.nextValue(random));
        return set;
    }

    @Test
    public void set()
    {
        testSerializationDeserialization((k, v) -> SetType.getInstance(v, false), CollectionTypesTest::randomSet, null);
    }
}
