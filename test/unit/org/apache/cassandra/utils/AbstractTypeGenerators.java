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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.UserType;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.SourceDSL;

import static org.apache.cassandra.utils.Generators.IDENTIFIER_GEN;

public final class AbstractTypeGenerators
{
    private static final Gen<Integer> VERY_SMALL_POSITIVE_SIZE_GEN = SourceDSL.integers().between(1, 3);
    private static final Gen<Boolean> BOOLEAN_GEN = SourceDSL.booleans().all();

    private static final Map<AbstractType<?>, TypeSupport<?>> PRIMITIVE_TYPE_DATA_GENS =
    Stream.of(TypeSupport.of(BooleanType.instance, BOOLEAN_GEN),
              TypeSupport.of(ByteType.instance, SourceDSL.integers().between(0, Byte.MAX_VALUE * 2 + 1).map(Integer::byteValue)),
              TypeSupport.of(ShortType.instance, SourceDSL.integers().between(0, Short.MAX_VALUE * 2 + 1).map(Integer::shortValue)),
              TypeSupport.of(Int32Type.instance, SourceDSL.integers().all()),
              TypeSupport.of(LongType.instance, SourceDSL.longs().all()),
              TypeSupport.of(FloatType.instance, SourceDSL.floats().any()),
              TypeSupport.of(DoubleType.instance, SourceDSL.doubles().any()),
              TypeSupport.of(BytesType.instance, Generators.bytes(1, 1024)),
              TypeSupport.of(UUIDType.instance, Generators.UUID_RANDOM_GEN),
              TypeSupport.of(InetAddressType.instance, Generators.INET_ADDRESS_UNRESOLVED_GEN), // serialization strips the hostname, only keeps the address
              TypeSupport.of(AsciiType.instance, SourceDSL.strings().ascii().ofLengthBetween(1, 1024)),
              TypeSupport.of(UTF8Type.instance, Generators.utf8(1, 1024)),
              TypeSupport.of(TimestampType.instance, Generators.DATE_GEN),
              // null is desired here as #decompose will call org.apache.cassandra.serializers.EmptySerializer.serialize which ignores the input and returns empty bytes
              TypeSupport.of(EmptyType.instance, rnd -> null)
              //TODO add the following
              // IntegerType.instance,
              // DecimalType.instance,
              // TimeUUIDType.instance,
              // LexicalUUIDType.instance,
              // SimpleDateType.instance,
              // TimeType.instance,
              // DurationType.instance,
    ).collect(Collectors.toMap(t -> t.type, t -> t));
    // NOTE not supporting reversed as CQL doesn't allow nested reversed types
    // when generating part of the clustering key, it would be good to allow reversed types as the top level
    private static final Gen<AbstractType<?>> PRIMITIVE_TYPE_GEN = SourceDSL.arbitrary().pick(new ArrayList<>(PRIMITIVE_TYPE_DATA_GENS.keySet()));

    private AbstractTypeGenerators()
    {

    }

    public enum TypeKind
    {PRIMITIVE, SET, LIST, MAP, TUPLE, UDT}

    private static final Gen<TypeKind> TYPE_KIND_GEN = SourceDSL.arbitrary().enumValuesWithNoOrder(TypeKind.class);

    public static Gen<AbstractType<?>> primitiveTypeGen()
    {
        return PRIMITIVE_TYPE_GEN;
    }

    public static Gen<AbstractType<?>> typeGen()
    {
        return typeGen(3);
    }

    public static Gen<AbstractType<?>> typeGen(int maxDepth)
    {
        return typeGen(maxDepth, TYPE_KIND_GEN, VERY_SMALL_POSITIVE_SIZE_GEN);
    }

    public static Gen<AbstractType<?>> typeGen(int maxDepth, Gen<TypeKind> typeKindGen, Gen<Integer> sizeGen)
    {
        assert maxDepth >= 0 : "max depth must be positive or zero; given " + maxDepth;
        boolean atBottom = maxDepth == 0;
        return rnd -> {
            // figure out type to get
            TypeKind kind = typeKindGen.generate(rnd);
            switch (kind)
            {
                case PRIMITIVE:
                    return PRIMITIVE_TYPE_GEN.generate(rnd);
                case SET:
                    return setTypeGen(atBottom ? PRIMITIVE_TYPE_GEN : typeGen(maxDepth - 1, typeKindGen, sizeGen)).generate(rnd);
                case LIST:
                    return listTypeGen(atBottom ? PRIMITIVE_TYPE_GEN : typeGen(maxDepth - 1, typeKindGen, sizeGen)).generate(rnd);
                case MAP:
                    return mapTypeGen(atBottom ? PRIMITIVE_TYPE_GEN : typeGen(maxDepth - 1, typeKindGen, sizeGen)).generate(rnd);
                case TUPLE:
                    return tupleTypeGen(atBottom ? PRIMITIVE_TYPE_GEN : typeGen(maxDepth - 1, typeKindGen, sizeGen), sizeGen).generate(rnd);
                case UDT:
                    return userTypeGen(atBottom ? PRIMITIVE_TYPE_GEN : typeGen(maxDepth - 1, typeKindGen, sizeGen), sizeGen).generate(rnd);
                default:
                    throw new IllegalArgumentException("Unknown kind: " + kind);
            }
        };
    }

    @SuppressWarnings("unused")
    public static Gen<SetType<?>> setTypeGen()
    {
        return setTypeGen(typeGen(2)); // lower the default depth since this is already a nested type
    }

    public static Gen<SetType<?>> setTypeGen(Gen<AbstractType<?>> typeGen)
    {
        return rnd -> SetType.getInstance(typeGen.generate(rnd), BOOLEAN_GEN.generate(rnd));
    }

    @SuppressWarnings("unused")
    public static Gen<ListType<?>> listTypeGen()
    {
        return listTypeGen(typeGen(2)); // lower the default depth since this is already a nested type
    }

    public static Gen<ListType<?>> listTypeGen(Gen<AbstractType<?>> typeGen)
    {
        return rnd -> ListType.getInstance(typeGen.generate(rnd), BOOLEAN_GEN.generate(rnd));
    }

    @SuppressWarnings("unused")
    public static Gen<MapType<?, ?>> mapTypeGen()
    {
        return mapTypeGen(typeGen(2)); // lower the default depth since this is already a nested type
    }

    public static Gen<MapType<?, ?>> mapTypeGen(Gen<AbstractType<?>> typeGen)
    {
        return mapTypeGen(typeGen, typeGen);
    }

    public static Gen<MapType<?, ?>> mapTypeGen(Gen<AbstractType<?>> keyGen, Gen<AbstractType<?>> valueGen)
    {
        return rnd -> MapType.getInstance(keyGen.generate(rnd), valueGen.generate(rnd), BOOLEAN_GEN.generate(rnd));
    }

    public static Gen<TupleType> tupleTypeGen()
    {
        return tupleTypeGen(typeGen(2)); // lower the default depth since this is already a nested type
    }

    public static Gen<TupleType> tupleTypeGen(Gen<AbstractType<?>> elementGen)
    {
        return tupleTypeGen(elementGen, VERY_SMALL_POSITIVE_SIZE_GEN);
    }

    public static Gen<TupleType> tupleTypeGen(Gen<AbstractType<?>> elementGen, Gen<Integer> sizeGen)
    {
        return rnd -> {
            int numElements = sizeGen.generate(rnd);
            List<AbstractType<?>> elements = new ArrayList<>(numElements);
            for (int i = 0; i < numElements; i++)
                elements.add(elementGen.generate(rnd));
            return new TupleType(elements);
        };
    }

    public static Gen<UserType> userTypeGen()
    {
        return userTypeGen(typeGen(2)); // lower the default depth since this is already a nested type
    }

    public static Gen<UserType> userTypeGen(Gen<AbstractType<?>> elementGen)
    {
        return userTypeGen(elementGen, VERY_SMALL_POSITIVE_SIZE_GEN);
    }

    public static Gen<UserType> userTypeGen(Gen<AbstractType<?>> elementGen, Gen<Integer> sizeGen)
    {
        Gen<FieldIdentifier> fieldNameGen = IDENTIFIER_GEN.map(FieldIdentifier::forQuoted);
        return rnd -> {
            boolean multiCell = BOOLEAN_GEN.generate(rnd);
            int numElements = sizeGen.generate(rnd);
            List<AbstractType<?>> fieldTypes = new ArrayList<>(numElements);
            LinkedHashSet<FieldIdentifier> fieldNames = new LinkedHashSet<>(numElements);
            String ks = IDENTIFIER_GEN.generate(rnd);
            ByteBuffer name = AsciiType.instance.decompose(IDENTIFIER_GEN.generate(rnd));

            Gen<FieldIdentifier> distinctNameGen = Generators.filter(fieldNameGen, 30, e -> !fieldNames.contains(e));
            // UDTs don't allow duplicate names, so make sure all names are unique
            for (int i = 0; i < numElements; i++)
            {
                fieldTypes.add(elementGen.generate(rnd));
                fieldNames.add(distinctNameGen.generate(rnd));
            }
            return new UserType(ks, name, new ArrayList<>(fieldNames), fieldTypes, multiCell);
        };
    }

    public static Gen<AbstractType<?>> allowReversed(Gen<AbstractType<?>> gen)
    {
        return rnd -> BOOLEAN_GEN.generate(rnd) ? ReversedType.getInstance(gen.generate(rnd)) : gen.generate(rnd);
    }

    /**
     * For a type, create generators for data that matches that type
     */
    public static <T> TypeSupport<T> getTypeSupport(AbstractType<T> type)
    {
        return getTypeSupport(type, VERY_SMALL_POSITIVE_SIZE_GEN);
    }

    /**
     * For a type, create generators for data that matches that type
     */
    public static <T> TypeSupport<T> getTypeSupport(AbstractType<T> type, Gen<Integer> sizeGen)
    {
        // this doesn't affect the data, only sort order, so drop it
        if (type.isReversed())
            type = ((ReversedType<T>) type).baseType;
        // cast is safe since type is a constant and was type cast while inserting into the map
        @SuppressWarnings("unchecked")
        TypeSupport<T> gen = (TypeSupport<T>) PRIMITIVE_TYPE_DATA_GENS.get(type);
        if (gen != null)
            return gen;
        // might be... complex...
        if (type instanceof SetType)
        {
            // T = Set<A> so can not use T here
            SetType<Object> setType = (SetType<Object>) type;
            TypeSupport<?> elementSupport = getTypeSupport(setType.getElementsType(), sizeGen);
            @SuppressWarnings("unchecked")
            TypeSupport<T> support = (TypeSupport<T>) TypeSupport.of(setType, rnd -> {
                int size = sizeGen.generate(rnd);
                HashSet<Object> set = Sets.newHashSetWithExpectedSize(size);
                for (int i = 0; i < size; i++)
                    set.add(elementSupport.valueGen.generate(rnd));
                return set;
            });
            return support;
        }
        else if (type instanceof ListType)
        {
            // T = List<A> so can not use T here
            ListType<Object> listType = (ListType<Object>) type;
            TypeSupport<?> elementSupport = getTypeSupport(listType.getElementsType(), sizeGen);
            @SuppressWarnings("unchecked")
            TypeSupport<T> support = (TypeSupport<T>) TypeSupport.of(listType, rnd -> {
                int size = sizeGen.generate(rnd);
                List<Object> list = new ArrayList<>(size);
                for (int i = 0; i < size; i++)
                    list.add(elementSupport.valueGen.generate(rnd));
                return list;
            });
            return support;
        }
        else if (type instanceof MapType)
        {
            // T = Map<A, B> so can not use T here
            MapType<Object, Object> mapType = (MapType<Object, Object>) type;
            TypeSupport<?> keySupport = getTypeSupport(mapType.getKeysType(), sizeGen);
            TypeSupport<?> valueSupport = getTypeSupport(mapType.getValuesType(), sizeGen);
            @SuppressWarnings("unchecked")
            TypeSupport<T> support = (TypeSupport<T>) TypeSupport.of(mapType, rnd -> {
                int size = sizeGen.generate(rnd);
                Map<Object, Object> map = Maps.newHashMapWithExpectedSize(size);
                // if there is conflict thats fine
                for (int i = 0; i < size; i++)
                    map.put(keySupport.valueGen.generate(rnd), valueSupport.valueGen.generate(rnd));
                return map;
            });
            return support;
        }
        else if (type instanceof TupleType) // includes UserType
        {
            // T is ByteBuffer
            TupleType tupleType = (TupleType) type;
            @SuppressWarnings("unchecked")
            TypeSupport<T> support = (TypeSupport<T>) TypeSupport.of(tupleType, new TupleGen(tupleType, sizeGen));
            return support;
        }
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    private static final class TupleGen implements Gen<ByteBuffer>
    {
        private final List<TypeSupport<Object>> elementsSupport;

        @SuppressWarnings("unchecked")
        private TupleGen(TupleType tupleType, Gen<Integer> sizeGen)
        {
            this.elementsSupport = tupleType.allTypes().stream().map(t -> getTypeSupport((AbstractType<Object>) t, sizeGen)).collect(Collectors.toList());
        }

        public ByteBuffer generate(RandomnessSource rnd)
        {
            List<TypeSupport<Object>> eSupport = this.elementsSupport;
            ByteBuffer[] elements = new ByteBuffer[eSupport.size()];
            for (int i = 0; i < eSupport.size(); i++)
            {
                TypeSupport<Object> support = eSupport.get(i);
                elements[i] = support.type.decompose(support.valueGen.generate(rnd));
            }
            return TupleType.buildValue(elements);
        }
    }

    /**
     * Pair of {@link AbstractType} and a Generator of values that are handled by that type.
     */
    public static final class TypeSupport<T>
    {
        public final AbstractType<T> type;
        public final Gen<T> valueGen;

        private TypeSupport(AbstractType<T> type, Gen<T> valueGen)
        {
            this.type = Objects.requireNonNull(type);
            this.valueGen = Objects.requireNonNull(valueGen);
        }

        public static <T> TypeSupport<T> of(AbstractType<T> type, Gen<T> valueGen)
        {
            return new TypeSupport<>(type, valueGen);
        }

        /**
         * Generator which composes the values gen with {@link AbstractType#decompose(Object)}
         */
        public Gen<ByteBuffer> bytesGen()
        {
            return rnd -> type.decompose(valueGen.generate(rnd));
        }

        public String toString()
        {
            return "TypeSupport{" +
                   "type=" + type +
                   '}';
        }
    }
}
