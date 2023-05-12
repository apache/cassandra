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
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.FrozenType;
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
import org.apache.cassandra.db.marshal.VectorType;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.JavaRandom;

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
              TypeSupport.of(BytesType.instance, Generators.bytes(0, 1024)),
              TypeSupport.of(UUIDType.instance, Generators.UUID_RANDOM_GEN),
              TypeSupport.of(InetAddressType.instance, Generators.INET_ADDRESS_UNRESOLVED_GEN), // serialization strips the hostname, only keeps the address
              TypeSupport.of(AsciiType.instance, SourceDSL.strings().ascii().ofLengthBetween(0, 1024)),
              TypeSupport.of(UTF8Type.instance, Generators.utf8(0, 1024)),
              TypeSupport.of(TimestampType.instance, Generators.DATE_GEN),
              // null is desired here as #decompose will call org.apache.cassandra.serializers.EmptySerializer.serialize which ignores the input and returns empty bytes
              TypeSupport.of(EmptyType.instance, rnd -> null),
              TypeSupport.of(DurationType.instance, CassandraGenerators.duration())
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
    private static final Gen<AbstractType<?>> PRIMITIVE_TYPE_GEN;
    static
    {
        ArrayList<AbstractType<?>> types = new ArrayList<>(PRIMITIVE_TYPE_DATA_GENS.keySet());
        Collections.sort(types, Comparator.comparing(a -> a.getClass().getName()));
        PRIMITIVE_TYPE_GEN = SourceDSL.arbitrary().pick(types);
    }
    private static final Set<Class<? extends AbstractType>> NON_PRIMITIVE_TYPES = ImmutableSet.<Class<? extends AbstractType>>builder()
                                                                                              .add(SetType.class)
                                                                                              .add(ListType.class)
                                                                                              .add(MapType.class)
                                                                                              .add(TupleType.class)
                                                                                              .add(UserType.class)
                                                                                              .add(VectorType.class)
                                                                                              .build();

    private AbstractTypeGenerators()
    {

    }

    public enum TypeKind
    {PRIMITIVE, SET, LIST, MAP, TUPLE, UDT, VECTOR}

    private static final Gen<TypeKind> TYPE_KIND_GEN = SourceDSL.arbitrary().enumValuesWithNoOrder(TypeKind.class);

    public static Set<Class<? extends AbstractType>> knownTypes()
    {
        Set<Class<? extends AbstractType>> types = PRIMITIVE_TYPE_DATA_GENS.keySet().stream().map(a -> a.getClass()).collect(Collectors.toSet());
        types.addAll(NON_PRIMITIVE_TYPES);
        types.add(FrozenType.class);
        types.add(ReversedType.class);
        return types;
    }

    public static Gen<AbstractType<?>> primitiveTypeGen()
    {
        return PRIMITIVE_TYPE_GEN;
    }

    public static class TypeGenBuilder
    {
        private int maxDepth = 3;
        private EnumSet<TypeKind> kinds;
        private Gen<TypeKind> typeKindGen;
        private Gen<Integer> defaultSizeGen = VERY_SMALL_POSITIVE_SIZE_GEN;
        private Gen<Integer> vectorSizeGen, vectorSizeNonPrimitiveGen, tupleSizeGen, udtSizeGen;
        private Gen<AbstractType<?>> primitiveGen = PRIMITIVE_TYPE_GEN;
        private Gen<String> userTypeKeyspaceGen = IDENTIFIER_GEN;
        private Function<Integer, Gen<AbstractType<?>>> defaultSetKeyFunc;

        public TypeGenBuilder() {}

        public TypeGenBuilder(TypeGenBuilder other)
        {
            maxDepth = other.maxDepth;
            kinds = other.kinds == null ? null : EnumSet.copyOf(other.kinds);
            typeKindGen = other.typeKindGen;
            defaultSizeGen = other.defaultSizeGen;
            vectorSizeGen = other.vectorSizeGen;
            vectorSizeNonPrimitiveGen = other.vectorSizeNonPrimitiveGen;
            tupleSizeGen = other.tupleSizeGen;
            udtSizeGen = other.udtSizeGen;
            primitiveGen = other.primitiveGen;
            userTypeKeyspaceGen = other.userTypeKeyspaceGen;
            defaultSetKeyFunc = other.defaultSetKeyFunc;
        }

        public TypeGenBuilder withDefaultSetKey(Function<Integer, Gen<AbstractType<?>>> mapKeyFunc)
        {
            this.defaultSetKeyFunc = mapKeyFunc;
            return this;
        }

        public TypeGenBuilder withDefaultSetKey(TypeGenBuilder builder)
        {
            this.defaultSetKeyFunc = maxDepth -> builder.buildRecursive(maxDepth);
            return this;
        }

        public TypeGenBuilder withUserTypeKeyspace(String keyspace)
        {
            userTypeKeyspaceGen = SourceDSL.arbitrary().constant(keyspace);
            return this;
        }

        public TypeGenBuilder withDefaultSizeGen(Gen<Integer> sizeGen)
        {
            this.defaultSizeGen = sizeGen;
            return this;
        }

        public TypeGenBuilder withVectorSizeGen(Gen<Integer> sizeGen)
        {
            this.vectorSizeGen = sizeGen;
            return this;
        }

        public TypeGenBuilder withVectorSizeNonPrimitiveGen(Gen<Integer> sizeGen)
        {
            this.vectorSizeNonPrimitiveGen = sizeGen;
            return this;
        }

        public TypeGenBuilder withTupleSizeGen(Gen<Integer> sizeGen)
        {
            this.tupleSizeGen = sizeGen;
            return this;
        }

        public TypeGenBuilder withUDTSizeGen(Gen<Integer> sizeGen)
        {
            this.udtSizeGen = sizeGen;
            return this;
        }

        public TypeGenBuilder withoutEmpty()
        {
            return withoutPrimitive(EmptyType.instance);
        }

        public TypeGenBuilder withoutPrimitive(AbstractType<?> instance)
        {
            if (!PRIMITIVE_TYPE_DATA_GENS.keySet().contains(instance))
                throw new IllegalArgumentException("Type " + instance + " is not a primitive type, or PRIMITIVE_TYPE_DATA_GENS needs to add support");
            primitiveGen = Generators.filter(primitiveGen, t -> t != instance);
            return this;
        }

        public TypeGenBuilder withMaxDepth(int value)
        {
            this.maxDepth = value;
            return this;
        }

        public TypeGenBuilder withoutTypeKinds(TypeKind... values)
        {
            checkTypeKindValues();
            for (TypeKind kind : values)
                kinds.remove(kind);
            return this;
        }

        public TypeGenBuilder withTypeKinds(TypeKind... values)
        {
            checkTypeKindValues();
            kinds.clear();
            for (TypeKind k : values)
                kinds.add(k);
            return this;
        }

        private void checkTypeKindValues()
        {
            if (typeKindGen != null)
                throw new IllegalArgumentException("Mixed both generator and individaul values for type kind");
            if (kinds == null)
                kinds = EnumSet.allOf(TypeKind.class);
        }

        public TypeGenBuilder withTypeKinds(Gen<TypeKind> typeKindGen)
        {
            if (kinds != null)
                throw new IllegalArgumentException("Mixed both generator and individaul values for type kind");
            this.typeKindGen = Objects.requireNonNull(typeKindGen);
            return this;
        }

        public Gen<AbstractType<?>> build()
        {
            return buildRecursive(maxDepth);
        }

        private Gen<AbstractType<?>> buildRecursive(int maxDepth)
        {
            Gen<TypeKind> kindGen;
            if (typeKindGen != null)
                kindGen = typeKindGen;
            else if (kinds != null)
            {
                ArrayList<TypeKind> ts = new ArrayList<>(kinds);
                Collections.sort(ts);
                kindGen = SourceDSL.arbitrary().pick(ts);
            }
            else
                kindGen = SourceDSL.arbitrary().enumValues(TypeKind.class);
            return buildRecursive(maxDepth, kindGen);
        }

        private Gen<AbstractType<?>> buildRecursive(int maxDepth, Gen<TypeKind> typeKindGen)
        {
            if (maxDepth == -1)
                return primitiveGen;
            assert maxDepth >= 0 : "max depth must be positive or zero; given " + maxDepth;
            boolean atBottom = maxDepth == 0;
            return rnd -> {
                // figure out type to get
                TypeKind kind = typeKindGen.generate(rnd);
                switch (kind)
                {
                    case PRIMITIVE:
                        return primitiveGen.generate(rnd);
                    case SET:
                        if (defaultSetKeyFunc != null)
                            return setTypeGen(defaultSetKeyFunc.apply(maxDepth - 1)).generate(rnd);
                        return setTypeGen(atBottom ? primitiveGen : buildRecursive(maxDepth - 1, typeKindGen)).generate(rnd);
                    case LIST:
                        return listTypeGen(atBottom ? primitiveGen : buildRecursive(maxDepth - 1, typeKindGen)).generate(rnd);
                    case MAP:
                        if (defaultSetKeyFunc != null)
                            return mapTypeGen(defaultSetKeyFunc.apply(maxDepth - 1), buildRecursive(maxDepth - 1, typeKindGen)).generate(rnd);
                        return mapTypeGen(atBottom ? primitiveGen : buildRecursive(maxDepth - 1, typeKindGen)).generate(rnd);
                    case TUPLE:
                        return tupleTypeGen(atBottom ? primitiveGen : buildRecursive(maxDepth - 1, typeKindGen), tupleSizeGen != null ? tupleSizeGen : defaultSizeGen).generate(rnd);
                    case UDT:
                        return userTypeGen(atBottom ? primitiveGen : buildRecursive(maxDepth - 1, typeKindGen), udtSizeGen != null ? udtSizeGen : defaultSizeGen, userTypeKeyspaceGen).generate(rnd);
                    case VECTOR:
                    {
                        Gen<Integer> sizeGen = vectorSizeGen != null ? vectorSizeGen : defaultSizeGen;
                        if (!atBottom && vectorSizeNonPrimitiveGen != null)
                            sizeGen = vectorSizeNonPrimitiveGen;
                        return vectorTypeGen(atBottom ? primitiveGen : buildRecursive(maxDepth - 1, typeKindGen), sizeGen).generate(rnd);
                    }
                    default:
                        throw new IllegalArgumentException("Unknown kind: " + kind);
                }
            };
        }
    }

    public static TypeGenBuilder builder()
    {
        return new TypeGenBuilder();
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
        return builder().withMaxDepth(maxDepth).withTypeKinds(typeKindGen).withDefaultSizeGen(sizeGen).build();
    }
    public static Gen<VectorType<?>> vectorTypeGen()
    {
        return vectorTypeGen(typeGen(2)); // lower the default depth since this is already a nested type
    }

    public static Gen<VectorType<?>> vectorTypeGen(Gen<AbstractType<?>> typeGen)
    {
        return vectorTypeGen(typeGen, SourceDSL.integers().between(1, 100));
    }

    public static Gen<VectorType<?>> vectorTypeGen(Gen<AbstractType<?>> typeGen, Gen<Integer> dimentionGen)
    {
        return rnd -> {
            int dimention = dimentionGen.generate(rnd);
            AbstractType<?> element = typeGen.generate(rnd);
            // empty type not supported
            while (element == EmptyType.instance)
                element = typeGen.generate(rnd);
            return VectorType.getInstance(element, dimention);
        };
    }

    @SuppressWarnings("unused")
    public static Gen<SetType<?>> setTypeGen()
    {
        return setTypeGen(typeGen(2)); // lower the default depth since this is already a nested type
    }

    public static Gen<SetType<?>> setTypeGen(Gen<AbstractType<?>> typeGen)
    {
        return rnd -> SetType.getInstance(typeGen.generate(rnd).freeze(), BOOLEAN_GEN.generate(rnd));
    }

    @SuppressWarnings("unused")
    public static Gen<ListType<?>> listTypeGen()
    {
        return listTypeGen(typeGen(2)); // lower the default depth since this is already a nested type
    }

    public static Gen<ListType<?>> listTypeGen(Gen<AbstractType<?>> typeGen)
    {
        return rnd -> ListType.getInstance(typeGen.generate(rnd).freeze(), BOOLEAN_GEN.generate(rnd));
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
        return rnd -> MapType.getInstance(keyGen.generate(rnd).freeze(), valueGen.generate(rnd).freeze(), BOOLEAN_GEN.generate(rnd));
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
        return userTypeGen(elementGen, sizeGen, IDENTIFIER_GEN);
    }

    public static Gen<UserType> userTypeGen(Gen<AbstractType<?>> elementGen, Gen<Integer> sizeGen, Gen<String> ksGen)
    {
        Gen<FieldIdentifier> fieldNameGen = IDENTIFIER_GEN.map(FieldIdentifier::forQuoted);
        return rnd -> {
            boolean multiCell = BOOLEAN_GEN.generate(rnd);
            int numElements = sizeGen.generate(rnd);
            List<AbstractType<?>> fieldTypes = new ArrayList<>(numElements);
            LinkedHashSet<FieldIdentifier> fieldNames = new LinkedHashSet<>(numElements);
            String ks = ksGen.generate(rnd);
            ByteBuffer name = AsciiType.instance.decompose(IDENTIFIER_GEN.generate(rnd));

            Gen<FieldIdentifier> distinctNameGen = Generators.filter(fieldNameGen, 30, e -> !fieldNames.contains(e));
            // UDTs don't allow duplicate names, so make sure all names are unique
            for (int i = 0; i < numElements; i++)
            {
                fieldTypes.add(elementGen.generate(rnd).freeze());
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

    public static <T> TypeSupport<T> getTypeSupportWithNulls(AbstractType<T> type, Gen<Boolean> nulls)
    {
        return getTypeSupport(type, VERY_SMALL_POSITIVE_SIZE_GEN, nulls);
    }

    public static <T> TypeSupport<T> getTypeSupport(AbstractType<T> type, Gen<Integer> sizeGen)
    {
        return getTypeSupport(type, sizeGen, null);
    }

    /**
     * For a type, create generators for data that matches that type
     */
    public static <T> TypeSupport<T> getTypeSupport(AbstractType<T> type, Gen<Integer> sizeGen, @Nullable Gen<Boolean> nulls)
    {
        Objects.requireNonNull(sizeGen, "sizeGen");
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
            TypeSupport<?> elementSupport = getTypeSupport(setType.getElementsType(), sizeGen, nulls);
            @SuppressWarnings("unchecked")
            TypeSupport<T> support = (TypeSupport<T>) TypeSupport.of(setType, rnd -> {
                int size = sizeGen.generate(rnd);
                size = normalizeSizeFromType(elementSupport, size);
                HashSet<Object> set = Sets.newHashSetWithExpectedSize(size);
                for (int i = 0; i < size; i++)
                {
                    Object generate = elementSupport.valueGen.generate(rnd);
                    for (int attempts = 0; set.contains(generate); attempts++)
                    {
                        if (attempts == 42)
                            throw new AssertionError(String.format("Unable to get unique element for type %s with the size %d", elementSupport.type.asCQL3Type(), size));
                        rnd = JavaRandom.wrap(rnd);
                        generate = elementSupport.valueGen.generate(rnd);
                    }

                    set.add(generate);
                }
                return set;
            });
            return support;
        }
        else if (type instanceof ListType)
        {
            // T = List<A> so can not use T here
            ListType<Object> listType = (ListType<Object>) type;
            TypeSupport<?> elementSupport = getTypeSupport(listType.getElementsType(), sizeGen, nulls);
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
            TypeSupport<?> keySupport = getTypeSupport(mapType.getKeysType(), sizeGen, nulls);
            TypeSupport<?> valueSupport = getTypeSupport(mapType.getValuesType(), sizeGen, nulls);
            @SuppressWarnings("unchecked")
            TypeSupport<T> support = (TypeSupport<T>) TypeSupport.of(mapType, rnd -> {
                int size = sizeGen.generate(rnd);
                size = normalizeSizeFromType(keySupport, size);
                Map<Object, Object> map = Maps.newHashMapWithExpectedSize(size);
                // if there is conflict thats fine
                for (int i = 0; i < size; i++)
                {
                    Object key = keySupport.valueGen.generate(rnd);
                    for (int attempts = 0; map.containsKey(key); attempts++)
                    {
                        if (attempts == 42)
                            throw new AssertionError(String.format("Unable to get unique element for type %s with the size %d", keySupport.type.asCQL3Type(), size));
                        rnd = JavaRandom.wrap(rnd);
                        key = keySupport.valueGen.generate(rnd);
                    }
                    map.put(key, valueSupport.valueGen.generate(rnd));
                }
                return map;
            });
            return support;
        }
        else if (type instanceof TupleType) // includes UserType
        {
            // T is ByteBuffer
            TupleType tupleType = (TupleType) type;
            @SuppressWarnings("unchecked")
            TypeSupport<T> support = (TypeSupport<T>) TypeSupport.of(tupleType, new TupleGen(tupleType, sizeGen, nulls));
            return support;
        }
        else if (type instanceof VectorType)
        {
            VectorType<Object> vectorType = (VectorType<Object>) type;
            TypeSupport<?> elementSupport = getTypeSupport(vectorType.elementType, sizeGen, nulls);
            return (TypeSupport<T>) TypeSupport.of(vectorType, rnd -> {
                List<Object> list = new ArrayList<>(vectorType.dimension);
                for (int i = 0; i < vectorType.dimension; i++)
                {
                    Object generate = elementSupport.valueGen.generate(rnd);
                    if (generate == null)
                        throw new AssertionError(String.format("TypeSupport(%s) generated a null value", vectorType.elementType.asCQL3Type()));
                    list.add(generate);
                }
                return list;
            });
        }
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    private static int uniqueElementsForDomain(AbstractType<?> type)
    {
        type = type.unwrap();
        if (type instanceof BooleanType)
            return 2;
        if (type instanceof EmptyType)
            return 1;
        if (type instanceof SetType)
            return uniqueElementsForDomain(((SetType<?>) type).getElementsType());
        if (type instanceof MapType)
            return uniqueElementsForDomain(((MapType<?, ?>) type).getKeysType());
        if (type instanceof VectorType)
        {
            VectorType<?> vector = (VectorType<?>) type;
            int uniq = uniqueElementsForDomain(vector.elementType);
            if (uniq == 1)
                return 1;
            if (uniq != -1)
                return uniq * vector.dimension;
        }
        if (type instanceof TupleType)
        {
            TupleType tt = (TupleType) type;
            int product = 1;
            for (AbstractType<?> f : tt.subTypes())
            {
                int uniq = uniqueElementsForDomain(f);
                if (uniq == -1)
                    return -1;
                product *= uniq;
            }
            return product;
        }
        return -1;
    }

    private static int normalizeSizeFromType(TypeSupport<?> keySupport, int size)
    {
        int uniq = uniqueElementsForDomain(keySupport.type);
        if (uniq == -1)
            return size;
        return Math.min(size, uniq);
    }

    public static Set<UserType> extractUDTs(AbstractType<?> type)
    {
        Set<UserType> matches = new HashSet<>();
        extractUDTs(type, matches);
        return matches;
    }

    public static void extractUDTs(AbstractType<?> type, Set<UserType> matches)
    {
        if (type instanceof ReversedType)
            type = ((ReversedType) type).baseType;
        if (type instanceof UserType)
            matches.add((UserType) type);
        for (AbstractType<?> t : type.subTypes())
            extractUDTs(t, matches);
    }

    private static final class TupleGen implements Gen<ByteBuffer>
    {
        private final List<TypeSupport<Object>> elementsSupport;
        private final @Nullable Gen<Boolean> nulls;

        @SuppressWarnings("unchecked")
        private TupleGen(TupleType tupleType, Gen<Integer> sizeGen, @Nullable Gen<Boolean> nulls)
        {
            this.elementsSupport = tupleType.allTypes().stream().map(t -> getTypeSupport((AbstractType<Object>) t, sizeGen, nulls)).collect(Collectors.toList());
            this.nulls = nulls;
        }

        public ByteBuffer generate(RandomnessSource rnd)
        {
            List<TypeSupport<Object>> eSupport = this.elementsSupport;
            ByteBuffer[] elements = new ByteBuffer[eSupport.size()];
            for (int i = 0; i < eSupport.size(); i++)
            {
                TypeSupport<Object> support = eSupport.get(i);
                elements[i] = nulls != null && nulls.generate(rnd) ? null : support.type.decompose(support.valueGen.generate(rnd));
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
