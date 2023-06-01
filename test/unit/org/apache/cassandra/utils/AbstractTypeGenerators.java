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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.lang3.ArrayUtils;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.FrozenType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LegacyTimeUUIDType;
import org.apache.cassandra.db.marshal.LexicalUUIDType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.PartitionerDefinedOrder;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.StringType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
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

    private static final Map<Class<? extends AbstractType<?>>, String> UNSUPPORTED_PRIMITIVES = ImmutableMap.<Class<? extends AbstractType<?>>, String>builder()
                                                                                                            .put(DateType.class, "Says its CQL type is timestamp, but that maps to TimestampType; is this actually dead code at this point?")
                                                                                                            .put(LegacyTimeUUIDType.class, "Says its CQL timeuuid type, but that maps to TimeUUIDType; is this actually dead code at this point?")
                                                                                                            .put(PartitionerDefinedOrder.class, "This is a fake type used for ordering partitions using a Partitioner")
                                                                                                            .build();

    /**
     * Java does a char by char compare, but Cassandra does a byte ordered compare.  This mostly overlaps but some cases
     * where chars are mixed between 1 and 2 bytes, you can get a different ordering than java's.  One argument in favor
     * of this is that byte order is far faster than char order, which only violates a few cases but not the general case:
     * {@code "David" > "david"}.  Without more research, it also isn't clear if this at all violates any UTF-8 spec (wikipedia
     * mentions "sorting the corresponding byte sequences").
     *
     * @see <a href="https://the-asf.slack.com/archives/CK23JSY2K/p1684257304714649">Slack</a>
     */
    private static Comparator<String> stringComparator(StringType st)
    {
        return (String a, String b) -> FastByteOperations.compareUnsigned(st.decompose(a), st.decompose(b));
    }


    private static final Map<AbstractType<?>, TypeSupport<?>> PRIMITIVE_TYPE_DATA_GENS =
    Stream.of(TypeSupport.of(BooleanType.instance, BOOLEAN_GEN),
              TypeSupport.of(ByteType.instance, SourceDSL.integers().between(0, Byte.MAX_VALUE * 2 + 1).map(Integer::byteValue)),
              TypeSupport.of(ShortType.instance, SourceDSL.integers().between(0, Short.MAX_VALUE * 2 + 1).map(Integer::shortValue)),
              TypeSupport.of(Int32Type.instance, SourceDSL.integers().all()),
              TypeSupport.of(LongType.instance, SourceDSL.longs().all()),
              TypeSupport.of(FloatType.instance, SourceDSL.floats().any()),
              TypeSupport.of(DoubleType.instance, SourceDSL.doubles().any()),
              TypeSupport.of(BytesType.instance, Generators.bytes(0, 1024), FastByteOperations::compareUnsigned), // use the faster version...
              TypeSupport.of(UUIDType.instance, Generators.UUID_RANDOM_GEN),
              TypeSupport.of(TimeUUIDType.instance, Generators.UUID_TIME_GEN.map(TimeUUID::fromUuid)),
              TypeSupport.of(LexicalUUIDType.instance, Generators.UUID_RANDOM_GEN.mix(Generators.UUID_TIME_GEN)),
              TypeSupport.of(InetAddressType.instance, Generators.INET_ADDRESS_UNRESOLVED_GEN, (a, b) -> FastByteOperations.compareUnsigned(a.getAddress(), b.getAddress())), // serialization strips the hostname, only keeps the address
              TypeSupport.of(AsciiType.instance, SourceDSL.strings().ascii().ofLengthBetween(0, 1024), stringComparator(AsciiType.instance)),
              TypeSupport.of(UTF8Type.instance, Generators.utf8(0, 1024), stringComparator(UTF8Type.instance)),
              TypeSupport.of(TimestampType.instance, Generators.DATE_GEN),
              TypeSupport.of(SimpleDateType.instance, SourceDSL.integers().between(0, Integer.MAX_VALUE)), // can't use time gen as this is an int, and in Milliseconds... so overflows...
              // null is desired here as #decompose will call org.apache.cassandra.serializers.EmptySerializer.serialize which ignores the input and returns empty bytes
              TypeSupport.of(EmptyType.instance, rnd -> null, (a, b) -> 0),
              TypeSupport.of(DurationType.instance, CassandraGenerators.duration(), Comparator.comparingInt(Duration::getMonths)
                                                                                              .thenComparingInt(Duration::getDays)
                                                                                              .thenComparingLong(Duration::getNanoseconds)),
              TypeSupport.of(IntegerType.instance, Generators.bigInt()),
              TypeSupport.of(DecimalType.instance, Generators.bigDecimal())
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
        types.addAll(UNSUPPORTED_PRIMITIVES.keySet());
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

        public TypeGenBuilder withPrimitives(AbstractType<?> first, AbstractType<?>... remaining)
        {
            // any previous filters will be ignored...
            primitiveGen = SourceDSL.arbitrary().pick(ArrayUtils.add(remaining, first));
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

        // used during iteration, not something pluggable for users
        private Gen<String> udtName = null;

        public Gen<AbstractType<?>> build()
        {
            udtName = Generators.unique(IDENTIFIER_GEN);
            return buildRecursive(maxDepth);
        }

        private Gen<AbstractType<?>> buildRecursive(int maxDepth)
        {
            if (udtName == null)
                udtName = Generators.unique(IDENTIFIER_GEN);
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
            return buildRecursive(maxDepth, maxDepth, kindGen, BOOLEAN_GEN);
        }

        private Gen<AbstractType<?>> buildRecursive(int maxDepth, int level, Gen<TypeKind> typeKindGen, Gen<Boolean> multiCellGen)
        {
            if (level == -1)
                return primitiveGen;
            assert level >= 0 : "max depth must be positive or zero; given " + level;
            boolean atBottom = level == 0;
            boolean atTop = maxDepth == level;
            Gen<Boolean> multiCell = atTop ? Generators.cached(multiCellGen) : multiCellGen;
            return rnd -> {
                Supplier<Gen<AbstractType<?>>> next = () -> atBottom ? primitiveGen : buildRecursive(maxDepth, level - 1, typeKindGen, multiCell);

                // figure out type to get
                TypeKind kind = typeKindGen.generate(rnd);
                switch (kind)
                {
                    case PRIMITIVE:
                        return primitiveGen.generate(rnd);
                    case SET:
                        if (defaultSetKeyFunc != null)
                            return setTypeGen(defaultSetKeyFunc.apply(level - 1), multiCell).generate(rnd);
                        return setTypeGen(next.get(), multiCell).generate(rnd);
                    case LIST:
                        return listTypeGen(next.get(), multiCell).generate(rnd);
                    case MAP:
                        if (defaultSetKeyFunc != null)
                            return mapTypeGen(defaultSetKeyFunc.apply(level - 1), next.get(), multiCell).generate(rnd);
                        return mapTypeGen(next.get(), next.get(), multiCell).generate(rnd);
                    case TUPLE:
                        return tupleTypeGen(next.get().map(AbstractType::freeze), tupleSizeGen != null ? tupleSizeGen : defaultSizeGen).generate(rnd);
                    case UDT:
                        return userTypeGen(next.get(), udtSizeGen != null ? udtSizeGen : defaultSizeGen, userTypeKeyspaceGen, udtName, multiCell).generate(rnd);
                    case VECTOR:
                    {
                        Gen<Integer> sizeGen = vectorSizeGen != null ? vectorSizeGen : defaultSizeGen;
                        if (!atBottom && vectorSizeNonPrimitiveGen != null)
                            sizeGen = vectorSizeNonPrimitiveGen;
                        return vectorTypeGen(next.get().map(AbstractType::freeze), sizeGen).generate(rnd);
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
        return setTypeGen(typeGen, BOOLEAN_GEN);
    }

    public static Gen<SetType<?>> setTypeGen(Gen<AbstractType<?>> typeGen, Gen<Boolean> multiCell)
    {
        return rnd -> SetType.getInstance(typeGen.generate(rnd).freeze(), multiCell.generate(rnd));
    }

    @SuppressWarnings("unused")
    public static Gen<ListType<?>> listTypeGen()
    {
        return listTypeGen(typeGen(2)); // lower the default depth since this is already a nested type
    }

    public static Gen<ListType<?>> listTypeGen(Gen<AbstractType<?>> typeGen)
    {
        return listTypeGen(typeGen, BOOLEAN_GEN);
    }

    public static Gen<ListType<?>> listTypeGen(Gen<AbstractType<?>> typeGen, Gen<Boolean> multiCell)
    {
        return rnd -> ListType.getInstance(typeGen.generate(rnd).freeze(), multiCell.generate(rnd));
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
        return mapTypeGen(keyGen, valueGen, BOOLEAN_GEN);
    }

    public static Gen<MapType<?, ?>> mapTypeGen(Gen<AbstractType<?>> keyGen, Gen<AbstractType<?>> valueGen, Gen<Boolean> multiCell)
    {
        return rnd -> MapType.getInstance(keyGen.generate(rnd).freeze(), valueGen.generate(rnd).freeze(), multiCell.generate(rnd));
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
        return userTypeGen(elementGen, sizeGen, ksGen, IDENTIFIER_GEN);
    }

    public static Gen<UserType> userTypeGen(Gen<AbstractType<?>> elementGen, Gen<Integer> sizeGen, Gen<String> ksGen, Gen<String> nameGen)
    {
        return userTypeGen(elementGen, sizeGen, ksGen, nameGen, BOOLEAN_GEN);
    }

    public static Gen<UserType> userTypeGen(Gen<AbstractType<?>> elementGen, Gen<Integer> sizeGen, Gen<String> ksGen, Gen<String> nameGen, Gen<Boolean> multiCellGen)
    {
        Gen<FieldIdentifier> fieldNameGen = IDENTIFIER_GEN.map(FieldIdentifier::forQuoted);
        return rnd -> {
            boolean multiCell = multiCellGen.generate(rnd);
            int numElements = sizeGen.generate(rnd);
            List<AbstractType<?>> fieldTypes = new ArrayList<>(numElements);
            LinkedHashSet<FieldIdentifier> fieldNames = new LinkedHashSet<>(numElements);
            String ks = ksGen.generate(rnd);
            ByteBuffer name = AsciiType.instance.decompose(nameGen.generate(rnd));

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
        type = type.unwrap();
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
            TypeSupport<Object> elementSupport = getTypeSupport(setType.getElementsType(), sizeGen, nulls);
            Comparator<Object> elComparator = elementSupport.valueComparator;
            Comparator<List<Object>> setComparator = listComparator(elComparator);
            Comparator<Set<Object>> comparator = (Set<Object> a, Set<Object> b) -> {
                List<Object> as = new ArrayList<>(a);
                Collections.sort(as, elComparator);
                List<Object> bs = new ArrayList<>(b);
                Collections.sort(bs, elComparator);
                return setComparator.compare(as, bs);
            };
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
                            throw new AssertionError(String.format("Unable to get unique element for type %s with the size %d", typeTree(elementSupport.type), size));
                        rnd = JavaRandom.wrap(rnd);
                        generate = elementSupport.valueGen.generate(rnd);
                    }

                    set.add(generate);
                }
                return set;
            }, comparator);
            return support;
        }
        else if (type instanceof ListType)
        {
            // T = List<A> so can not use T here
            ListType<Object> listType = (ListType<Object>) type;
            TypeSupport<Object> elementSupport = getTypeSupport(listType.getElementsType(), sizeGen, nulls);
            @SuppressWarnings("unchecked")
            TypeSupport<T> support = (TypeSupport<T>) TypeSupport.of(listType, rnd -> {
                int size = sizeGen.generate(rnd);
                List<Object> list = new ArrayList<>(size);
                for (int i = 0; i < size; i++)
                    list.add(elementSupport.valueGen.generate(rnd));
                return list;
            }, listComparator(elementSupport.valueComparator));
            return support;
        }
        else if (type instanceof MapType)
        {
            // T = Map<A, B> so can not use T here
            MapType<Object, Object> mapType = (MapType<Object, Object>) type;
            TypeSupport<Object> keySupport = getTypeSupport(mapType.getKeysType(), sizeGen, nulls);
            Comparator<Object> keyType = keySupport.valueComparator;
            TypeSupport<Object> valueSupport = getTypeSupport(mapType.getValuesType(), sizeGen, nulls);
            Comparator<Object> valueType = valueSupport.valueComparator;
            Comparator<Map<Object, Object>> comparator = (Map<Object, Object> a, Map<Object, Object> b) -> {
                List<Object> ak = new ArrayList<>(a.keySet());
                Collections.sort(ak, keyType);
                List<Object> bk = new ArrayList<>(b.keySet());
                Collections.sort(bk, keyType);
                for (int i = 0, size = Math.min(ak.size(), bk.size()); i < size; i++)
                {
                    int rc = keyType.compare(ak.get(i), bk.get(i));
                    if (rc != 0)
                        return rc;
                    // why can't we use the same key?  DecimalType uses BigDecimal.compareTo, which doesn't account for scale differences
                    // so equality won't match, but comparator says they do!
                    rc = valueType.compare(a.get(ak.get(i)), b.get(bk.get(i)));
                    if (rc != 0)
                        return rc;
                }
                return Integer.compare(a.size(), b.size());
            };
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
                            throw new AssertionError(String.format("Unable to get unique element for type %s with the size %d", typeTree(keySupport.type), size));
                        rnd = JavaRandom.wrap(rnd);
                        key = keySupport.valueGen.generate(rnd);
                    }
                    map.put(key, valueSupport.valueGen.generate(rnd));
                }
                return map;
            }, comparator);
            return support;
        }
        else if (type instanceof TupleType) // includes UserType
        {
            // T is ByteBuffer
            TupleType tupleType = (TupleType) type;
            List<Comparator<Object>> columns = (List<Comparator<Object>>) (List<?>) tupleType.allTypes().stream().map(AbstractTypeGenerators::comparator).collect(Collectors.toList());
            Comparator<List<Object>> listCompar = listComparator((i, a, b) -> columns.get(i).compare(a, b));
            Comparator<ByteBuffer> comparator = (ByteBuffer a, ByteBuffer b) -> {
                ByteBuffer[] abb = tupleType.split(ByteBufferAccessor.instance, a);
                List<Object> av = IntStream.range(0, abb.length).mapToObj(i -> tupleType.type(i).compose(abb[i])).collect(Collectors.toList());

                ByteBuffer[] bbb = tupleType.split(ByteBufferAccessor.instance, b);
                List<Object> bv = IntStream.range(0, bbb.length).mapToObj(i -> tupleType.type(i).compose(bbb[i])).collect(Collectors.toList());
                return listCompar.compare(av, bv);
            };
            TypeSupport<ByteBuffer> support = TypeSupport.of(tupleType, new TupleGen(tupleType, sizeGen, nulls), comparator);
            return (TypeSupport<T>) support;
        }
        else if (type instanceof VectorType)
        {
            VectorType<Object> vectorType = (VectorType<Object>) type;
            TypeSupport<Object> elementSupport = getTypeSupport(vectorType.elementType, sizeGen, nulls);
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
            }, listComparator(elementSupport.valueComparator));
        }
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    public static <T> Comparator<T> comparator(AbstractType<T> type)
    {
        return getTypeSupport(type).valueComparator;
    }

    private static <T> Comparator<List<T>> listComparator(Comparator<T> elements)
    {
        return listComparator((ignore, a, b) -> elements.compare(a, b));
    }

    private interface IndexComparator<T>
    {
        int compare(int index, T a, T b);
    }
    private static <T> Comparator<List<T>> listComparator(IndexComparator<T> ordering)
    {
        return (a, b) -> {
            for (int i = 0, size = Math.min(a.size(), b.size()); i < size; i++)
            {
                int rc = ordering.compare(i, a.get(i), b.get(i));
                if (rc != 0)
                    return rc;
            }
            return Integer.compare(a.size(), b.size());
        };
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
            if (uniq != -1)
                return uniq == 1 ? 1 : uniq * vector.dimension;
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

    public static String typeTree(AbstractType<?> type)
    {
        StringBuilder sb = new StringBuilder();
        typeTree(sb, type, 0);
        return sb.toString().trim();
    }

    private static void typeTree(StringBuilder sb, AbstractType<?> type, int indent)
    {
        if (type.isUDT())
        {
            if (indent != 0)
            {
                indent += 2;
                newline(sb, indent);
            }
            UserType ut = (UserType) type;
            sb.append("udt[").append(ColumnIdentifier.maybeQuote(ut.elementName())).append("]:");
            int elementIndent = indent + 2;
            for (int i = 0; i < ut.size(); i++)
            {
                newline(sb, elementIndent);
                FieldIdentifier fieldName = ut.fieldName(i);
                AbstractType<?> fieldType = ut.fieldType(i);
                sb.append(ColumnIdentifier.maybeQuote(fieldName.toString())).append(": ");
                typeTree(sb, fieldType, elementIndent);
            }
            newline(sb, elementIndent);
        }
        else if (type.isTuple())
        {
            if (indent != 0)
            {
                indent += 2;
                newline(sb, indent);
            }
            TupleType tt = (TupleType) type;
            sb.append("tuple:");
            int elementIndent = indent + 2;
            for (int i = 0; i < tt.size(); i++)
            {
                newline(sb, elementIndent);
                AbstractType<?> fieldType = tt.type(i);
                sb.append(i).append(": ");
                typeTree(sb, fieldType, elementIndent);
            }
        }
        else if (type.isVector())
        {
            if (indent != 0)
            {
                indent += 2;
                newline(sb, indent);
            }
            VectorType<?> vt = (VectorType<?>) type;
            sb.append("vector[").append(vt.dimension).append("]: ");
            indent += 2;
            typeTree(sb, vt.elementType, indent);
        }
        else if (type.isCollection())
        {
            CollectionType<?> ct = (CollectionType<?>) type;
            switch (ct.kind)
            {
                case MAP:
                {
                    if (indent != 0)
                    {
                        indent += 2;
                        newline(sb, indent);
                    }
                    MapType<?, ?> mt = (MapType<?, ?>) type;
                    sb.append("map:");
                    indent += 2;
                    newline(sb, indent);
                    sb.append("key: ");
                    int subTypeIndent = indent + 2;
                    typeTree(sb, mt.getKeysType(), subTypeIndent);
                    newline(sb, indent);
                    sb.append("value: ");
                    typeTree(sb, mt.getValuesType(), subTypeIndent);
                }
                break;
                case LIST:
                {
                    if (indent != 0)
                    {
                        indent += 2;
                        newline(sb, indent);
                    }
                    ListType<?> lt = (ListType<?>) type;
                    sb.append("list: ");
                    indent += 2;
                    typeTree(sb, lt.getElementsType(), indent);
                }
                break;
                case SET:
                {
                    if (indent != 0)
                    {
                        indent += 2;
                        newline(sb, indent);
                    }
                    SetType<?> st = (SetType<?>) type;
                    sb.append("set: ");
                    indent += 2;
                    typeTree(sb, st.getElementsType(), indent);
                }
                break;
                default:
                    throw new UnsupportedOperationException("Unknown kind: " + ct.kind);
            }
        }
        else
        {
            sb.append(type.asCQL3Type());
        }
    }

    private static void newline(StringBuilder sb, int indent)
    {
        sb.append('\n');
        for (int i = 0; i < indent; i++)
            sb.append(' ');
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
        public final Comparator<T> valueComparator;

        private TypeSupport(AbstractType<T> type, Gen<T> valueGen, Comparator<T> valueComparator)
        {
            this.type = Objects.requireNonNull(type);
            this.valueGen = Objects.requireNonNull(valueGen);
            this.valueComparator = Objects.requireNonNull(valueComparator);
        }

        public static <T extends Comparable<T>> TypeSupport<T> of(AbstractType<T> type, Gen<T> valueGen)
        {
            return new TypeSupport<>(type, valueGen, Comparator.naturalOrder());
        }

        public static <T> TypeSupport<T> of(AbstractType<T> type, Gen<T> valueGen, Comparator<T> valueComparator)
        {
            return new TypeSupport<>(type, valueGen, valueComparator);
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
