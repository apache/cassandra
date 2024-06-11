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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.AbstractCompositeType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.DynamicCompositeType;
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
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.serializers.MarshalException;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.JavaRandom;

import static org.apache.cassandra.utils.Generators.IDENTIFIER_GEN;

@SuppressWarnings({ "unchecked", "rawtypes" })
public final class AbstractTypeGenerators
{
    private final static Logger logger = LoggerFactory.getLogger(AbstractTypeGenerators.class);

    private static final Gen<Integer> VERY_SMALL_POSITIVE_SIZE_GEN = SourceDSL.integers().between(1, 3);
    private static final Gen<Boolean> BOOLEAN_GEN = SourceDSL.booleans().all();


    @SuppressWarnings("RedundantCast")
    public static final Map<Class<? extends AbstractType<?>>, String> UNSUPPORTED = ImmutableMap.<Class<? extends AbstractType<?>>, String>builder()
                                                                                                .put(DateType.class, "Says its CQL type is timestamp, but that maps to TimestampType; is this actually dead code at this point?")
                                                                                                .put(LegacyTimeUUIDType.class, "Says its CQL timeuuid type, but that maps to TimeUUIDType; is this actually dead code at this point?")
                                                                                                .put(PartitionerDefinedOrder.class, "This is a fake type used for ordering partitions using a Partitioner")
                                                                                                // ignore intellij saying that "(Class<? extends AbstractType>)" isn't needed; jdk8 fails to compile without it!
                                                                                                .put((Class<? extends AbstractType<?>>) (Class<? extends AbstractType>) ReversedType.class, "Implementation detail for cluster ordering... its expected the caller will unwrap the clustering type to always get access to the real type")
                                                                                                .put(DynamicCompositeType.FixedValueComparator.class, "Hack type used for special ordering case, not a real/valid type")
                                                                                                .put(FrozenType.class, "Fake class only used during parsing... the parsing creates this and the real type under it, then this gets swapped for the real type")
                                                                                                .build();

    public static final Map<AbstractType<?>, TypeSupport<?>> PRIMITIVE_TYPE_DATA_GENS =
    Stream.of(TypeSupport.of(BooleanType.instance, BOOLEAN_GEN),
              TypeSupport.of(ByteType.instance, SourceDSL.integers().between(0, Byte.MAX_VALUE * 2 + 1).map(Integer::byteValue)),
              TypeSupport.of(ShortType.instance, SourceDSL.integers().between(0, Short.MAX_VALUE * 2 + 1).map(Integer::shortValue)),
              TypeSupport.of(Int32Type.instance, SourceDSL.integers().all()),
              TypeSupport.of(LongType.instance, SourceDSL.longs().all()),
              TypeSupport.of(FloatType.instance, SourceDSL.floats().any()),
              TypeSupport.of(DoubleType.instance, SourceDSL.doubles().any()),
              TypeSupport.of(BytesType.instance, Generators.bytes(1, 1024)),
              TypeSupport.of(UUIDType.instance, Generators.UUID_RANDOM_GEN),
              TypeSupport.of(TimeUUIDType.instance, Generators.UUID_TIME_GEN.map(TimeUUID::fromUuid)),
              TypeSupport.of(LexicalUUIDType.instance, Generators.UUID_RANDOM_GEN.mix(Generators.UUID_TIME_GEN)),
              TypeSupport.of(InetAddressType.instance, Generators.INET_ADDRESS_UNRESOLVED_GEN), // serialization strips the hostname, only keeps the address
              TypeSupport.of(AsciiType.instance, SourceDSL.strings().ascii().ofLengthBetween(1, 1024)),
              TypeSupport.of(UTF8Type.instance, Generators.utf8(1, 1024)),
              TypeSupport.of(TimestampType.instance, Generators.DATE_GEN),
              TypeSupport.of(SimpleDateType.instance, SourceDSL.integers().between(0, Integer.MAX_VALUE)), // can't use time gen as this is an int, and in Milliseconds... so overflows...
              TypeSupport.of(TimeType.instance, SourceDSL.longs().between(0, 24L * 60L * 60L * 1_000_000_000L - 1L)),
              // null is desired here as #decompose will call org.apache.cassandra.serializers.EmptySerializer.serialize which ignores the input and returns empty bytes
              TypeSupport.of(EmptyType.instance, rnd -> null),
              TypeSupport.of(DurationType.instance, CassandraGenerators.duration()),
              TypeSupport.of(IntegerType.instance, Generators.bigInt()),
              TypeSupport.of(DecimalType.instance, Generators.bigDecimal())
    ).collect(Collectors.toMap(t -> t.type, t -> t));
    // NOTE not supporting reversed as CQL doesn't allow nested reversed types
    // when generating part of the clustering key, it would be good to allow reversed types as the top level
    private static final Gen<AbstractType<?>> PRIMITIVE_TYPE_GEN = SourceDSL.arbitrary().pick(new ArrayList<>(PRIMITIVE_TYPE_DATA_GENS.keySet()));

    private static final Set<Class<? extends AbstractType>> NON_PRIMITIVE_TYPES = ImmutableSet.<Class<? extends AbstractType>>builder()
                                                                                              .add(SetType.class)
                                                                                              .add(ListType.class)
                                                                                              .add(MapType.class)
                                                                                              .add(TupleType.class)
                                                                                              .add(UserType.class)
                                                                                              .add(CompositeType.class)
                                                                                              .add(DynamicCompositeType.class)
                                                                                              .add(CounterColumnType.class)
                                                                                              .build();

    private AbstractTypeGenerators()
    {

    }

    public enum TypeKind
    {
        PRIMITIVE,
        SET, LIST, MAP,
        TUPLE, UDT,
        COMPOSITE, DYNAMIC_COMPOSITE,
        COUNTER
    }

    private static final Gen<TypeKind> TYPE_KIND_GEN = SourceDSL.arbitrary().pick(ImmutableList.copyOf(EnumSet.complementOf(EnumSet.of(TypeKind.COUNTER))));

    public static Set<Class<? extends AbstractType>> knownTypes()
    {
        Set<Class<? extends AbstractType>> types = PRIMITIVE_TYPE_DATA_GENS.keySet().stream().map(a -> a.getClass()).collect(Collectors.toSet());
        types.addAll(NON_PRIMITIVE_TYPES);
        types.addAll(UNSUPPORTED.keySet());
        return types;
    }

    public static Collection<AbstractType<?>> primitiveTypes()
    {
        return PRIMITIVE_TYPE_DATA_GENS.keySet();
    }

    public static Stream<Pair<AbstractType<?>, AbstractType<?>>> primitiveTypePairs()
    {
        return primitiveTypePairs(a -> true);
    }

    public static Stream<Pair<AbstractType<?>, AbstractType<?>>> primitiveTypePairs(Predicate<AbstractType<?>> filter)
    {
        return primitiveTypes().stream().filter(filter).flatMap(a -> primitiveTypes().stream().filter(filter).map(b -> Pair.create(a, b)));
    }

    public static Gen<AbstractType<?>> primitiveTypeGen()
    {
        return PRIMITIVE_TYPE_GEN;
    }

    public static Gen<AbstractType<?>> primitiveTypeGen(AbstractType<?>... without)
    {
        List<AbstractType<?>> types = new ArrayList<>(PRIMITIVE_TYPE_DATA_GENS.keySet());
        for (AbstractType<?> t : without)
            types.remove(t);
        return SourceDSL.arbitrary().pick(types);
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
                case COMPOSITE:
                    return compositeTypeGen(atBottom ? PRIMITIVE_TYPE_GEN : typeGen(maxDepth - 1, typeKindGen, sizeGen), sizeGen).generate(rnd);
                case DYNAMIC_COMPOSITE:
                    Gen<Byte> aliasGen = Generators.letterOrDigit().map(c -> (byte) c.charValue());
                    // stores alias names by class and not what is actually valid by cql... so only primitive types match!
                    return dynamicCompositeGen(PRIMITIVE_TYPE_GEN, aliasGen, sizeGen).generate(rnd);
                case COUNTER:
                    return CounterColumnType.instance;
                default:
                    throw new IllegalArgumentException("Unknown kind: " + kind);
            }
        };
    }

    @SuppressWarnings("unused")
    public static Gen<CompositeType> compositeTypeGen()
    {
        return compositeTypeGen(typeGen(2));
    }

    public static Gen<CompositeType> compositeTypeGen(Gen<AbstractType<?>> typeGen)
    {
        return compositeTypeGen(typeGen, VERY_SMALL_POSITIVE_SIZE_GEN);
    }

    public static Gen<CompositeType> compositeTypeGen(Gen<AbstractType<?>> typeGen, Gen<Integer> sizeGen)
    {
        return rnd -> {
            int size = sizeGen.generate(rnd);
            List<AbstractType<?>> types = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
            {
                AbstractType<?> type = typeGen.generate(rnd);
                while (type == BytesType.instance || type == UUIDType.instance)
                    type = typeGen.generate(rnd);
                types.add(type);
            }
            return CompositeType.getInstance(types);
        };
    }

    public static Gen<DynamicCompositeType> dynamicCompositeGen(Gen<AbstractType<?>> typeGen, Gen<Byte> aliasGen, Gen<Integer> sizeGen)
    {
        return rnd -> {
            int size = sizeGen.generate(rnd);
            Map<Byte, AbstractType<?>> aliases = Maps.newHashMapWithExpectedSize(size);
            for (int i = 0; i < size; i++)
            {
                byte alias = aliasGen.generate(rnd);
                while (aliases.containsKey(alias))
                    alias = aliasGen.generate(rnd);
                aliases.put(alias, unfreeze(typeGen.generate(rnd)));
            }
            return DynamicCompositeType.getInstance(aliases);
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

    public static Gen<MapType<?, ?>> mapTypeGen(Gen<? extends AbstractType<?>> typeGen)
    {
        return mapTypeGen(typeGen, typeGen);
    }

    public static Gen<MapType<?, ?>> mapTypeGen(Gen<? extends AbstractType<?>> keyGen, Gen<? extends AbstractType<?>> valueGen)
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

    public enum ValueDomain
    {NULL, EMPTY_BYTES, NORMAL}

    public static <T> TypeSupport<T> getTypeSupport(AbstractType<T> type, Gen<Integer> sizeGen)
    {
        return getTypeSupport(type, sizeGen, null);
    }

    /**
     * For a type, create generators for data that matches that type
     */
    public static <T> TypeSupport<T> getTypeSupport(AbstractType<T> type, Gen<Integer> sizeGen, @Nullable Gen<ValueDomain> valueDomainGen)
    {
        Objects.requireNonNull(sizeGen, "sizeGen");
        // this doesn't affect the data, only sort order, so drop it
        type = unwrap(type);
        // cast is safe since type is a constant and was type cast while inserting into the map
        @SuppressWarnings("unchecked")
        TypeSupport<T> gen = (TypeSupport<T>) PRIMITIVE_TYPE_DATA_GENS.get(type);
        TypeSupport<T> support;
        if (gen != null)
        {
            support = gen;
        }
        // might be... complex...
        else if (type instanceof SetType)
        {
            // T = Set<A> so can not use T here
            SetType<Object> setType = (SetType<Object>) type;
            TypeSupport<Object> elementSupport = getTypeSupport(setType.getElementsType(), sizeGen, valueDomainGen);
            support = (TypeSupport<T>) TypeSupport.of(setType, rnd -> {
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
            });
        }
        else if (type instanceof ListType)
        {
            // T = List<A> so can not use T here
            ListType<Object> listType = (ListType<Object>) type;
            TypeSupport<Object> elementSupport = getTypeSupport(listType.getElementsType(), sizeGen, valueDomainGen);
            support = (TypeSupport<T>) TypeSupport.of(listType, rnd -> {
                int size = sizeGen.generate(rnd);
                List<Object> list = new ArrayList<>(size);
                for (int i = 0; i < size; i++)
                    list.add(elementSupport.valueGen.generate(rnd));
                return list;
            });
        }
        else if (type instanceof MapType)
        {
            // T = Map<A, B> so can not use T here
            MapType<Object, Object> mapType = (MapType<Object, Object>) type;
            // do not use valueDomainGen as map doesn't allow null/empty
            TypeSupport<Object> keySupport = getTypeSupport(mapType.getKeysType(), sizeGen, valueDomainGen);
            TypeSupport<Object> valueSupport = getTypeSupport(mapType.getValuesType(), sizeGen, valueDomainGen);
            support = (TypeSupport<T>) TypeSupport.of(mapType, rnd -> {
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
            });
        }
        else if (type instanceof TupleType) // includes UserType
        {
            // T is ByteBuffer
            TupleType tupleType = (TupleType) type;
            support = (TypeSupport<T>) TypeSupport.of(tupleType, new TupleGen(tupleType, sizeGen, valueDomainGen));
        }
        else if (type instanceof CompositeType)
        {
            CompositeType ct = (CompositeType) type;
            List<TypeSupport<Object>> elementSupport = (List<TypeSupport<Object>>) (List<?>) ct.types.stream().map(AbstractTypeGenerators::getTypeSupport).collect(Collectors.toList());
            //noinspection Convert2Diamond
            Serde<ByteBuffer, List<Object>> serde = new Serde<ByteBuffer, List<Object>>()
            {
                @Override
                public ByteBuffer from(List<Object> objects)
                {
                    return ct.decompose(objects.toArray());
                }

                @Override
                public List<Object> to(ByteBuffer buffer)
                {
                    ByteBuffer[] bbs = ct.split(buffer);
                    List<Object> values = new ArrayList<>(bbs.length);
                    for (int i = 0; i < bbs.length; i++)
                        values.add(bbs[i] == null ? null : elementSupport.get(i).type.compose(bbs[i]));
                    return values;
                }
            };
            support = (TypeSupport<T>) TypeSupport.of(ct, serde, rnd -> {
                List<Object> values = new ArrayList<>(ct.types.size());
                for (int i = 0, size = ct.types.size(); i < size; i++)
                    values.add(elementSupport.get(i).valueGen.generate(rnd));
                return values;
            });
        }
        else if (type instanceof DynamicCompositeType)
        {
            // data generation limits to what is valid for the type, and sadly the type
            DynamicCompositeType dct = (DynamicCompositeType) type;
            Map<Byte, TypeSupport<?>> supports = new TreeMap<>();
            for (Map.Entry<Byte, AbstractType<?>> e : dct.aliases.entrySet())
                supports.put(e.getKey(), getTypeSupport(e.getValue()));
            List<Byte> orderedAliases = new ArrayList<>(supports.keySet());
            //noinspection Convert2Diamond
            Serde<ByteBuffer, Map<Byte, Object>> serde = new Serde<ByteBuffer, Map<Byte, Object>>()
            {
                @Override
                public ByteBuffer from(Map<Byte, Object> byteObjectMap)
                {
                    return new DynamicCompositeTypeValueBuilder(dct).build(byteObjectMap);
                }

                @Override
                public Map<Byte, Object> to(ByteBuffer buffer)
                {
                    ByteBuffer[] parts = dct.split(buffer);
                    Map<Byte, Object> values = Maps.newHashMapWithExpectedSize(parts.length);
                    for (int i = 0; i < parts.length; i++)
                    {
                        Byte alias = orderedAliases.get(i);
                        AbstractType<?> type = dct.aliases.get(alias);
                        ByteBuffer bytes = parts[i];
                        type.validate(bytes);
                        values.put(alias, type.compose(bytes));
                    }
                    return values;
                }
            };
            support = (TypeSupport<T>) TypeSupport.of(dct, serde, rnd -> {
                Map<Byte, Object> byteObjectMap = new TreeMap<>();
                for (Byte alias : orderedAliases)
                    byteObjectMap.put(alias, supports.get(alias).valueGen.generate(rnd));
                return byteObjectMap;
            });
        }
        else if (type instanceof CounterColumnType)
        {
            support = (TypeSupport<T>) TypeSupport.of(CounterColumnType.instance, SourceDSL.longs().all());
        }
        else
        {
            throw new UnsupportedOperationException("No TypeSupport for: " + type);
        }
        return support.withValueDomain(valueDomainGen);
    }

    private static int uniqueElementsForDomain(AbstractType<?> type)
    {
        type = unwrap(type);
        if (type instanceof BooleanType)
            return 2;
        if (type instanceof EmptyType)
            return 1;
        if (type instanceof SetType)
            return uniqueElementsForDomain(((SetType<?>) type).getElementsType());
        if (type instanceof MapType)
            return uniqueElementsForDomain(((MapType<?, ?>) type).getKeysType());
        if (type instanceof TupleType || type instanceof CompositeType || type instanceof DynamicCompositeType)
        {
            int product = 1;
            for (AbstractType<?> f : type.subTypes())
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
            if (!type.isMultiCell()) sb.append("frozen ");
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
        else if (type.isCollection())
        {
            CollectionType<?> ct = (CollectionType<?>) type;
            if (indent != 0)
            {
                indent += 2;
                newline(sb, indent);
            }
            if (!type.isMultiCell()) sb.append("frozen ");
            switch (ct.kind)
            {
                case MAP:
                {
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
                    ListType<?> lt = (ListType<?>) type;
                    sb.append("list: ");
                    indent += 2;
                    typeTree(sb, lt.getElementsType(), indent);
                }
                break;
                case SET:
                {
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
        else if (type instanceof CompositeType)
        {
            CompositeType ct = (CompositeType) type;
            if (indent != 0)
            {
                indent += 2;
                newline(sb, indent);
            }
            sb.append("CompositeType:");
            indent += 2;
            int idx = 0;
            for (AbstractType<?> subtype : ct.subTypes())
            {
                newline(sb, indent);
                sb.append(idx++).append(": ");
                typeTree(sb, subtype, indent);
            }
        }
        else
        {
            sb.append(type.asCQL3Type().toString().replaceAll("org.apache.cassandra.db.marshal.", ""));
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

        @SuppressWarnings("unchecked")
        private TupleGen(TupleType tupleType, Gen<Integer> sizeGen, @Nullable Gen<ValueDomain> valueDomainGen)
        {
            this.elementsSupport = tupleType.allTypes().stream().map(t -> getTypeSupport((AbstractType<Object>) t, sizeGen, valueDomainGen)).collect(Collectors.toList());
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

    public interface Serde<A, B>
    {
        A from(B b);
        B to(A a);
    }

    /**
     * Pair of {@link AbstractType} and a Generator of values that are handled by that type.
     */
    public static final class TypeSupport<T>
    {
        public final AbstractType<T> type;
        public final Gen<T> valueGen;
        private final Gen<ByteBuffer> bytesGen;

        private TypeSupport(AbstractType<T> type, Gen<T> valueGen)
        {
            this.type = Objects.requireNonNull(type);
            this.valueGen = Objects.requireNonNull(valueGen);
            this.bytesGen = rnd -> type.decompose(valueGen.generate(rnd));
        }

        private TypeSupport(AbstractType<T> type, Gen<T> valueGen, Gen<ByteBuffer> bytesGen)
        {
            this.type = type;
            this.valueGen = valueGen;
            this.bytesGen = bytesGen;
        }

        public static <T> TypeSupport<T> of(AbstractType<T> type, Gen<T> valueGen)
        {
            return new TypeSupport<>(type, valueGen);
        }

        public static <A, B> TypeSupport<A> of(AbstractType<A> type, Serde<A, B> serde, Gen<B> valueGen)
        {
            return of(type, valueGen.map(serde::from));
        }

        /**
         * Generator which composes the values gen with {@link AbstractType#decompose(Object)}
         */
        public Gen<ByteBuffer> bytesGen()
        {
            return bytesGen;
        }

        public TypeSupport<T> withValueDomain(@Nullable Gen<ValueDomain> valueDomainGen)
        {
            if (valueDomainGen == null || !allowsEmpty(type))
                return this;
            Gen<ByteBuffer> gen = rnd -> {
                ValueDomain domain = valueDomainGen.generate(rnd);
                ByteBuffer value;
                switch (domain)
                {
                    case NULL:
                        value = null;
                        break;
                    case EMPTY_BYTES:
                        value = ByteBufferUtil.EMPTY_BYTE_BUFFER;
                        break;
                    case NORMAL:
                        value = bytesGen.generate(rnd);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown domain: " + domain);
                }
                return value;
            };
            return new TypeSupport<>(type, valueGen, gen);
        }

        public String toString()
        {
            return "TypeSupport{" +
                   "type=" + type +
                   '}';
        }
    }

    public static boolean allowsEmpty(AbstractType type)
    {
        try
        {
            type.validate(ByteBufferUtil.EMPTY_BYTE_BUFFER);
            return true;
        }
        catch (MarshalException e)
        {
            return false;
        }
    }

    public static AbstractType unwrap(AbstractType type)
    {
        if (type instanceof ReversedType)
            return ((ReversedType) type).baseType;
        return type;
    }

    public static AbstractType unfreeze(AbstractType t)
    {
        if (t.isMultiCell())
            return t;

        AbstractType<?> unfrozen = TypeParser.parse(t.toString(true));
        if (unfrozen.isMultiCell())
            return unfrozen;

        return t;
    }

    private static class DynamicCompositeTypeValueBuilder
    {
        private final Map<Byte, AbstractType<?>> aliases;

        public DynamicCompositeTypeValueBuilder(DynamicCompositeType dct)
        {
            this.aliases = dct.aliases;
        }

        public ByteBuffer build(Map<Byte, Object> valuesMap)
        {
            Sets.SetView<Byte> unknownAliases = Sets.difference(valuesMap.keySet(), aliases.keySet());
            if (!unknownAliases.isEmpty())
                throw new IllegalArgumentException(String.format("Aliases %s used; only valid values are %s", unknownAliases, aliases.keySet()));
            List<AbstractType<?>> types = new ArrayList<>(valuesMap.size());
            List<ByteBuffer> values = new ArrayList<>(valuesMap.size());
            List<Byte> aliasesList = new ArrayList<>(valuesMap.size());
            for (Map.Entry<Byte, Object> e : valuesMap.entrySet())
            {
                @SuppressWarnings("rawtype")
                AbstractType type = aliases.get(e.getKey());
                types.add(type);
                values.add(type.decompose(e.getValue()));
                aliasesList.add(e.getKey());
            }
            return build(ByteBufferAccessor.instance, types, values, aliasesList, (byte) 0);
        }

        @VisibleForTesting
        public static <V> V build(ValueAccessor<V> accessor,
                                  List<AbstractType<?>> types,
                                  List<V> values,
                                  List<Byte> aliasesList,
                                  byte lastEoc)
        {
            assert types.size() == values.size();

            int numComponents = types.size();
            // Compute the total number of bytes that we'll need to store the types and their payloads.
            int totalLength = 0;
            for (int i = 0; i < numComponents; ++i)
            {
                AbstractType<?> type = types.get(i);
                Byte alias = aliasesList.get(i);
                int typeNameLength = alias == null ? type.toString().getBytes(StandardCharsets.UTF_8).length : 0;
                // The type data will be stored by means of the type's fully qualified name, not by aliasing, so:
                //   1. The type data header should be the fully qualified name length in bytes.
                //   2. The length should be small enough so that it fits in 15 bits (2 bytes with the first bit zero).
                assert typeNameLength <= 0x7FFF;
                int valueLength = accessor.size(values.get(i));
                // The value length should also expect its first bit to be 0, as the length should be stored as a signed
                // 2-byte value (short).
                assert valueLength <= 0x7FFF;
                totalLength += 2 + typeNameLength + 2 + valueLength + 1;
            }

            V result = accessor.allocate(totalLength);
            int offset = 0;
            for (int i = 0; i < numComponents; ++i)
            {
                AbstractType<?> type = types.get(i);
                Byte alias = aliasesList.get(i);
                if (alias == null)
                {
                    // Write the type data (2-byte length header + the fully qualified type name in UTF-8).
                    byte[] typeNameBytes = type.toString().getBytes(StandardCharsets.UTF_8);
                    accessor.putShort(result,
                                      offset,
                                      (short) typeNameBytes.length); // this should work fine also if length >= 32768
                    offset += 2;
                    accessor.copyByteArrayTo(typeNameBytes, 0, result, offset, typeNameBytes.length);
                    offset += typeNameBytes.length;
                }
                else
                {
                    accessor.putShort(result, offset, (short) (alias | 0x8000));
                    offset += 2;
                }

                // Write the type payload data (2-byte length header + the payload).
                V value = values.get(i);
                int bytesToCopy = accessor.size(value);
                if ((short) bytesToCopy != bytesToCopy)
                    throw new IllegalArgumentException(String.format("Value of type %s is of length %d; does not fit in a short", type.asCQL3Type(), bytesToCopy));
                accessor.putShort(result, offset, (short) bytesToCopy);
                offset += 2;
                accessor.copyTo(value, 0, result, accessor, offset, bytesToCopy);
                offset += bytesToCopy;

                // Write the end-of-component byte.
                accessor.putByte(result, offset, i != numComponents - 1 ? (byte) 0 : lastEoc);
                offset += 1;
            }
            return result;
        }
    }

    public static void forEachTypesPair(boolean withVariants, BiConsumer<AbstractType, AbstractType> typesPairConsumer)
    {
        forEachPrimitiveTypePair(typesPairConsumer);
        forEachMapTypesPair(withVariants, typesPairConsumer);
        forEachSetTypesPair(withVariants, typesPairConsumer);
        forEachListTypesPair(withVariants, typesPairConsumer);
        forEachUserTypesPair(withVariants, typesPairConsumer);
        forEachCompositeTypesPair(withVariants, typesPairConsumer);
    }

    public static void forEachPrimitiveTypePair(BiConsumer<AbstractType, AbstractType> typePairConsumer)
    {
        logger.info("Iterating over primitive types pairs...");
        primitiveTypePairs().forEach(p -> typePairConsumer.accept(p.left, p.right));
    }

    private static <T extends AbstractType<?>> Set<T> frozenAndUnfrozen(T... types)
    {
        return Stream.of(types).flatMap(t -> Stream.of((T) t.freeze(), (T) unfreeze(t))).collect(Collectors3.toImmutableSet());
    }

    private static UserType withAddedField(UserType type, String fieldName, AbstractType<?> fieldType)
    {
        ArrayList<FieldIdentifier> fieldNames = new ArrayList<>(type.fieldNames());
        fieldNames.add(FieldIdentifier.forUnquoted(fieldName));
        List<AbstractType<?>> fieldTypes = new ArrayList<>(type.fieldTypes());
        fieldTypes.add(fieldType);
        return new UserType(type.keyspace, type.name, fieldNames, fieldTypes, true);
    }

    private static Set<TupleType> tupleTypeVariants(UserType type)
    {
        UserType extType = withAddedField(type, "extra", EmptyType.instance);
        return frozenAndUnfrozen(type,
                                 new TupleType(type.subTypes(), false),
                                 extType,
                                 new TupleType(extType.subTypes(), false));
    }

    private static void forEachUserTypeVariantPair(UserType leftType, UserType rightType, BiConsumer<? super TupleType, ? super TupleType> typePairConsumer)
    {
        forEachTypesPair(tupleTypeVariants(leftType), tupleTypeVariants(rightType), typePairConsumer);
    }

    private static DynamicCompositeType withAddedField(DynamicCompositeType type, AbstractType<?> fieldType)
    {
        Map<Byte, AbstractType<?>> aliases = new HashMap<>(type.aliases);
        aliases.put((byte) ('a' + type.aliases.size()), fieldType);
        return DynamicCompositeType.getInstance(aliases);
    }

    private static Set<AbstractCompositeType> compositeTypeVariants(DynamicCompositeType type)
    {
        DynamicCompositeType extType = withAddedField(type, EmptyType.instance);
        return frozenAndUnfrozen(type,
                                 CompositeType.getInstance(new TreeMap<>(type.aliases).values()),
                                 extType,
                                 CompositeType.getInstance(new TreeMap<>(extType.aliases).values()));
    }

    private static void forEachCompositeTypeVariantsPair(DynamicCompositeType leftType, DynamicCompositeType rightType, BiConsumer<? super AbstractCompositeType, ? super AbstractCompositeType> typePairConsumer)
    {
        forEachTypesPair(compositeTypeVariants(leftType), compositeTypeVariants(rightType), typePairConsumer);
    }

    public static void forEachMapTypesPair(boolean withVariants, BiConsumer<? super MapType, ? super MapType> typePairConsumer)
    {
        logger.info("Iterating over map types pairs...");
        primitiveTypePairs(t -> t.getClass() != EmptyType.class).forEach(keyPair -> { // key cannot be empty
            primitiveTypePairs().forEach(valuePair -> {
                MapType<?, ?> leftMap = MapType.getInstance(keyPair.left, valuePair.left, true);
                MapType<?, ?> rightMap = MapType.getInstance(keyPair.right, valuePair.right, true);
                if (withVariants)
                    forEachCollectionTypeVariantsPair(leftMap, rightMap, typePairConsumer);
                else
                    typePairConsumer.accept(leftMap, rightMap);
            });
        });
    }

    public static void forEachSetTypesPair(boolean withVariants, BiConsumer<? super SetType, ? super SetType> typePairConsumer)
    {
        logger.info("Iterating over set types pairs...");
        primitiveTypePairs().forEach(keyPair -> {
            SetType<?> leftSet = SetType.getInstance(keyPair.left, true);
            SetType<?> rightSet = SetType.getInstance(keyPair.right, true);
            if (withVariants)
                forEachCollectionTypeVariantsPair(leftSet, rightSet, typePairConsumer);
            else
                typePairConsumer.accept(leftSet, rightSet);
        });
    }

    public static void forEachListTypesPair(boolean withVariants, BiConsumer<? super ListType, ? super ListType> typePairConsumer)
    {
        logger.info("Iterating over list types pairs...");
        primitiveTypePairs().forEach(valuePair -> {
            ListType<?> leftList = ListType.getInstance(valuePair.left, true);
            ListType<?> rightList = ListType.getInstance(valuePair.right, true);
            if (withVariants)
                forEachCollectionTypeVariantsPair(leftList, rightList, typePairConsumer);
            else
                typePairConsumer.accept(leftList, rightList);
        });
    }

    public static void forEachUserTypesPair(boolean withVariants, BiConsumer<? super TupleType, ? super TupleType> typePairConsumer)
    {
        logger.info("Iterating over user types pairs...");

        String ks = "ks";
        ByteBuffer t = ByteBufferUtil.bytes("t");
        List<FieldIdentifier> names = Stream.of("a", "b").map(FieldIdentifier::forUnquoted).collect(Collectors3.toImmutableList());

        primitiveTypePairs().forEach(elem1Pair -> {
            primitiveTypePairs().forEach(elem2Pair -> {
                UserType leftType = new UserType(ks, t, names, ImmutableList.of(elem1Pair.left, elem2Pair.left), true);
                UserType rightType = new UserType(ks, t, names, ImmutableList.of(elem1Pair.right, elem2Pair.right), true);
                if (withVariants)
                    forEachUserTypeVariantPair(leftType, rightType, typePairConsumer);
                else
                    typePairConsumer.accept(leftType, rightType);
            });
        });
    }

    public static void forEachCompositeTypesPair(boolean withVariants, BiConsumer<? super AbstractCompositeType, ? super AbstractCompositeType> typePairConsumer)
    {
        logger.info("Iterating over composite types pairs...");
        primitiveTypePairs().forEach(elem1Pair -> {
            primitiveTypePairs().forEach(elem2Pair -> {
                DynamicCompositeType leftType = DynamicCompositeType.getInstance(ImmutableMap.of((byte) 'a', elem1Pair.left, (byte) 'b', elem2Pair.left));
                DynamicCompositeType rightType = DynamicCompositeType.getInstance(ImmutableMap.of((byte) 'a', elem1Pair.right, (byte) 'b', elem2Pair.right));
                if (withVariants)
                    forEachCompositeTypeVariantsPair(leftType, rightType, typePairConsumer);
                else
                    typePairConsumer.accept(leftType, rightType);
            });
        });
    }

    private static <T extends AbstractType> void forEachTypesPair(Collection<? extends T> leftVariants, Collection<? extends T> rightVariants, BiConsumer<? super T, ? super T> typePairConsumer)
    {
        for (T left : leftVariants)
            for (T right : rightVariants)
                typePairConsumer.accept(left, right);
    }


    private static <T extends AbstractType> void forEachCollectionTypeVariantsPair(T l, T r, BiConsumer<? super T, ? super T> typePairConsumer)
    {
        forEachTypesPair(frozenAndUnfrozen(l), frozenAndUnfrozen(r), typePairConsumer);
    }

}
