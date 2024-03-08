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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.CQLTypeParser;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators.Releaser;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeGenBuilder;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.Generators;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.description.Description;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ConfigurationBuilder;

import static org.apache.cassandra.db.marshal.AbstractTypeTest.TypesCompatibility.inverse;
import static org.apache.cassandra.utils.AbstractTypeGenerators.PRIMITIVE_TYPE_DATA_GENS;
import static org.apache.cassandra.utils.AbstractTypeGenerators.TypeKind.COMPOSITE;
import static org.apache.cassandra.utils.AbstractTypeGenerators.TypeKind.COUNTER;
import static org.apache.cassandra.utils.AbstractTypeGenerators.TypeKind.DYNAMIC_COMPOSITE;
import static org.apache.cassandra.utils.AbstractTypeGenerators.TypeKind.PRIMITIVE;
import static org.apache.cassandra.utils.AbstractTypeGenerators.TypeKind.UDT;
import static org.apache.cassandra.utils.AbstractTypeGenerators.TypeSupport.of;
import static org.apache.cassandra.utils.AbstractTypeGenerators.extractUDTs;
import static org.apache.cassandra.utils.AbstractTypeGenerators.getTypeSupport;
import static org.apache.cassandra.utils.AbstractTypeGenerators.overridePrimitiveTypeSupport;
import static org.apache.cassandra.utils.AbstractTypeGenerators.primitiveTypePairs;
import static org.apache.cassandra.utils.AbstractTypeGenerators.primitiveTypes;
import static org.apache.cassandra.utils.AbstractTypeGenerators.stringComparator;
import static org.apache.cassandra.utils.AbstractTypeGenerators.typeTree;
import static org.apache.cassandra.utils.ByteBufferUtil.bytesToHex;
import static org.apache.commons.math3.util.MathUtils.copySign;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;
import static org.quicktheories.generators.SourceDSL.doubles;
import static org.quicktheories.generators.SourceDSL.floats;
import static org.quicktheories.generators.SourceDSL.integers;

@SuppressWarnings("unchecked")
public class AbstractTypeTest
{
    private final static Logger logger = LoggerFactory.getLogger(AbstractTypeTest.class);
    private static final Pattern TYPE_PREFIX_PATTERN = Pattern.compile("org\\.apache\\.cassandra\\.db\\.marshal\\.");

    static
    {
        // make sure blob is always the same
        CassandraRelevantProperties.TEST_BLOB_SHARED_SEED.setInt(42);
    }

    private static final Reflections reflections = new Reflections(new ConfigurationBuilder()
                                                                   .forPackage("org.apache.cassandra")
                                                                   .setScanners(Scanners.SubTypes)
                                                                   .setExpandSuperTypes(true)
                                                                   .setParallel(true));

    private final static TypesCompatibility typesCompatibility = Cassandra50BetaTypesCompatibility.instance;


    // TODO
    // withUpdatedUserType/expandUserTypes/referencesDuration - types that recursive check types

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void maskedValue()
    {
        qt().forAll(genBuilder().withoutTypeKinds(COMPOSITE, DYNAMIC_COMPOSITE).build())
            .checkAssert(type -> {
                ByteBuffer maskedValue = type.getMaskedValue();
                type.validate(maskedValue);

                Object composed = type.compose(maskedValue);
                ByteBuffer decomposed = ((AbstractType) type).decompose(composed);
                assertThat(decomposed).isEqualTo(maskedValue);
            });
    }

    @Test
    public void empty()
    {
        qt().forAll(genBuilder().build()).checkAssert(type -> {
            if (type.allowsEmpty())
            {
                type.validate(ByteBufferUtil.EMPTY_BYTE_BUFFER);
                // empty container or null is valid; only checks that this method doesn't fail
                type.compose(ByteBufferUtil.EMPTY_BYTE_BUFFER);
            }
            else
            {
                assertThatThrownBy(() -> type.validate(ByteBufferUtil.EMPTY_BYTE_BUFFER)).isInstanceOf(MarshalException.class);
                assertThatThrownBy(() -> type.getSerializer().validate(ByteBufferUtil.EMPTY_BYTE_BUFFER)).isInstanceOf(MarshalException.class);
                // ByteSerializer returns null
//                assertThatThrownBy(() -> type.compose(ByteBufferUtil.EMPTY_BYTE_BUFFER)).isInstanceOf(MarshalException.class);
            }
        });
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void allTypesCovered()
    {
        // this test just makes sure that all types are covered and no new type is left out
        Set<Class<? extends AbstractType>> subTypes = reflections.getSubTypesOf(AbstractType.class);
        Set<Class<? extends AbstractType>> coverage = AbstractTypeGenerators.knownTypes();
        StringBuilder sb = new StringBuilder();
        for (Class<? extends AbstractType> klass : Sets.difference(subTypes, coverage))
        {
            if (Modifier.isAbstract(klass.getModifiers()))
                continue;
            if (isTestType(klass))
                continue;
            String name = klass.getCanonicalName();
            if (name == null)
                name = klass.getName();
            sb.append(name).append('\n');
        }
        if (sb.length() > 0)
            throw new AssertionError("Uncovered types:\n" + sb);
    }

    @SuppressWarnings("rawtypes")
    private boolean isTestType(Class<? extends AbstractType> klass)
    {
        String name = klass.getCanonicalName();
        if (name == null)
            name = klass.getName();
        if (name == null)
            name = klass.toString();
        if (name.contains("Test"))
            return true;
        ProtectionDomain domain = klass.getProtectionDomain();
        if (domain == null) return false;
        CodeSource src = domain.getCodeSource();
        if (src == null) return false;
        return "test".equals(new File(src.getLocation().getPath()).name());
    }

    @Test
    public void unsafeSharedSerializer()
    {
        // For all types, make sure the serializer returned is unique to that type,
        // this is required as some places, such as SetSerializer, cache at this level!
        Map<TypeSerializer<?>, AbstractType<?>> lookup = new HashMap<>();
        qt().forAll(genBuilder().withMaxDepth(0).build()).checkAssert(t -> {
            AbstractType<?> old = lookup.put(t.getSerializer(), t);
            // for frozen types, ignore the fact that the mapping breaks...  The reason this test exists is that
            // org.apache.cassandra.db.marshal.AbstractType.comparatorSet needs to match the serializer, but when serialziers
            // break this mapping they may cause the wrong comparator (happened in cases like uuid and lexecal uuid; which have different orderings!).
            // Frozen types (as of this writing) do not change the sort ordering, so this simplification is fine...
            if (old != null && !old.unfreeze().equals(t.unfreeze()))
                throw new AssertionError(String.format("Different types detected that shared the same serializer: %s != %s", old.asCQL3Type(), t.asCQL3Type()));
        });
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void eqHashSafe()
    {
        StringBuilder sb = new StringBuilder();
        outter: for (Class<? extends AbstractType> type : reflections.getSubTypesOf(AbstractType.class))
        {
            if (Modifier.isAbstract(type.getModifiers()) || isTestType(type) || AbstractTypeGenerators.UNSUPPORTED.containsKey(type))
                continue;
            boolean hasEq = false;
            boolean hasHashCode = false;
            for (Class<? extends AbstractType> t = type; !t.equals(AbstractType.class); t = (Class<? extends AbstractType>) t.getSuperclass())
            {
                try
                {
                    t.getDeclaredMethod("getInstance");
                    continue outter;
                }
                catch (NoSuchMethodException e)
                {
                    // ignore
                }
                try
                {
                    t.getDeclaredField("instance");
                    continue outter;
                }
                catch (NoSuchFieldException e)
                {
                    // ignore
                }
                try
                {
                    t.getDeclaredMethod("equals", Object.class);
                    hasEq = true;
                }
                catch (NoSuchMethodException e)
                {
                    // ignore
                }
                try
                {
                    t.getDeclaredMethod("hashCode");
                    hasHashCode = true;
                }
                catch (NoSuchMethodException e)
                {
                    // ignore
                }
                if (hasEq && hasHashCode)
                    continue outter;
            }
            sb.append("AbstractType must be safe for map keys, so must either be a singleton or define ");
            if (!hasEq)
                sb.append("equals");
            if (!hasHashCode)
            {
                if (!hasEq)
                    sb.append('/');
                sb.append("hashCode");
            }
            sb.append("; ").append(type).append('\n');
        }
        if (sb.length() != 0)
        {
            sb.setLength(sb.length() - 1);
            throw new AssertionError(sb.toString());
        }
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void comparableBytes()
    {
        // decimal "normalizes" the data to compare, so primary columns "may" mutate the data, causing missmatches
        // see CASSANDRA-18530
        TypeGenBuilder baseline = genBuilder().withoutPrimitive(DecimalType.instance)
                                              .withoutTypeKinds(COUNTER);
        // composite requires all elements fit into Short.MAX_VALUE bytes
        // so try to limit the possible expansion of types
        Gen<AbstractType<?>> gen = baseline.withCompositeElementGen(new TypeGenBuilder(baseline).withDefaultSizeGen(1).withMaxDepth(1).build())
                                   .build();
        qt().withShrinkCycles(0).forAll(examples(1, gen)).checkAssert(example -> {
            AbstractType type = example.type;
            for (Object value : example.samples)
            {
                ByteBuffer bb = type.decompose(value);
                for (ByteComparable.Version bcv : ByteComparable.Version.values())
                {
                    // LEGACY, // Encoding used in legacy sstable format; forward (value to byte-comparable) translation only
                    // Legacy is a one-way conversion, so for this test ignore
                    if (bcv == ByteComparable.Version.LEGACY)
                        continue;
                    // Test normal type APIs
                    ByteSource.Peekable comparable = ByteSource.peekable(type.asComparableBytes(bb, bcv));
                    if (comparable == null)
                        throw new NullPointerException();
                    ByteBuffer read;
                    try
                    {
                        read = type.fromComparableBytes(comparable, bcv);
                    }
                    catch (Exception | Error e)
                    {
                        throw new AssertionError(String.format("Unable to parse comparable bytes for type %s and version %s; value %s", type.asCQL3Type(), bcv, type.toCQLString(bb)), e);
                    }
                    assertBytesEquals(read, bb, "fromComparableBytes(asComparableBytes(bb)) != bb; version %s", bcv);

                    // test byte[] api
                    byte[] bytes = ByteSourceInverse.readBytes(type.asComparableBytes(bb, bcv));
                    assertBytesEquals(type.fromComparableBytes(ByteSource.peekable(ByteSource.fixedLength(bytes)), bcv), bb, "fromOrderedBytes(toOrderedBytes(bb)) != bb");
                }
            }
        });
    }

    @Test
    public void knowThySelf()
    {
        qt().withShrinkCycles(0).forAll(AbstractTypeGenerators.typeGen()).checkAssert(type -> {
            assertThat(type.testAssignment(null, new ColumnSpecification(null, null, null, type))).isEqualTo(AssignmentTestable.TestResult.EXACT_MATCH);
            assertThat(type.testAssignment(type)).isEqualTo(AssignmentTestable.TestResult.EXACT_MATCH);
        });
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void json()
    {
        // Double type is special as NaN and Infinite are treated differently than other code paths as they are convered to null!
        // This is fine in most cases, but when found in a collection, this is not allowed and can cause flakeyness
        try (Releaser i1 = overridePrimitiveTypeSupport(DoubleType.instance,
                                                        of(DoubleType.instance, doubles().between(Double.MIN_VALUE, Double.MAX_VALUE)));
             Releaser i2 = overridePrimitiveTypeSupport(FloatType.instance,
                                                        of(FloatType.instance, floats().between(Float.MIN_VALUE, Float.MAX_VALUE))))
        {
            Gen<AbstractType<?>> typeGen = genBuilder()
                                           .withDefaultSetKey(AbstractTypeGenerators.withoutUnsafeEquality().withoutTypeKinds(COMPOSITE, DYNAMIC_COMPOSITE, COUNTER))
                                           // toCQLLiteral is lossy, which causes deserialization to produce different bytes
                                           .withoutPrimitive(DecimalType.instance)
                                           // does not support toJSONString
                                           .withoutTypeKinds(COMPOSITE, DYNAMIC_COMPOSITE, COUNTER)
                                           .build();
            qt().withShrinkCycles(0).forAll(examples(1, typeGen)).checkAssert(es -> {
                AbstractType type = es.type;
                for (Object example : es.samples)
                {
                    ByteBuffer bb = type.decompose(example);
                    String json = type.toJSONString(bb, ProtocolVersion.CURRENT);
                    ColumnMetadata column = fake(type);
                    String cqlJson = "{\"" + column.name + "\": " + json + "}";
                    try
                    {
                        Json.Prepared prepared = new Json.Literal(cqlJson).prepareAndCollectMarkers(null, Collections.singletonList(column), VariableSpecifications.empty());
                        Term.Raw literal = prepared.getRawTermForColumn(column, false);
                        assertThat(literal).isNotEqualTo(Constants.NULL_LITERAL);
                        Term term = literal.prepare(column.ksName, column);
                        ByteBuffer read = term.bindAndGet(QueryOptions.DEFAULT);
                        assertBytesEquals(read, bb, "fromJSONString(toJSONString(bb)) != bb");
                    }
                    catch (Exception e)
                    {
                        throw new AssertionError("Unable to parse JSON for " + json + "; type " + type.asCQL3Type(), e);
                    }
                }
            });
        }
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void nested()
    {
        Map<Class<? extends AbstractType>, Function<? super AbstractType<?>, Integer>> complexTypes = ImmutableMap.of(MapType.class, ignore -> 2,
                                                                                                                      TupleType.class, t -> ((TupleType) t).size(),
                                                                                                                      UserType.class, t -> ((UserType) t).size(),
                                                                                                                      CompositeType.class, t -> ((CompositeType) t).types.size(),
                                                                                                                      DynamicCompositeType.class, t -> ((DynamicCompositeType) t).size());
        qt().withShrinkCycles(0).forAll(AbstractTypeGenerators.builder().withoutTypeKinds(PRIMITIVE, COUNTER).build()).checkAssert(type -> {
            int expectedSize = complexTypes.containsKey(type.getClass()) ? complexTypes.get(type.getClass()).apply(type) : 1;
            assertThat(type.subTypes()).hasSize(expectedSize);
        });
    }

    /**
     * @see <pre>CASSANDRA-18526: TupleType getString and fromString are not safe with string types</pre>
     */
    private static boolean containsUnsafeGetString(AbstractType<?> type)
    {
        type = type.unwrap();
        if (type instanceof TupleType)
        {
            TupleType tt = (TupleType) type;
            for (AbstractType<?> e : tt.subTypes())
            {
                AbstractType<?> unwrap = e.unwrap();
                if (unwrap instanceof StringType || unwrap instanceof TupleType)
                    return true;
            }
        }
        else if (type instanceof DynamicCompositeType || type instanceof CompositeType)
        {
            for (AbstractType<?> e : type.subTypes())
            {
                AbstractType<?> unwrap = e.unwrap();
                if (unwrap instanceof StringType)
                    return true;
            }
        }
        for (AbstractType<?> e : type.subTypes())
        {
            if (containsUnsafeGetString(e))
                return true;
        }
        return false;
    }

    private boolean containsUnsafeToLiteral(AbstractType<?> type)
    {
        type = type.unwrap();
        if (type instanceof DecimalType)
            // toCQLLiteral is loss
            return true;
        for (AbstractType<?> e : type.subTypes())
        {
            if (containsUnsafeToLiteral(e))
                return true;
        }
        return false;
    }

    @Test
    public void typeParser()
    {
        Gen<AbstractType<?>> gen = genBuilder()
                                   .withMaxDepth(1)
                                   // UDTs produce bad type strings, which is required by org.apache.cassandra.io.sstable.SSTableHeaderFix
                                   // fixing this may have bad side effects between 3.6 upgrading to 5.0...
                                   .withoutTypeKinds(UDT)
                                   .build();
        qt().withShrinkCycles(0).forAll(gen).checkAssert(type -> {
            AbstractType<?> parsed = TypeParser.parse(type.toString());
            assertThat(parsed).describedAs("TypeParser mismatch:\nExpected: %s\nActual: %s", typeTree(type), typeTree(parsed)).isEqualTo(type);
        });
    }

    @Test
    public void toStringIsCQLYo()
    {
        cqlTypeSerde(type -> "'" + type.toString() + "'");
    }

    @Test
    public void cqlTypeSerde()
    {
        cqlTypeSerde(type -> type.asCQL3Type().toString());
    }

    private static void cqlTypeSerde(Function<AbstractType<?>, String> cqlFunc)
    {
        // TODO : add UDT back
        // exclude UDT from CQLTypeParser as the different toString methods do not produce a consistent types, unlike TypeParser
        Gen<AbstractType<?>> gen = genBuilder()
                                   .withDefaultSetKey(AbstractTypeGenerators.withoutUnsafeEquality().withoutTypeKinds(UDT))
                                   .withoutTypeKinds(UDT)
                                   .build();
        qt().withShrinkCycles(0).forAll(gen).checkAssert(type -> {
            // to -> from cql
            String cqlType = cqlFunc.apply(type);
            // just easier to read this way...
            cqlType = cqlType.replaceAll("org.apache.cassandra.db.marshal.", "");
            AbstractType<?> fromCQLTypeParser = CQLTypeParser.parse(null, cqlType, toTypes(extractUDTs(type)));
            assertThat(fromCQLTypeParser)
            .describedAs("CQL type %s parse did not match the expected type:\nExpected: %s\nActual: %s", cqlType, typeTree(type), typeTree(fromCQLTypeParser))
            .isEqualTo(type);
        });
    }

    @Test
    public void serdeFromString()
    {
        // avoid empty bytes as fromString can't figure out what to do in cases such as tuple(bytes); the tuple getString = "" so was the column not defined or was it empty?
        try (Releaser i1 = overridePrimitiveTypeSupport(BytesType.instance, of(BytesType.instance, Generators.bytes(1, 1024), FastByteOperations::compareUnsigned));
             Releaser i2 = overridePrimitiveTypeSupport(AsciiType.instance, of(AsciiType.instance, SourceDSL.strings().ascii().ofLengthBetween(1, 1024), stringComparator(AsciiType.instance)));
             Releaser i3 = overridePrimitiveTypeSupport(UTF8Type.instance, of(UTF8Type.instance, Generators.utf8(1, 1024), stringComparator(UTF8Type.instance))))
        {
            Gen<AbstractType<?>> typeGen = genBuilder()
                                           // a type maybe safe, but for some container types, specific element types are unsafe
                                           .withTypeFilter(type -> !containsUnsafeGetString(type))
                                           // fromString(getString(bb)) does not work
                                           .withoutPrimitive(DurationType.instance)
                                           .withDefaultSetKey(AbstractTypeGenerators.withoutUnsafeEquality().withTypeFilter(type -> !containsUnsafeGetString(type)))
                                           // composite requires all elements fit into Short.MAX_VALUE bytes
                                           // so try to limit the possible expansion of types
                                           .withCompositeElementGen(genBuilder().withoutPrimitive(DurationType.instance).withDefaultSizeGen(1).withMaxDepth(1).withTypeFilter(type -> !containsUnsafeGetString(type)).build())
                                           .build();
            qt().withShrinkCycles(0).forAll(examples(1, typeGen)).checkAssert(example -> {
                AbstractType type = example.type;

                for (Object expected : example.samples)
                {
                    ByteBuffer bb = type.decompose(expected);
                    type.validate(bb);
                    String str = type.getString(bb);
                    assertBytesEquals(type.fromString(str), bb, "fromString(getString(bb)) != bb; %s", str);
                }
            });
        }
    }

    @Test
    public void serdeFromCQLLiteral()
    {
        Gen<AbstractType<?>> typeGen = genBuilder()
                                       // parseLiteralType(toCQLLiteral(bb)) does not work
                                       .withoutPrimitive(DurationType.instance)
                                       .withDefaultSetKey(AbstractTypeGenerators.withoutUnsafeEquality())
                                       // composite requires all elements fit into Short.MAX_VALUE bytes
                                       // so try to limit the possible expansion of types
                                       .withCompositeElementGen(genBuilder().withoutPrimitive(DurationType.instance).withDefaultSizeGen(1).withMaxDepth(1).build())
                                       .build();
        qt().withShrinkCycles(0).forAll(examples(1, typeGen)).checkAssert(example -> {
            AbstractType type = example.type;

            for (Object expected : example.samples)
            {
                ByteBuffer bb = type.decompose(expected);
                type.validate(bb);

                String literal = type.asCQL3Type().toCQLLiteral(bb);
                ByteBuffer cqlBB = parseLiteralType(type, literal);
                assertBytesEquals(cqlBB, bb, "Deserializing literal %s did not match expected bytes", literal);
            }});
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void serde()
    {
        Gen<AbstractType<?>> typeGen = genBuilder()
                                       .withDefaultSetKey(AbstractTypeGenerators.withoutUnsafeEquality())
                                       // composite requires all elements fit into Short.MAX_VALUE bytes
                                       // so try to limit the possible expansion of types
                                       .withCompositeElementGen(genBuilder().withDefaultSizeGen(1).withMaxDepth(1).build())
                                       .build();
        qt().withShrinkCycles(0).forAll(examples(1, typeGen)).checkAssert(example -> {
            AbstractType type = example.type;

            for (Object expected : example.samples)
            {
                ByteBuffer bb = type.decompose(expected);
                int position = bb.position();
                type.validate(bb);
                Object read = type.compose(bb);
                assertThat(bb.position()).describedAs("ByteBuffer was mutated by %s", type).isEqualTo(position);
                assertThat(read).isEqualTo(expected);

                try (DataOutputBuffer out = DataOutputBuffer.scratchBuffer.get())
                {
                    type.writeValue(bb, out);
                    ByteBuffer written = out.unsafeGetBufferAndFlip();
                    DataInputPlus in = new DataInputBuffer(written, true);
                    assertBytesEquals(type.readBuffer(in), bb, "readBuffer(writeValue(bb)) != bb");
                    in = new DataInputBuffer(written, false);
                    type.skipValue(in);
                    assertThat(written.remaining()).isEqualTo(0);
                }
                catch (IOException e)
                {
                    throw new UncheckedIOException(e);
                }
            }
        });
    }

    private static void assertBytesEquals(ByteBuffer actual, ByteBuffer expected, String msg, Object... args)
    {
        assertThat(bytesToHex(actual)).describedAs(msg, args).isEqualTo(bytesToHex(expected));
    }

    private static ColumnMetadata fake(AbstractType<?> type)
    {
        return new ColumnMetadata(null, null, new ColumnIdentifier("", true), type, 0, ColumnMetadata.Kind.PARTITION_KEY, null);
    }

    private static ByteBuffer parseLiteralType(AbstractType<?> type, String literal)
    {
        try
        {
            return type.asCQL3Type().fromCQLLiteral(literal);
        }
        catch (Exception e)
        {
            throw new AssertionError(String.format("Unable to parse CQL literal %s from type %s", literal, type.asCQL3Type()), e);
        }
    }

    private static Types toTypes(Set<UserType> udts)
    {
        if (udts.isEmpty())
            return Types.none();
        Types.Builder builder = Types.builder();
        for (UserType udt : udts)
            builder.add(udt.unfreeze());
        return builder.build();
    }

    private static ByteComparable fromBytes(AbstractType<?> type, ByteBuffer bb)
    {
        return version -> type.asComparableBytes(bb, version);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void ordering()
    {
        TypeGenBuilder baseline = genBuilder()
                                  .withoutPrimitive(DurationType.instance) // this uses byte ordering and vint, which makes the ordering effectivlly random from a user's point of view
                                  .withoutTypeKinds(COUNTER); // counters don't allow ordering
        // composite requires all elements fit into Short.MAX_VALUE bytes
        // so try to limit the possible expansion of types
        Gen<AbstractType<?>> types = baseline.withCompositeElementGen(new TypeGenBuilder(baseline).withDefaultSizeGen(1).withMaxDepth(1).build())
                                             .build();
        qt().withShrinkCycles(0).forAll(examples(10, types)).checkAssert(example -> {
            AbstractType type = example.type;
            List<ByteBuffer> actual = decompose(type, example.samples);
            actual.sort(type);
            List<ByteBuffer>[] byteOrdered = new List[ByteComparable.Version.values().length];
            List<OrderedBytes>[] rawByteOrdered = new List[ByteComparable.Version.values().length];
            for (int i = 0; i < byteOrdered.length; i++)
            {
                byteOrdered[i] = new ArrayList<>(actual);
                ByteComparable.Version version = ByteComparable.Version.values()[i];
                byteOrdered[i].sort((a, b) -> ByteComparable.compare(fromBytes(type, a), fromBytes(type, b), version));

                rawByteOrdered[i] = actual.stream()
                                          .map(bb -> new OrderedBytes(ByteSourceInverse.readBytes(fromBytes(type, bb).asComparableBytes(version)), bb))
                                          .collect(Collectors.toList());
                rawByteOrdered[i].sort(Comparator.naturalOrder());
            }

            example.samples.sort(comparator(type));
            List<Object> real = new ArrayList<>(actual.size());
            for (ByteBuffer bb : actual)
                real.add(type.compose(bb));
            assertThat(real).isEqualTo(example.samples);
            List<Object>[] realBytesOrder = new List[byteOrdered.length];
            for (int i = 0; i < realBytesOrder.length; i++)
            {
                ByteComparable.Version version = ByteComparable.Version.values()[i];
                assertThat(compose(type, byteOrdered[i])).describedAs("Bad ordering for type %s", version).isEqualTo(real);
                assertThat(compose(type, rawByteOrdered[i].stream().map(ob -> ob.src).collect(Collectors.toList()))).describedAs("Bad ordering for type %s", version).isEqualTo(real);
            }
        });
    }

    /**
     * For {@link AbstractType#asComparableBytes(ByteBuffer, ByteComparable.Version)} not all versions can be inverted,
     * but all versions must be comparable... so this class stores the ordered bytes and the original src.
     */
    private static class OrderedBytes implements Comparable<OrderedBytes>
    {
        private final byte[] orderedBytes;
        private final ByteBuffer src;

        private OrderedBytes(byte[] orderedBytes, ByteBuffer src)
        {
            this.orderedBytes = orderedBytes;
            this.src = src;
        }

        @Override
        public int compareTo(OrderedBytes o)
        {
            return FastByteOperations.compareUnsigned(orderedBytes, o.orderedBytes);
        }
    }

    private static List<Object> compose(AbstractType<?> type, List<ByteBuffer> bbs)
    {
        List<Object> os = new ArrayList<>(bbs.size());
        for (ByteBuffer bb : bbs)
            os.add(type.compose(bb));
        return os;
    }

    @SuppressWarnings("unchecked")
    private static Comparator<Object> comparator(AbstractType<?> type)
    {
        return (Comparator<Object>) AbstractTypeGenerators.comparator(type);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private List<ByteBuffer> decompose(AbstractType type, List<Object> value)
    {
        List<ByteBuffer> expected = new ArrayList<>(value.size());
        for (int i = 0; i < value.size(); i++)
            expected.add(type.decompose(value.get(i)));
        return expected;
    }

    private static TypeGenBuilder genBuilder()
    {
        return AbstractTypeGenerators.builder()
                                     // empty is a legacy from 2.x and is only allowed in special cases and not allowed in all... as this class tests all cases, need to limit this type out
                                     .withoutEmpty();
    }

    private static Gen<Example> examples(int samples, Gen<AbstractType<?>> typeGen)
    {
        Gen<Example> gen = rnd -> {
            AbstractType<?> type = typeGen.generate(rnd);
            AbstractTypeGenerators.TypeSupport<?> support = getTypeSupport(type);
            List<Object> list = new ArrayList<>(samples);
            for (int i = 0; i < samples; i++)
                list.add(support.valueGen.generate(rnd));
            return new Example(type, list);
        };
        return gen.describedAs(e -> {
            StringBuilder sb = new StringBuilder();
            sb.append("Type:\n").append(typeTree(e.type));
            sb.append("\nValues: ").append(e.samples);
            return sb.toString();
        });
    }

    private static class Example
    {
        private final AbstractType<?> type;
        private final List<Object> samples;

        private Example(AbstractType<?> type, List<Object> samples)
        {
            this.type = type;
            this.samples = samples;
        }

        @Override
        public String toString()
        {
            return "{" +
                   "type=" + type +
                   ", value=" + samples +
                   '}';
        }
    }

    /**
     * This test case checks the output of is*CompatibleWith methods for primitive types - whether the output is
     * consistent with the assumed relation defined in this test class (expected compatibility entered manually).
     */
    @Test
    public void testAssumedPrimitiveTypesCompatibility()
    {
        testAssumedPrimitiveTypesCompatibility(typesCompatibility);
        if (typesCompatibility.prev != null)
            testAssumedPrimitiveTypesCompatibility(typesCompatibility.prev);
    }

    public void testAssumedPrimitiveTypesCompatibility(TypesCompatibility typesCompatibility)
    {
        SoftAssertions assertions = new SoftAssertions();

        // assumed compatibility
        typesCompatibility.compatibleWith.forEach((l, r) -> assertions.assertThat(l.isCompatibleWith(r)).describedAs(isCompatibleWithDesc(l, r)).isTrue());
        typesCompatibility.valueCompatibleWith.forEach((l, r) -> assertions.assertThat(l.isValueCompatibleWith(r)).describedAs(isValueCompatibleWithDesc(l, r)).isTrue());
        typesCompatibility.serializationCompatibleWith.forEach((l, r) -> assertions.assertThat(l.isSerializationCompatibleWith(r)).describedAs(isSerializationCompatibleWithDesc(l, r)).isTrue());

        // assumed incompatibility
        inverse(typesCompatibility.compatibleWith).forEach(p -> assertions.assertThat(p.left.isCompatibleWith(p.right)).describedAs(isCompatibleWithDesc(p.left, p.right)).isFalse());
        inverse(typesCompatibility.valueCompatibleWith).forEach(p -> assertions.assertThat(p.left.isValueCompatibleWith(p.right)).describedAs(isValueCompatibleWithDesc(p.left, p.right)).isFalse());
        inverse(typesCompatibility.serializationCompatibleWith).forEach(p -> assertions.assertThat(p.left.isSerializationCompatibleWith(p.right)).describedAs(isSerializationCompatibleWithDesc(p.left, p.right)).isFalse());

        // it is implied that isSerializationCompatibleWith is a subset of isValueCompatibleWith
        typesCompatibility.serializationCompatibleWith.forEach((l, r) -> assertions.assertThat(typesCompatibility.valueCompatibleWith.containsEntry(l, r)).describedAs(isValueCompatibleWithDesc(l, r)).isTrue());
        assertions.assertAll();
    }

    /**
     * Assuming that {@link #testAssumedPrimitiveTypesCompatibility()} passes, this test case verifies whether the types
     * said to be compatible hold the assumed properties. In particular:
     * <li>L {@link AbstractType#isValueCompatibleWith(AbstractType)} R - if it is possible to {@code L.compose(R.decompose(v))} for any v valid for type R, and the converted value of type L still makes sense</li>
     * <li>L {@link AbstractType#isSerializationCompatibleWith(AbstractType)} R - if it is possible to read a cell written using R's type serializer with L's type serializer</li>
     * <li>L {@link AbstractType#isCompatibleWith(AbstractType)} R - L isSerializationCompatibleWith R and it is possible to compare decomposed values of R's type using L's type comparator</li>
     */
    @Test
    public void testPrimitiveTypesCompatibility()
    {
        SoftAssertions assertions = new SoftAssertions();
        typesCompatibility.valueCompatibleWith.forEach((left, right) -> assertions.check(() -> verifyTypesCompatibility(left, right, getTypeSupport(right).valueGen)));
        assertions.assertAll();
    }

    private static <L, R> void verifyTypesCompatibility(AbstractType left, AbstractType right, Gen rightGen)
    {
        if (left.equals(right))
            return;

        if (!left.isValueCompatibleWith(right))
            return;

        ColumnMetadata rightColumn = new ColumnMetadata("k", "t", ColumnIdentifier.getInterned("c", false), right, ColumnMetadata.NO_POSITION, ColumnMetadata.Kind.REGULAR, null);
        ColumnMetadata leftColumn = new ColumnMetadata("k", "t", ColumnIdentifier.getInterned("c", false), left, ColumnMetadata.NO_POSITION, ColumnMetadata.Kind.REGULAR, null);

        TableMetadata leftTable = TableMetadata.builder("k", "t").addPartitionKeyColumn("pk", EmptyType.instance).addColumn(leftColumn).build();
        TableMetadata rightTable = TableMetadata.builder("k", "t").addPartitionKeyColumn("pk", EmptyType.instance).addColumn(rightColumn).build();

        SerializationHeader leftHeader = new SerializationHeader(false, leftTable, leftTable.regularAndStaticColumns(), EncodingStats.NO_STATS);
        SerializationHeader rightHeader = new SerializationHeader(false, rightTable, rightTable.regularAndStaticColumns(), EncodingStats.NO_STATS);

        DeserializationHelper leftHelper = new DeserializationHelper(leftTable, MessagingService.current_version, DeserializationHelper.Flag.LOCAL, ColumnFilter.all(leftTable));
        SerializationHelper rightHelper = new SerializationHelper(rightHeader);

        qt().withExamples(10).forAll(rightGen).checkAssert(v -> {
            Assertions.assertThatNoException().describedAs(typeRelDesc(".decompose", left, right)).isThrownBy(() -> {

                // value compatibility means that we can use left's type serializer to decompose a value of right's type
                ByteBuffer rightDecomposed = right.decompose((R) v);
                L leftComposed = (L) left.compose(rightDecomposed);
                ByteBuffer leftDecomposed = left.decompose(leftComposed);
                assertThat(leftDecomposed.hasRemaining()).isEqualTo(rightDecomposed.hasRemaining());
            });

            Assertions.assertThatNoException().describedAs(typeRelDesc(".deserialize", left, right)).isThrownBy(() -> {
                // serialization compatibility means that we can read a cell written using right's type serializer with left's type serializer;
                // this additinoally imposes the requirement for storing the buffer lenght in the serialized form if the value is of variable length
                // as well as, either both types serialize into a single or multiple cells
                if (left.isSerializationCompatibleWith(right))
                {
                    if (!left.isMultiCell() && !right.isMultiCell())
                        verifySerializationCompatibilityForSimpleCells(left, right, v, rightTable, rightColumn, rightHelper, leftHeader, leftHelper, leftColumn);
                    else
                        verifySerializationCompatibilityForComplexCells(left, right, v, rightTable, rightColumn, rightHelper, leftHeader, leftHelper, leftColumn);
                }
            });
        });

        if (!left.isCompatibleWith(right) || right.comparisonType == AbstractType.ComparisonType.NOT_COMPARABLE || left.comparisonType == AbstractType.ComparisonType.NOT_COMPARABLE)
            return;

        // types compatibility means that we can compare values of right's type using left's type comparator additionally
        // to types being serialization compatible
        qt().withExamples(10).forAll(rightGen, rightGen).checkAssert((r1, r2) -> {
            Assertions.assertThatNoException().describedAs(typeRelDesc(".compare", left, right)).isThrownBy(() -> {
                if (!left.isMultiCell() && !right.isMultiCell())
                {
                    // make sure that frozen<left> isCompatibleWith frozen<right> ==> left isCompatibleWith right
                    assertThat(left.unfreeze().isCompatibleWith(right.unfreeze())).isTrue();
                }

                ByteBuffer rBuf1 = right.decompose(r1);
                ByteBuffer rBuf2 = right.decompose(r2);
                ByteBuffer lBuf1 = left.decompose(left.compose(rBuf1));
                ByteBuffer lBuf2 = left.decompose(left.compose(rBuf2));

                int c = right.compare(rBuf1, rBuf2);
                // first just check that the comparison is antisymmetric
                assertThat(copySign(1, right.compare(rBuf2, rBuf1))).isEqualTo(copySign(1, -c));

                // then, check if we can compare buffers using left's comparator
                assertThat(copySign(1, left.compare(lBuf1, lBuf2))).isEqualTo(copySign(1, c));
                assertThat(copySign(1, left.compare(lBuf1, rBuf2))).isEqualTo(copySign(1, c));
                assertThat(copySign(1, left.compare(rBuf1, lBuf2))).isEqualTo(copySign(1, c));
                assertThat(copySign(1, left.compare(rBuf1, rBuf2))).isEqualTo(copySign(1, c));

                assertThat(copySign(1, left.compare(lBuf2, lBuf1))).isEqualTo(copySign(1, -c));
                assertThat(copySign(1, left.compare(lBuf2, rBuf1))).isEqualTo(copySign(1, -c));
                assertThat(copySign(1, left.compare(rBuf2, lBuf1))).isEqualTo(copySign(1, -c));
                assertThat(copySign(1, left.compare(rBuf2, rBuf1))).isEqualTo(copySign(1, -c));
            });
        });
    }

    private static void verifySerializationCompatibilityForSimpleCells(AbstractType left, AbstractType right, Object v,
                                                                       TableMetadata rightTable, ColumnMetadata rightColumn, SerializationHelper rightHelper,
                                                                       SerializationHeader leftHeader, DeserializationHelper leftHelper, ColumnMetadata leftColumn) throws IOException
    {
        Row rightRow = Rows.simpleBuilder(rightTable).noPrimaryKeyLivenessInfo().add(rightColumn.name.toString(), v).build();
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            UnfilteredSerializer.serializer.serialize(rightRow, rightHelper, out, MessagingService.current_version);
            try (DataInputBuffer in = new DataInputBuffer(out.getData()))
            {
                Row.Builder builder = BTreeRow.sortedBuilder();
                builder.addPrimaryKeyLivenessInfo(rightRow.primaryKeyLivenessInfo());
                Row leftRow = (Row) UnfilteredSerializer.serializer.deserialize(in, leftHeader, leftHelper, builder);
                Cell leftData = (Cell) leftRow.getColumnData(leftColumn);
                Cell rightData = (Cell) rightRow.getColumnData(rightColumn);
                assertThat(leftData.buffer()).describedAs(typeRelDesc(".deserialize", left, right)).isEqualTo(rightData.buffer());
            }
        }
    }

    private static void verifySerializationCompatibilityForComplexCells(AbstractType left, AbstractType right, Object v,
                                                                        TableMetadata rightTable, ColumnMetadata rightColumn, SerializationHelper rightHelper,
                                                                        SerializationHeader leftHeader, DeserializationHelper leftHelper, ColumnMetadata leftColumn) throws IOException
    {
        Row rightRow = Rows.simpleBuilder(rightTable).noPrimaryKeyLivenessInfo().add(rightColumn.name.toString(), v).build();
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            UnfilteredSerializer.serializer.serialize(rightRow, rightHelper, out, MessagingService.current_version);
            try (DataInputBuffer in = new DataInputBuffer(out.getData()))
            {
                Row.Builder builder = BTreeRow.sortedBuilder();
                builder.addPrimaryKeyLivenessInfo(rightRow.primaryKeyLivenessInfo());
                Row leftRow = (Row) UnfilteredSerializer.serializer.deserialize(in, leftHeader, leftHelper, builder);
                ComplexColumnData leftData = leftRow.getComplexColumnData(leftColumn);
                ComplexColumnData rightData = rightRow.getComplexColumnData(rightColumn);
                assertThat(leftData.cellsCount()).isEqualTo(rightData.cellsCount());
                for (int i = 0; i < leftData.cellsCount(); i++)
                {
                    Cell leftCell = leftData.getCellByIndex(i);
                    Cell rightCell = rightData.getCellByIndex(i);
                    assertThat(leftCell.buffer()).describedAs(bytesToHex(leftCell.buffer())).isEqualTo(rightCell.buffer()).describedAs(bytesToHex(rightCell.buffer()));
                    assertThat(leftCell.path().size()).isEqualTo(rightCell.path().size());
                    for (int j = 0; j < leftCell.path().size(); j++)
                        assertThat(leftCell.path().get(j)).describedAs(bytesToHex(leftCell.path().get(j))).isEqualTo(rightCell.path().get(j)).describedAs(bytesToHex(rightCell.path().get(j)));
                }
            }
        }
    }

    @Test
    public void testMultiCellSupport()
    {
        if (typesCompatibility.prev != null)
        {
            // for compatibility, we ensure that this version can read values of all the types the previous version can write
            assertThat(typesCompatibility.multiCellSupportingTypesForReading).containsAll(typesCompatibility.prev.multiCellSupportingTypes);
        }

        // all primitive types should be freezing agnostic
        primitiveTypes().forEach(type -> {
            assertThat(type.freeze()).isSameAs(type);
            assertThat(type.unfreeze()).isSameAs(type);
        });

        qt().forAll(genBuilder().withoutTypeKinds(PRIMITIVE)
                                .withMaxDepth(1)
                                .multiCellIfRelevant()
                                .withUserTypeKeyspace("ks")
                                .withUDTNames(arbitrary().constant("ut"))
                                .build())
            .checkAssert(type -> {
                if (typesCompatibility.multiCellSupportingTypesForReading.contains(type.getClass()))
                {
                    assertThat(type.isMultiCell()).isTrue();
                    assertThat(type.freeze()).isNotEqualTo(type);
                    assertThat(type.freeze().unfreeze()).isNotEqualTo(type.freeze());
                }
                else
                {
                    assertThat(type.isMultiCell()).isFalse();
                    assertThat(type.freeze()).isSameAs(type);
                    assertThat(type.unfreeze()).isSameAs(type);
                }
            });
    }

    @Test
    public void testMapTypesCompatibility()
    {
        SoftAssertions assertions = new SoftAssertions();
        SoftAssertions upgradeAssertions = new SoftAssertions();

        primitiveTypePairs(t -> t.getClass() != EmptyType.class).forEach(keyPair -> { // key cannot be empty
            primitiveTypePairs().forEach(valuePair -> {
                MapType<?, ?> leftMap = MapType.getInstance(keyPair.left, valuePair.left, true);
                MapType<?, ?> rightMap = MapType.getInstance(keyPair.right, valuePair.right, true);

                // note that we can assume is*Compatible methods are correct for primitive types because they are verified in other tests
                typesCompatibility.checkMapTypeCompatibility(leftMap, rightMap, assertions);
                if (typesCompatibility.prev != null)
                    typesCompatibility.prev.checkMapTypeCompatibility(leftMap, rightMap, upgradeAssertions);
            });
        });

        assertions.assertAll();
        upgradeAssertions.assertAll();
    }

    @Test
    public void testSetTypesCompatibility()
    {
        SoftAssertions assertions = new SoftAssertions();
        SoftAssertions upgradeAssertions = new SoftAssertions();

        primitiveTypePairs().forEach(keyPair -> {
            SetType<?> leftSet = SetType.getInstance(keyPair.left, true);
            SetType<?> rightSet = SetType.getInstance(keyPair.right, true);
            // note that we can assume is*Compatible methods are correct for primitive types because they are verified in other tests
            typesCompatibility.checkSetTypeCompatibility(leftSet, rightSet, assertions);
            if (typesCompatibility.prev != null)
                typesCompatibility.prev.checkSetTypeCompatibility(leftSet, rightSet, upgradeAssertions);
        });

        assertions.assertAll();
        upgradeAssertions.assertAll();
    }

    @Test
    public void testListTypesCompatibility()
    {
        SoftAssertions assertions = new SoftAssertions();
        SoftAssertions upgradeAssertions = new SoftAssertions();

        primitiveTypePairs().forEach(valuePair -> {
            ListType<?> leftList = ListType.getInstance(valuePair.left, true);
            ListType<?> rightList = ListType.getInstance(valuePair.right, true);
            // note that we can assume is*Compatible methods are correct for primitive types because they are verified in other tests
            typesCompatibility.checkListTypeCompatibility(leftList, rightList, assertions);
            if (typesCompatibility.prev != null)
                typesCompatibility.prev.checkListTypeCompatibility(leftList, rightList, upgradeAssertions);
        });

        assertions.assertAll();
        upgradeAssertions.assertAll();
    }

    @Test
    public void testUserTypesCompatibility()
    {
        SoftAssertions assertions = new SoftAssertions();
        SoftAssertions upgradeAssertions = new SoftAssertions();

        String ks = "ks";
        ByteBuffer t = ByteBufferUtil.bytes("t");
        List<FieldIdentifier> names = Stream.of("a", "b").map(FieldIdentifier::forUnquoted).collect(Collectors.toUnmodifiableList());

        primitiveTypePairs().forEach(elem1Pair -> {
            primitiveTypePairs().forEach(elem2Pair -> {
                UserType leftType = new UserType(ks, t, names, List.of(elem1Pair.left, elem2Pair.left), true);
                UserType rightType = new UserType(ks, t, names, List.of(elem1Pair.right, elem2Pair.right), true);

                // note that we can assume is*Compatible methods are correct for primitive types because they are verified in other tests
                typesCompatibility.checkUserTypeCompatibility(leftType, rightType, assertions);
                if (typesCompatibility.prev != null)
                    typesCompatibility.prev.checkUserTypeCompatibility(leftType, rightType, upgradeAssertions);
            });
        });

        assertions.assertAll();
        upgradeAssertions.assertAll();
    }

    @Test
    public void testCompositeTypesCompatibility()
    {
        SoftAssertions assertions = new SoftAssertions();
        SoftAssertions upgradeAssertions = new SoftAssertions();

        primitiveTypePairs().forEach(elem1Pair -> {
            primitiveTypePairs().forEach(elem2Pair -> {
                DynamicCompositeType leftType = DynamicCompositeType.getInstance(Map.of((byte) 0, elem1Pair.left, (byte) 1, elem2Pair.left));
                DynamicCompositeType rightType = DynamicCompositeType.getInstance(Map.of((byte) 0, elem1Pair.right, (byte) 1, elem2Pair.right));
                // note that we can assume is*Compatible methods are correct for primitive types because they are verified in other tests
                typesCompatibility.checkCompositeTypeCompatibility(leftType, rightType, assertions);
                if (typesCompatibility.prev != null)
                    typesCompatibility.prev.checkCompositeTypeCompatibility(leftType, rightType, upgradeAssertions);
            });
        });

        assertions.assertAll();
        upgradeAssertions.assertAll();
    }

    private static Description typeRelDesc(String rel, AbstractType<?> left, AbstractType<?> right)
    {
        return typeRelDesc(rel, left, right, null);
    }

    private static Description typeRelDesc(String rel, AbstractType<?> left, AbstractType<?> right, String extraInfo)
    {
        return new Description()
        {
            @Override
            public String value()
            {
                if (extraInfo != null)
                {
                    return TYPE_PREFIX_PATTERN.matcher(String.format("%s %s %s, %s", left, rel, right, extraInfo)).replaceAll("");
                }
                else if (!left.equals(right))
                {
                    String extraInfo = Streams.zip(left.subTypes().stream(), right.subTypes().stream(), (l, r) -> {
                        if (l.equals(r))
                            return "";

                        StringBuilder out = new StringBuilder();
                        if (l.isCompatibleWith(r))
                            out.append(" cmp");
                        if (l.isValueCompatibleWith(r))
                            out.append(" val");
                        if (l.isSerializationCompatibleWith(r))
                            out.append(" ser");
                        if (out.length() > 0)
                            return String.format("%s is%s compatible with %s", l, out, r);
                        else
                            return String.format("%s is not compatible with %s", l, r);
                    }).collect(Collectors.joining("; ", "{", "}"));
                    return TYPE_PREFIX_PATTERN.matcher(String.format("%s %s %s, %s", left, rel, right, extraInfo)).replaceAll("");
                }
                else
                {
                    return TYPE_PREFIX_PATTERN.matcher(String.format("%s %s %s", left, rel, right)).replaceAll("");
                }
            }
        };
    }

    private static Description isCompatibleWithDesc(AbstractType<?> left, AbstractType<?> right)
    {
        return typeRelDesc("isCompatibleWith", left, right);
    }

    private static Description isValueCompatibleWithDesc(AbstractType<?> left, AbstractType<?> right)
    {
        return typeRelDesc("isValueCompatibleWith", left, right);
    }

    private static Description isSerializationCompatibleWithDesc(AbstractType<?> left, AbstractType<?> right)
    {
        return typeRelDesc("isSerializationCompatibleWith", left, right);
    }

    /**
     * The instances of this class provides types compatibility checks valid for a certain version of Cassandra.
     * This way we can verify whether the current implementation satisfy assumed compatibility rules, as well as
     * upgrade compatibility (that is, whether the new implementation ensures the compatibility rules from the previous
     * verion of Cassandra are still satisfied).
     */
    public abstract static class TypesCompatibility
    {
        public final Set<Class<? extends AbstractType>> multiCellSupportingTypes = new HashSet<>();
        public final Set<Class<? extends AbstractType>> multiCellSupportingTypesForReading = new HashSet<>();


        public final Multimap<AbstractType<?>, AbstractType<?>> valueCompatibleWith = HashMultimap.create();
        public final Multimap<AbstractType<?>, AbstractType<?>> serializationCompatibleWith = HashMultimap.create();
        public final Multimap<AbstractType<?>, AbstractType<?>> compatibleWith = HashMultimap.create();

        public final TypesCompatibility prev;

        protected static final Set<AbstractType<?>> PRIMITIVE_TYPES = PRIMITIVE_TYPE_DATA_GENS.keySet();

        public TypesCompatibility(TypesCompatibility prev)
        {
            this.prev = prev;
        }

        static <T> Multimap<T, T> buildTransitiveClosure(Multimap<T, T> relation, Multimap<T, T> transitiveClosure, Collection<T> domain)
        {
            transitiveClosure.clear();
            Deque<T> path = new LinkedList<>();
            domain.forEach(lt -> {
                transitiveClosure.put(lt, lt);
                path.addLast(lt);
                relation.get(lt).forEach(rt -> addToTransitiveClosure(relation, transitiveClosure, path, rt));
                path.removeLast();
                assert path.isEmpty();
            });
            return transitiveClosure;
        }

        private static <T> void addToTransitiveClosure(Multimap<T, T> relation, Multimap<T, T> transitiveClosure, Deque<T> path, T element)
        {
            assert !path.isEmpty();

            if (path.contains(element))
                return; // cycle

            path.forEach(lt -> transitiveClosure.put(lt, element));
            path.addLast(element);
            relation.get(element).forEach(t -> addToTransitiveClosure(relation, transitiveClosure, path, t));
            path.removeLast();
        }

        static Stream<Pair<AbstractType<?>, AbstractType<?>>> inverse(Multimap<AbstractType<?>, AbstractType<?>> relation, Collection<AbstractType<?>> domain)
        {
            return domain.stream()
                         .flatMap(l -> domain.stream()
                                             .filter(r -> !relation.containsEntry(l, r))
                                             .map(r -> Pair.create(l, r)));
        }

        static Stream<Pair<AbstractType<?>, AbstractType<?>>> inverse(Multimap<AbstractType<?>, AbstractType<?>> relation)
        {
            return inverse(relation, PRIMITIVE_TYPES);
        }

        <L extends AbstractType, R extends AbstractType> void checkMultiCellTypeCompatibility(Collection<L> leftVariants, Collection<R> rightVariants, BiPredicate<L, R> checkPredicate, BiPredicate<L, R> expectPredicate, BiFunction<L, R, Description> descProvider, SoftAssertions assertions)
        {
            for (L left : leftVariants)
                if (left.isMultiCell() && multiCellSupportingTypes.contains(left.getClass()) || !left.isMultiCell())
                    for (R right : rightVariants)
                        if (right.isMultiCell() && multiCellSupportingTypes.contains(right.getClass()) || !right.isMultiCell())
                        {
                            assertions.assertThat(checkPredicate.test(left, right))
                                      .as(descProvider.apply(left, right))
                                      .isEqualTo(expectPredicate.test(left, right));

                            verifyTypesCompatibility(left, right, getTypeSupport(right, integers().between(2, 2), SourceDSL.arbitrary().constant(AbstractTypeGenerators.ValueDomain.NORMAL)).valueGen);
                        }
        }

        <L extends AbstractType, R extends AbstractType> void checkCollectionTypeCompatibility(L left, R right, BiPredicate<L, R> checkPredicate, BiPredicate<L, R> expectPredicate, BiFunction<L, R, Description> descProvider, SoftAssertions assertions)
        {
            checkMultiCellTypeCompatibility(frozenAndUnfrozen(left), frozenAndUnfrozen(right), checkPredicate, expectPredicate, descProvider, assertions);
        }

        private <T extends AbstractType> Set<T> frozenAndUnfrozen(T type)
        {
            return Set.of((T) type.freeze(), (T) type.unfreeze());
        }

        void checkMapTypeCompatibility(MapType left, MapType right, SoftAssertions assertions)
        {
            this.checkCollectionTypeCompatibility(left, right, MapType::isCompatibleWith, this::isMapCompatibleWith, AbstractTypeTest::isCompatibleWithDesc, assertions);
            this.checkCollectionTypeCompatibility(left, right, MapType::isValueCompatibleWith, this::isMapValueCompatibleWith, AbstractTypeTest::isValueCompatibleWithDesc, assertions);
            this.checkCollectionTypeCompatibility(left, right, MapType::isSerializationCompatibleWith, this::isMapSerializationCompatibleWith, AbstractTypeTest::isSerializationCompatibleWithDesc, assertions);
        }

        void checkSetTypeCompatibility(SetType left, SetType right, SoftAssertions assertions)
        {
            this.checkCollectionTypeCompatibility(left, right, SetType::isCompatibleWith, this::isSetCompatibleWith, AbstractTypeTest::isCompatibleWithDesc, assertions);
            this.checkCollectionTypeCompatibility(left, right, SetType::isValueCompatibleWith, this::isSetValueCompatibleWith, AbstractTypeTest::isValueCompatibleWithDesc, assertions);
            this.checkCollectionTypeCompatibility(left, right, SetType::isSerializationCompatibleWith, this::isSetSerializationCompatibleWith, AbstractTypeTest::isSerializationCompatibleWithDesc, assertions);
        }

        void checkListTypeCompatibility(ListType left, ListType right, SoftAssertions assertions)
        {
            this.checkCollectionTypeCompatibility(left, right, ListType::isCompatibleWith, this::isListCompatibleWith, AbstractTypeTest::isCompatibleWithDesc, assertions);
            this.checkCollectionTypeCompatibility(left, right, ListType::isValueCompatibleWith, this::isListValueCompatibleWith, AbstractTypeTest::isValueCompatibleWithDesc, assertions);
            this.checkCollectionTypeCompatibility(left, right, ListType::isSerializationCompatibleWith, this::isListSerializationCompatibleWith, AbstractTypeTest::isSerializationCompatibleWithDesc, assertions);
        }

        public void checkUserTypeCompatibility(UserType leftType, UserType rightType, SoftAssertions assertions)
        {
            TupleType leftTypeAsTuple = (TupleType) new TupleType(leftType.types, false).unfreeze();
            TupleType rightTypeAsTuple = (TupleType) new TupleType(rightType.types, false).unfreeze();

            UserType extLeftType = withAddedField(leftType, "extra", EmptyType.instance);
            UserType extRightType = withAddedField(rightType, "extra", EmptyType.instance);

            TupleType extLeftTypeAsTuple = (TupleType) new TupleType(extLeftType.types, false).unfreeze();
            TupleType extRightTypeAsTuple = (TupleType) new TupleType(extRightType.types, false).unfreeze();

            Set<TupleType> leftTypes = ImmutableSet.of(leftType, leftTypeAsTuple, extLeftType, extLeftTypeAsTuple,
                                                       leftType.freeze(), (TupleType) leftTypeAsTuple.freeze(), extLeftType.freeze(), (TupleType) extLeftTypeAsTuple.freeze());
            Set<TupleType> rightTypes = ImmutableSet.of(rightType, rightTypeAsTuple, extRightType, extRightTypeAsTuple,
                                                        rightType.freeze(), (TupleType) rightTypeAsTuple.freeze(), extRightType.freeze(), (TupleType) extRightTypeAsTuple.freeze());

            this.checkMultiCellTypeCompatibility(leftTypes, rightTypes, TupleType::isCompatibleWith, this::isTupleCompatibleWith, AbstractTypeTest::isCompatibleWithDesc, assertions);
            this.checkMultiCellTypeCompatibility(leftTypes, rightTypes, TupleType::isValueCompatibleWith, this::isTupleValueCompatibleWith, AbstractTypeTest::isValueCompatibleWithDesc, assertions);
            this.checkMultiCellTypeCompatibility(leftTypes, rightTypes, TupleType::isSerializationCompatibleWith, this::isTupleSerializationCompatibleWith, AbstractTypeTest::isSerializationCompatibleWithDesc, assertions);
        }

        private UserType withAddedField(UserType type, String fieldName, AbstractType<?> fieldType)
        {
            ArrayList<FieldIdentifier> fieldNames = new ArrayList<>(type.fieldNames());
            fieldNames.add(FieldIdentifier.forUnquoted(fieldName));
            List<AbstractType<?>> fieldTypes = new ArrayList<>(type.fieldTypes());
            fieldTypes.add(fieldType);
            return new UserType(type.keyspace, type.name, fieldNames, fieldTypes, true);
        }

        public void checkCompositeTypeCompatibility(DynamicCompositeType leftType, DynamicCompositeType rightType, SoftAssertions assertions)
        {
            CompositeType leftTypeNonDymamic = CompositeType.getInstance(leftType.aliases.values());
            CompositeType rightTypeNonDynamic = CompositeType.getInstance(rightType.aliases.values());

            DynamicCompositeType extLeftType = withAddedField(leftType, EmptyType.instance);
            DynamicCompositeType extRightType = withAddedField(rightType, EmptyType.instance);

            CompositeType extLeftTypeNonDynamic = CompositeType.getInstance(extLeftType.aliases.values());
            CompositeType extRightTypeNonDynamic = CompositeType.getInstance(extRightType.aliases.values());

            Set<AbstractCompositeType> leftTypes = Stream.of(leftType, leftTypeNonDymamic, extLeftType, extLeftTypeNonDynamic,
                                                             leftType.freeze(), leftTypeNonDymamic.freeze(), extLeftType.freeze(), extLeftTypeNonDynamic.freeze())
                                                         .map(AbstractCompositeType.class::cast).collect(Collectors.toUnmodifiableSet());
            Set<AbstractCompositeType> rightTypes = Stream.of(rightType, rightTypeNonDynamic, extRightType, extRightTypeNonDynamic,
                                                              rightType.freeze(), rightTypeNonDynamic.freeze(), extRightType.freeze(), extRightTypeNonDynamic.freeze())
                                                          .map(AbstractCompositeType.class::cast).collect(Collectors.toUnmodifiableSet());

            this.checkMultiCellTypeCompatibility(leftTypes, rightTypes, AbstractCompositeType::isCompatibleWith, this::isCompositeCompatibleWith, AbstractTypeTest::isCompatibleWithDesc, assertions);
            this.checkMultiCellTypeCompatibility(leftTypes, rightTypes, AbstractCompositeType::isValueCompatibleWith, this::isCompositeValueCompatibleWith, AbstractTypeTest::isValueCompatibleWithDesc, assertions);
            this.checkMultiCellTypeCompatibility(leftTypes, rightTypes, AbstractCompositeType::isSerializationCompatibleWith, this::isCompositeSerializationCompatibleWith, AbstractTypeTest::isSerializationCompatibleWithDesc, assertions);
        }

        private DynamicCompositeType withAddedField(DynamicCompositeType type, AbstractType<?> fieldType)
        {
            Map<Byte, AbstractType<?>> aliases = new HashMap<>(type.aliases);
            aliases.put((byte) (type.aliases.size() + 1), fieldType);
            return DynamicCompositeType.getInstance(aliases);
        }

        abstract <L extends MapType, R extends MapType> boolean isMapCompatibleWith(L left, R right);

        abstract <L extends MapType, R extends MapType> boolean isMapValueCompatibleWith(L left, R right);

        abstract <L extends MapType, R extends MapType> boolean isMapSerializationCompatibleWith(L left, R right);

        abstract <L extends SetType, R extends SetType> boolean isSetCompatibleWith(L left, R right);

        abstract <L extends SetType, R extends SetType> boolean isSetValueCompatibleWith(L left, R right);

        abstract <L extends SetType, R extends SetType> boolean isSetSerializationCompatibleWith(L left, R right);

        abstract <L extends ListType, R extends ListType> boolean isListCompatibleWith(L left, R right);

        abstract <L extends ListType, R extends ListType> boolean isListValueCompatibleWith(L left, R right);

        abstract <L extends ListType, R extends ListType> boolean isListSerializationCompatibleWith(L left, R right);

        abstract <L extends TupleType, R extends TupleType> boolean isTupleCompatibleWith(L left, R right);

        abstract <L extends TupleType, R extends TupleType> boolean isTupleValueCompatibleWith(L left, R right);

        abstract <L extends TupleType, R extends TupleType> boolean isTupleSerializationCompatibleWith(L left, R right);

        abstract <L extends AbstractCompositeType, R extends AbstractCompositeType> boolean isCompositeCompatibleWith(L left, R right);

        abstract <L extends AbstractCompositeType, R extends AbstractCompositeType> boolean isCompositeValueCompatibleWith(L left, R right);

        abstract <L extends AbstractCompositeType, R extends AbstractCompositeType> boolean isCompositeSerializationCompatibleWith(L left, R right);
    }

    private static class Cassandra50BetaTypesCompatibility extends TypesCompatibility
    {
        public final static Cassandra50BetaTypesCompatibility instance = new Cassandra50BetaTypesCompatibility(null);

        private Cassandra50BetaTypesCompatibility(TypesCompatibility prev)
        {
            super(prev);
            valueCompatibleWith.put(BytesType.instance, AsciiType.instance);
            valueCompatibleWith.put(BytesType.instance, BooleanType.instance);
            valueCompatibleWith.put(BytesType.instance, ByteType.instance);
            valueCompatibleWith.put(BytesType.instance, DecimalType.instance);
            valueCompatibleWith.put(BytesType.instance, DoubleType.instance);
            valueCompatibleWith.put(BytesType.instance, DurationType.instance);
            valueCompatibleWith.put(BytesType.instance, EmptyType.instance);
            valueCompatibleWith.put(BytesType.instance, FloatType.instance);
            valueCompatibleWith.put(BytesType.instance, InetAddressType.instance);
            valueCompatibleWith.put(BytesType.instance, Int32Type.instance);
            valueCompatibleWith.put(BytesType.instance, IntegerType.instance);
            valueCompatibleWith.put(BytesType.instance, LexicalUUIDType.instance);
            valueCompatibleWith.put(BytesType.instance, LongType.instance);
            valueCompatibleWith.put(BytesType.instance, ShortType.instance);
            valueCompatibleWith.put(BytesType.instance, SimpleDateType.instance);
            valueCompatibleWith.put(BytesType.instance, TimeType.instance);
            valueCompatibleWith.put(BytesType.instance, TimeUUIDType.instance);
            valueCompatibleWith.put(BytesType.instance, TimestampType.instance);
            valueCompatibleWith.put(BytesType.instance, UTF8Type.instance);
            valueCompatibleWith.put(BytesType.instance, UUIDType.instance);
            valueCompatibleWith.put(IntegerType.instance, Int32Type.instance);
            valueCompatibleWith.put(IntegerType.instance, LongType.instance);
            valueCompatibleWith.put(IntegerType.instance, TimestampType.instance);
            valueCompatibleWith.put(LongType.instance, TimestampType.instance);
            valueCompatibleWith.put(SimpleDateType.instance, Int32Type.instance);
            valueCompatibleWith.put(TimeType.instance, LongType.instance);
            valueCompatibleWith.put(TimestampType.instance, LongType.instance);
            valueCompatibleWith.put(UTF8Type.instance, AsciiType.instance);
            valueCompatibleWith.put(UUIDType.instance, TimeUUIDType.instance);

            serializationCompatibleWith.put(BytesType.instance, AsciiType.instance);
            serializationCompatibleWith.put(BytesType.instance, ByteType.instance);
            serializationCompatibleWith.put(BytesType.instance, DecimalType.instance);
            serializationCompatibleWith.put(BytesType.instance, DurationType.instance);
            serializationCompatibleWith.put(BytesType.instance, InetAddressType.instance);
            serializationCompatibleWith.put(BytesType.instance, IntegerType.instance);
            serializationCompatibleWith.put(BytesType.instance, ShortType.instance);
            serializationCompatibleWith.put(BytesType.instance, SimpleDateType.instance);
            serializationCompatibleWith.put(BytesType.instance, TimeType.instance);
            serializationCompatibleWith.put(BytesType.instance, UTF8Type.instance);
            serializationCompatibleWith.put(LongType.instance, TimestampType.instance);
            serializationCompatibleWith.put(TimestampType.instance, LongType.instance);
            serializationCompatibleWith.put(UTF8Type.instance, AsciiType.instance);
            serializationCompatibleWith.put(UUIDType.instance, TimeUUIDType.instance);

            compatibleWith.put(BytesType.instance, AsciiType.instance);
            compatibleWith.put(BytesType.instance, UTF8Type.instance);
            compatibleWith.put(UTF8Type.instance, AsciiType.instance);

            for (AbstractType<?> t : PRIMITIVE_TYPES)
            {
                valueCompatibleWith.put(t, t);
                serializationCompatibleWith.put(t, t);
                compatibleWith.put(t, t);
            }

            multiCellSupportingTypes.add(MapType.class);
            multiCellSupportingTypes.add(SetType.class);
            multiCellSupportingTypes.add(ListType.class);

            multiCellSupportingTypesForReading.addAll(multiCellSupportingTypes);
            multiCellSupportingTypesForReading.add(UserType.class); // Cassandra 3.11 was able to write multi-cel user types?
        }

        @Override
        boolean isMapCompatibleWith(MapType left, MapType right)
        {
            if (left.isMultiCell() && right.isMultiCell())
                // multicell
                return left.getKeysType().isCompatibleWith(right.getKeysType()) && left.getValuesType().isSerializationCompatibleWith(right.getValuesType());
            else if (!left.isMultiCell() && !right.isMultiCell())
                // frozen
                return left.getKeysType().isCompatibleWith(right.getKeysType()) && left.getValuesType().isCompatibleWith(right.getValuesType());
            else
                return false;
        }

        @Override
        boolean isMapValueCompatibleWith(MapType left, MapType right)
        {
            if (left.isMultiCell() && right.isMultiCell())
                return left.getKeysType().isCompatibleWith(right.getKeysType()) && left.getValuesType().isSerializationCompatibleWith(right.getValuesType());
            else if (!left.isMultiCell() && !right.isMultiCell())
                return left.getKeysType().isCompatibleWith(right.getKeysType()) && left.getValuesType().isValueCompatibleWith(right.getValuesType());
            else
                return false;
        }

        @Override
        boolean isMapSerializationCompatibleWith(MapType left, MapType right)
        {
            if (left.isMultiCell() && right.isMultiCell())
                return left.getKeysType().isCompatibleWith(right.getKeysType()) && left.getValuesType().isValueCompatibleWith(right.getValuesType()) && left.getValuesType().isSerializationCompatibleWith(right.getValuesType());
            else if (!left.isMultiCell() && !right.isMultiCell())
                return left.getKeysType().isCompatibleWith(right.getKeysType()) && left.getValuesType().isValueCompatibleWith(right.getValuesType()) && left.getValuesType().isSerializationCompatibleWith(right.getValuesType());
            else
                return false;
        }

        @Override
        boolean isSetCompatibleWith(SetType left, SetType right)
        {
            if (left.isMultiCell() && right.isMultiCell())
                // multicell
                return left.getElementsType().isCompatibleWith(right.getElementsType());
            else if (!left.isMultiCell() && !right.isMultiCell())
                // frozen
                return left.getElementsType().isCompatibleWith(right.getElementsType());
            else
                return false;
        }

        @Override
        boolean isSetValueCompatibleWith(SetType left, SetType right)
        {
            if (left.isMultiCell() && right.isMultiCell())
                return left.getElementsType().isCompatibleWith(right.getElementsType());
            else if (!left.isMultiCell() && !right.isMultiCell())
                return left.getElementsType().isCompatibleWith(right.getElementsType());
            else
                return false;
        }

        @Override
        boolean isSetSerializationCompatibleWith(SetType left, SetType right)
        {
            if (left.isMultiCell() && right.isMultiCell())
                return left.getElementsType().isCompatibleWith(right.getElementsType());
            else if (!left.isMultiCell() && !right.isMultiCell())
                return left.getElementsType().isCompatibleWith(right.getElementsType());
            else
                return false;
        }

        @Override
        boolean isListCompatibleWith(ListType left, ListType right)
        {
            if (left.isMultiCell() && right.isMultiCell())
                // multicell
                return left.getElementsType().isSerializationCompatibleWith(right.getElementsType());
            else if (!left.isMultiCell() && !right.isMultiCell())
                // frozen
                return left.getElementsType().isCompatibleWith(right.getElementsType());
            else
                return false;
        }

        @Override
        boolean isListValueCompatibleWith(ListType left, ListType right)
        {
            if (left.isMultiCell() && right.isMultiCell())
                return left.getElementsType().isSerializationCompatibleWith(right.getElementsType());
            else if (!left.isMultiCell() && !right.isMultiCell())
                return left.getElementsType().isValueCompatibleWith(right.getElementsType());
            else
                return false;
        }

        @Override
        boolean isListSerializationCompatibleWith(ListType left, ListType right)
        {
            if (left.isMultiCell() && right.isMultiCell())
                return left.getElementsType().isValueCompatibleWith(right.getElementsType()) && left.getElementsType().isSerializationCompatibleWith(right.getElementsType());
            else if (!left.isMultiCell() && !right.isMultiCell())
                return left.getElementsType().isValueCompatibleWith(right.getElementsType()) && left.getElementsType().isSerializationCompatibleWith(right.getElementsType());
            else
                return false;
        }

        boolean isSubTypesCompatible(AbstractType<?> left, AbstractType<?> right, BiPredicate<AbstractType<?>, AbstractType<?>> subTypePredicate)
        {
            if (left.subTypes().size() < right.subTypes().size())
                return false;

            List<AbstractType<?>> leftSubTypes;
            List<AbstractType<?>> rightSubTypes;
            if (left.getClass() == DynamicCompositeType.class)
            {
                leftSubTypes = Arrays.asList(new AbstractType<?>[256]);
                ((DynamicCompositeType) left).aliases.forEach((k, v) -> leftSubTypes.set(k + 128, v));
            }
            else
                leftSubTypes = left.subTypes();

            if (right.getClass() == DynamicCompositeType.class)
            {
                rightSubTypes = Arrays.asList(new AbstractType<?>[256]);
                ((DynamicCompositeType) right).aliases.forEach((k, v) -> rightSubTypes.set(k + 128, v));
            }
            else
                rightSubTypes = right.subTypes();

            return Streams.zip(leftSubTypes.subList(0, rightSubTypes.size()).stream(),
                               rightSubTypes.stream(),
                               subTypePredicate::test).allMatch(b -> b);
        }

        @Override
        boolean isTupleCompatibleWith(TupleType left, TupleType right)
        {
            return isSubTypesCompatible(left, right, AbstractType::isCompatibleWith);
        }

        @Override
        boolean isTupleValueCompatibleWith(TupleType left, TupleType right)
        {
            if (left.isUDT())
            {
                if (!right.isUDT())
                    return false;

                if (left.isMultiCell() != right.isMultiCell())
                    return false;

                return isSubTypesCompatible(left, right, AbstractType::isCompatibleWith);
            }
            else
            {
                return isSubTypesCompatible(left, right, AbstractType::isValueCompatibleWith);
            }
        }

        @Override
        <L extends TupleType, R extends TupleType> boolean isTupleSerializationCompatibleWith(L left, R right)
        {
            if (left.isMultiCell() != right.isMultiCell())
                return false;

            if (left.valueLengthIfFixed() != right.valueLengthIfFixed())
                return false;

            if (left.isUDT())
            {
                if (!right.isUDT())
                    return false;

                return isSubTypesCompatible(left, right, AbstractType::isCompatibleWith);
            }
            else
            {
                return isSubTypesCompatible(left, right, AbstractType::isValueCompatibleWith);
            }
        }

        @Override
        boolean isCompositeCompatibleWith(AbstractCompositeType left, AbstractCompositeType right)
        {
            if (left.getClass() != right.getClass())
                return false;

            if (left.isMultiCell() != right.isMultiCell())
                return false;

            if (left.getClass() == DynamicCompositeType.class)
            {
                return isSubTypesCompatible(left, right, (l, r) -> r == null || l == r);
            }
            else
            {
                return isSubTypesCompatible(left, right, (l, r) -> r == null || l != null && l.isCompatibleWith(r));
            }
        }

        @Override
        boolean isCompositeValueCompatibleWith(AbstractCompositeType left, AbstractCompositeType right)
        {
            if (left.getClass() != right.getClass())
                return false;

            if (left.isMultiCell() != right.isMultiCell())
                return false;

            if (left.getClass() == DynamicCompositeType.class)
            {
                return isSubTypesCompatible(left, right, (l, r) -> r == null || l == r);
            }
            else
            {
                return isSubTypesCompatible(left, right, (l, r) -> r == null || l != null && l.isValueCompatibleWith(r));
            }
        }

        @Override
        boolean isCompositeSerializationCompatibleWith(AbstractCompositeType left, AbstractCompositeType right)
        {
            if (left.getClass() != right.getClass())
                return false;

            if (left.isMultiCell() != right.isMultiCell())
                return false;

            if (left.valueLengthIfFixed() != right.valueLengthIfFixed())
                return false;

            if (left.getClass() == DynamicCompositeType.class)
            {
                return isSubTypesCompatible(left, right, (l, r) -> r == null || l == r);
            }
            else
            {
                return isSubTypesCompatible(left, right, (l, r) -> r == null || l != null && l.isValueCompatibleWith(r));
            }
        }
    }

    private static class Cassandra50TypesCompatibility extends Cassandra50BetaTypesCompatibility
    {
        public final static Cassandra50TypesCompatibility instance = new Cassandra50TypesCompatibility(Cassandra50BetaTypesCompatibility.instance);

        private Cassandra50TypesCompatibility(TypesCompatibility prev)
        {
            super(prev);
            Multimap<AbstractType<?>, AbstractType<?>> valueCompatibleWith = Multimaps.newSetMultimap(new HashMap<>(), Sets::newHashSet);
            valueCompatibleWith.putAll(this.valueCompatibleWith);
            // extra compatibility that we have for free
            valueCompatibleWith.put(Int32Type.instance, SimpleDateType.instance);
            valueCompatibleWith.put(IntegerType.instance, ByteType.instance);
            valueCompatibleWith.put(IntegerType.instance, ShortType.instance);
            valueCompatibleWith.put(IntegerType.instance, SimpleDateType.instance);
            valueCompatibleWith.put(IntegerType.instance, TimeType.instance);
            valueCompatibleWith.put(LongType.instance, TimeType.instance);
            buildTransitiveClosure(valueCompatibleWith, this.valueCompatibleWith, PRIMITIVE_TYPES);

            Multimap<AbstractType<?>, AbstractType<?>> serializationCompatibleWith = Multimaps.newSetMultimap(new HashMap<>(), Sets::newHashSet);
            serializationCompatibleWith.putAll(this.serializationCompatibleWith);
            // extra compatibility that we have for free
            serializationCompatibleWith.put(SimpleDateType.instance, Int32Type.instance);
            serializationCompatibleWith.put(TimeType.instance, LongType.instance);
            buildTransitiveClosure(serializationCompatibleWith, this.serializationCompatibleWith, PRIMITIVE_TYPES);

            Multimap<AbstractType<?>, AbstractType<?>> compatibleWith = Multimaps.newSetMultimap(new HashMap<>(), Sets::newHashSet);
            compatibleWith.putAll(this.compatibleWith);
            buildTransitiveClosure(compatibleWith, this.compatibleWith, PRIMITIVE_TYPES);
        }
    }
}