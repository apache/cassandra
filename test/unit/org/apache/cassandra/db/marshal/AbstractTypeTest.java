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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.CQLTypeParser;
import org.apache.cassandra.schema.ColumnMetadata;
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
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ConfigurationBuilder;

import static org.apache.cassandra.utils.AbstractTypeGenerators.TypeKind.*;
import static org.apache.cassandra.utils.AbstractTypeGenerators.TypeSupport.of;
import static org.apache.cassandra.utils.AbstractTypeGenerators.extractUDTs;
import static org.apache.cassandra.utils.AbstractTypeGenerators.overridePrimitiveTypeSupport;
import static org.apache.cassandra.utils.AbstractTypeGenerators.stringComparator;
import static org.apache.cassandra.utils.AbstractTypeGenerators.typeTree;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.doubles;
import static org.quicktheories.generators.SourceDSL.floats;

public class AbstractTypeTest
{
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

    // TODO
    // isCompatibleWith/isValueCompatibleWith/isSerializationCompatibleWith,
    // withUpdatedUserType/expandUserTypes/referencesDuration - types that recursive check types

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
        assertThat(ByteBufferUtil.bytesToHex(actual)).describedAs(msg, args).isEqualTo(ByteBufferUtil.bytesToHex(expected));
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
            AbstractTypeGenerators.TypeSupport<?> support = AbstractTypeGenerators.getTypeSupport(type);
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
}