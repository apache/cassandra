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
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.google.common.base.Charsets;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import net.openhft.chronicle.core.util.ThrowingFunction;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
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
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.Generators;
import org.apache.cassandra.utils.asserts.SoftAssertionsWithLimit;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.description.Description;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ConfigurationBuilder;

import static org.apache.cassandra.db.marshal.AbstractType.ComparisonType.CUSTOM;
import static org.apache.cassandra.utils.AbstractTypeGenerators.TypeKind.COMPOSITE;
import static org.apache.cassandra.utils.AbstractTypeGenerators.TypeKind.COUNTER;
import static org.apache.cassandra.utils.AbstractTypeGenerators.TypeKind.DYNAMIC_COMPOSITE;
import static org.apache.cassandra.utils.AbstractTypeGenerators.TypeKind.PRIMITIVE;
import static org.apache.cassandra.utils.AbstractTypeGenerators.TypeKind.UDT;
import static org.apache.cassandra.utils.AbstractTypeGenerators.TypeSupport.of;
import static org.apache.cassandra.utils.AbstractTypeGenerators.UNSUPPORTED;
import static org.apache.cassandra.utils.AbstractTypeGenerators.allowsEmpty;
import static org.apache.cassandra.utils.AbstractTypeGenerators.extractUDTs;
import static org.apache.cassandra.utils.AbstractTypeGenerators.forEachPrimitiveTypePair;
import static org.apache.cassandra.utils.AbstractTypeGenerators.forEachTypesPair;
import static org.apache.cassandra.utils.AbstractTypeGenerators.getTypeSupport;
import static org.apache.cassandra.utils.AbstractTypeGenerators.overridePrimitiveTypeSupport;
import static org.apache.cassandra.utils.AbstractTypeGenerators.stringComparator;
import static org.apache.cassandra.utils.AbstractTypeGenerators.typeTree;
import static org.apache.cassandra.utils.AbstractTypeGenerators.unfreeze;
import static org.apache.cassandra.utils.AbstractTypeGenerators.unwrap;
import static org.apache.cassandra.utils.ByteBufferUtil.bytesToHex;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.doubles;
import static org.quicktheories.generators.SourceDSL.floats;

@SuppressWarnings({ "unchecked", "rawtypes" })
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

    // TODO
    // withUpdatedUserType/expandUserTypes/referencesDuration - types that recursive check types

    private static TypesCompatibility currentTypesCompatibility;
    private static LoadedTypesCompatibility cassandra40TypesCompatibility;
    private static LoadedTypesCompatibility cassandra41TypesCompatibility;
    private static LoadedTypesCompatibility cassandra50TypesCompatibility;

    private final static String CASSANDRA_VERSION = new CassandraVersion(FBUtilities.getReleaseVersionString()).toMajorMinorString();
    private final static Path BASE_OUTPUT_PATH = Paths.get("test", "data", "types-compatibility");

    @BeforeClass
    public static void beforeClass() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        cassandra40TypesCompatibility = new LoadedTypesCompatibility(compatibilityFile(CassandraVersion.CASSANDRA_4_0.toMajorMinorString()), Set.of());
        cassandra41TypesCompatibility = new LoadedTypesCompatibility(compatibilityFile(CassandraVersion.CASSANDRA_4_1.toMajorMinorString()), Set.of());
        cassandra50TypesCompatibility = new LoadedTypesCompatibility(compatibilityFile(CassandraVersion.CASSANDRA_5_0.toMajorMinorString()), Set.of());
        currentTypesCompatibility = new CurrentTypesCompatibility();
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

    @Test
    public void testAssumedCompatibility()
    {
        SoftAssertions assertions = new SoftAssertionsWithLimit(100);
        forEachPrimitiveTypePair((l, r) -> currentTypesCompatibility.checkExpectedTypeCompatibility(l, r, assertions));
        assertions.assertAll();
    }

    @Test
    public void testBackwardCompatibility()
    {
        cassandra40TypesCompatibility.assertLoaded();
        testBackwardCompatibility(currentTypesCompatibility, cassandra40TypesCompatibility);

        cassandra41TypesCompatibility.assertLoaded();
        testBackwardCompatibility(currentTypesCompatibility, cassandra41TypesCompatibility);

        cassandra50TypesCompatibility.assertLoaded();
        testBackwardCompatibility(currentTypesCompatibility, cassandra50TypesCompatibility);
    }

    public void testBackwardCompatibility(TypesCompatibility upgradeTo, TypesCompatibility upgradeFrom)
    {
        SoftAssertions assertions = new SoftAssertionsWithLimit(100);

        assertions.assertThat(upgradeTo.knownTypes()).containsAll(upgradeFrom.knownTypes());
        assertions.assertThat(upgradeTo.primitiveTypes()).containsAll(upgradeFrom.primitiveTypes());

        // for compatibility, we ensure that this version can read values of all the types the previous version can write
        assertions.assertThat(upgradeTo.multiCellSupportingTypesForReading()).containsAll(upgradeFrom.multiCellSupportingTypes());

        forEachTypesPair(true, (l, r) -> {
            if (upgradeFrom.expectCompatibleWith(l, r))
                assertions.assertThat(upgradeTo.expectCompatibleWith(l, r)).describedAs(isCompatibleWithDesc(l, r)).isTrue();
            if (upgradeFrom.expectSerializationCompatibleWith(l, r))
                assertions.assertThat(upgradeTo.expectSerializationCompatibleWith(l, r)).describedAs(isSerializationCompatibleWithDesc(l, r)).isTrue();
            if (upgradeFrom.expectValueCompatibleWith(l, r))
                assertions.assertThat(upgradeTo.expectValueCompatibleWith(l, r)).describedAs(isValueCompatibleWithDesc(l, r)).isTrue();
        });

        assertions.assertAll();
    }

    @Test
    public void testImplementedCompatibility()
    {
        SoftAssertions assertions = new SoftAssertionsWithLimit(100);

        forEachTypesPair(true, (l, r) -> {
            assertions.assertThat(l.equals(r)).describedAs("equals symmetricity for %s and %s", l, r).isEqualTo(r.equals(l));
            verifyTypesCompatibility(l, r, getTypeSupport(r).valueGen, assertions);
        });

        assertions.assertAll();
    }

    private static Path compatibilityFile(String version)
    {
        return BASE_OUTPUT_PATH.resolve(String.format("%s.json.gz", version));
    }

    @Test
    @Ignore
    public void testStoreAllCompatibleTypePairs() throws IOException
    {
        currentTypesCompatibility.store(compatibilityFile(CASSANDRA_VERSION));
    }

    private static void verifyTypesCompatibility(AbstractType left, AbstractType right, Gen rightGen, SoftAssertions assertions)
    {
        if (left.equals(right))
            return;

        verifyTypeSerializers(left, right, assertions);
        if (!left.isValueCompatibleWith(right))
            return;

        ColumnMetadata rightColumn1 = new ColumnMetadata("k", "t", ColumnIdentifier.getInterned("c", false), right, ColumnMetadata.NO_POSITION, ColumnMetadata.Kind.REGULAR, null);
        ColumnMetadata rightColumn2 = new ColumnMetadata("k", "t", ColumnIdentifier.getInterned("d", false), right, ColumnMetadata.NO_POSITION, ColumnMetadata.Kind.REGULAR, null);
        ColumnMetadata leftColumn1 = new ColumnMetadata("k", "t", ColumnIdentifier.getInterned("c", false), left, ColumnMetadata.NO_POSITION, ColumnMetadata.Kind.REGULAR, null);
        ColumnMetadata leftColumn2 = new ColumnMetadata("k", "t", ColumnIdentifier.getInterned("d", false), left, ColumnMetadata.NO_POSITION, ColumnMetadata.Kind.REGULAR, null);

        TableMetadata leftTable = TableMetadata.builder("k", "t").addPartitionKeyColumn("pk", EmptyType.instance).addColumn(leftColumn1).addColumn(leftColumn2).build();
        TableMetadata rightTable = TableMetadata.builder("k", "t").addPartitionKeyColumn("pk", EmptyType.instance).addColumn(rightColumn1).addColumn(rightColumn2).build();

        SerializationHeader leftHeader = new SerializationHeader(false, leftTable, leftTable.regularAndStaticColumns(), EncodingStats.NO_STATS);
        SerializationHeader rightHeader = new SerializationHeader(false, rightTable, rightTable.regularAndStaticColumns(), EncodingStats.NO_STATS);

        DeserializationHelper leftHelper = new DeserializationHelper(leftTable, MessagingService.current_version, DeserializationHelper.Flag.LOCAL, ColumnFilter.all(leftTable));
        SerializationHelper rightHelper = new SerializationHelper(rightHeader);

        assertions.assertThatCode(() -> {
            qt().withExamples(10).forAll(rightGen).checkAssert(v -> {
                // value compatibility means that we can use left's type serializer to decompose a value of right's type
                ByteBuffer rightDecomposed = right.decompose(v);
                Object leftComposed = left.compose(rightDecomposed);
                ByteBuffer leftDecomposed = left.decompose(leftComposed);
                assertThat(leftDecomposed.hasRemaining()).describedAs(typeRelDesc(".decompose", left, right)).isEqualTo(rightDecomposed.hasRemaining());

                // serialization compatibility means that we can read a cell written using right's type serializer with left's type serializer;
                // this additinoally imposes the requirement for storing the buffer lenght in the serialized form if the value is of variable length
                // as well as, either both types serialize into a single or multiple cells
                if (left.isSerializationCompatibleWith(right))
                {
                    if (!left.isMultiCell() && !right.isMultiCell())
                        verifySerializationCompatibilityForSimpleCells(left, right, v, rightTable, rightColumn1, rightHelper, leftHeader, leftHelper, leftColumn1);
                    else if (currentTypesCompatibility.multiCellSupportingTypes().contains(left.getClass()) && currentTypesCompatibility.multiCellSupportingTypes().contains(right.getClass()))
                        verifySerializationCompatibilityForComplexCells(left, right, v, rightTable, rightColumn1, rightHelper, leftHeader, leftHelper, leftColumn1);
                }
            });
        }).describedAs(typeRelDesc("isSerializationCompatibleWith", left, right)).doesNotThrowAnyException();

        // if types are not (comparison) compatible, no reason to verify that
        if (!left.isCompatibleWith(right) || right.comparisonType == AbstractType.ComparisonType.NOT_COMPARABLE || left.comparisonType == AbstractType.ComparisonType.NOT_COMPARABLE)
            return;

        // types compatibility means that we can compare values of right's type using left's type comparator additionally
        // to types being serialization compatible
        if (!left.isMultiCell() && !right.isMultiCell())
        {
            // make sure that frozen<left> isCompatibleWith frozen<right> ==> left isCompatibleWith right
            assertions.assertThat(unfreeze(left).isCompatibleWith(unfreeze(right))).isTrue();

            assertions.assertThatCode(() -> qt().withExamples(10)
                                                .forAll(rightGen, rightGen)
                                                .checkAssert((rightValue1, rightValue2) -> verifyComparisonCompatibilityForSimpleCells(left, right, rightValue1, rightValue2)))
                      .describedAs(typeRelDesc("isCompatibleWith", left, right)).doesNotThrowAnyException();
        }
        else if (left.isMultiCell() && right.isMultiCell())
        {
            if (currentTypesCompatibility.multiCellSupportingTypes().contains(left.getClass()) && currentTypesCompatibility.multiCellSupportingTypes().contains(right.getClass()))
            {
                assertions.assertThatCode(() -> qt().withExamples(10)
                                                    .forAll(rightGen, rightGen)
                                                    .checkAssert((rightValue1, rightValue2) -> verifyComparisonCompatibilityForMultiCell(left, right, rightValue1, rightValue2, rightTable, rightColumn1, rightColumn2, rightHelper, leftHeader, leftHelper, leftColumn1, leftColumn2)))
                          .describedAs(typeRelDesc("isCompatibleWith", left, right)).doesNotThrowAnyException();
            }
        }
    }

    /**
     * Assert that (comparison) incompatible types which use custom comparison are not using the same serializer.
     */
    private static void verifyTypeSerializers(AbstractType l, AbstractType r, SoftAssertions assertions)
    {
        AbstractType lt = unfreeze(unwrap(l));
        AbstractType rt = unfreeze(unwrap(r));

        if (lt.comparisonType != CUSTOM && rt.comparisonType != CUSTOM)
            return;

        if (lt.isCompatibleWith(rt) && rt.isCompatibleWith(lt))
            return;

        assertions.assertThat(l.getSerializer()).describedAs(typeRelDesc("should have different serializer to", l, r)).isNotEqualTo(r.getSerializer());
    }
    private static int sign(int value)
    {
        return Integer.compare(value, 0);
    }
    
    private static <T> void verifyComparison(Comparator<T> leftComparator, Comparator<T> rightComparator, T lv1, T lv2, T rv1, T rv2, int expectedResult, Function<String, Description> desc)
    {
        SoftAssertions checks = new SoftAssertions();

        expectedResult = sign(expectedResult);

        // first just check that the comparison is antisymmetric
        checks.assertThat(sign(rightComparator.compare(rv2, rv1))).describedAs(desc.apply("Using R for inverse comparison of R values")).isEqualTo(-expectedResult);

        // then, check if we can compare buffers using left's comparator
        checks.assertThat(sign(leftComparator.compare(lv1, lv2))).describedAs(desc.apply("Using L for comparison of L values")).isEqualTo(expectedResult);
        checks.assertThat(sign(leftComparator.compare(lv1, rv2))).describedAs(desc.apply("Using L for comparison of L and R values")).isEqualTo(expectedResult);
        checks.assertThat(sign(leftComparator.compare(rv1, lv2))).describedAs(desc.apply("Using L for comparison of R and L values")).isEqualTo(expectedResult);
        checks.assertThat(sign(leftComparator.compare(rv1, rv2))).describedAs(desc.apply("Using L for comparison of R values")).isEqualTo(expectedResult);

        checks.assertThat(sign(leftComparator.compare(lv2, lv1))).describedAs(desc.apply("Using L for inverse comparison of L values")).isEqualTo(-expectedResult);
        checks.assertThat(sign(leftComparator.compare(lv2, rv1))).describedAs(desc.apply("Using L for inverse comparison of L and R values")).isEqualTo(-expectedResult);
        checks.assertThat(sign(leftComparator.compare(rv2, lv1))).describedAs(desc.apply("Using L for inverse comparison of R and L values")).isEqualTo(-expectedResult);
        checks.assertThat(sign(leftComparator.compare(rv2, rv1))).describedAs(desc.apply("Using L for inverse comparison of R values")).isEqualTo(-expectedResult);

        checks.assertAll();
    }

    private static void verifyComparisonCompatibilityForSimpleCells(AbstractType left, AbstractType right, Object r1, Object r2)
    {
        Function<String, Description> desc = s -> typeRelDesc(".compare", left, right, String.format("%s: '%s' and '%s'", s, r1, r2));

        ByteBuffer rBuf1 = right.decompose(r1);
        ByteBuffer rBuf2 = right.decompose(r2);
        ByteBuffer lBuf1 = left.decompose(left.compose(rBuf1));
        ByteBuffer lBuf2 = left.decompose(left.compose(rBuf2));

        int c = right.compare(rBuf1, rBuf2);
        verifyComparison(left, right, lBuf1, lBuf2, rBuf1, rBuf2, c, desc);
    }

    private static void verifyComparisonCompatibilityForMultiCell(AbstractType left, AbstractType right, Object r1, Object r2,
                                                                  TableMetadata rightTable, ColumnMetadata rightColumn1, ColumnMetadata rightColumn2, SerializationHelper rightHelper,
                                                                  SerializationHeader leftHeader, DeserializationHelper leftHelper, ColumnMetadata leftColumn1, ColumnMetadata leftColumn2)
    {
        Function<String, Description> desc = s -> typeRelDesc(".compare", left, right, String.format("%s: %s and %s", s, r1, r2));

        Row rightRow = Rows.simpleBuilder(rightTable)
                           .noPrimaryKeyLivenessInfo()
                           .add(rightColumn1.name.toString(), r1)
                           .add(rightColumn2.name.toString(), r2)
                           .build();

        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            UnfilteredSerializer.serializer.serialize(rightRow, rightHelper, out, MessagingService.current_version);
            try (DataInputBuffer in = new DataInputBuffer(out.getData()))
            {
                Row.Builder builder = BTreeRow.sortedBuilder();
                builder.addPrimaryKeyLivenessInfo(rightRow.primaryKeyLivenessInfo());
                Row leftRow = (Row) UnfilteredSerializer.serializer.deserialize(in, leftHeader, leftHelper, builder);
                ComplexColumnData leftData1 = leftRow.getComplexColumnData(leftColumn1);
                ComplexColumnData leftData2 = leftRow.getComplexColumnData(leftColumn2);
                ComplexColumnData rightData1 = rightRow.getComplexColumnData(rightColumn1);
                ComplexColumnData rightData2 = rightRow.getComplexColumnData(rightColumn2);

                for (int i = 0; i < Math.min(leftData1.cellsCount(), leftData2.cellsCount()); i++)
                {
                    CellPath lp1 = leftData1.getCellByIndex(i).path();
                    CellPath lp2 = leftData2.getCellByIndex(i).path();
                    CellPath rp1 = rightData1.getCellByIndex(i).path();
                    CellPath rp2 = rightData2.getCellByIndex(i).path();

                    int c = rightColumn1.cellPathComparator().compare(rp1, rp2);
                    verifyComparison(leftColumn1.cellPathComparator(), rightColumn1.cellPathComparator(), lp1, lp2, rp1, rp2, c, desc);
                }
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void verifySerializationCompatibilityForSimpleCells(AbstractType left, AbstractType right, Object v,
                                                                       TableMetadata rightTable, ColumnMetadata rightColumn, SerializationHelper rightHelper,
                                                                       SerializationHeader leftHeader, DeserializationHelper leftHelper, ColumnMetadata leftColumn)
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
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void verifySerializationCompatibilityForComplexCells(AbstractType left, AbstractType right, Object v,
                                                                        TableMetadata rightTable, ColumnMetadata rightColumn, SerializationHelper rightHelper,
                                                                        SerializationHeader leftHeader, DeserializationHelper leftHelper, ColumnMetadata leftColumn)
    {
        SoftAssertions checks = new SoftAssertions();
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
                checks.assertThat(leftData.cellsCount()).describedAs(typeRelDesc(".cellsCountIsEqualTo", left, right)).isEqualTo(rightData.cellsCount());
                for (int i = 0; i < leftData.cellsCount(); i++)
                {
                    Cell leftCell = leftData.getCellByIndex(i);
                    Cell rightCell = rightData.getCellByIndex(i);
                    checks.assertThat(leftCell.buffer()).describedAs(bytesToHex(leftCell.buffer())).isEqualTo(rightCell.buffer()).describedAs(bytesToHex(rightCell.buffer()));
                    checks.assertThat(leftCell.path().size()).describedAs(typeRelDesc(".cellPathSizeIsEqualTo", left, right)).isEqualTo(rightCell.path().size());
                    for (int j = 0; j < leftCell.path().size(); j++)
                        checks.assertThat(leftCell.path().get(j)).describedAs(bytesToHex(leftCell.path().get(j))).isEqualTo(rightCell.path().get(j)).describedAs(bytesToHex(rightCell.path().get(j)));
                }
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        checks.assertAll();
    }

    @Test
    public void testMultiCellSupport()
    {
        SoftAssertions assertions = new SoftAssertions();

        Set<Class<? extends AbstractType>> multiCellSupportingTypesForReading = new HashSet<>();
        Set<Class<? extends AbstractType>> multiCellSupportingTypes = new HashSet<>();

        forEachTypesPair(true, (l, r) -> {
            if (l.equals(r))
            {
                if (l.isMultiCell())
                {
                    // types which can be created as multicell
                    multiCellSupportingTypes.add(l.getClass());

                    AbstractType frozen = l.freeze();
                    assertThat(frozen.isMultiCell()).isFalse();
                    assertions.assertThat(l).isNotEqualTo(frozen);
                }
                else
                {
                    // some complex types cannot be created as multicell, but can be parsed as multicell for backward
                    // compatibility; here we want to collect such types
                    AbstractType<?> t = TypeParser.parse(l.toString(true));
                    if (t.isMultiCell())
                    {
                        multiCellSupportingTypesForReading.add(l.getClass());

                        assertions.assertThat(t).isNotEqualTo(l);
                        assertions.assertThat(t.freeze()).isNotEqualTo(t);
                        assertions.assertThat(t.freeze()).isEqualTo(l);
                    }
                    else
                    {
                        assertions.assertThat(l.freeze()).isSameAs(l);
                        assertions.assertThat(unfreeze(l)).isSameAs(l);
                        assertions.assertThat(unfreeze(l)).isEqualTo(l.unfreeze());
                    }
                }

                assertions.assertThat(l.allowsEmpty()).isEqualTo(allowsEmpty(l));
            }
        });

        assertions.assertThat(multiCellSupportingTypes).isEqualTo(currentTypesCompatibility.multiCellSupportingTypes());
        assertions.assertThat(multiCellSupportingTypesForReading).isEqualTo(currentTypesCompatibility.multiCellSupportingTypesForReading());

        // all primitive types should be freezing agnostic
        currentTypesCompatibility.primitiveTypes().forEach(type -> {
            assertThat(type.freeze()).isSameAs(type);
            assertThat(unfreeze(type)).isSameAs(type);
        });
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
        protected static final String KNOWN_TYPES_KEY = "known_types";
        protected static final String MULTICELL_TYPES_KEY = "multicell_types";
        protected static final String MULTICELL_TYPES_FOR_READING_KEY = "multicell_types_for_reading";
        protected static final String PRIMITIVE_TYPES_KEY = "primitive_types";
        protected static final String COMPATIBLE_TYPES_KEY = "compatible_types";
        protected static final String SERIALIZATION_COMPATIBLE_TYPES_KEY = "serialization_compatible_types";
        protected static final String VALUE_COMPATIBLE_TYPES_KEY = "value_compatible_types";
        protected static final String UNSUPPORTED_TYPES_KEY = "unsupported_types";

        public final String name;

        public TypesCompatibility(String name)
        {
            this.name = name;
        }

        public <T extends AbstractType> void checkExpectedTypeCompatibility(T left, T right, SoftAssertions assertions)
        {
            assertions.assertThat(left.isCompatibleWith(right)).as(isCompatibleWithDesc(left, right)).isEqualTo(expectCompatibleWith(left, right));
            assertions.assertThat(left.isSerializationCompatibleWith(right)).as(isSerializationCompatibleWithDesc(left, right)).isEqualTo(expectSerializationCompatibleWith(left, right));
            assertions.assertThat(left.isValueCompatibleWith(right)).as(isValueCompatibleWithDesc(left, right)).isEqualTo(expectValueCompatibleWith(left, right));
        }

        public abstract boolean expectCompatibleWith(AbstractType left, AbstractType right);

        public abstract boolean expectValueCompatibleWith(AbstractType left, AbstractType right);

        public abstract boolean expectSerializationCompatibleWith(AbstractType left, AbstractType right);

        public abstract Set<Class<? extends AbstractType>> knownTypes();

        public abstract Set<AbstractType<?>> primitiveTypes();

        public abstract Set<Class<? extends AbstractType>> multiCellSupportingTypes();

        public abstract Set<Class<? extends AbstractType>> multiCellSupportingTypesForReading();

        public abstract Set<Class<? extends AbstractType>> unsupportedTypes();

        public void store(Path path) throws IOException
        {
            Set<Class<? extends AbstractType>> primitiveTypeClasses = primitiveTypes().stream().map(AbstractType::getClass).collect(Collectors.toSet());
            HashSet<Class<? extends AbstractType>> knownTypes = new HashSet<>(knownTypes());
            knownTypes.removeAll(unsupportedTypes());
            Multimap<Class<?>, Class<?>> knownPairs = Multimaps.newMultimap(new HashMap<>(), HashSet::new);
            knownTypes.forEach(l -> knownTypes.forEach(r -> {
                if (l == r)
                    knownPairs.put(l, r);
                if (primitiveTypeClasses.contains(l) && primitiveTypeClasses.contains(r))
                    knownPairs.put(l, r);
            }));

            Multimap<String, String> compatibleWithMap = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);
            Multimap<String, String> serializationCompatibleWithMap = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);
            Multimap<String, String> valueCompatibleWithMap = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);

            Map<AbstractType<?>, String> typeToStringMap = new HashMap<>();
            Map<String, AbstractType<?>> stringToTypeMap = new HashMap<>();

            forEachTypesPair(true, (l, r) -> {
                knownPairs.remove(l.getClass(), r.getClass());

                if (l.equals(r))
                    return;

                AbstractType<?> l1 = TypeParser.parse(l.toString());
                AbstractType<?> r1 = TypeParser.parse(r.toString());
                assertThat(l1).isEqualTo(l);
                assertThat(r1).isEqualTo(r);

                if (l.isCompatibleWith(r))
                {
                    assertThat(l1.isCompatibleWith(r1)).isTrue();
                    compatibleWithMap.put(l.toString(), r.toString());
                }

                if (l.isSerializationCompatibleWith(r))
                {
                    assertThat(l1.isSerializationCompatibleWith(r1)).isTrue();
                    serializationCompatibleWithMap.put(l.toString(), r.toString());
                }

                if (l.isValueCompatibleWith(r))
                {
                    assertThat(l1.isValueCompatibleWith(r1)).isTrue();
                    valueCompatibleWithMap.put(l.toString(), r.toString());
                }
            });

            // make sure that all pairs were covered
            assertThat(knownPairs.entries()).isEmpty();

            assertThat(typeToStringMap).hasSameSizeAs(stringToTypeMap);

            JSONObject json = new JSONObject();
            json.put(KNOWN_TYPES_KEY, knownTypes().stream().map(Class::getName).collect(Collectors.toList()));
            json.put(MULTICELL_TYPES_KEY, multiCellSupportingTypes().stream().map(Class::getName).collect(Collectors.toList()));
            json.put(MULTICELL_TYPES_FOR_READING_KEY, multiCellSupportingTypesForReading().stream().map(Class::getName).collect(Collectors.toList()));
            json.put(COMPATIBLE_TYPES_KEY, compatibleWithMap.asMap());
            json.put(SERIALIZATION_COMPATIBLE_TYPES_KEY, serializationCompatibleWithMap.asMap());
            json.put(VALUE_COMPATIBLE_TYPES_KEY, valueCompatibleWithMap.asMap());
            json.put(UNSUPPORTED_TYPES_KEY, unsupportedTypes().stream().map(Class::getName).collect(Collectors.toList()));
            json.put(PRIMITIVE_TYPES_KEY, primitiveTypes().stream().map(AbstractType::toString).collect(Collectors.toList()));

            try (GZIPOutputStream out = new GZIPOutputStream(Files.newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)))
            {
                out.write(json.toJSONString().getBytes(Charsets.UTF_8));
            }

            logger.info("Stored types compatibility to {}: knownTypes: {}, multiCellSupportingTypes: {}, " +
                        "multiCellSupportingTypesForReading: {}, unsupportedTypes: {}, primitiveTypes: {}, " +
                        "compatibleWith: {}, serializationCompatibleWith: {}, valueCompatibleWith: {}",
                        path.getFileName(), knownTypes().size(), multiCellSupportingTypes().size(),
                        multiCellSupportingTypesForReading().size(), unsupportedTypes().size(), primitiveTypes().size(),
                        compatibleWithMap.entries().size(), serializationCompatibleWithMap.entries().size(), valueCompatibleWithMap.entries().size());
        }

        @Override
        public String toString()
        {
            return String.format("TypesCompatibility[%s]", name);
        }
    }

    private final static class LoadedTypesCompatibility extends TypesCompatibility
    {
        private final Multimap<AbstractType<?>, AbstractType<?>> valueCompatibleWith;
        private final Multimap<AbstractType<?>, AbstractType<?>> serializationCompatibleWith;
        private final Multimap<AbstractType<?>, AbstractType<?>> compatibleWith;
        private final Set<Class<? extends AbstractType>> unsupportedTypes;
        private final Set<Class<? extends AbstractType>> knownTypes;
        private final Set<Class<? extends AbstractType>> multiCellSupportingTypes;
        private final Set<Class<? extends AbstractType>> multiCellSupportingTypesForReading;
        private final Set<AbstractType<?>> primitiveTypes;
        private final SoftAssertions loadAssertions = new SoftAssertionsWithLimit(100);
        private final Set<String> excludedTypes;

        private <T> Function<Object, Stream<T>> safeParse(ThrowingFunction<String, T, Exception> consumer)
        {
            return obj -> {
                String typeName = (String) obj;
                if (refersExcludedType(typeName))
                    return Stream.empty();
                try
        {
                    return Stream.of(consumer.apply(typeName));
                }
                catch (InterruptedException ex)
                {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ex);
                }
                catch (Exception th)
                {
                    loadAssertions.fail("Failed to parse type: " + typeName, th);
                }
                return Stream.empty();
            };
        }

        private boolean refersExcludedType(String typeName)
        {
            for (String unsupportedType : excludedTypes)
                if (typeName.contains(unsupportedType))
                    return true;
            return false;
        }

        private Set<Class<? extends AbstractType>> getTypesArray(Object json)
        {
            return ((JSONArray) json).stream()
                                     .map(String.class::cast)
                                     .flatMap(safeParse(((ThrowingFunction<String, Class<? extends AbstractType>, Exception>) className -> (Class<? extends AbstractType>) Class.forName(className))))
                                     .collect(Collectors.toUnmodifiableSet());
        }

        private Multimap<AbstractType<?>, AbstractType<?>> getTypesCompatibilityMultimap(Object json)
        {
            Multimap<AbstractType<?>, AbstractType<?>> map = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);
            ((JSONObject) json).forEach((l, collection) -> safeParse(TypeParser::parse).apply(l).forEach(left -> {
                ((JSONArray) collection).forEach(r -> {
                    safeParse(TypeParser::parse).apply(r).forEach(right -> map.put(left, right));
                });
            }));
            return map;
        }

        private LoadedTypesCompatibility(Path path, Set<String> excludedTypes) throws IOException
        {
            super(path.getFileName().toString());

            this.excludedTypes = ImmutableSet.copyOf(excludedTypes);
            logger.info("Loading types compatibility from {} skipping {} as unsupported", path.toAbsolutePath(), excludedTypes);
            try (GZIPInputStream in = new GZIPInputStream(Files.newInputStream(path)))
            {
                JSONObject json = (JSONObject) new JSONParser().parse(new InputStreamReader(in, Charsets.UTF_8));
                knownTypes = getTypesArray(json.get(KNOWN_TYPES_KEY));
                multiCellSupportingTypes = getTypesArray(json.get(MULTICELL_TYPES_KEY));
                multiCellSupportingTypesForReading = getTypesArray(json.get(MULTICELL_TYPES_FOR_READING_KEY));
                unsupportedTypes = getTypesArray(json.get(UNSUPPORTED_TYPES_KEY));
                primitiveTypes = ((JSONArray) json.get(PRIMITIVE_TYPES_KEY)).stream().flatMap(safeParse(TypeParser::parse)).collect(Collectors.toSet());
                compatibleWith = getTypesCompatibilityMultimap(json.get(COMPATIBLE_TYPES_KEY));
                serializationCompatibleWith = getTypesCompatibilityMultimap(json.get(SERIALIZATION_COMPATIBLE_TYPES_KEY));
                valueCompatibleWith = getTypesCompatibilityMultimap(json.get(VALUE_COMPATIBLE_TYPES_KEY));
            }
            catch (ParseException | NoSuchFileException e)
            {
                throw new IOException(path.toAbsolutePath().toString(), e);
            }

            logger.info("Loaded types compatibility from {}: knownTypes: {}, multiCellSupportingTypes: {}, " +
                        "multiCellSupportingTypesForReading: {}, unsupportedTypes: {}, primitiveTypes: {}, " +
                        "compatibleWith: {}, serializationCompatibleWith: {}, valueCompatibleWith: {}",
                        path.getFileName(), knownTypes.size(), multiCellSupportingTypes.size(),
                        multiCellSupportingTypesForReading.size(), unsupportedTypes.size(), primitiveTypes.size(),
                        compatibleWith.size(), serializationCompatibleWith.size(), valueCompatibleWith.size());
        }

        public void assertLoaded()
        {
            loadAssertions.assertAll();
        }
        @Override
        public boolean expectCompatibleWith(AbstractType left, AbstractType right)
        {
            return compatibleWith.containsEntry(left, right);
        }

        @Override
        public boolean expectValueCompatibleWith(AbstractType left, AbstractType right)
        {
            return valueCompatibleWith.containsEntry(left, right);
        }

        @Override
        public boolean expectSerializationCompatibleWith(AbstractType left, AbstractType right)
        {
            return serializationCompatibleWith.containsEntry(left, right);
        }

        @Override
        public Set<Class<? extends AbstractType>> knownTypes()
        {
            return knownTypes;
        }

        @Override
        public Set<AbstractType<?>> primitiveTypes()
        {
            return primitiveTypes;
        }

        @Override
        public Set<Class<? extends AbstractType>> multiCellSupportingTypes()
        {
            return multiCellSupportingTypes;
        }

        @Override
        public Set<Class<? extends AbstractType>> multiCellSupportingTypesForReading()
        {
            return multiCellSupportingTypesForReading;
        }

        @Override
        public Set<Class<? extends AbstractType>> unsupportedTypes()
        {
            return unsupportedTypes;
        }
    }

    private static class CurrentTypesCompatibility extends TypesCompatibility
    {
        protected final Multimap<AbstractType<?>, AbstractType<?>> primitiveValueCompatibleWith = HashMultimap.create();
        protected final Multimap<AbstractType<?>, AbstractType<?>> primitiveSerializationCompatibleWith = HashMultimap.create();
        protected final Multimap<AbstractType<?>, AbstractType<?>> primitiveCompatibleWith = HashMultimap.create();

        protected final Set<Class<? extends AbstractType>> knownTypes = ImmutableSet.copyOf(AbstractTypeGenerators.knownTypes());
        protected final Set<AbstractType<?>> primitiveTypes = ImmutableSet.copyOf(AbstractTypeGenerators.primitiveTypes());
        protected final Set<Class<? extends AbstractType>> unsupportedTypes = ImmutableSet.<Class<? extends AbstractType>>builder().addAll(UNSUPPORTED.keySet()).add(CounterColumnType.class).build();
        protected final Set<Class<? extends AbstractType>> multiCellSupportingTypes;
        protected final Set<Class<? extends AbstractType>> multiCellSupportingTypesForReading;

        private CurrentTypesCompatibility()
        {
            super("current");
            primitiveValueCompatibleWith.put(BytesType.instance, AsciiType.instance);
            primitiveValueCompatibleWith.put(BytesType.instance, BooleanType.instance);
            primitiveValueCompatibleWith.put(BytesType.instance, ByteType.instance);
            primitiveValueCompatibleWith.put(BytesType.instance, DecimalType.instance);
            primitiveValueCompatibleWith.put(BytesType.instance, DoubleType.instance);
            primitiveValueCompatibleWith.put(BytesType.instance, DurationType.instance);
            primitiveValueCompatibleWith.put(BytesType.instance, EmptyType.instance);
            primitiveValueCompatibleWith.put(BytesType.instance, FloatType.instance);
            primitiveValueCompatibleWith.put(BytesType.instance, InetAddressType.instance);
            primitiveValueCompatibleWith.put(BytesType.instance, Int32Type.instance);
            primitiveValueCompatibleWith.put(BytesType.instance, IntegerType.instance);
            primitiveValueCompatibleWith.put(BytesType.instance, LexicalUUIDType.instance);
            primitiveValueCompatibleWith.put(BytesType.instance, LongType.instance);
            primitiveValueCompatibleWith.put(BytesType.instance, ShortType.instance);
            primitiveValueCompatibleWith.put(BytesType.instance, SimpleDateType.instance);
            primitiveValueCompatibleWith.put(BytesType.instance, TimeType.instance);
            primitiveValueCompatibleWith.put(BytesType.instance, TimeUUIDType.instance);
            primitiveValueCompatibleWith.put(BytesType.instance, TimestampType.instance);
            primitiveValueCompatibleWith.put(BytesType.instance, UTF8Type.instance);
            primitiveValueCompatibleWith.put(BytesType.instance, UUIDType.instance);
            primitiveValueCompatibleWith.put(IntegerType.instance, Int32Type.instance);
            primitiveValueCompatibleWith.put(IntegerType.instance, LongType.instance);
            primitiveValueCompatibleWith.put(IntegerType.instance, TimestampType.instance);
            primitiveValueCompatibleWith.put(LongType.instance, TimestampType.instance);
            primitiveValueCompatibleWith.put(SimpleDateType.instance, Int32Type.instance);
            primitiveValueCompatibleWith.put(TimeType.instance, LongType.instance);
            primitiveValueCompatibleWith.put(TimestampType.instance, LongType.instance);
            primitiveValueCompatibleWith.put(UTF8Type.instance, AsciiType.instance);
            primitiveValueCompatibleWith.put(UUIDType.instance, TimeUUIDType.instance);

            primitiveSerializationCompatibleWith.put(BytesType.instance, AsciiType.instance);
            primitiveSerializationCompatibleWith.put(BytesType.instance, ByteType.instance);
            primitiveSerializationCompatibleWith.put(BytesType.instance, DecimalType.instance);
            primitiveSerializationCompatibleWith.put(BytesType.instance, DurationType.instance);
            primitiveSerializationCompatibleWith.put(BytesType.instance, InetAddressType.instance);
            primitiveSerializationCompatibleWith.put(BytesType.instance, IntegerType.instance);
            primitiveSerializationCompatibleWith.put(BytesType.instance, ShortType.instance);
            primitiveSerializationCompatibleWith.put(BytesType.instance, SimpleDateType.instance);
            primitiveSerializationCompatibleWith.put(BytesType.instance, TimeType.instance);
            primitiveSerializationCompatibleWith.put(BytesType.instance, UTF8Type.instance);
            primitiveSerializationCompatibleWith.put(LongType.instance, TimestampType.instance);
            primitiveSerializationCompatibleWith.put(TimestampType.instance, LongType.instance);
            primitiveSerializationCompatibleWith.put(UTF8Type.instance, AsciiType.instance);
            primitiveSerializationCompatibleWith.put(UUIDType.instance, TimeUUIDType.instance);

            primitiveCompatibleWith.put(BytesType.instance, AsciiType.instance);
            primitiveCompatibleWith.put(BytesType.instance, UTF8Type.instance);
            primitiveCompatibleWith.put(UTF8Type.instance, AsciiType.instance);

            for (AbstractType<?> t : primitiveTypes)
            {
                primitiveValueCompatibleWith.put(t, t);
                primitiveSerializationCompatibleWith.put(t, t);
                primitiveCompatibleWith.put(t, t);
            }

            multiCellSupportingTypes = ImmutableSet.of(MapType.class, SetType.class, ListType.class);
            multiCellSupportingTypesForReading = ImmutableSet.of(MapType.class, SetType.class, ListType.class, UserType.class);
        }

        @Override
        public Set<Class<? extends AbstractType>> knownTypes()
        {
            return knownTypes;
        }

        @Override
        public Set<AbstractType<?>> primitiveTypes()
        {
            return primitiveTypes;
        }

        @Override
        public Set<Class<? extends AbstractType>> multiCellSupportingTypes()
        {
            return multiCellSupportingTypes;
        }

        @Override
        public Set<Class<? extends AbstractType>> multiCellSupportingTypesForReading()
        {
            return multiCellSupportingTypesForReading;
        }

        @Override
        public Set<Class<? extends AbstractType>> unsupportedTypes()
        {
            return unsupportedTypes;
        }

        private boolean expectedCompatibility(AbstractType left, AbstractType right, BiPredicate<AbstractType, AbstractType> primitiveTypesPredicate, BiPredicate<AbstractType, AbstractType> complexTypesPredicate)
        {
            if (left.equals(right))
                return true;

            boolean leftIsPrimitve = primitiveTypes().contains(left);
            boolean rightIsPrimitve = primitiveTypes().contains(right);

            if (leftIsPrimitve && rightIsPrimitve)
                return primitiveTypesPredicate.test(left, right);

            if (leftIsPrimitve || rightIsPrimitve)
                return false;

            return complexTypesPredicate.test(left, right);
        }

        @Override
        public boolean expectCompatibleWith(AbstractType left, AbstractType right)
        {
            return expectedCompatibility(left, right, primitiveCompatibleWith::containsEntry, AbstractType::isCompatibleWith);
        }

        @Override
        public boolean expectValueCompatibleWith(AbstractType left, AbstractType right)
        {
            return expectedCompatibility(left, right, primitiveValueCompatibleWith::containsEntry, AbstractType::isValueCompatibleWith);
        }

        @Override
        public boolean expectSerializationCompatibleWith(AbstractType left, AbstractType right)
        {
            return expectedCompatibility(left, right, primitiveSerializationCompatibleWith::containsEntry, AbstractType::isSerializationCompatibleWith);
        }
    }
}
