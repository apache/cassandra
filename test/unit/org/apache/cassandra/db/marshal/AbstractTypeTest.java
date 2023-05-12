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
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.cql3.FieldIdentifier;
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
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.quicktheories.core.Gen;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ConfigurationBuilder;

import static org.apache.cassandra.utils.AbstractTypeGenerators.TypeKind.*;
import static org.apache.cassandra.utils.AbstractTypeGenerators.extractUDTs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.QuickTheory.qt;

public class AbstractTypeTest
{
    static
    {
        // make sure blob is always the same
        System.setProperty("cassandra.test.blob.shared.seed", "42");
    }

    //TODO
    // isCompatibleWith/isValueCompatibleWith/isSerializationCompatibleWith,
    // withUpdatedUserType/expandUserTypes/referencesDuration - types that recursive check types
    // getMaskedValue

    @Test
    public void allTypesCovered()
    {
        // this test just makes sure that all types are covered and no new type is left out
        Reflections reflections = new Reflections(new ConfigurationBuilder()
                                                  .forPackage("org.apache.cassandra")
                                                  .setScanners(Scanners.SubTypes)
                                                  .setExpandSuperTypes(true)
                                                  .setParallel(true));
        Set<Class<? extends AbstractType>> subTypes = reflections.getSubTypesOf(AbstractType.class);
        Set<Class<? extends AbstractType>> coverage = AbstractTypeGenerators.knownTypes();
        StringBuilder sb = new StringBuilder();
        for (Class<? extends AbstractType> klass : Sets.difference(subTypes, coverage))
        {
            if (Modifier.isAbstract(klass.getModifiers()))
                continue;
            if ("test".equals(new File(klass.getProtectionDomain().getCodeSource().getLocation().getPath()).name()))
                continue;
            String name = klass.getCanonicalName();
            if (name == null)
                name = klass.getName();
            sb.append(name).append('\n');
        }
        if (sb.length() > 0)
            throw new AssertionError("Uncovered types:\n" + sb);
    }

    @Test
    public void comparableBytes()
    {
        qt().withShrinkCycles(0).forAll(examples(1)).checkAssert(example -> {
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
                    assertBytesEquals(read, bb, "fromComparableBytes(asComparableBytes(bb)) != bb");

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
    public void json()
    {
        qt().withShrinkCycles(0).forAll(examples(1)).checkAssert(es -> {
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

    @Test
    public void nested()
    {
        qt().withShrinkCycles(0).forAll(AbstractTypeGenerators.builder().withoutTypeKinds(PRIMITIVE).build()).checkAssert(type -> {
            List<AbstractType<?>> subtypes = type.subTypes();
            assertThat(subtypes).hasSize(type instanceof MapType ? 2 : type instanceof TupleType ? ((TupleType) type).size() : 1);
        });
    }

    @Test
    public void serde()
    {
        Gen<AbstractType<?>> typeGen = genBuilder()
                                       // fromCQL(toCQL()) does not work
                                       .withoutPrimitive(DurationType.instance)
                                       .build();
        qt().withShrinkCycles(0).forAll(examples(1, typeGen)).checkAssert(example -> {
            AbstractType type = example.type;

            // to -> from cql
            String cqlType = type.asCQL3Type().toString();
            assertThat(CQLTypeParser.parse(null, cqlType, toTypes(extractUDTs(type)))).describedAs("CQL type %s parse did not match the expected type", cqlType).isEqualTo(type);

            for (Object expected : example.samples)
            {
                ByteBuffer bb = type.decompose(expected);
                int position = bb.position();
                type.validate(bb);
                Object read = type.compose(bb);
                assertThat(bb.position()).describedAs("ByteBuffer was mutated by %s", type).isEqualTo(position);
                assertThat(read).isEqualTo(expected);

                String str = type.getString(bb);
                assertBytesEquals(type.fromString(str), bb, "fromString(getString(bb)) != bb; %s", str);

                String literal = type.asCQL3Type().toCQLLiteral(bb, ProtocolVersion.CURRENT);
                ByteBuffer cqlBB = parseLiteralType(type, literal);
                assertBytesEquals(cqlBB, bb, "Deserializing literal %s did not match expected bytes", literal);

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
    public void ordering()
    {
        Gen<AbstractType<?>> types = genBuilder()
                                     .withoutPrimitive(DurationType.instance) // this uses byte ordering and vint, which makes the ordering effectivlly random from a user's point of view
                                     .build();
        qt().withShrinkCycles(0).forAll(examples(10, types)).checkAssert(example -> {
            AbstractType type = example.type;
            List<ByteBuffer> actual = decompose(type, example.samples);
            Collections.sort(actual, type);
            List<ByteBuffer>[] byteOrdered = new List[ByteComparable.Version.values().length];
            List<OrderedBytes>[] rawByteOrdered = new List[ByteComparable.Version.values().length];
            for (int i = 0; i < byteOrdered.length; i++)
            {
                byteOrdered[i] = new ArrayList<>(actual);
                ByteComparable.Version version = ByteComparable.Version.values()[i];
                Collections.sort(byteOrdered[i], (a, b) -> ByteComparable.compare(fromBytes(type, a), fromBytes(type, b), version));

                rawByteOrdered[i] = actual.stream()
                                          .map(bb -> new OrderedBytes(ByteSourceInverse.readBytes(fromBytes(type, bb).asComparableBytes(version)), bb))
                                          .collect(Collectors.toList());
                Collections.sort(rawByteOrdered[i]);
            }

            Collections.sort(example.samples, comparator(type));
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

    private static Comparator<Object> comparator(AbstractType<?> type)
    {
        if (type instanceof NumberType || type instanceof BooleanType || type instanceof UUIDType || type instanceof TimestampType)
            return (Comparator<Object>) (Comparator<?>) Comparator.naturalOrder();
        if (type instanceof EmptyType)
            return (a, b) -> 0;
        if (type instanceof BytesType)
            return (Comparator<Object>) (Comparator<?>) (Comparator<ByteBuffer>) FastByteOperations::compareUnsigned;
        if (type instanceof InetAddressType)
            return (Comparator<Object>) (Comparator<?>) (InetAddress a, InetAddress b) -> FastByteOperations.compareUnsigned(a.getAddress(), b.getAddress());
        if (type instanceof StringType)
        {
            StringType st = (StringType) type;
            return (Comparator<Object>) (Comparator<?>) (String a, String b) -> FastByteOperations.compareUnsigned(st.decompose(a), st.decompose(b));
        }
        if (type instanceof ListType)
            return listComparator(comparator(((ListType) type).getElementsType()));
        if (type instanceof VectorType)
            return listComparator(comparator(((VectorType) type).elementType));
        if (type instanceof SetType)
        {
            SetType st = (SetType) type;
            Comparator<Object> elComparator = comparator(st.getElementsType());
            Comparator<Object> setComparator = listComparator(elComparator);
            return (Comparator<Object>) (Comparator<?>) (Set a, Set b) -> {
                List<Object> as = new ArrayList<>(a);
                Collections.sort(as, elComparator);
                List<Object> bs = new ArrayList<>(b);
                Collections.sort(bs, elComparator);
                return setComparator.compare(as, bs);
            };
        }
        if (type instanceof MapType)
        {
            MapType mt = (MapType) type;
            Comparator<Object> keyType = comparator(mt.getKeysType());
            Comparator<Object> valueType = comparator(mt.getValuesType());
            return (Comparator<Object>) (Comparator<?>) (Map a, Map b) -> {
                List<Object> ak = new ArrayList<>(a.keySet());
                Collections.sort(ak, keyType);
                List<Object> bk = new ArrayList<>(b.keySet());
                Collections.sort(bk, keyType);
                for (int i = 0, size = Math.min(ak.size(), bk.size()); i < size; i++)
                {
                    int rc = keyType.compare(ak.get(i), bk.get(i));
                    if (rc != 0)
                        return rc;
                    Object key = ak.get(i);
                    rc = valueType.compare(a.get(key), b.get(key));
                    if (rc != 0)
                        return rc;
                }
                return Integer.compare(a.size(), b.size());
            };
        }
        if (type instanceof TupleType)
        {
            TupleType tt = (TupleType) type;
            List<Comparator<Object>> columns = tt.types.stream().map(AbstractTypeTest::comparator).collect(Collectors.toList());
            Comparator<Object> listCompar = listComparator((i, a, b) -> columns.get(i).compare(a, b));
            return (Comparator<Object>) (Comparator<?>) (ByteBuffer a, ByteBuffer b) -> {
                ByteBuffer[] abb = tt.split(ByteBufferAccessor.instance, a);
                List<Object> av = IntStream.range(0, abb.length).mapToObj(i -> tt.type(i).compose(abb[i])).collect(Collectors.toList());

                ByteBuffer[] bbb = tt.split(ByteBufferAccessor.instance, b);
                List<Object> bv = IntStream.range(0, bbb.length).mapToObj(i -> tt.type(i).compose(bbb[i])).collect(Collectors.toList());
                return listCompar.compare(av, bv);
            };
        }
        if (type instanceof DurationType)
        {
            return (Comparator<Object>) (Comparator<?>) Comparator.comparingInt(Duration::getMonths)
                                                                  .thenComparingInt(Duration::getDays)
                                                                  .thenComparingLong(Duration::getNanoseconds);
        }
        throw new AssertionError("Unexpected type: " + type);
    }

    private static Comparator<Object> listComparator(Comparator<Object> ordering)
    {
        return listComparator((ignore, a, b) -> ordering.compare(a, b));
    }

    private interface IndexComparator<T>
    {
        int compare(int index, T a, T b);
    }
    private static Comparator<Object> listComparator(IndexComparator ordering)
    {
        return (Comparator<Object>) (Comparator<?>) (List a, List b) -> {
            for (int i = 0, size = Math.min(a.size(), b.size()); i < size; i++)
            {
                int rc = ordering.compare(i, a.get(i), b.get(i));
                if (rc != 0)
                    return rc;
            }
            return Integer.compare(a.size(), b.size());
        };
    }

    private List<ByteBuffer> decompose(AbstractType type, List<Object> value)
    {
        List<ByteBuffer> expected = new ArrayList<>(value.size());
        for (int i = 0; i < value.size(); i++)
            expected.add(type.decompose(value.get(i)));
        return expected;
    }

    private static AbstractTypeGenerators.TypeGenBuilder genBuilder()
    {
        return AbstractTypeGenerators.builder()
                                     // empty is a legacy from 2.x and is only allowed in special cases and not allowed in all... as this class tests all cases, need to limit this type out
                                     .withoutEmpty();
    }

    private static Gen<Example> examples(int samples)
    {
        return examples(samples, genBuilder().build());
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

    private static String typeTree(AbstractType<?> type)
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
            newline(sb, elementIndent);
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