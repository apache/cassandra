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

package org.apache.cassandra.io.sstable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.SourceDSL;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.io.tries.Walker;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeKind;
import org.apache.cassandra.utils.AbstractTypeGenerators.ValueDomain;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.quicktheories.QuickTheory.qt;

/** Verifies {@link ClusteringDescriptorPrefixView} decodes the clustering wire format exactly as
 *  the reference decoder {@link ClusteringDescriptor#toClusteringPrefix(List)}, producing the same
 *  components and the same byte-comparable encoding. */
public class ClusteringDescriptorPrefixViewTest
{
    /** The byte-comparable version the row trie uses. */
    private static final ByteComparable.Version BYTE_COMPARABLE_VERSION = Walker.BYTE_COMPARABLE_VERSION;

    /** Passed to the serializer; neither the header nor the component encoding depends on it. */
    private static final int SERIALIZATION_VERSION = 0;

    /** Large enough to reach a second header block. */
    private static final int MAX_CLUSTERING_COLUMNS = 40;

    /** Every kind that can reach the row trie. */
    private static final List<ClusteringPrefix.Kind> KINDS =
        Arrays.asList(ClusteringPrefix.Kind.CLUSTERING,
                      ClusteringPrefix.Kind.INCL_START_BOUND,
                      ClusteringPrefix.Kind.EXCL_START_BOUND,
                      ClusteringPrefix.Kind.INCL_END_BOUND,
                      ClusteringPrefix.Kind.EXCL_END_BOUND,
                      ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY,
                      ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY);

    /** Weighted towards NORMAL so enough present values exercise the length walks. */
    private static final List<ValueDomain> VALUE_DOMAINS =
        Arrays.asList(ValueDomain.NORMAL, ValueDomain.NORMAL, ValueDomain.NORMAL,
                      ValueDomain.NORMAL, ValueDomain.NORMAL, ValueDomain.NORMAL,
                      ValueDomain.NULL, ValueDomain.EMPTY_BYTES);

    private static final int SEEN_FIXED_PRESENT = 0;
    private static final int SEEN_VARIABLE_PRESENT = 1;
    private static final int SEEN_NULL = 2;
    private static final int SEEN_EMPTY = 3;
    private static final int SEEN_SECOND_HEADER_BLOCK = 4;
    private static final int SEEN_PRESENT_NOT_LAST = 5;
    private static final String[] BRANCH_LABELS = { "a present fixed-length component (parse: valueLengthIfFixed)",
                                                    "a present variable-length component (parse: the length vint)",
                                                    "a null component (parse: the 2i+1 bit)",
                                                    "an empty component (parse: the 2i bit)",
                                                    "a prefix longer than 32 components (parse: the second header vint)",
                                                    "a present component followed by another (parse: pos += len)" };

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /** Every generated prefix must decode through the view exactly as through the reference, and
     *  produce the same byte-comparable bytes. */
    @Test
    public void parseMatchesTheSerializer()
    {
        int[] counters = new int[BRANCH_LABELS.length];
        qt().withExamples(CassandraRelevantProperties.TEST_CLUSTERING_PREFIX_VIEW_EXAMPLES.getInt())
            .forAll(exampleGen())
            .checkAssert(example -> {
                TestDescriptor descriptor = example.load(new TestDescriptor(example.types));
                ClusteringComparator comparator = new ClusteringComparator(example.types);
                ClusteringPrefix<byte[]> reference = referenceOf(descriptor, example.types);

                // snapshotOf re-parses a copy of the bytes
                assertSamePrefix("snapshotOf", reference,
                                 ClusteringDescriptorPrefixView.snapshotOf(descriptor, example.types), comparator);

                recordBranches(counters, reference, example.types);
            });
        assertCorpusReachedEveryBranch(counters);
    }

    /** Fixed sizes either side of every header-block boundary, with null and empty positions
     *  asserted absolutely. */
    @Test
    public void headerBlockBoundaries()
    {
        for (int count : new int[]{ 1, 2, 31, 32, 33, 40, 64, 65 })
        {
            AbstractType<?>[] types = deterministicTypes(count);
            ByteBuffer[] values = new ByteBuffer[count];
            for (int i = 0; i < count; i++)
                values[i] = deterministicValue(types[i], i);

            TestDescriptor descriptor = new TestDescriptor(types);
            descriptor.load(ClusteringPrefix.Kind.CLUSTERING, count, serialize(types, values));

            ClusteringDescriptorPrefixView view = ClusteringDescriptorPrefixView.snapshotOf(descriptor, types);
            assertSamePrefix("size " + count, referenceOf(descriptor, types), view, new ClusteringComparator(types));

            // asserted absolutely, not against either implementation
            for (int i = 0; i < count; i++)
            {
                ByteBuffer component = view.get(i);
                if (i % 5 == 3)
                    assertNull("size " + count + " component " + i + " must be null", component);
                else if (i % 5 == 4)
                    assertEquals("size " + count + " component " + i + " must be empty",
                                 0, component.remaining());
                else
                    assertTrue("size " + count + " component " + i + " must carry bytes",
                               component.remaining() > 0);
            }
        }
    }

    /** A bound with no components must carry only its kind. */
    @Test
    public void emptyBoundsCarryOnlyTheirKind()
    {
        AbstractType<?>[] types = { Int32Type.instance, UTF8Type.instance };
        ClusteringComparator comparator = new ClusteringComparator(types);
        TestDescriptor descriptor = new TestDescriptor(types);

        descriptor.resetMaxStart();
        assertSamePrefix("max start", referenceOf(descriptor, types),
                         ClusteringDescriptorPrefixView.snapshotOf(descriptor, types), comparator);

        descriptor.resetMinEnd();
        assertSamePrefix("min end", referenceOf(descriptor, types),
                         ClusteringDescriptorPrefixView.snapshotOf(descriptor, types), comparator);
    }

    /** A snapshot must survive the source descriptor being overwritten. */
    @Test
    public void aSnapshotSurvivesTheDescriptorBeingOverwritten()
    {
        AbstractType<?>[] types = { Int32Type.instance, UTF8Type.instance };
        ClusteringComparator comparator = new ClusteringComparator(types);

        TestDescriptor descriptor = new TestDescriptor(types);
        descriptor.load(ClusteringPrefix.Kind.CLUSTERING, 2,
                        serialize(types, values(Int32Type.instance.decompose(7),
                                                UTF8Type.instance.decompose("before"))));
        ClusteringPrefix<byte[]> expected = referenceOf(descriptor, types);
        byte[] backing = descriptor.clusteringBytes();

        ClusteringDescriptorPrefixView snapshot = ClusteringDescriptorPrefixView.snapshotOf(descriptor, types);

        // same length: the descriptor is rewritten in place
        descriptor.load(ClusteringPrefix.Kind.CLUSTERING, 2,
                        serialize(types, values(Int32Type.instance.decompose(-7),
                                                UTF8Type.instance.decompose("AFTER!"))));
        assertSame("the overwrite must have stayed in the same array, or an aliasing snapshot " +
                   "would pass this test by accident",
                   backing, descriptor.clusteringBytes());
        assertSamePrefix("snapshot after an in-place overwrite", expected, snapshot, comparator);

        // And one that replaces the array outright.
        descriptor.load(ClusteringPrefix.Kind.CLUSTERING, 2,
                        serialize(types, values(Int32Type.instance.decompose(11),
                                                UTF8Type.instance.decompose(repeat('x', 300)))));
        assertNotSame(backing, descriptor.clusteringBytes());
        assertSamePrefix("snapshot after a resizing overwrite", expected, snapshot, comparator);
    }

    /** A snapshot already owns its bytes, so it is its own retainable form. */
    @Test
    public void aSnapshotIsItsOwnRetainable()
    {
        AbstractType<?>[] types = { Int32Type.instance };
        TestDescriptor descriptor = new TestDescriptor(types);
        descriptor.load(ClusteringPrefix.Kind.CLUSTERING, 1,
                        serialize(types, values(Int32Type.instance.decompose(3))));

        ClusteringDescriptorPrefixView snapshot = ClusteringDescriptorPrefixView.snapshotOf(descriptor, types);
        assertSame(snapshot, snapshot.retainable());
    }

    // ------------------------------------------------------------------------------------------
    // oracle

    private static void assertSamePrefix(String context,
                                         ClusteringPrefix<byte[]> reference,
                                         ClusteringPrefix<ByteBuffer> view,
                                         ClusteringComparator comparator)
    {
        assertEquals(context + ": kind", reference.kind(), view.kind());
        assertEquals(context + ": size", reference.size(), view.size());

        // get(i) returns one shared window; consume each component before asking for the next
        for (int i = 0; i < reference.size(); i++)
        {
            byte[] expected = reference.get(i);
            ByteBuffer actual = view.get(i);
            if (expected == null)
            {
                assertNull(context + ": component " + i + " must be null", actual);
            }
            else
            {
                assertNotNull(context + ": component " + i + " must not be null", actual);
                assertArrayEquals(context + ": component " + i, expected, ByteBufferUtil.getArray(actual));
            }
        }

        // Byte for byte, not ByteComparable.compare: a prefix relationship must fail here.
        assertArrayEquals(context + ": byte-comparable encoding",
                          drain(comparator.asByteComparable(reference)),
                          drain(comparator.asByteComparable(view)));
    }

    private static byte[] drain(ByteComparable comparable)
    {
        ByteSource source = comparable.asComparableBytes(BYTE_COMPARABLE_VERSION);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int b = source.next(); b != ByteSource.END_OF_STREAM; b = source.next())
            out.write(b);
        return out.toByteArray();
    }

    @SuppressWarnings("unchecked")
    private static ClusteringPrefix<byte[]> referenceOf(ClusteringDescriptor descriptor, AbstractType<?>[] types)
    {
        return (ClusteringPrefix<byte[]>) descriptor.toClusteringPrefix(types);
    }

    // ------------------------------------------------------------------------------------------
    // input

    /** Fills a descriptor the way {@code SSTableCursorReader.readUnfilteredClustering} does,
     *  without a data file. */
    private static class TestDescriptor extends ClusteringDescriptor
    {
        TestDescriptor(AbstractType<?>[] types)
        {
            super(types);
        }

        void load(ClusteringPrefix.Kind kind, int bound, byte[] serialized)
        {
            clusteringKind(kind);
            clusteringColumnsBound = bound;
            overwrite(serialized, serialized.length);
        }
    }

    private static class Example
    {
        final AbstractType<?>[] types;
        final ClusteringPrefix.Kind kind;
        final ByteBuffer[] values;
        final byte[] serialized;

        Example(AbstractType<?>[] types, ClusteringPrefix.Kind kind, ByteBuffer[] values)
        {
            this.types = types;
            this.kind = kind;
            this.values = values;
            this.serialized = serialize(types, values);
        }

        TestDescriptor load(TestDescriptor descriptor)
        {
            descriptor.load(kind, values.length, serialized);
            return descriptor;
        }

        /** Enough to rebuild the example by hand from a failure message. */
        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder(kind.toString()).append(" over ").append(types.length)
                                                                 .append(" types, ").append(values.length)
                                                                 .append(" components:");
            for (int i = 0; i < types.length; i++)
            {
                sb.append("\n  [").append(i).append("] ").append(types[i].asCQL3Type())
                  .append(types[i].isValueLengthFixed() ? " fixed(" + types[i].valueLengthIfFixed() + ")" : " variable")
                  .append(" = ");
                if (i >= values.length)
                    sb.append("<absent>");
                else if (values[i] == null)
                    sb.append("null");
                else if (!values[i].hasRemaining())
                    sb.append("empty");
                else
                    sb.append("0x").append(ByteBufferUtil.bytesToHex(values[i]));
            }
            return sb.append("\n  bytes = 0x").append(ByteBufferUtil.bytesToHex(ByteBuffer.wrap(serialized))).toString();
        }
    }

    /** Generates fixed- and variable-length primitive types, mixing in vectors over fixed-length
     *  primitives. */
    private static Gen<AbstractType<?>> componentTypeGen()
    {
        Gen<AbstractType<?>> primitiveGen = AbstractTypeGenerators.builder()
                                                                  .withoutUnsafeEquality()
                                                                  .withTypeKinds(TypeKind.PRIMITIVE)
                                                                  .withMaxDepth(0)
                                                                  .build();
        Gen<AbstractType<?>> fixedPrimitiveGen = AbstractTypeGenerators.builder()
                                                                       .withoutUnsafeEquality()
                                                                       .withTypeKinds(TypeKind.PRIMITIVE)
                                                                       .withMaxDepth(0)
                                                                       .withTypeFilter(AbstractType::isValueLengthFixed)
                                                                       .build();
        Gen<AbstractType<?>> vectorGen = AbstractTypeGenerators.vectorTypeGen(fixedPrimitiveGen,
                                                                             SourceDSL.integers().between(1, 3))
                                                              .map(vector -> (AbstractType<?>) vector);
        Gen<Boolean> vectorChance = SourceDSL.integers().between(0, 4).map(i -> i == 0);
        return AbstractTypeGenerators.allowReversed(
            rnd -> vectorChance.generate(rnd) ? vectorGen.generate(rnd) : primitiveGen.generate(rnd));
    }

    private static Gen<Example> exampleGen()
    {
        Gen<AbstractType<?>> typeGen = componentTypeGen();
        Gen<Integer> countGen = SourceDSL.integers().between(1, MAX_CLUSTERING_COLUMNS);
        Gen<ClusteringPrefix.Kind> kindGen = SourceDSL.arbitrary().pick(KINDS);
        Gen<ValueDomain> domainGen = SourceDSL.arbitrary().pick(VALUE_DOMAINS);

        return rnd -> {
            int count = countGen.generate(rnd);
            AbstractType<?>[] types = new AbstractType<?>[count];
            for (int i = 0; i < count; i++)
                types[i] = typeGen.generate(rnd);

            ClusteringPrefix.Kind kind = kindGen.generate(rnd);
            // A bound or boundary may stop short of the full clustering key; a Clustering may not.
            int bound = kind == ClusteringPrefix.Kind.CLUSTERING
                        ? count
                        : SourceDSL.integers().between(1, count).generate(rnd);

            ByteBuffer[] values = new ByteBuffer[bound];
            for (int i = 0; i < bound; i++)
                values[i] = value(types[i], domainGen.generate(rnd), rnd);
            return new Example(types, kind, values);
        };
    }

    /** Produces a value in the requested domain; empty is only offered to types that accept
     *  empty bytes. */
    private static ByteBuffer value(AbstractType<?> type, ValueDomain domain, RandomnessSource rnd)
    {
        if (domain == ValueDomain.NULL)
            return null;
        if (domain == ValueDomain.EMPTY_BYTES && type.unwrap().allowsEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        return AbstractTypeGenerators.getTypeSupport(type).bytesGen().generate(rnd);
    }

    /** Encodes the values with the production encoder over the matching prefix of the type list. */
    private static byte[] serialize(AbstractType<?>[] types, ByteBuffer[] values)
    {
        Clustering<ByteBuffer> clustering = values.length == 0
                                            ? ByteBufferAccessor.instance.factory().clustering()
                                            : ByteBufferAccessor.instance.factory().clustering(values);
        AbstractType<?>[] present = Arrays.copyOf(types, values.length);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Clustering.serializer.serialize(clustering, out, SERIALIZATION_VERSION, present);
            return out.toByteArray();
        }
        catch (IOException e)
        {
            throw new AssertionError("writing to an in-memory buffer must not fail", e);
        }
    }

    private static ByteBuffer[] values(ByteBuffer... values)
    {
        return values;
    }

    /** Cycles fixed and variable, plain and reversed, so no size lands on a single shape. */
    private static AbstractType<?>[] deterministicTypes(int count)
    {
        AbstractType<?>[] pattern = { Int32Type.instance,
                                      UTF8Type.instance,
                                      LongType.instance,
                                      ReversedType.getInstance(Int32Type.instance),
                                      BytesType.instance,
                                      UUIDType.instance,
                                      ReversedType.getInstance(UTF8Type.instance) };
        AbstractType<?>[] types = new AbstractType<?>[count];
        for (int i = 0; i < count; i++)
            types[i] = pattern[i % pattern.length];
        return types;
    }

    /** null at {@code i % 5 == 3}, empty at {@code i % 5 == 4}, a distinct value elsewhere. */
    private static ByteBuffer deterministicValue(AbstractType<?> type, int i)
    {
        if (i % 5 == 3)
            return null;
        if (i % 5 == 4)
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        AbstractType<?> base = type.unwrap();
        if (base == Int32Type.instance)
            return Int32Type.instance.decompose(i);
        if (base == LongType.instance)
            return LongType.instance.decompose(i * 1_000_003L);
        if (base == UUIDType.instance)
            return UUIDType.instance.decompose(new UUID(i, ~i));
        if (base == UTF8Type.instance)
            return UTF8Type.instance.decompose("component-" + i);
        if (base == BytesType.instance)
            return ByteBuffer.wrap(new byte[]{ (byte) i, (byte) (i >>> 8), 0x7f });
        throw new AssertionError("no deterministic value defined for " + type);
    }

    private static String repeat(char c, int length)
    {
        char[] chars = new char[length];
        Arrays.fill(chars, c);
        return new String(chars);
    }

    // ------------------------------------------------------------------------------------------
    // corpus gate: prove the generated input reached the branches this test exists for

    private static void recordBranches(int[] counters, ClusteringPrefix<byte[]> reference, AbstractType<?>[] types)
    {
        int size = reference.size();
        if (size > 32)
            counters[SEEN_SECOND_HEADER_BLOCK]++;
        for (int i = 0; i < size; i++)
        {
            byte[] component = reference.get(i);
            if (component == null)
            {
                counters[SEEN_NULL]++;
            }
            else if (component.length == 0)
            {
                counters[SEEN_EMPTY]++;
            }
            else
            {
                counters[types[i].isValueLengthFixed() ? SEEN_FIXED_PRESENT : SEEN_VARIABLE_PRESENT]++;
                if (i < size - 1)
                    counters[SEEN_PRESENT_NOT_LAST]++;
            }
        }
    }

    private static void assertCorpusReachedEveryBranch(int[] counters)
    {
        List<String> missed = new ArrayList<>();
        for (int i = 0; i < counters.length; i++)
        {
            if (counters[i] == 0)
                missed.add(BRANCH_LABELS[i]);
        }
        if (!missed.isEmpty())
            fail("the generated corpus never reached: " + String.join("; ", missed) +
                 ". The test passed without exercising them, so raise " +
                 CassandraRelevantProperties.TEST_CLUSTERING_PREFIX_VIEW_EXAMPLES.getKey() +
                 " or fix the generator; do not treat this run as coverage.");
    }
}
