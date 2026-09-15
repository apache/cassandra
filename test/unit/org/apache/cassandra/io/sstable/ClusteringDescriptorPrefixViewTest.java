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

/**
 * Pins {@link ClusteringDescriptorPrefixView} to the wire format it re-implements.
 *
 * The view is the only cursor-specific input to the BTI row trie: {@code BtiCursorIndexWriter}
 * snapshots one per index block and hands it to {@code RowIndexWriter}, which turns it into a
 * {@link ByteComparable} through {@link ClusteringComparator#asByteComparable}. Its
 * {@code parse} method is a hand-written decoder for the same bytes
 * {@link ClusteringPrefix.Serializer#deserializeValuesWithoutSize} reads, so a divergence there
 * writes a trie that indexes the wrong rows without corrupting a single byte of the data file.
 *
 * <h2>Oracle</h2>
 *
 * Reference implementation, already in the tree:
 * {@link ClusteringDescriptor#toClusteringPrefix(List)} decodes the descriptor's bytes through
 * {@code Clustering.serializer} / {@code ClusteringPrefix.serializer}. The bytes themselves are
 * produced by the production encoder, {@code Clustering.serializer.serialize}, which is what
 * {@code SSTableCursorReader.readUnfilteredClustering} copies verbatim off disk into the
 * descriptor. So encoder and reference decoder are both production code; only the view is new.
 *
 * Three things are compared per example: kind and size, every component (null, empty and valued
 * distinguished), and the full byte-comparable encoding compared byte for byte rather than through
 * {@code ByteComparable.compare}, so a prefix relationship fails instead of passing.
 *
 * <h2>What this test cannot see</h2>
 *
 * <ul>
 * <li>Everything the two sides share. The reference and the view both call the same
 *     {@code AbstractType.asComparableBytes}, the same {@code isValueLengthFixed}, and the same
 *     {@code ClusteringComparator}; a defect in any of those is invisible here. Only the decode
 *     of the header bits and the component walk is genuinely differential.</li>
 * <li>Whether the bytes in a real descriptor match what this test writes. The test encodes with
 *     {@code Clustering.serializer.serialize}; production fills the descriptor through
 *     {@code SSTableCursorReader.readUnfilteredClustering}. Those two agreeing is asserted
 *     elsewhere (the differential compaction suite), not here.</li>
 * <li>Concurrency and reuse across threads. The view is single-threaded by construction.</li>
 * <li>Whether a live (non-snapshot) view left pointing at a resized descriptor still reads
 *     sensibly. It does not, by design; {@code retainable()} exists for that, and that is what is
 *     asserted below.</li>
 * <li>The trie itself. A view that byte-compares identically to the reference can still be
 *     mis-used by the caller; that is {@code BtiCursorIndexWriter}'s coverage, not this class's.</li>
 * </ul>
 */
public class ClusteringDescriptorPrefixViewTest
{
    /** The version the row trie is built and read at; see {@code RowIndexWriter}. */
    private static final ByteComparable.Version BYTE_COMPARABLE_VERSION = Walker.BYTE_COMPARABLE_VERSION;

    /**
     * Passed to the serializer and to {@link ClusteringDescriptor#toClusteringPrefix(List)}, which
     * hardcodes 0. Neither the header nor the component encoding depends on it.
     */
    private static final int SERIALIZATION_VERSION = 0;

    /**
     * Past two header blocks, so {@code i == 32} and {@code i == 33} are reached by the generator
     * and not only by {@link #headerBlockBoundaries()}.
     */
    private static final int MAX_CLUSTERING_COLUMNS = 40;

    /** Every kind that can reach the row trie. STATIC_CLUSTERING carries no bytes and no components. */
    private static final List<ClusteringPrefix.Kind> KINDS =
        Arrays.asList(ClusteringPrefix.Kind.CLUSTERING,
                      ClusteringPrefix.Kind.INCL_START_BOUND,
                      ClusteringPrefix.Kind.EXCL_START_BOUND,
                      ClusteringPrefix.Kind.INCL_END_BOUND,
                      ClusteringPrefix.Kind.EXCL_END_BOUND,
                      ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY,
                      ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY);

    /**
     * Weighted towards NORMAL. An unweighted pick over 40 components leaves too few present values
     * to exercise the fixed-length and vint length walks; {@link #assertCorpusReachedEveryBranch}
     * is the check that this weighting actually paid off.
     */
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

    /**
     * The property. Every generated prefix must decode through the view exactly as it decodes
     * through {@link ClusteringDescriptor#toClusteringPrefix(List)}, and must produce the same
     * byte-comparable bytes.
     *
     * Shrinking is deliberately left on: this is a pure function, so quicktheories can shrink a
     * failure to a minimal type list and value set. A failure prints "Seed was N" and the shrunk
     * example in full, including its serialized bytes; replay it with {@code -DQT_SEED=N}.
     */
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

                assertSamePrefix("reset", reference,
                                 new ClusteringDescriptorPrefixView(example.types).reset(descriptor), comparator);
                // The snapshot copies the bytes and re-parses them, so it is a second, independent
                // trip through parse over the same input.
                assertSamePrefix("snapshotOf", reference,
                                 ClusteringDescriptorPrefixView.snapshotOf(descriptor, example.types), comparator);

                recordBranches(counters, reference, example.types);
            });
        assertCorpusReachedEveryBranch(counters);
    }

    /**
     * Fixed sizes either side of every header-block boundary, with the null and empty positions
     * asserted absolutely rather than against the reference. 33 is the first size that needs a
     * second header vint; 65 the first that needs a third.
     */
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

            ClusteringDescriptorPrefixView view = new ClusteringDescriptorPrefixView(types).reset(descriptor);
            assertSamePrefix("size " + count, referenceOf(descriptor, types), view, new ClusteringComparator(types));

            // Absolute: the answer is stated here, not read off either implementation.
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

    /**
     * A bound with no components. {@code resetMaxStart} and {@code resetMinEnd} are the production
     * calls that produce it, and the view must carry only the kind.
     */
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

    /**
     * Covers both sides of the {@code backing != bytes} identity check in
     * {@link ClusteringDescriptorPrefixView#reset}: a reload that keeps the descriptor's array must
     * re-parse without re-wrapping, and a different descriptor must re-wrap. The array identity is
     * asserted rather than assumed, so the branch claim is checkable.
     */
    @Test
    public void resetRewrapsOnlyOnANewBackingArray()
    {
        AbstractType<?>[] types = { Int32Type.instance, UTF8Type.instance, LongType.instance };
        ClusteringComparator comparator = new ClusteringComparator(types);

        TestDescriptor first = new TestDescriptor(types);
        first.load(ClusteringPrefix.Kind.CLUSTERING, 3,
                   serialize(types, values(Int32Type.instance.decompose(1),
                                           UTF8Type.instance.decompose("a"),
                                           LongType.instance.decompose(2L))));
        byte[] backing = first.clusteringBytes();

        ClusteringDescriptorPrefixView view = new ClusteringDescriptorPrefixView(types);
        view.reset(first);
        assertSamePrefix("first parse", referenceOf(first, types), view, comparator);

        // Same array, different content and a different length: the identity check must take the
        // "already wrapped" path and parse must still honour the new limit.
        first.load(ClusteringPrefix.Kind.INCL_END_BOUND, 2,
                   serialize(types, values(null, UTF8Type.instance.decompose("bbbbbbbb"))));
        assertSame("the reload must not have resized the descriptor, or the backing == bytes branch " +
                   "is not the one being covered",
                   backing, first.clusteringBytes());
        view.reset(first);
        assertSamePrefix("same backing array", referenceOf(first, types), view, comparator);

        // A different descriptor owns a different array, so the view must re-wrap.
        TestDescriptor second = new TestDescriptor(types);
        second.load(ClusteringPrefix.Kind.CLUSTERING, 3,
                    serialize(types, values(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                            UTF8Type.instance.decompose("c"),
                                            LongType.instance.decompose(-9L))));
        assertNotSame(first.clusteringBytes(), second.clusteringBytes());
        view.reset(second);
        assertSamePrefix("new backing array", referenceOf(second, types), view, comparator);
    }

    /**
     * A snapshot, and a {@code retainable()} taken from a live view, must both survive the source
     * descriptor being overwritten.
     *
     * Two overwrites, because only the first one catches a snapshot that aliased the descriptor's
     * array instead of copying it: the second is long enough to force a resize, which leaves an
     * aliasing snapshot pointing at the old array and still reading the right answer by accident.
     */
    @Test
    public void retainedViewsSurviveTheDescriptorBeingOverwritten()
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
        ClusteringDescriptorPrefixView live = new ClusteringDescriptorPrefixView(types).reset(descriptor);
        ClusteringPrefix<?> retained = live.retainable();
        assertNotSame("retainable() on a live view must copy", live, retained);

        // Same serialized length, so the descriptor is rewritten in place, in the array the
        // snapshot would be aliasing if it had not copied.
        descriptor.load(ClusteringPrefix.Kind.CLUSTERING, 2,
                        serialize(types, values(Int32Type.instance.decompose(-7),
                                                UTF8Type.instance.decompose("AFTER!"))));
        assertSame("the overwrite must have stayed in the same array, or an aliasing snapshot " +
                   "would pass this test by accident",
                   backing, descriptor.clusteringBytes());
        assertSamePrefix("snapshot after an in-place overwrite", expected, snapshot, comparator);
        assertSamePrefix("retainable after an in-place overwrite", expected,
                         (ClusteringDescriptorPrefixView) retained, comparator);

        // And one that replaces the array outright.
        descriptor.load(ClusteringPrefix.Kind.CLUSTERING, 2,
                        serialize(types, values(Int32Type.instance.decompose(11),
                                                UTF8Type.instance.decompose(repeat('x', 300)))));
        assertNotSame(backing, descriptor.clusteringBytes());
        assertSamePrefix("snapshot after a resizing overwrite", expected, snapshot, comparator);
        assertSamePrefix("retainable after a resizing overwrite", expected,
                         (ClusteringDescriptorPrefixView) retained, comparator);
    }

    /** A view that already owns its bytes returns itself and refuses to be re-pointed. */
    @Test
    public void anOwnedViewIsItsOwnRetainableAndRejectsReset()
    {
        AbstractType<?>[] types = { Int32Type.instance };
        TestDescriptor descriptor = new TestDescriptor(types);
        descriptor.load(ClusteringPrefix.Kind.CLUSTERING, 1,
                        serialize(types, values(Int32Type.instance.decompose(3))));

        ClusteringDescriptorPrefixView snapshot = ClusteringDescriptorPrefixView.snapshotOf(descriptor, types);
        assertSame(snapshot, snapshot.retainable());
        try
        {
            snapshot.reset(descriptor);
            fail("a snapshot owns its bytes and must refuse reset");
        }
        catch (IllegalStateException expected)
        {
            // the contract in the javadoc of reset
        }
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

        // get(i) hands back one shared, repositioned window, so each component is consumed before
        // the next is asked for.
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
        return (ClusteringPrefix<byte[]>) descriptor.toClusteringPrefix(Arrays.asList(types));
    }

    // ------------------------------------------------------------------------------------------
    // input

    /**
     * Fills a descriptor the way {@code SSTableCursorReader.readUnfilteredClustering} does, without
     * a data file. The subclass exists only to reach {@code ResizableByteBuffer.overwrite}; no
     * production accessor was added for the test.
     */
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

    /**
     * Primitives supply both halves of the branch {@code parse} actually turns on, fixed-length and
     * variable-length. Vectors over fixed-length primitives are mixed in for a fixed width wider
     * than any primitive's; their elements are restricted to fixed-length types because
     * {@code AbstractTypeGenerators} can otherwise generate a vector holding an empty element,
     * which {@code VectorType.unpack} rejects on read. Frozen collections and tuples are left out
     * deliberately: they are all variable-length, so they add no branch here, and the differential
     * compaction suite covers them end to end.
     */
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

    /**
     * A null component never reaches its type: the header carries it and
     * {@link ClusteringComparator#asByteComparable} emits NEXT_COMPONENT_NULL, so NULL is legal for
     * every type. An empty component is only offered to types that accept empty bytes;
     * {@code VectorType.asComparableBytes} throws on one, which says nothing about {@code parse}
     * and would only make the oracle unusable. Enough fixed-length primitives allow empty
     * (int, bigint, boolean, uuid, timestamp, ...) to keep the empty-on-fixed-length case, which is
     * the one that matters here: {@code parse} must not advance by valueLengthIfFixed for it.
     */
    private static ByteBuffer value(AbstractType<?> type, ValueDomain domain, RandomnessSource rnd)
    {
        if (domain == ValueDomain.NULL)
            return null;
        if (domain == ValueDomain.EMPTY_BYTES && type.unwrap().allowsEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        return AbstractTypeGenerators.getTypeSupport(type).bytesGen().generate(rnd);
    }

    /**
     * Encodes with the production encoder. The bytes are a function of the values and the types
     * only, so serialising the present components as a {@code Clustering} over the matching prefix
     * of the type list produces exactly what a bound of that length holds on disk.
     */
    private static byte[] serialize(AbstractType<?>[] types, ByteBuffer[] values)
    {
        Clustering<ByteBuffer> clustering = values.length == 0
                                            ? ByteBufferAccessor.instance.factory().clustering()
                                            : ByteBufferAccessor.instance.factory().clustering(values);
        List<AbstractType<?>> present = Arrays.asList(types).subList(0, values.length);
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
