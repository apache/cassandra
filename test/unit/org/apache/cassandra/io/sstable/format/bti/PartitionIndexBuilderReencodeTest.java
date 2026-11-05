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
package org.apache.cassandra.io.sstable.format.bti;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.tries.IncrementalTrieWriter;
import org.apache.cassandra.io.tries.Walker;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Differential test for the reduced-allocation key encoding in {@link PartitionIndexBuilder}.
 * <p>
 * The builder no longer re-encodes each key with {@link DecoratedKey#asComparableBytes} several times.  It captures the
 * comparable bytes once into a scratch array, does {@code diffPoint} as an array compare, and feeds the trie a
 * fixed-length source over the captured bytes.  A three-slot ring keeps the previous key unchanged while the trie
 * reads it again when the next key is added.
 * <p>
 * {@link #testRecordingTrieByteIdentity} runs the real patched builder with a recording {@link IncrementalTrieWriter}
 * injected through a package-private constructor.  It captures the exact sequence of prefixes and payloads passed to
 * the trie, and asserts they are byte-for-byte identical to the original {@code diffPoint}/{@code cut} reference.  It
 * also reads each added prefix again on the next key, so it fails if the ring lets a slot be overwritten while the trie
 * still needs it (for example a change from three slots to two).
 * <p>
 * {@link #testAdversarialByteKeys} and {@link #testMurmur3TokenKeyCombinations} build real on-disk indexes and assert
 * lookups.  They cover byte shapes and a non-default partitioner that {@link PartitionIndexTest} does not.
 */
public class PartitionIndexBuilderReencodeTest
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static final ByteComparable.Version VERSION = Walker.BYTE_COMPARABLE_VERSION;

    private final PartitionIndexTest base = new PartitionIndexTest();

    // ---------------------------------------------------------------------------------------------------------------
    // Byte-identity through the real builder.
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void testRecordingTrieByteIdentity() throws IOException
    {
        for (List<DecoratedKey> keys : encodingKeySets())
            assertRecordedMatchesStock(keys);

        // Zero keys: complete() takes the lastKey == null branch and adds nothing to the trie.
        assertRecordedMatchesStock(Collections.emptyList());

        // One key: complete() flushes the tail with m == 0 (empty prefix), no add during the loop.
        List<DecoratedKey> single = new ArrayList<>();
        single.add(base.partitioner.decorateKey(ByteBufferUtil.bytes("only-key")));
        assertRecordedMatchesStock(single);

        // Long key first, short key three positions later reuses the same ring slot after growth.  The slot still
        // holds the long key's trailing bytes; only the recorded length may be read, never the stale bytes.
        assertRecordedMatchesStock(distinctSorted(longThenShortSameSlot()));
    }

    /**
     * Runs the real patched builder over the keys with a recording trie writer, then asserts the recorded prefixes and
     * payload positions equal the original {@code diffPoint}/{@code cut} reference, and that the previous prefix the
     * trie reads again on each key still matches what was added before.
     */
    private void assertRecordedMatchesStock(List<DecoratedKey> keys) throws IOException
    {
        RecordingTrieWriter rec = new RecordingTrieWriter();
        File file = FileUtils.createTempFile("PartitionIndexBuilderReencodeTest", "");
        try (SequentialWriter writer = base.makeWriter(file);
             PartitionIndexBuilder builder = new PartitionIndexBuilder(writer, base.makeHandle(file), rec))
        {
            for (int i = 0; i < keys.size(); i++)
                builder.addEntry(keys.get(i), i);
            builder.complete();
        }

        // Original reference: exactly the logic of the unpatched addEntry and complete.
        List<byte[]> refPrefixes = new ArrayList<>();
        List<Long> refPositions = new ArrayList<>();
        DecoratedKey last = null;
        int lastDiff = 0;
        long lastPos = -1;
        for (int i = 0; i < keys.size(); i++)
        {
            DecoratedKey key = keys.get(i);
            if (last != null)
            {
                int diff = ByteComparable.diffPoint(last, key, VERSION);
                refPrefixes.add(ByteSourceInverse.readBytes(ByteComparable.cut(last, Math.max(diff, lastDiff)).asComparableBytes(VERSION)));
                refPositions.add(lastPos);
                lastDiff = diff;
            }
            last = key;
            lastPos = i;
        }
        if (last != null)
        {
            refPrefixes.add(ByteSourceInverse.readBytes(ByteComparable.cut(last, lastDiff).asComparableBytes(VERSION)));
            refPositions.add(lastPos);
        }

        String label = keys.size() + "-key set";
        assertEquals("prefix count for " + label, refPrefixes.size(), rec.next.size());
        assertEquals("payload count for " + label, refPositions, rec.positions);
        for (int i = 0; i < refPrefixes.size(); i++)
            assertArrayEquals("prefix " + i + " for " + label, refPrefixes.get(i), rec.next.get(i));

        // Ring lifetime: the prefix read again as the previous key on key k+1 must equal the prefix added on key k.
        // A two-slot ring overwrites that buffer while the next key is encoded, so this fails.
        assertEquals("prev re-read count for " + label, Math.max(0, rec.next.size() - 1), rec.prevReread.size());
        for (int i = 0; i < rec.prevReread.size(); i++)
            assertArrayEquals("prev re-read " + i + " for " + label, rec.next.get(i), rec.prevReread.get(i));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // On-disk lookups against the patched builder.
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void testAdversarialByteKeys() throws IOException
    {
        List<DecoratedKey> keys = new ArrayList<>();
        // single byte keys
        for (int b = 0; b < 8; b++)
            keys.add(base.partitioner.decorateKey(ByteBuffer.wrap(new byte[]{ (byte) b })));
        // empty key
        keys.add(base.partitioner.decorateKey(ByteBufferUtil.EMPTY_BYTE_BUFFER));
        // keys with embedded 0x00 runs, to exercise the escaper
        keys.add(base.partitioner.decorateKey(ByteBuffer.wrap(new byte[]{ 0, 0, 0 })));
        keys.add(base.partitioner.decorateKey(ByteBuffer.wrap(new byte[]{ 0, 1, 0, 2 })));
        keys.add(base.partitioner.decorateKey(ByteBuffer.wrap(new byte[]{ 1, 0, 0, 0, 1 })));
        // shared long prefix
        byte[] prefix = new byte[500];
        new Random(1).nextBytes(prefix);
        for (int i = 0; i < 8; i++)
        {
            byte[] k = prefix.clone();
            k[499] = (byte) i;
            keys.add(base.partitioner.decorateKey(ByteBuffer.wrap(k)));
        }
        // near-max-length key (short length is an unsigned 16-bit value in complete())
        byte[] big = new byte[60000];
        new Random(2).nextBytes(big);
        keys.add(base.partitioner.decorateKey(ByteBuffer.wrap(big)));

        // Each lookup assertion closes the index it is given, so build a fresh index per call.
        List<DecoratedKey> sorted = distinctSorted(keys);
        base.testGetEq(buildIndex(base.partitioner, sorted));
        base.testGetGe(buildIndex(base.partitioner, sorted));
        base.testGetLt(buildIndex(base.partitioner, sorted));
    }

    @Test
    public void testMurmur3TokenKeyCombinations() throws IOException
    {
        Murmur3Partitioner p = Murmur3Partitioner.instance;
        List<DecoratedKey> keys = new ArrayList<>();

        // same key bytes, different tokens
        ByteBuffer sharedKey = ByteBufferUtil.bytes("shared-key-bytes");
        keys.add(new BufferDecoratedKey(new Murmur3Partitioner.LongToken(-500), sharedKey));
        keys.add(new BufferDecoratedKey(new Murmur3Partitioner.LongToken(0), sharedKey));
        keys.add(new BufferDecoratedKey(new Murmur3Partitioner.LongToken(500), sharedKey));

        // same token, different key bytes
        long sharedToken = 12345;
        keys.add(new BufferDecoratedKey(new Murmur3Partitioner.LongToken(sharedToken), ByteBufferUtil.bytes("aaa")));
        keys.add(new BufferDecoratedKey(new Murmur3Partitioner.LongToken(sharedToken), ByteBufferUtil.bytes("bbb")));
        keys.add(new BufferDecoratedKey(new Murmur3Partitioner.LongToken(sharedToken), ByteBufferUtil.bytes("ccc")));

        // real Murmur3 keys, seeded, to fill the index
        Random rand = new Random(3);
        for (int i = 0; i < 5000; i++)
        {
            ByteBuffer bb = ByteBufferUtil.bytes(new UUID(rand.nextLong(), rand.nextLong()));
            keys.add(p.decorateKey(bb));
        }

        List<DecoratedKey> sorted = distinctSorted(keys);
        Pair<List<DecoratedKey>, PartitionIndex> data = buildIndex(p, sorted);
        verifyExactAndRange(data);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------------------------

    /**
     * A recording {@link IncrementalTrieWriter} that does not build a trie.  It reads each added prefix in full when
     * the key is added (next), and reads the previously added prefix again on the following key (prevReread), which is
     * exactly when the real trie reads it again as the previous key.  It records the payload position of each key.
     */
    private static final class RecordingTrieWriter implements IncrementalTrieWriter<PartitionIndex.Payload>
    {
        final List<byte[]> next = new ArrayList<>();
        final List<byte[]> prevReread = new ArrayList<>();
        final List<Long> positions = new ArrayList<>();
        private ByteComparable prev;

        public void add(ByteComparable n, PartitionIndex.Payload value)
        {
            next.add(ByteSourceInverse.readBytes(n.asComparableBytes(VERSION)));
            positions.add(value.position);
            if (prev != null)
                prevReread.add(ByteSourceInverse.readBytes(prev.asComparableBytes(VERSION)));
            prev = n;
        }

        public long count()
        {
            return next.size();
        }

        public long complete()
        {
            return 0;
        }

        public void reset()
        {
        }

        public void close()
        {
        }

        public PartialTail makePartialRoot()
        {
            throw new UnsupportedOperationException();
        }
    }

    private List<List<DecoratedKey>> encodingKeySets()
    {
        List<List<DecoratedKey>> sets = new ArrayList<>();

        // random UUID keys, default partitioner
        Random rand = new Random(11);
        List<DecoratedKey> uuids = new ArrayList<>();
        for (int i = 0; i < 2000; i++)
            uuids.add(base.partitioner.decorateKey(ByteBufferUtil.bytes(new UUID(rand.nextLong(), rand.nextLong()))));
        sets.add(distinctSorted(uuids));

        // shared long prefixes
        List<DecoratedKey> longKeys = new ArrayList<>();
        for (int i = 0; i < 500; i++)
            longKeys.add(base.generateLongKey());
        sets.add(distinctSorted(longKeys));

        // adversarial byte shapes: single byte, empty, embedded 0x00, near-max-length
        List<DecoratedKey> edge = new ArrayList<>();
        for (int b = 0; b < 8; b++)
            edge.add(base.partitioner.decorateKey(ByteBuffer.wrap(new byte[]{ (byte) b })));
        edge.add(base.partitioner.decorateKey(ByteBufferUtil.EMPTY_BYTE_BUFFER));
        edge.add(base.partitioner.decorateKey(ByteBuffer.wrap(new byte[]{ 0, 0, 0 })));
        edge.add(base.partitioner.decorateKey(ByteBuffer.wrap(new byte[]{ 0, 1, 0, 2 })));
        byte[] big = new byte[60000];
        new Random(12).nextBytes(big);
        edge.add(base.partitioner.decorateKey(ByteBuffer.wrap(big)));
        sets.add(distinctSorted(edge));

        // Murmur3 token/key combinations
        List<DecoratedKey> m3 = new ArrayList<>();
        ByteBuffer sharedKey = ByteBufferUtil.bytes("shared");
        m3.add(new BufferDecoratedKey(new Murmur3Partitioner.LongToken(-7), sharedKey));
        m3.add(new BufferDecoratedKey(new Murmur3Partitioner.LongToken(7), sharedKey));
        m3.add(new BufferDecoratedKey(new Murmur3Partitioner.LongToken(3), ByteBufferUtil.bytes("x")));
        m3.add(new BufferDecoratedKey(new Murmur3Partitioner.LongToken(3), ByteBufferUtil.bytes("y")));
        Murmur3Partitioner p = Murmur3Partitioner.instance;
        Random mr = new Random(13);
        for (int i = 0; i < 1000; i++)
            m3.add(p.decorateKey(ByteBufferUtil.bytes(new UUID(mr.nextLong(), mr.nextLong()))));
        sets.add(distinctSorted(m3));

        return sets;
    }

    /**
     * Four ByteOrderedPartitioner keys whose sort order is fixed by the leading byte, independent of length.  Key 0 is
     * long (forces the slot 0 buffer to grow), keys 1 and 2 are short, key 3 is short and reuses slot 0.
     */
    private List<DecoratedKey> longThenShortSameSlot()
    {
        List<DecoratedKey> keys = new ArrayList<>();
        byte[] longKey = new byte[300];
        new Random(21).nextBytes(longKey);
        longKey[0] = 'a';   // sorts first, before 'b', 'c', 'd', regardless of length
        keys.add(base.partitioner.decorateKey(ByteBuffer.wrap(longKey)));
        keys.add(base.partitioner.decorateKey(ByteBuffer.wrap(new byte[]{ 'b' })));
        keys.add(base.partitioner.decorateKey(ByteBuffer.wrap(new byte[]{ 'c' })));
        keys.add(base.partitioner.decorateKey(ByteBuffer.wrap(new byte[]{ 'd' })));
        return keys;
    }

    private static List<DecoratedKey> distinctSorted(List<DecoratedKey> keys)
    {
        TreeSet<DecoratedKey> set = new TreeSet<>(keys);
        return new ArrayList<>(set);
    }

    /**
     * Builds a real partition index over the given sorted, distinct keys with the patched builder.  It uses the same
     * writer and handle setup as {@link PartitionIndexTest#generateIndex}, but loads with the supplied partitioner so
     * non-default (Murmur3) key sets work.
     */
    private Pair<List<DecoratedKey>, PartitionIndex> buildIndex(IPartitioner p, List<DecoratedKey> sorted) throws IOException
    {
        File file = FileUtils.createTempFile("PartitionIndexBuilderReencodeTest", "");
        FileHandle.Builder fhBuilder = base.makeHandle(file);
        try (SequentialWriter writer = base.makeWriter(file);
             PartitionIndexBuilder builder = new PartitionIndexBuilder(writer, fhBuilder))
        {
            for (int i = 0; i < sorted.size(); i++)
                builder.addEntry(sorted.get(i), i);
            builder.complete();
            return Pair.create(sorted, PartitionIndex.load(fhBuilder, p, false));
        }
    }

    /** Exact-match and range checks for a set built with a non-default partitioner. */
    private void verifyExactAndRange(Pair<List<DecoratedKey>, PartitionIndex> data) throws IOException
    {
        List<DecoratedKey> keys = data.left;
        try (PartitionIndex summary = data.right;
             PartitionIndex.Reader reader = summary.openReader())
        {
            assertEquals(keys.size(), summary.size());
            for (int i = 0; i < keys.size(); i++)
            {
                assertEquals("exact candidate for key " + i, i, reader.exactCandidate(keys.get(i)));
                assertEquals("ceiling of key " + i, i, ceiling(keys, keys.get(i), reader));
                assertEquals("floor of key " + i, i, floor(keys, keys.get(i), reader));
            }
        }
    }

    private long ceiling(List<DecoratedKey> keys, DecoratedKey key, PartitionIndex.Reader reader) throws IOException
    {
        Long r = reader.ceiling(key, (pos, assumeNoMatch, sk) -> (assumeNoMatch || keys.get((int) pos).compareTo(sk) >= 0) ? pos : null);
        return r == null ? -1L : r;
    }

    private long floor(List<DecoratedKey> keys, DecoratedKey key, PartitionIndex.Reader reader) throws IOException
    {
        Long r = reader.floor(key, (pos, assumeNoMatch, sk) -> (assumeNoMatch || keys.get((int) pos).compareTo(sk) <= 0) ? pos : null);
        return r == null ? -1L : r;
    }
}
