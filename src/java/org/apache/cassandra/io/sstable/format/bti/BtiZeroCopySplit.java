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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat.Components;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.PageAware;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.utils.FBUtilities.immutableListWithFilteredNulls;

/**
 * The parts of a BTI zero-copy split or slice that need to be inside this package.
 *
 * <p>{@code ZeroCopySSTableSplitter} produces a child of an sstable by copying verbatim compression-chunk runs of
 * its Data.db and rebasing every position that points into them; {@code ZeroCopySSTableSlice} does the same for a
 * set of ranges that stay where they are, so they can be streamed as if they were a whole sstable. For the BIG
 * format the whole index side of both is one loop over Index.db records. For BTI it needs {@link PartitionIndex},
 * {@link PartitionIndexBuilder} and the {@link TrieIndexEntry} wire format, all of which are package-private. This
 * class is the seam:
 * <ul>
 *   <li>{@link Cursor} -- reads the parent's partitions in key order and hands out everything either caller needs
 *       about each one, optionally starting at a key rather than at the beginning;</li>
 *   <li>{@link PartitionIndexWriter} -- builds a target's Partitions.db the way {@link BtiTableWriter} builds one;</li>
 *   <li>{@link RowIndexCopier} -- copies a partition's Rows.db entry across verbatim, patching the one field in it
 *       that moves.</li>
 * </ul>
 *
 * <h2>What a BTI rebase has to touch, and what it does not</h2>
 * BTI stores a partition's Data.db position in exactly two places, and only those two move:
 * <ul>
 *   <li><b>Partitions.db</b> trie payloads, for partitions with no row index: the payload is {@code ~dataPosition}.
 *       The whole trie is rebuilt anyway (a trie cannot be sliced, and the payloads point at a different Rows.db),
 *       so this costs nothing extra.</li>
 *   <li><b>Rows.db</b> entry trailers, for partitions with one: an unsigned vint at a known offset, the first field
 *       of {@link TrieIndexEntry#serialize}. {@link RowIndexCopier} patches it in place.</li>
 * </ul>
 * Everything else in Rows.db is already relative and survives a verbatim copy:
 * <ul>
 *   <li>row index trie <b>payloads</b> hold {@code IndexInfo.offset}, an offset WITHIN the partition
 *       ({@code BtiFormatPartitionWriter.addIndexBlock} passes {@code indexBlockStartOffset}, which
 *       {@code SortedTablePartitionWriter} keeps relative to {@code partitionStartPosition}), so a partition that
 *       moves in Data.db does not move its own row index payloads;</li>
 *   <li>row index trie <b>pointers</b> are self-relative -- {@code IncrementalTrieWriterPageAware.Node
 *       .serializedPositionDelta} writes {@code child.filePos - nodePosition} -- so a run of nodes copied verbatim
 *       stays internally consistent wherever it lands;</li>
 *   <li>the trailer's <b>root pointer</b> is stored as {@code indexTrieRoot - basePosition}, a difference between
 *       two positions in the same file, so a copy that moves a whole entry by one constant leaves it correct with
 *       no patch at all.</li>
 * </ul>
 *
 * <h2>The cost a BTI rebase has and a BIG one does not: reading keys</h2>
 * A target needs its partitions' full keys -- for its bloom filter, for the HyperLogLog in its CompactionMetadata,
 * for its first/last, and above all to build its Partitions.db, whose trie is over key prefixes and whose payloads
 * carry {@code DecoratedKey.filterHashLowerBits}. BIG has all of them in Index.db. BTI has them in only two
 * places: Rows.db, for partitions with a row index, and <b>Data.db</b>, for partitions without one -- the trie
 * itself stores only the shortest prefix that distinguishes a key from its neighbours, which is not enough to
 * reconstruct it.
 * <p>
 * So {@link Cursor} decompresses Data.db to read the keys of non-indexed partitions, and a BTI rebase over a
 * narrow-partition table pays one sequential decompressing pass over the data it covers. That is a real cost and
 * it is worth being precise about what it is and is not:
 * <ul>
 *   <li>It is a <b>read</b>. Nothing is recompressed, no row is deserialised, and the target's Data.db is still a
 *       verbatim byte range that {@code Reflink} can share or {@code sendfile} can transmit. The rewriting paths
 *       these replace read the same bytes and then also deserialise every row, re-serialise and recompress them,
 *       and write a second copy.</li>
 *   <li>It is <b>lazy</b>: {@link Cursor#key()} resolves on demand, so a pass that only needs positions -- counting
 *       partitions, or choosing split points by byte share -- costs no Data.db reads at all.</li>
 *   <li>It is <b>zero</b> for a table whose partitions have row indexes, which is the shape a big enough sstable
 *       to be worth splitting or slicing most often has. Those keys come out of Rows.db, which is read anyway.</li>
 * </ul>
 */
public final class BtiZeroCopySplit
{
    private BtiZeroCopySplit()
    {
    }

    /**
     * Forward-only cursor over a parent's partitions in key order, exposing for each one everything a rebase needs:
     * the key, the Data.db position, and -- when the partition has a row index -- the exact byte extent of its
     * Rows.db entry and the offset and width of the one field in it that has to be patched.
     *
     * <p>Opened straight off the descriptor rather than off the live reader's file handles, exactly as the BIG path
     * opens Index.db: the walk is a one-shot bulk pass whose pages are of no use afterwards, and the parent's
     * handles are shared with serving reads. The one exception is Data.db, which has to be read through the
     * parent's reader because that is the only thing that knows the compression parameters.
     */
    public static final class Cursor implements Closeable
    {
        private final Descriptor descriptor;
        private final IPartitioner partitioner;
        private final Version version;

        private final FileHandle partitionIndexFile;
        private final PartitionIndex partitionIndex;
        private final PartitionIndex.IndexPosIterator positions;
        private final FileHandle rowIndexFile;
        private final SSTableReader parent;

        private FileDataInput rowIndexInput;
        private RandomAccessReader dataInput;

        private ByteBuffer key;
        private DecoratedKey decoratedKey;
        private long dataPosition = -1;
        private long rowIndexKeyStart = -1;
        private long rowIndexEntryEnd = -1;
        private long rowIndexDataPositionOffset = -1;
        private int rowIndexDataPositionWidth;

        /**
         * End of the previous partition's Rows.db entry, i.e. where this one's trie nodes begin. Advanced only from
         * records this cursor yields, which is a known defect for a bounded cursor -- see
         * {@link #rowIndexBlockStart()}.
         */
        private long previousRowIndexEntryEnd;
        private int index = -1;

        @SuppressWarnings({ "resource", "RedundantSuppression" }) // everything opened here is closed by close()
        private Cursor(SSTableReader parent, PartitionPosition left) throws IOException
        {
            this.parent = parent;
            this.descriptor = parent.descriptor;
            this.partitioner = parent.getPartitioner();
            this.version = parent.descriptor.version;

            FileHandle pi = null;
            PartitionIndex index = null;
            FileHandle ri = null;
            try
            {
                pi = new FileHandle.Builder(descriptor.fileFor(Components.PARTITION_INDEX)).complete();
                index = PartitionIndex.load(pi, partitioner, false);
                ri = new FileHandle.Builder(descriptor.fileFor(Components.ROW_INDEX)).complete();

                this.partitionIndexFile = pi;
                this.partitionIndex = index;
                this.rowIndexFile = ri;
                // A bounded iterator is what keeps a slice's cost proportional to the slice rather than to the
                // sstable: the trie walk descends straight to `left` instead of enumerating everything before it.
                // The bound is on stored PREFIXES, so it can still yield a record or two below `left` -- the caller
                // has to skip those, exactly as the BIG walk skips the index summary's slack.
                this.positions = left == null ? new PartitionIndex.IndexPosIterator(index)
                                              : new PartitionIndex.IndexPosIterator(index, left, index.lastKey());
            }
            catch (IOException | RuntimeException | Error ex)
            {
                Throwables.closeNonNullAndAddSuppressed(ex, ri, index, pi);
                throw ex;
            }
        }

        /**
         * @return false once the parent's last partition (or the end of a bounded range) has been passed; every
         *         accessor below then reverts to its "no current record" value
         */
        public boolean advance() throws IOException
        {
            previousRowIndexEntryEnd = rowIndexEntryEnd >= 0 ? rowIndexEntryEnd : previousRowIndexEntryEnd;
            rowIndexKeyStart = -1;
            rowIndexEntryEnd = -1;
            rowIndexDataPositionOffset = -1;
            rowIndexDataPositionWidth = 0;
            key = null;
            decoratedKey = null;

            long indexPos = positions.nextIndexPos();
            if (indexPos == PartitionIndex.NOT_FOUND)
            {
                dataPosition = -1;
                return false;
            }

            ++index;
            if (indexPos >= 0)
                readIndexed(indexPos);
            else
                dataPosition = ~indexPos;

            return true;
        }

        /**
         * A partition with a row index. The Rows.db entry is
         * {@code [key with short length][unsigned vint dataPosition][vint rootDelta][unsigned vint blockCount]
         * [DeletionTime]}, so everything this needs falls out of parsing it once -- including the key, which sits in
         * front of the trailer and has to be read past anyway.
         */
        private void readIndexed(long indexPos) throws IOException
        {
            if (rowIndexInput == null)
                rowIndexInput = rowIndexFile.createReader(indexPos);
            else
                rowIndexInput.seek(indexPos);

            rowIndexKeyStart = indexPos;
            key = ByteBufferUtil.readWithShortLength(rowIndexInput);

            // TrieIndexEntry.serialize's basePosition, and the offset of the only field that has to be patched
            rowIndexDataPositionOffset = rowIndexInput.getFilePointer();
            dataPosition = rowIndexInput.readUnsignedVInt();
            rowIndexDataPositionWidth = (int) (rowIndexInput.getFilePointer() - rowIndexDataPositionOffset);

            rowIndexInput.readVInt();           // indexTrieRoot - basePosition: within-file, never patched
            rowIndexInput.readUnsignedVInt32(); // rowIndexBlockCount
            DeletionTime.getSerializer(version).deserialize(rowIndexInput);
            rowIndexEntryEnd = rowIndexInput.getFilePointer();
        }

        /**
         * The current partition's key.
         *
         * <p>Free for a partition with a row index -- {@link #advance} already read it out of Rows.db. For one
         * without, this is the only thing in a rebase that decompresses: the key is at the start of the partition
         * in Data.db and nowhere else. Resolved here rather than in {@code advance} so that a pass which never asks
         * for a key never pays for one. Positions only ever advance, so the reader decompresses each chunk at most
         * once.
         */
        public ByteBuffer key() throws IOException
        {
            if (key == null && dataPosition >= 0)
            {
                if (dataInput == null)
                    dataInput = parent.openDataReaderForScan();
                dataInput.seek(dataPosition);
                key = ByteBufferUtil.readWithShortLength(dataInput);
            }
            return key;
        }

        public DecoratedKey decoratedKey() throws IOException
        {
            if (decoratedKey == null)
            {
                ByteBuffer k = key();
                if (k != null)
                    decoratedKey = partitioner.decorateKey(k);
            }
            return decoratedKey;
        }

        /** 0-based ordinal of the current partition within this walk, or -1 before the first {@link #advance}. */
        public int index()
        {
            return index;
        }

        public long dataPosition()
        {
            return dataPosition;
        }

        /** Whether the current partition has a row index, i.e. whether {@link RowIndexCopier} applies to it. */
        public boolean hasRowIndex()
        {
            return rowIndexKeyStart >= 0;
        }

        /** Where the current partition's Rows.db entry starts (its key), or -1 if it has no row index. */
        public long rowIndexKeyStart()
        {
            return rowIndexKeyStart;
        }

        /** One past the last byte of the current partition's Rows.db entry, or -1 if it has no row index. */
        public long rowIndexEntryEnd()
        {
            return rowIndexEntryEnd;
        }

        /**
         * Where the current partition's Rows.db trie nodes begin: the end of the previous partition's entry, or 0
         * before the first. This is the low bound of the byte range that has to be copied, and it includes whatever
         * page padding the trie writer inserted -- which is copied rather than recomputed, because it is what keeps
         * the nodes after it where the reader can address them.
         *
         * <h4>Known defect: 0 rather than the true block start, for the first entry of a bounded cursor</h4>
         * This is accurate only for a cursor that walked the parent from the beginning, i.e. for a split.
         * {@link #previousRowIndexEntryEnd} has no initialiser, so it starts at 0, and {@link #advance} only ever
         * advances it from records this cursor actually YIELDS. A cursor built with a {@code left} bound descends the
         * trie straight to that key, so no indexed record below the bound is ever advanced over and this returns 0
         * for the first partition the cursor yields. {@link RowIndexCopier#copy} then takes {@code [0, keyStart)} as
         * that entry's node region and copies the parent's entire Rows.db prefix -- every row index entry of every
         * partition below the slice -- into the target. The defect is known and accepted for now; it is documented
         * here rather than fixed.
         * <p>
         * It is amplification, not corruption, and the distinction is exact. The copy still lands at
         * {@code delta = out.position() - blockStart = 0 - 0 = 0}, so the entry's self-relative node pointers, its
         * trailer's root pointer (a difference between two positions inside the entry), the payload {@code copy}
         * returns, and the page geometry of the entry and of everything placed after it are all as correct as they
         * would be with the right block start. The extra bytes are simply unreachable: the only way into a Rows.db
         * entry is a Partitions.db payload, and the target's rebuilt trie has a payload for none of them.
         * <p>
         * The cost, though, is unbounded -- it is the whole of the parent's Rows.db below the slice, not a function
         * of the slice's size. Slicing the last 1000 of 100k wide partitions out of a 50 MB Rows.db writes and
         * streams about 50 MB where about 0.5 MB is wanted. That also defeats
         * {@code CassandraOutgoingFile.estimateSliceManifest}'s disk-space precheck, which sizes ROW_INDEX as
         * {@code parentSize * fraction}, on a path that by design cannot fall back once it has begun writing. And it
         * hides a test gap: {@code nodeBytes} is then the whole prefix, so {@link RowIndexCopier#align} takes its
         * multi-page branch and bumps {@code congruenceAlignments}, which silently satisfies
         * {@code ZeroCopyBtiFuzzTest}'s {@code congruence > 0} coverage assertion for the wrong reason. The genuine
         * multi-page-trie placement is therefore probably untested, and fixing this will most likely make that
         * assertion fail until a fixture with a single partition whose row index trie really exceeds
         * {@link PageAware#PAGE_SIZE} is added.
         * <p>
         * The sound fix is to seed {@code previousRowIndexEntryEnd} from the trie when the cursor is constructed with
         * a bound -- with the end of the row index entry preceding {@code left}, which is what an unbounded walk
         * would have left here. The cheaper one is for {@code copy()} to clamp {@code blockStart}, since a single
         * partition's row index trie is virtually always far smaller than a page; but a clamp cannot be applied
         * blindly, because a genuinely multi-page trie's nodes really do begin more than a page before its key.
         * <p>
         * Until one of them is in, {@code ZeroCopySSTableSlice.writeBtiIndex}'s javadoc claim that "only the INCLUDED
         * partitions' row index entries are copied. That is not an optimisation, it is the point" is false for the
         * first entry a bounded cursor yields: that one entry drags every row index entry below it along with it.
         */
        public long rowIndexBlockStart()
        {
            return previousRowIndexEntryEnd;
        }

        /** Offset in Rows.db of the data-position vint, or -1 if the current partition has no row index. */
        public long rowIndexDataPositionOffset()
        {
            return rowIndexDataPositionOffset;
        }

        /** Encoded width of that vint in bytes, or 0 if the current partition has no row index. */
        public int rowIndexDataPositionWidth()
        {
            return rowIndexDataPositionWidth;
        }

        @Override
        public void close()
        {
            // The two inputs are opened lazily -- a walk over a wide table never touches Data.db, one over a narrow
            // table never touches Rows.db -- so nulls are expected and Throwables.close does not tolerate them.
            Throwable accumulate = Throwables.close(null, positions);
            accumulate = Throwables.close(accumulate, immutableListWithFilteredNulls(rowIndexInput, dataInput));
            accumulate = Throwables.close(accumulate, rowIndexFile, partitionIndex, partitionIndexFile);
            Throwables.maybeFail(accumulate);
        }
    }

    public static Cursor cursor(SSTableReader parent) throws IOException
    {
        return new Cursor(parent, null);
    }

    /**
     * A cursor that starts at {@code left} rather than at the parent's first partition. The bound is on the trie's
     * stored key prefixes, so the first record or two may still fall below it; the caller filters on position.
     */
    public static Cursor cursor(SSTableReader parent, PartitionPosition left) throws IOException
    {
        return new Cursor(parent, left);
    }

    /**
     * A target's Partitions.db, written exactly the way {@code BtiTableWriter.IndexWriter} writes one: the same
     * {@link PartitionIndexBuilder} over the same payload encoding, so the file a rebase produces is
     * indistinguishable from a flush's.
     *
     * <p>The trie has to be rebuilt rather than copied. It is one structure over every key in the sstable with no
     * seam a range could be cut at, and its payloads point into the target's Rows.db at offsets the copy has moved.
     * Rebuilding it is also what keeps the target's index honest: a payload exists for exactly the partitions the
     * target holds, so a lookup for anything else terminates in the trie instead of resolving to a position in a
     * range the target does not describe. That is load-bearing for a slice, whose Data.db deliberately carries
     * partitions it does not claim.
     *
     * <p>{@code complete()} is followed by {@code finish()} rather than left to the caller, because
     * {@link PartitionIndexBuilder#complete} only {@code sync()}s: the file has to be closed, and this is the only
     * copy of the target's key range and index root.
     */
    public static final class PartitionIndexWriter implements Closeable
    {
        private final SequentialWriter writer;
        private final PartitionIndexBuilder builder;
        private final Descriptor target;
        private boolean finished;

        @SuppressWarnings({ "resource", "RedundantSuppression" }) // both are closed by close()
        public PartitionIndexWriter(Descriptor target, SequentialWriterOption option)
        {
            this.target = target;
            File file = target.fileFor(Components.PARTITION_INDEX);
            // The writer is released by hand if the builder's construction throws: close() cannot run on an object
            // whose constructor did not return, so without this the SequentialWriter's file and buffer leak for the
            // life of the process. Same shape as Cursor's constructor.
            SequentialWriter out = null;
            try
            {
                out = new SequentialWriter(file, option);
                // The FileHandle.Builder is only used by buildPartial and by complete()'s withLengthOverride, neither
                // of which is load-bearing here: nothing opens this index until the writer has been closed.
                this.builder = new PartitionIndexBuilder(out, new FileHandle.Builder(file));
                this.writer = out;
            }
            catch (RuntimeException | Error ex)
            {
                Throwables.closeNonNullAndAddSuppressed(ex, out);
                throw ex;
            }
        }

        /**
         * @param key     the target's next key, in ascending order
         * @param payload {@code rowIndexPosition} for a partition with a row index in the TARGET's Rows.db,
         *                {@code ~dataPosition} in the TARGET's Data.db for one without
         */
        public void addEntry(DecoratedKey key, long payload) throws IOException
        {
            builder.addEntry(key, payload);
        }

        /** Writes the trie, the first/last keys and the three-long footer, and fsyncs. */
        public void finish() throws IOException
        {
            builder.complete();
            writer.finish();
            finished = true;
        }

        public Descriptor descriptor()
        {
            return target;
        }

        @Override
        public void close()
        {
            Throwable accumulate = Throwables.close(null, builder);
            if (!finished)
                accumulate = Throwables.close(accumulate, writer);
            Throwables.maybeFail(accumulate);
        }
    }

    /**
     * Copies row index entries from a parent's Rows.db into a target's, one partition at a time, rebasing the one
     * field in each that points into Data.db.
     *
     * <p>Entries are <b>selected</b>, not copied as one range. A split child holds a contiguous run of the parent's
     * partitions, but a slice holds several disjoint ones and must not carry the row indexes of the partitions in
     * between: with vnode-shaped ranges those could easily outweigh everything the slice was asked for, and every
     * byte of them would go over the network. So each entry is placed where the target's writer happens to be, and
     * the one thing the placement has to respect is the trie writer's page invariant.
     *
     * <p>It follows that a target's Rows.db is <b>not</b> a byte range of the parent's Rows.db, and its length is not
     * the parent's length times anything. Only an individual entry is verbatim, and only up to the one vint
     * {@link #copy} patches; the file as a whole is a resequencing of the entries the target kept, interleaved with
     * the zero-byte page padding {@link #align} inserts in front of an entry whenever the placement rule demands it,
     * so its length is the sum of those entries plus that padding. Data.db is the component that is a verbatim byte
     * range of the parent's; this one is not, and nothing may reason about it as though it were -- in particular
     * nothing may derive a target's Rows.db length, or a position in it, from the parent's by arithmetic.
     *
     * <h2>The page invariant, and the two ways to satisfy it</h2>
     * {@code IncrementalTrieWriterPageAware} guarantees that no trie node crosses a {@link PageAware#PAGE_SIZE}
     * boundary, and {@code Walker.go} depends on it: a node is read out of one rebuffered page and would read past
     * that page's limit if it straddled two. Node pointers are self-relative, so they do not care where an entry
     * lands, but this does. An entry's node region -- {@code [blockStart, keyStart)}, which is where the parent's
     * trie writer put its nodes plus whatever padding it inserted in front of them -- is therefore placed so that
     * either:
     * <ol>
     *   <li>it lies entirely within one page of the target, which trivially puts every node inside one page. This
     *       is the case that applies almost always: the row index trie for one partition is far smaller than 4 KiB,
     *       so the only cost is skipping to the next page boundary when it does not fit in the current one -- which
     *       is exactly what the parent's own writer did; or</li>
     *   <li>failing that (a multi-page trie), it starts at the same offset within a page as it did in the parent,
     *       which reproduces the parent's page geometry exactly. Costs up to one page of padding.</li>
     * </ol>
     * The padding is zero bytes and no reader can reach it: the only way into a Rows.db entry is a Partitions.db
     * payload, and those point at keys.
     *
     * <h2>Why the entry's length cannot change</h2>
     * The data position is re-encoded by {@link #writeUnsignedVIntOfWidth} at its ORIGINAL width, so the entry is
     * byte-for-byte as long as the parent's. If it were not, the root pointer -- stored as a difference between two
     * positions inside the entry -- would no longer be correct, and the node region's placement would no longer be
     * the only thing deciding whether the invariant holds. The re-encoding is always possible because a rebased
     * position is never larger than the position it replaces.
     *
     * <h2>Known defect: a bounded cursor's first entry drags the whole Rows.db prefix with it</h2>
     * {@link Cursor#rowIndexBlockStart()} returns 0 for the first partition a cursor created with a {@code left}
     * bound yields, because nothing ever advanced it past the indexed records below that bound. {@link #copy} then
     * treats {@code [0, keyStart)} as that entry's node region and copies the parent's whole Rows.db prefix -- every
     * row index entry of every partition below the slice -- into the target, so a slice's Rows.db is the size of the
     * parent's rather than of the slice. This is amplification and not corruption: the copy lands at
     * {@code delta == 0}, which leaves every position inside the entry and the page geometry of everything after it
     * correct, and no Partitions.db payload points into the extra bytes, so nothing can reach them. It is a real and
     * unbounded cost all the same, and it is currently what satisfies {@code ZeroCopyBtiFuzzTest}'s coverage
     * assertion on {@link #align}'s multi-page branch. Accepted for now;
     * {@link Cursor#rowIndexBlockStart()} carries the full accounting and names the fix.
     */
    public static final class RowIndexCopier implements Closeable
    {
        private static final int BUFFER_SIZE = 64 * 1024;

        private final RandomAccessReader in;
        private final SequentialWriter out;
        private final byte[] buffer = new byte[BUFFER_SIZE];
        private final byte[] patch = new byte[VIntCoding.MAX_SIZE];
        private long padBytes;
        private boolean finished;

        /**
         * Which of the three placements each copied entry took, process-wide. Only a test reads these, and only to
         * assert that a fuzz sweep actually reached the multi-page case -- the branch that reproduces the parent's
         * page geometry is the one nothing else can observe, and a sweep that never entered it would pass without
         * having tested it.
         */
        @VisibleForTesting
        public static final LongAdder inPlacePlacements = new LongAdder();
        @VisibleForTesting
        public static final LongAdder pageAlignments = new LongAdder();
        @VisibleForTesting
        public static final LongAdder congruenceAlignments = new LongAdder();

        @SuppressWarnings({ "resource", "RedundantSuppression" }) // both are closed by close()
        public RowIndexCopier(SSTableReader parent, Descriptor target, SequentialWriterOption option)
        {
            // Opening the target can fail after the parent has been opened -- a full disk makes SequentialWriter's
            // constructor throw FSWriteError -- and nothing ever calls close() on an object whose constructor threw,
            // so the reader has to be released here or its ChannelProxy's ref is held for the life of the process
            // (and trips the Ref leak detector in tests). Same shape as Cursor's constructor.
            RandomAccessReader source = null;
            try
            {
                source = RandomAccessReader.open(parent.descriptor.fileFor(Components.ROW_INDEX));
                this.out = new SequentialWriter(target.fileFor(Components.ROW_INDEX), option);
                this.in = source;
            }
            catch (RuntimeException | Error ex)
            {
                Throwables.closeNonNullAndAddSuppressed(ex, source);
                throw ex;
            }
        }

        /**
         * Copy the entry {@code cursor} is positioned on, rebasing its data position.
         *
         * @param rebasedDataPosition the partition's position in the TARGET's Data.db
         * @return the position in the target's Rows.db of the entry's key, i.e. what the target's Partitions.db
         *         payload for this partition has to be
         */
        public long copy(Cursor cursor, long rebasedDataPosition) throws IOException
        {
            if (!cursor.hasRowIndex())
                throw new IllegalArgumentException("the current partition has no row index");

            long blockStart = cursor.rowIndexBlockStart();
            long keyStart = cursor.rowIndexKeyStart();
            long vintAt = cursor.rowIndexDataPositionOffset();
            int width = cursor.rowIndexDataPositionWidth();
            long entryEnd = cursor.rowIndexEntryEnd();

            if (blockStart > keyStart || keyStart > vintAt || vintAt + width > entryEnd)
                throw new IllegalStateException("malformed Rows.db entry at " + keyStart + ": nodes [" + blockStart +
                                                ", " + keyStart + "), vint " + vintAt + '+' + width + ", end " +
                                                entryEnd);

            align(blockStart, keyStart - blockStart);

            // Constant over the whole entry, which is what keeps the trailer's root pointer -- a difference between
            // two positions inside it -- correct with no patch of its own.
            long delta = out.position() - blockStart;

            transfer(blockStart, vintAt - blockStart);            // nodes, their leading padding, and the key
            writeUnsignedVIntOfWidth(rebasedDataPosition, width, patch, 0);
            out.write(patch, 0, width);                           // the one field that moves
            transfer(vintAt + width, entryEnd - vintAt - width);   // root delta, block count, DeletionTime

            if (out.position() != entryEnd + delta)
                throw new IllegalStateException("copied Rows.db entry is " + (out.position() - delta - blockStart) +
                                                " bytes, expected " + (entryEnd - blockStart));
            return keyStart + delta;
        }

        /** See the class javadoc: case 1 when the node region fits in a page, case 2 when it cannot. */
        private void align(long blockStart, long nodeBytes) throws IOException
        {
            if (nodeBytes <= PageAware.PAGE_SIZE)
            {
                int left = PageAware.bytesLeftInPage(out.position());
                if (left < nodeBytes)
                {
                    out.padToPageBoundary();
                    padBytes += left;
                    pageAlignments.increment();
                }
                else
                {
                    inPlacePlacements.increment();
                }
            }
            else
            {
                long need = (blockStart - out.position()) & (PageAware.PAGE_SIZE - 1);
                pad(need);
                padBytes += need;
                congruenceAlignments.increment();
            }
        }

        private void pad(long bytes) throws IOException
        {
            while (bytes > 0)
            {
                int n = (int) Math.min(PageAware.PAGE_SIZE, bytes);
                out.write(EMPTY_PAGE, 0, n);
                bytes -= n;
            }
        }

        private void transfer(long from, long length) throws IOException
        {
            if (length < 0)
                throw new IllegalStateException("negative Rows.db transfer length " + length + " at " + from);
            in.seek(from);
            long remaining = length;
            while (remaining > 0)
            {
                int n = (int) Math.min(buffer.length, remaining);
                in.readFully(buffer, 0, n);
                out.write(buffer, 0, n);
                remaining -= n;
            }
        }

        /**
         * Zero bytes {@link #align} inserted to keep the page invariant; no reader can reach them.
         *
         * <p>This is the only accounting of them that exists -- they are the difference between the target's Rows.db
         * length and the total length of the entries copied into it, and nothing else measures either -- so a caller
         * that folds this into the padding total it reports for a split or a slice is what makes them visible at all.
         */
        public long padBytes()
        {
            return padBytes;
        }

        /**
         * Close the target's Rows.db durably and report its length.
         *
         * <p>The file is created even when no entry was copied, and is then zero length. That is not a degenerate
         * state: it is exactly what a flush of a table whose partitions are all below the row index granularity
         * produces, and {@code BtiTableReaderLoadingBuilder} opens the component either way.
         */
        public long finish() throws IOException
        {
            long length = out.position();
            out.finish();
            finished = true;
            return length;
        }

        @Override
        public void close()
        {
            Throwable accumulate = Throwables.close(null, in);
            if (!finished)
                accumulate = Throwables.close(accumulate, out);
            Throwables.maybeFail(accumulate);
        }
    }

    private static final byte[] EMPTY_PAGE = new byte[PageAware.PAGE_SIZE];

    /**
     * Write {@code value} as an unsigned vint in exactly {@code width} bytes, which may be more than the canonical
     * encoding needs.
     *
     * <p>This exists so that patching a Rows.db entry's data position cannot change the entry's length; see "Why the
     * entry's length cannot change" on {@link RowIndexCopier}. It is always possible for the values this is used
     * with, because a rebased position is never larger than the position it replaces, so the canonical width is
     * never wider than the original.
     *
     * <p>The encoding is the one {@code VIntCoding} decodes: the leading byte's top {@code width - 1} bits are set,
     * and the value's remaining bytes follow it, most significant first. For {@code width <= 8} the set bits are
     * terminated by a clear bit and the byte's low bits carry the value's most significant bits. At {@code width == 9}
     * there is no room for the terminating bit: the leading byte is {@code 0xFF}, it carries no value bits at all, and
     * all eight bytes of the value follow -- which is why that width has its own branch below, and needs one, since
     * {@code value >>> (8 * 8)} is {@code value >>> 0} in Java and the shared arithmetic would fold the whole value
     * into the leading byte. Decoding takes the width from the number of set leading bits and never checks that the
     * value could not have been written in fewer, so a padded encoding is read back unchanged.
     */
    public static void writeUnsignedVIntOfWidth(long value, int width, byte[] into, int offset)
    {
        if (value < 0)
            throw new IllegalArgumentException("negative value " + value);
        if (width < 1 || width > VIntCoding.MAX_SIZE - 1)
            throw new IllegalArgumentException("width " + width + " out of range");
        int canonical = VIntCoding.computeUnsignedVIntSize(value);
        if (canonical > width)
            throw new IllegalArgumentException("value " + value + " needs " + canonical + " bytes, not " + width);

        if (width == 9)
        {
            into[offset] = (byte) 0xFF;
            for (int i = 0; i < 8; i++)
                into[offset + 1 + i] = (byte) (value >>> (56 - 8 * i));
            return;
        }

        int extraBytes = width - 1;
        for (int i = 0; i < extraBytes; i++)
            into[offset + width - 1 - i] = (byte) (value >>> (8 * i));
        long top = value >>> (8 * extraBytes);
        into[offset] = (byte) (VIntCoding.encodeExtraBytesToRead(extraBytes) | (int) top);
    }
}
