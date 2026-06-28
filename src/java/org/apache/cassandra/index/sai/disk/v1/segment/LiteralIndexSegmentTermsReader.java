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
package org.apache.cassandra.index.sai.disk.v1.segment;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;

import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.QueryCancelledException;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.disk.v1.postings.MergePostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.index.sai.disk.v1.trie.TrieTermsDictionaryReader;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.index.sai.disk.v1.SAICodecUtils.validate;

/**
 * Synchronous reader of terms dictionary and postings lists to produce a {@link PostingList} with matching row ids.
 *
 * {@link #exactMatch(ByteComparable, QueryEventListener.TrieIndexEventListener, QueryContext)} does:
 * <ul>
 * <li>{@link TermQuery#lookupPostingsOffset(ByteComparable)}: does term dictionary lookup to find the posting list file
 * position</li>
 * <li>{@link TermQuery#getPostingsReader(long)}: reads posting list block summary and initializes posting read which
 * reads the first block of the posting list into memory</li>
 * </ul>
 */
public class LiteralIndexSegmentTermsReader implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(LiteralIndexSegmentTermsReader.class);

    private final IndexIdentifier indexIdentifier;
    private final FileHandle termDictionaryFile;
    private final FileHandle postingsFile;
    private final long termDictionaryRoot;
    private final boolean isV2;

    public LiteralIndexSegmentTermsReader(IndexIdentifier indexIdentifier,
                                          FileHandle termsData,
                                          FileHandle postingLists,
                                          long root,
                                          long termsFooterPointer) throws IOException
    {
        this(indexIdentifier, termsData, postingLists, root, termsFooterPointer, false);
    }

    public LiteralIndexSegmentTermsReader(IndexIdentifier indexIdentifier,
                                          FileHandle termsData,
                                          FileHandle postingLists,
                                          long root,
                                          long termsFooterPointer,
                                          boolean isV2) throws IOException
    {
        this.indexIdentifier = indexIdentifier;
        termDictionaryFile = termsData;
        postingsFile = postingLists;
        termDictionaryRoot = root;
        this.isV2 = isV2;

        try (final IndexInput indexInput = IndexFileUtils.instance.openInput(termDictionaryFile))
        {
            validate(indexInput, termsFooterPointer);
        }

        try (final IndexInput indexInput = IndexFileUtils.instance.openInput(postingsFile))
        {
            validate(indexInput);
        }
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(termDictionaryFile);
        FileUtils.closeQuietly(postingsFile);
    }

    public PostingList exactMatch(ByteComparable term, QueryEventListener.TrieIndexEventListener perQueryEventListener, QueryContext context)
    {
        perQueryEventListener.onSegmentHit();
        return new TermQuery(term, perQueryEventListener, context).execute();
    }

    /**
     * Returns a posting list of all rows whose indexed term starts with the queried prefix (this includes a row whose
     * term equals the prefix exactly). Requires a V2 (prefix-enabled) segment.
     *
     * @param start the prefix term (inclusive lower bound of the trie scan)
     * @param end   the lexicographic successor of the prefix, or null for an unbounded upper bound
     */
    public PostingList prefixMatch(ByteComparable start, ByteComparable end,
                                   QueryEventListener.TrieIndexEventListener perQueryEventListener, QueryContext context)
    {
        perQueryEventListener.onSegmentHit();
        return new PrefixQuery(start, end, perQueryEventListener, context).execute();
    }

    /**
     * Counts nodes visited by {@link PrefixQuery#collectFromNode} during a prefix traversal.
     * Populated only when passed to {@link #prefixMatchWithStats}; the production
     * {@link #prefixMatch} path passes {@code null} and pays zero overhead.
     */
    @VisibleForTesting
    public static class TraversalStats
    {
        /** Nodes where {@code suffixIndex > prefixIndex}: combined section used, subtree skipped. */
        public int combinedSectionHits;
        /** Nodes where {@code prefixIndex > 0} but no combined section: exact section read, recursion continued. */
        public int exactSectionHits;
        /** Nodes with no payload: recursion only. */
        public int emptyNodes;
    }

    /**
     * Like {@link #prefixMatch} but also populates {@code stats} with counts of each traversal
     * branch taken. For use in tests only.
     */
    @VisibleForTesting
    public PostingList prefixMatchWithStats(ByteComparable start, ByteComparable end,
                                            QueryEventListener.TrieIndexEventListener listener,
                                            QueryContext context,
                                            TraversalStats stats)
    {
        listener.onSegmentHit();
        return new PrefixQuery(start, end, listener, context, stats).execute();
    }

    @VisibleForTesting
    public class TermQuery
    {
        private final IndexInput postingsInput;
        private final IndexInput postingsSummaryInput;
        private final QueryEventListener.TrieIndexEventListener listener;
        private final long lookupStartTime;
        private final QueryContext context;
        private final ByteComparable term;

        TermQuery(ByteComparable term, QueryEventListener.TrieIndexEventListener listener, QueryContext context)
        {
            this.listener = listener;
            postingsInput = IndexFileUtils.instance.openInput(postingsFile);
            postingsSummaryInput = IndexFileUtils.instance.openInput(postingsFile);
            this.term = term;
            lookupStartTime = Clock.Global.nanoTime();
            this.context = context;
        }

        public PostingList execute()
        {
            try
            {
                long postingOffset = lookupPostingsOffset(term);
                if (postingOffset == PostingList.OFFSET_NOT_FOUND)
                {
                    FileUtils.closeQuietly(postingsInput);
                    FileUtils.closeQuietly(postingsSummaryInput);
                    return null;
                }

                context.checkpoint();

                // when posting is found, resources will be closed when posting reader is closed.
                return getPostingsReader(postingOffset);
            }
            catch (Throwable e)
            {
                if (!(e instanceof QueryCancelledException))
                    logger.error(indexIdentifier.logMessage("Failed to execute term query"), e);

                closeOnException();
                throw Throwables.cleaned(e);
            }
        }

        private void closeOnException()
        {
            FileUtils.closeQuietly(postingsInput);
            FileUtils.closeQuietly(postingsSummaryInput);
        }

        public long lookupPostingsOffset(ByteComparable term)
        {
            try (TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(null), termDictionaryRoot))
            {
                final long offset = reader.exactMatch(term);

                listener.onTraversalComplete(Clock.Global.nanoTime() - lookupStartTime, TimeUnit.NANOSECONDS);

                if (offset == TrieTermsDictionaryReader.NOT_FOUND)
                    return PostingList.OFFSET_NOT_FOUND;

                return offset;
            }
        }

        public PostingsReader getPostingsReader(long offset) throws IOException
        {
            PostingsReader.BlocksSummary header = new PostingsReader.BlocksSummary(postingsSummaryInput, offset, isV2);

            if (isV2)
                return PostingsReader.exactSection(postingsInput, header, listener.postingListEventListener());

            return new PostingsReader(postingsInput, header, listener.postingListEventListener());
        }
    }

    /**
     * Collects, from every term in the trie range {@code [start, end]}, a posting list of the rows for that term, and
     * merges them into a single ascending {@link PostingList}. The candidate set may slightly over-include at the
     * upper bound; the query layer applies the {@code LIKE} predicate as an exact post-filter.
     */
    public class PrefixQuery
    {
        private final ByteComparable start;
        private final QueryEventListener.TrieIndexEventListener listener;
        private final QueryContext context;
        @Nullable private final TraversalStats stats;

        PrefixQuery(ByteComparable start, ByteComparable end, QueryEventListener.TrieIndexEventListener listener, QueryContext context)
        {
            this(start, end, listener, context, null);
        }

        PrefixQuery(ByteComparable start, ByteComparable end, QueryEventListener.TrieIndexEventListener listener, QueryContext context,
                    @Nullable TraversalStats stats)
        {
            this.start = start;
            // end is unused: the trie DFS from the prefix node naturally covers the full subtree.
            this.listener = listener;
            this.context = context;
            this.stats = stats;
        }

        public PostingList execute()
        {
            List<PostingList> readers = new ArrayList<>();
            try
            {
                // BBTree-style single-pass DFS: navigate to the prefix node, then recursively
                // collect posting lists. At each node, if it has a combined (exact+prefix) section
                // the entire subtree is covered — add it and stop recursing (analogous to
                // BlockBalancedTreeReader.collectPostingLists returning when postingsIndex.exists()).
                try (TrieTermsDictionaryReader trieReader = new TrieTermsDictionaryReader(
                        termDictionaryFile.instantiateRebufferer(null), termDictionaryRoot))
                {
                    long prefixNode = trieReader.followToPrefix(start);
                    if (prefixNode != TrieTermsDictionaryReader.NOT_FOUND)
                        collectFromNode(trieReader, prefixNode, readers);
                }

                context.checkpoint();

                if (readers.isEmpty())
                    return null;
                if (readers.size() == 1)
                    return readers.get(0);
                return MergePostingList.merge(readers);
            }
            catch (Throwable e)
            {
                readers.forEach(FileUtils::closeQuietly);
                if (!(e instanceof QueryCancelledException))
                    logger.error(indexIdentifier.logMessage("Failed to execute prefix query"), e);
                throw Throwables.cleaned(e);
            }
        }

        /**
         * Recursively collects posting lists from the subtree rooted at {@code nodePos}.
         * <p>
         * Mirrors {@link org.apache.cassandra.index.sai.disk.v1.bbtree.BlockBalancedTreeReader}'s
         * {@code collectPostingLists()}:
         * <ul>
         *   <li>Combined (exact+prefix) section found → add it and <em>return</em> (subtree covered).</li>
         *   <li>Exact-only section found → add it, then recurse into children.</li>
         *   <li>No payload → recurse into children only.</li>
         * </ul>
         */
        private void collectFromNode(TrieTermsDictionaryReader trieReader, long nodePos, List<PostingList> readers) throws IOException
        {
            long offset = trieReader.payloadAt(nodePos);

            if (offset != TrieTermsDictionaryReader.NOT_FOUND)
            {
                // Peek at the BlocksSummary to determine which sections are present.
                // The peek input is closed immediately; readExactAndPrefixForOffset /
                // addReaderForTerm each open their own inputs for the actual readers.
                int prefixIndex, suffixIndex;
                try (IndexInput peek = IndexFileUtils.instance.openInput(postingsFile))
                {
                    PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(peek, offset, true);
                    prefixIndex = summary.prefixIndex;
                    suffixIndex = summary.suffixIndex;
                }

                if (suffixIndex > prefixIndex)
                {
                    // Combined section covers the entire subtree — add it and stop.
                    if (stats != null) stats.combinedSectionHits++;
                    readers.add(readExactAndPrefixForOffset(offset));
                    return;
                }

                if (prefixIndex > 0)
                {
                    if (stats != null) stats.exactSectionHits++;
                    addReaderForTerm(offset, readers);
                }
            }
            else
            {
                if (stats != null) stats.emptyNodes++;
            }

            // Collect children before recursing so Walker position is not clobbered mid-loop.
            for (long child : trieReader.childrenOf(nodePos))
                collectFromNode(trieReader, child, readers);
        }

        /**
         * Reads exact and prefix sections together for a given postings offset.
         */
        private PostingList readExactAndPrefixForOffset(long offset) throws IOException
        {
            IndexInput postings = IndexFileUtils.instance.openInput(postingsFile);
            IndexInput summaryInput = IndexFileUtils.instance.openInput(postingsFile);
            PostingsReader.BlocksSummary readSummary = new PostingsReader.BlocksSummary(summaryInput, offset, true);
            
            PostingList combined = PostingsReader.combinedExactAndPrefixSections(postings, readSummary, listener.postingListEventListener());
            if (combined == null)
            {
                FileUtils.closeQuietly(postings);
                readSummary.close();
            }
            return combined;
        }

        /** Adds the exact-match posting list for a single term's payload offset. */
        private void addReaderForTerm(long offset, List<PostingList> readers)
        {
            try
            {
                IndexInput postings = IndexFileUtils.instance.openInput(postingsFile);
                IndexInput summaryInput = IndexFileUtils.instance.openInput(postingsFile);
                PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(summaryInput, offset, true);

                if (summary.prefixIndex > 0)
                {
                    readers.add(PostingsReader.exactSection(postings, summary, listener.postingListEventListener()));
                }
                else
                {
                    FileUtils.closeQuietly(postings);
                    summary.close();
                }
            }
            catch (IOException e)
            {
                throw Throwables.unchecked(e);
            }
        }
    }
}
