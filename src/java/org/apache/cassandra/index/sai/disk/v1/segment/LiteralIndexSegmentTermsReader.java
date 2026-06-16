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
        private final ByteComparable end;
        private final QueryEventListener.TrieIndexEventListener listener;
        private final QueryContext context;

        PrefixQuery(ByteComparable start, ByteComparable end, QueryEventListener.TrieIndexEventListener listener, QueryContext context)
        {
            this.start = start;
            this.end = end;
            this.listener = listener;
            this.context = context;
        }

        public PostingList execute()
        {
            List<PostingList> readers = new ArrayList<>();
            try
            {
                // Fast path: if the prefix lands exactly on a node that carries an aggregated prefix section, read
                // that single section (plus the node's own exact rows) directly instead of scanning every term.
                PostingList aggregated = readAggregatedPrefixSection();
                if (aggregated != null)
                {
                    context.checkpoint();
                    return aggregated;
                }

                // Fallback: scan every term in [start, end] and merge their posting lists.
                try (TrieTermsDictionaryReader.PrefixIterator iterator =
                         new TrieTermsDictionaryReader.PrefixIterator(termDictionaryFile.instantiateRebufferer(null), termDictionaryRoot, start, end))
                {
                    long offset;
                    while ((offset = iterator.nextPayload()) != TrieTermsDictionaryReader.NOT_FOUND)
                        addReaderForTerm(offset, readers);
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
         * If the prefix term resolves to a node carrying an aggregated prefix section (written when the node is at an
         * eligible depth with at least {@code minimum_postings_leaves} descendants), returns a posting list covering
         * all of that node's exact and prefix postings — i.e. every row under the prefix — without scanning each term.
         * Returns null when there is no such section, so the caller falls back to the range scan.
         */
        private PostingList readAggregatedPrefixSection() throws IOException
        {
            long offset;
            try (TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(null), termDictionaryRoot))
            {
                offset = reader.exactMatch(start);
            }
            if (offset == TrieTermsDictionaryReader.NOT_FOUND)
                return null;

            int prefixIndex;
            int suffixIndex;
            try (IndexInput peek = IndexFileUtils.instance.openInput(postingsFile))
            {
                PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(peek, offset, true);
                prefixIndex = summary.prefixIndex;
                suffixIndex = summary.suffixIndex;
            }

            // No prefix section: this is just a leaf term, whose descendants still need the range scan.
            if (suffixIndex <= prefixIndex)
                return null;

            List<PostingList> readers = new ArrayList<>(2);
            try
            {
                if (prefixIndex > 0)
                {
                    PostingList exact = openSection(offset, false);
                    if (exact != null)
                        readers.add(exact);
                }
                PostingList prefix = openSection(offset, true);
                if (prefix != null)
                    readers.add(prefix);
            }
            catch (Throwable t)
            {
                readers.forEach(FileUtils::closeQuietly);
                throw t;
            }

            if (readers.isEmpty())
                return null;
            if (readers.size() == 1)
                return readers.get(0);
            return MergePostingList.merge(readers);
        }

        /** Opens a reader over the exact ({@code prefix == false}) or prefix ({@code prefix == true}) section. */
        private PostingList openSection(long offset, boolean prefix) throws IOException
        {
            IndexInput postings = IndexFileUtils.instance.openInput(postingsFile);
            IndexInput summaryInput = IndexFileUtils.instance.openInput(postingsFile);
            PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(summaryInput, offset, true);
            PostingList reader = prefix ? PostingsReader.prefixSection(postings, summary, listener.postingListEventListener())
                                        : PostingsReader.exactSection(postings, summary, listener.postingListEventListener());
            if (reader == null)
            {
                FileUtils.closeQuietly(postings);
                summary.close();
            }
            return reader;
        }

        /** Adds the exact-match posting list for a single term's payload offset (its rows under the prefix). */
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
