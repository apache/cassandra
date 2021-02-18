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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.store.IndexInput;

import static org.apache.cassandra.index.sai.utils.SAICodecUtils.validate;

/**
 * Synchronous reader of terms dictionary and postings lists to produce a {@link PostingList} with matching row ids.
 *
 * {@link #exactMatch(ByteComparable, QueryEventListener.TrieIndexEventListener, QueryContext)} does:
 * <ul>
 * <li>{@link TermQuery#lookupTermDictionary(ByteComparable)}: does term dictionary lookup to find the posting list file
 * position</li>
 * <li>{@link TermQuery#getPostingReader(long)}: reads posting list block summary and initializes posting read which
 * reads the first block of the posting list into memory</li>
 * </ul>
 */
public class TermsReader implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final IndexComponents indexComponents;
    private final FileHandle termDictionaryFile;
    private final FileHandle postingsFile;
    private final long termDictionaryRoot;

    public TermsReader(IndexComponents components, FileHandle termsData, FileHandle postingLists,
                       long root, long termsFooterPointer) throws IOException
    {
        this.indexComponents = components;
        termDictionaryFile = termsData;
        postingsFile = postingLists;
        termDictionaryRoot = root;

        try (final IndexInput indexInput = indexComponents.openInput(termDictionaryFile))
        {
            // if the pointer is -1 then this is a previous version of the index
            // use the old way to validate the footer
            // the footer pointer is used due to encrypted indexes padding extra bytes
            if (termsFooterPointer == -1)
            {
                validate(indexInput);
            }
            else
            {
                validate(indexInput, termsFooterPointer);
            }
        }

        try (final IndexInput indexInput = indexComponents.openInput(postingsFile))
        {
            validate(indexInput);
        }
    }

    public static int openPerIndexFiles()
    {
        // terms and postings
        return 2;
    }

    @Override
    public void close()
    {
        try
        {
            termDictionaryFile.close();
        }
        finally
        {
            postingsFile.close();
        }
    }

    public TermsIterator allTerms(long segmentOffset, QueryEventListener.TrieIndexEventListener listener)
    {
        // blocking, since we use it only for segment merging for now
        return new TermsScanner(segmentOffset, listener);
    }

    public PostingList exactMatch(ByteComparable term, QueryEventListener.TrieIndexEventListener perQueryEventListener, QueryContext context)
    {
        perQueryEventListener.onSegmentHit();
        return new TermQuery(term, perQueryEventListener, context).execute();
    }

    @VisibleForTesting
    public class TermQuery
    {
        private final IndexInput postingsInput;
        private final IndexInput postingsSummaryInput;
        private final QueryEventListener.TrieIndexEventListener listener;
        private final long lookupStartTime;
        private final QueryContext context;

        private ByteComparable term;

        TermQuery(ByteComparable term, QueryEventListener.TrieIndexEventListener listener, QueryContext context)
        {
            this.listener = listener;
            postingsInput = indexComponents.openInput(postingsFile);
            postingsSummaryInput = indexComponents.openInput(postingsFile);
            this.term = term;
            lookupStartTime = System.nanoTime();
            this.context = context;
        }

        public PostingList execute()
        {
            try
            {
                long postingOffset = lookupTermDictionary(term);
                if (postingOffset == PostingList.OFFSET_NOT_FOUND)
                {
                    FileUtils.closeQuietly(postingsInput);
                    FileUtils.closeQuietly(postingsSummaryInput);
                    return null;
                }

                context.checkpoint();

                // when posting is found, resources will be closed when posting reader is closed.
                return getPostingReader(postingOffset);
            }
            catch (Throwable e)
            {
                //TODO Is there an equivalent of AOE in OS?
                if (!(e instanceof AbortedOperationException))
                    logger.error(indexComponents.logMessage("Failed to execute term query"), e);

                closeOnException();
                throw Throwables.cleaned(e);
            }
        }

        private void closeOnException()
        {
            FileUtils.closeQuietly(postingsInput);
            FileUtils.closeQuietly(postingsSummaryInput);
        }

        public long lookupTermDictionary(ByteComparable term)
        {
            try (TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(), termDictionaryRoot))
            {
                final long offset = reader.exactMatch(term);

                listener.onTraversalComplete(System.nanoTime() - lookupStartTime, TimeUnit.NANOSECONDS);

                if (offset == TrieTermsDictionaryReader.NOT_FOUND)
                    return PostingList.OFFSET_NOT_FOUND;

                return offset;
            }
        }

        public PostingsReader getPostingReader(long offset) throws IOException
        {
            PostingsReader.BlocksSummary header = new PostingsReader.BlocksSummary(postingsSummaryInput, offset);

            return new PostingsReader(postingsInput, header, listener.postingListEventListener());
        }
    }

    // currently only used for testing
    private class TermsScanner implements TermsIterator
    {
        private final long segmentOffset;
        private final QueryEventListener.TrieIndexEventListener listener;
        private final TrieTermsDictionaryReader termsDictionaryReader;
        private final Iterator<Pair<ByteComparable, Long>> iterator;
        private final ByteBuffer minTerm, maxTerm;
        private Pair<ByteComparable, Long> entry;

        private TermsScanner(long segmentOffset, QueryEventListener.TrieIndexEventListener listener)
        {
            this.termsDictionaryReader = new TrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(), termDictionaryRoot);

            this.minTerm = ByteBuffer.wrap(ByteSourceInverse.readBytes(termsDictionaryReader.getMinTerm().asComparableBytes(ByteComparable.Version.OSS41)));
            this.maxTerm = ByteBuffer.wrap(ByteSourceInverse.readBytes(termsDictionaryReader.getMaxTerm().asComparableBytes(ByteComparable.Version.OSS41)));
            this.iterator = termsDictionaryReader.iterator();
            this.listener = listener;
            this.segmentOffset = segmentOffset;
        }

        @Override
        @SuppressWarnings("resource")
        public PostingList postings() throws IOException
        {
            assert entry != null;
            final IndexInput input = indexComponents.openInput(postingsFile);
            return new OffsetPostingList(segmentOffset, new PostingsReader(input, new PostingsReader.BlocksSummary(input, entry.right), listener.postingListEventListener()));
        }

        @Override
        public void close()
        {
            termsDictionaryReader.close();
        }

        @Override
        public ByteBuffer getMinTerm()
        {
            return minTerm;
        }

        @Override
        public ByteBuffer getMaxTerm()
        {
            return maxTerm;
        }

        @Override
        public ByteComparable next()
        {
            if (iterator.hasNext())
            {
                entry = iterator.next();
                return entry.left;
            }
            return null;
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }
    }

    private class OffsetPostingList implements PostingList
    {
        private final long offset;
        private final PostingList wrapped;

        OffsetPostingList(long offset, PostingList postingList)
        {
            this.offset = offset;
            this.wrapped = postingList;
        }

        @Override
        public long nextPosting() throws IOException
        {
            long next = wrapped.nextPosting();
            if (next == PostingList.END_OF_STREAM)
                return next;
            return next + offset;
        }

        @Override
        public long size()
        {
            return wrapped.size();
        }

        @Override
        public long advance(long targetRowID) throws IOException
        {
            long next = wrapped.advance(targetRowID);
            if (next == PostingList.END_OF_STREAM)
                return next;
            return next + offset;
        }
    }
}
