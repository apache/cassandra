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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.FeatureNeedsIndexRebuildException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.postings.IntersectingPostingList;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.metrics.MulticastQueryEventListeners;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.utils.BM25Utils;
import org.apache.cassandra.index.sai.utils.BM25Utils.EagerDocTF;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithScore;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.RowIdWithByteComparable;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.index.sai.disk.PostingList.END_OF_STREAM;
import static org.apache.cassandra.index.sai.disk.v1.SegmentMetadata.INVALID_TOTAL_TERM_COUNT;

/**
 * Executes {@link Expression}s against the trie-based terms dictionary for an individual index segment.
 */
public class InvertedIndexSearcher extends IndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final TermsReader reader;
    private final QueryEventListener.TrieIndexEventListener perColumnEventListener;
    private final Version version;
    private final boolean filterRangeResults;
    private final SSTableReader sstable;
    private final SegmentMetadata.ComponentMetadata docLengthsMeta;
    private final FileHandle docLengths;
    private final long segmentRowIdOffset;

    protected InvertedIndexSearcher(SSTableContext sstableContext,
                                    PerIndexFiles perIndexFiles,
                                    SegmentMetadata segmentMetadata,
                                    IndexContext indexContext,
                                    Version version,
                                    boolean filterRangeResults) throws IOException
    {
        super(sstableContext.primaryKeyMapFactory(), perIndexFiles, segmentMetadata, indexContext);
        this.sstable = sstableContext.sstable;

        long root = metadata.getIndexRoot(IndexComponentType.TERMS_DATA);
        assert root >= 0;

        this.version = version;
        this.filterRangeResults = filterRangeResults;
        perColumnEventListener = (QueryEventListener.TrieIndexEventListener)indexContext.getColumnQueryMetrics();
        this.segmentRowIdOffset = segmentMetadata.segmentRowIdOffset;
        this.docLengthsMeta = segmentMetadata.componentMetadatas.getOptional(IndexComponentType.DOC_LENGTHS);
        this.docLengths = docLengthsMeta == null ? null : indexFiles.docLengths();

        Map<String,String> map = metadata.componentMetadatas.get(IndexComponentType.TERMS_DATA).attributes;
        String footerPointerString = map.get(SAICodecUtils.FOOTER_POINTER);
        long footerPointer = footerPointerString == null ? -1 : Long.parseLong(footerPointerString);

        var perIndexComponents = perIndexFiles.usedPerIndexComponents();
        reader = new TermsReader(indexContext,
                                 indexFiles.termsData(),
                                 perIndexComponents.byteComparableVersionFor(IndexComponentType.TERMS_DATA),
                                 indexFiles.postingLists(),
                                 root,
                                 footerPointer,
                                 version);
    }

    @Override
    public long indexFileCacheSize()
    {
        // trie has no pre-allocated memory.
        // TODO: Is this still the case now the trie isn't using the chunk cache?
        return 0;
    }

    @SuppressWarnings("resource")
    public KeyRangeIterator search(Expression exp, AbstractBounds<PartitionPosition> keyRange, QueryContext context, boolean defer) throws IOException
    {
        PostingList postingList = searchPosting(exp, context);
        return toPrimaryKeyIterator(postingList, context);
    }

    private PostingList searchPosting(Expression exp, QueryContext context)
    {
        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Searching on expression '{}'..."), exp);

        // We use the version to encode the search boundaries for the trie to ensure we use version appropriate bounds.
        if (exp.getOp().isEquality() || exp.getOp() == Expression.Op.MATCH)
        {
            // Value is encoded in non-byte-comparable-version-specific fixed-length format.
            final ByteComparable term = version.onDiskFormat().encodeForTrie(exp.lower.value.encoded, indexContext.getValidator());
            QueryEventListener.TrieIndexEventListener listener = MulticastQueryEventListeners.of(context, perColumnEventListener);
            return reader.exactMatch(term, listener, context);
        }
        else if (exp.getOp() == Expression.Op.RANGE)
        {
            QueryEventListener.TrieIndexEventListener listener = MulticastQueryEventListeners.of(context, perColumnEventListener);
            var lower = exp.getEncodedLowerBoundByteComparable(version);
            var upper = exp.getEncodedUpperBoundByteComparable(version);
            return reader.rangeMatch(filterRangeResults ? exp : null, lower, upper, listener, context);
        }
        throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression: " + exp));
    }

    private Cell<?> readColumn(SSTableReader sstable, PrimaryKey primaryKey)
    {
        var dk = primaryKey.partitionKey();
        var slices = Slices.with(indexContext.comparator(), Slice.make(primaryKey.clustering()));
        try (var rowIterator = sstable.iterator(dk, slices, columnFilter, false, SSTableReadsListener.NOOP_LISTENER))
        {
            while (rowIterator.hasNext())
            {
                Unfiltered unfiltered = rowIterator.next();
                // A range tombstone marker can precede the requested row, so skip non-row unfiltereds.
                if (unfiltered.isRow())
                {
                    Row row = (Row) unfiltered;
                    return row.getCell(indexContext.getDefinition());
                }
            }
            return null;
        }
    }

    @Override
    public CloseableIterator<PrimaryKeyWithSortKey> orderBy(Orderer orderer, Expression slice, AbstractBounds<PartitionPosition> keyRange, QueryContext queryContext, int limit) throws IOException
    {
        if (!orderer.isBM25())
        {
            var iter = new RowIdWithTermsIterator(reader.allTerms(orderer.isAscending()));
            return toMetaSortedIterator(iter, queryContext);
        }
        if (docLengthsMeta == null)
        {
            throw new FeatureNeedsIndexRebuildException(String.format(QueryController.INDEX_VERSION_DOES_NOT_SUPPORT_BM25,
                                                                      indexContext.getIndexName()));
        }

        // find documents that match each term
        var queryTerms = orderer.getQueryTerms();
        var postingLists = queryTerms.stream()
                                     .collect(Collectors.toMap(Function.identity(), term ->
        {
            var encodedTerm = version.onDiskFormat().encodeForTrie(term, indexContext.getValidator());
            var listener = MulticastQueryEventListeners.of(queryContext, perColumnEventListener);
            var postings = reader.exactMatch(encodedTerm, listener, queryContext);
            return postings == null ? PostingList.EMPTY : postings;
        }));

        var pkm = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap();
        var merged = IntersectingPostingList.intersect(postingLists);
        var docLengthsReader = new DocLengthsReader(docLengths, docLengthsMeta, version);

        // Wrap the iterator with resource management
        var it = new AbstractIterator<BM25Utils.DocTF>() { // Anonymous class extends AbstractIterator
            private boolean closed;

            @Override
            protected BM25Utils.DocTF computeNext()
            {
                try
                {
                    int rowId = merged.nextPosting();
                    if (rowId == PostingList.END_OF_STREAM)
                        return endOfData();
                    // Reads from disk.
                    int docLength = docLengthsReader.get(rowId); // segment-local rowid
                    // We defer creating the primary key because it reads the token from disk, which is only needed
                    // for the top rows just before they are materialized from disk, so we wait until after scoring
                    // and sorting to read the token.
                    return new LazyDocTF(pkm, segmentRowIdOffset + rowId, docLength, merged.frequencies());
                }
                catch (IOException e)
                {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public void close()
            {
                if (closed) return;
                closed = true;
                FileUtils.closeQuietly(pkm, merged, docLengthsReader);
            }
        };
        return BM25Utils.computeScores(it,
                                       queryTerms,
                                       orderer.bm25stats,
                                       indexContext,
                                       sstable.descriptor.id,
                                       metadata.totalTermCount == INVALID_TOTAL_TERM_COUNT);
    }

    @Override
    public CloseableIterator<PrimaryKeyWithSortKey> orderResultsBy(SSTableReader reader, QueryContext queryContext, List<PrimaryKey> keys, Orderer orderer, int limit) throws IOException
    {
        if (!orderer.isBM25())
            return super.orderResultsBy(reader, queryContext, keys, orderer, limit);
        
        if (docLengthsMeta == null)
        {
            throw new InvalidRequestException(String.format(QueryController.INDEX_VERSION_DOES_NOT_SUPPORT_BM25,
                                                            indexContext.getIndexName()));
        }

        var queryTerms = orderer.getQueryTerms();
        var analyzer = indexContext.getAnalyzerFactory().create();
        var it = keys.stream()
                     .map(pk -> EagerDocTF.createFromDocument(pk, readColumn(sstable, pk), analyzer, queryTerms))
                     .filter(Objects::nonNull)
                     .iterator();
        return BM25Utils.computeScores(CloseableIterator.wrap(it),
                                       queryTerms,
                                       orderer.bm25stats,
                                       indexContext,
                                       sstable.descriptor.id,
                                       metadata.totalTermCount == INVALID_TOTAL_TERM_COUNT);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("indexContext", indexContext)
                          .toString();
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(reader, docLengths);
    }

    /**
     * An iterator that iterates over a source
     */
    private static class RowIdWithTermsIterator extends AbstractIterator<RowIdWithByteComparable>
    {
        private final TermsIterator source;
        private PostingList currentPostingList = PostingList.EMPTY;
        private ByteComparable currentTerm = null;

        RowIdWithTermsIterator(TermsIterator source)
        {
            this.source = source;
        }

        @Override
        protected RowIdWithByteComparable computeNext()
        {
            try
            {
                while (true)
                {
                    long nextPosting = currentPostingList.nextPosting();
                    if (nextPosting != END_OF_STREAM)
                        return new RowIdWithByteComparable(Math.toIntExact(nextPosting), currentTerm);

                    if (!source.hasNext())
                        return endOfData();

                    currentTerm = source.next();
                    FileUtils.closeQuietly(currentPostingList);
                    currentPostingList = source.postings();
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(source, currentPostingList);
        }
    }

    /**
     * A {@link BM25Utils.DocTF} that is lazy in that it does not create the {@link PrimaryKey} until it is required.
     */
    private static class LazyDocTF implements BM25Utils.DocTF
    {
        private final PrimaryKeyMap pkm;
        private final long sstableRowId;
        private final int docLength;
        private final Map<ByteBuffer, Integer> frequencies;

        LazyDocTF(PrimaryKeyMap pkm, long sstableRowId, int docLength, Map<ByteBuffer, Integer> frequencies)
        {
            this.pkm = pkm;
            this.sstableRowId = sstableRowId;
            this.docLength = docLength;
            this.frequencies = frequencies;
        }

        @Override
        public int getTermFrequency(ByteBuffer term)
        {
            return frequencies.getOrDefault(term, 0);
        }

        @Override
        public int termCount()
        {
            return docLength;
        }

        @Override
        public PrimaryKeyWithSortKey primaryKey(IndexContext context, Memtable source, float score)
        {
            // Only sstables use this class, so this should never be called
            throw new UnsupportedOperationException();
        }

        @Override
        public PrimaryKeyWithSortKey primaryKey(IndexContext context, SSTableId<?> source, float score)
        {
            // We can eagerly get the token now, even though it might not technically be required until we know
            // we have the best score. (Perhaps this should be lazy too?)
            // BM25 scores are exact.
            return new PrimaryKeyWithScore(context, source, pkm.primaryKeyFromRowId(sstableRowId), score, false);
        }
    }
}
