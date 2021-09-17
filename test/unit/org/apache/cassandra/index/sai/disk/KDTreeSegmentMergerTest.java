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
package org.apache.cassandra.index.sai.disk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.BKDReader;
import org.apache.cassandra.index.sai.disk.v1.BKDTreeRamBuffer;
import org.apache.cassandra.index.sai.disk.v1.NumericIndexWriter;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableUniqueIdentifier;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import static org.apache.cassandra.index.sai.disk.QueryEventListeners.NO_OP_BKD_LISTENER;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class KDTreeSegmentMergerTest extends SAITester
{
    private TemporaryFolder temporaryFolder = new TemporaryFolder();
    private Map<Integer, List<Long>> expected;
    private Map<Integer, List<Long>> actual;

    @BeforeClass
    public static void dbSetup() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup() throws Throwable
    {
        temporaryFolder.create();
        expected = new HashMap<>();
        actual = new HashMap<>();
    }

    @After
    public void teardown() throws Throwable
    {
        temporaryFolder.delete();
    }

    @Test
    public void compactionMergerTest() throws Throwable
    {
        performMerger(getRandom().nextIntBetween(1000, 15000), getRandom().nextIntBetween(2, 10), true);

        expected.keySet().forEach(term -> assertThat(expected.get(term), is(actual.get(term))));
    }

    @Test
    public void postBuildMergerTest() throws Throwable
    {
        performMerger(getRandom().nextIntBetween(1000, 15000), getRandom().nextIntBetween(2, 10), false);

        expected.keySet().forEach(term -> assertThat(expected.get(term), is(actual.get(term))));
    }

    @Test
    public void compactionQueryTest() throws Throwable
    {
        performCompaction(getRandom().nextIntBetween(1000, 15000), getRandom().nextIntBetween(2, 10), true);

        expected.keySet().forEach(term -> assertThat(expected.get(term), is(actual.get(term))));
    }

    @Test
    public void postBuildQueryTest() throws Throwable
    {
        performCompaction(getRandom().nextIntBetween(1000, 15000), getRandom().nextIntBetween(2, 10), false);

        expected.keySet().forEach(term -> assertThat(expected.get(term), is(actual.get(term))));
    }

    private void performMerger(int segmentSize, int segments, boolean compaction) throws Throwable
    {
        final List<BKDReader.IteratorState> segmentIterators = new ArrayList<>();

        byte[] scratch = new byte[Integer.BYTES];

        int maxSegmentRowId = 0;
        int generation = 1;

        for (int segment = 0; segment < segments; segment++)
        {
            BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(1, Integer.BYTES);

            for (int docID = segmentSize * segment; docID < (segmentSize * segment) + segmentSize; docID++)
            {
                int value = getRandom().nextIntBetween(0, 100);
                NumericUtils.intToSortableBytes(value, scratch, 0);
                buffer.addPackedValue(docID, new BytesRef(scratch));
                maxSegmentRowId = docID;
                List<Long> postings;
                if (expected.containsKey(value))
                    postings = expected.get(value);
                else
                {
                    postings = new ArrayList<>();
                    expected.put(value, postings);
                }
                postings.add(new Long(docID));
            }
            segmentIterators.add(createReader(buffer, maxSegmentRowId, generation).iteratorState());
            if (compaction)
                generation++;
        }

        MergeOneDimPointValues merger = new MergeOneDimPointValues(segmentIterators, Integer.BYTES);

        merger.intersect((rowId, packedValue) -> {
            int value = NumericUtils.sortableBytesToInt(packedValue, 0);
            List<Long> postings;
            if (actual.containsKey(value))
                postings = actual.get(value);
            else
            {
                postings = new ArrayList<>();
                actual.put(value, postings);
            }
            postings.add(rowId);
        });
    }

    private void performCompaction(int segmentSize, int segments, boolean compaction) throws Throwable
    {
        final List<BKDReader.IteratorState> segmentIterators = new ArrayList<>();

        byte[] scratch = new byte[Integer.BYTES];

        int maxSegmentRowId = 0;
        int generation = 1;
        int totalRows = 0;

        for (int segment = 0; segment < segments; segment++)
        {
            BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(1, Integer.BYTES);

            for (int docID = segmentSize * segment; docID < (segmentSize * segment) + segmentSize; docID++)
            {
                int value = getRandom().nextIntBetween(0, 100);
                NumericUtils.intToSortableBytes(value, scratch, 0);
                buffer.addPackedValue(docID, new BytesRef(scratch));
                maxSegmentRowId = docID;
                List<Long> postings;
                if (expected.containsKey(value))
                    postings = expected.get(value);
                else
                {
                    postings = new ArrayList<>();
                    expected.put(value, postings);
                }
                postings.add(new Long(docID));
                totalRows++;
            }
            segmentIterators.add(createReader(buffer, maxSegmentRowId, generation).iteratorState());
            if (compaction)
                generation++;
        }

        MergeOneDimPointValues merger = new MergeOneDimPointValues(segmentIterators, Integer.BYTES);

        IndexComponents components = IndexComponents.create("test", new Descriptor(temporaryFolder.newFolder(), "test", "test", new SequenceBasedSSTableUniqueIdentifier(20)), null);

        try (NumericIndexWriter indexWriter = new NumericIndexWriter(components, Integer.BYTES, maxSegmentRowId, totalRows, IndexWriterConfig.defaultConfig("test"), false))
        {
            SegmentMetadata.ComponentMetadataMap metadata = indexWriter.writeAll(merger);
            final long bkdPosition = metadata.get(IndexComponents.NDIType.KD_TREE).root;
            final long postingsPosition = metadata.get(IndexComponents.NDIType.KD_TREE_POSTING_LISTS).root;

            FileHandle kdtree = components.createFileHandle(components.kdTree);
            FileHandle kdtreePostings = components.createFileHandle(components.kdTreePostingLists);
            BKDReader reader = new BKDReader(components, kdtree, bkdPosition, kdtreePostings, postingsPosition);

            for (int term : expected.keySet())
            {
                PostingList postingList = reader.intersect(buildQuery(term, term), NO_OP_BKD_LISTENER, new QueryContext());

                while (true)
                {
                    long rowId = postingList.nextPosting();
                    if (rowId == PostingList.END_OF_STREAM)
                        break;
                    List<Long> postings;
                    if (actual.containsKey(term))
                        postings = actual.get(term);
                    else
                    {
                        postings = new ArrayList<>();
                        actual.put(term, postings);
                    }
                    postings.add(rowId);
                }
            }
        }
    }

    private BKDReader createReader(BKDTreeRamBuffer buffer, int maxSegmentRowId, int generation) throws Throwable
    {
        IndexComponents components = IndexComponents.create("test", new Descriptor(temporaryFolder.newFolder(), "test", "test", new SequenceBasedSSTableUniqueIdentifier(generation)), null);

        final NumericIndexWriter writer = new NumericIndexWriter(components, Integer.BYTES, maxSegmentRowId, buffer.numRows(), IndexWriterConfig.defaultConfig("test"), false);

        final SegmentMetadata.ComponentMetadataMap metadata = writer.writeAll(buffer.asPointValues());
        final long bkdPosition = metadata.get(IndexComponents.NDIType.KD_TREE).root;
        final long postingsPosition = metadata.get(IndexComponents.NDIType.KD_TREE_POSTING_LISTS).root;

        FileHandle kdtree = components.createFileHandle(components.kdTree);
        FileHandle kdtreePostings = components.createFileHandle(components.kdTreePostingLists);
        return new BKDReader(components, kdtree, bkdPosition, kdtreePostings, postingsPosition);
    }

    private BKDReader.IntersectVisitor buildQuery(int queryMin, int queryMax)
    {
        return new BKDReader.IntersectVisitor()
        {
            @Override
            public boolean visit(byte[] packedValue)
            {
                int x = NumericUtils.sortableBytesToInt(packedValue, 0);
                return x >= queryMin && x <= queryMax;
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
            {
                int min = NumericUtils.sortableBytesToInt(minPackedValue, 0);
                int max = NumericUtils.sortableBytesToInt(maxPackedValue, 0);
                assert max >= min;

                if (max < queryMin || min > queryMax)
                {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
                else if (min >= queryMin && max <= queryMax)
                {
                    return PointValues.Relation.CELL_INSIDE_QUERY;
                }
                else
                {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
            }
        };
    }
}
