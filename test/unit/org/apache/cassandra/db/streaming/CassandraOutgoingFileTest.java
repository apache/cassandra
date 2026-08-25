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

package org.apache.cassandra.db.streaming;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CassandraOutgoingFileTest
{
    public static final String KEYSPACE = "CassandraOutgoingFileTest";
    public static final String CF_STANDARD = "Standard1";
    public static final String CF_COMPRESSED = "Compressed";
    public static final String CF_INDEXED = "Indexed1";
    public static final String CF_STANDARDLOWINDEXINTERVAL = "StandardLowIndexInterval";

    private static SSTableReader sstable;
    private static ColumnFamilyStore store;

    @BeforeClass
    public static void defineSchemaAndPrepareSSTable()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_STANDARD),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_COMPRESSED)
                                                .compression(CompressionParams.lz4(4096)),
                                    SchemaLoader.compositeIndexCFMD(KEYSPACE, CF_INDEXED, true),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_STANDARDLOWINDEXINTERVAL)
                                                .minIndexInterval(8)
                                                .maxIndexInterval(256)
                                                .caching(CachingParams.CACHE_NOTHING));

        Keyspace keyspace = Keyspace.open(KEYSPACE);
        store = keyspace.getColumnFamilyStore(CF_STANDARD);

        // insert data and compact to a single sstable
        CompactionManager.instance.disableAutoCompaction();
        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .clustering("0")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }
        Util.flush(store);
        CompactionManager.instance.performMaximal(store);

        sstable = store.getLiveSSTables().iterator().next();
    }

    @Test
    public void validateFullyContainedIn_SingleContiguousRange_Succeeds()
    {
        List<Range<Token>> requestedRanges = Arrays.asList(new Range<>(store.getPartitioner().getMinimumToken(), sstable.getLast().getToken()));

        List<SSTableReader.PartitionPositionBounds> sections = sstable.getPositionsForRanges(requestedRanges);
        CassandraOutgoingFile cof = new CassandraOutgoingFile(StreamOperation.BOOTSTRAP, sstable.ref(),
                                                              sections,
                                                              requestedRanges, sstable.estimatedKeys());

        assertTrue(cof.contained(sections, sstable));
        assertTrue("ordinary pa SSTables do not require the split-prefix capability",
                   cof.computeShouldStreamEntireSSTables());
    }

    @Test
    public void validateFullyContainedIn_PartialOverlap_Fails()
    {
        List<Range<Token>> requestedRanges = Arrays.asList(new Range<>(store.getPartitioner().getMinimumToken(), getTokenAtIndex(2)));

        List<SSTableReader.PartitionPositionBounds> sections = sstable.getPositionsForRanges(requestedRanges);
        CassandraOutgoingFile cof = new CassandraOutgoingFile(StreamOperation.BOOTSTRAP, sstable.ref(),
                                                              sections,
                                                              requestedRanges, sstable.estimatedKeys());

        assertFalse(cof.contained(sections, sstable));
    }

    @Test
    public void validateFullyContainedIn_SplitRange_Succeeds()
    {
        List<Range<Token>> requestedRanges = Arrays.asList(new Range<>(store.getPartitioner().getMinimumToken(), getTokenAtIndex(4)),
                                                         new Range<>(getTokenAtIndex(2), getTokenAtIndex(6)),
                                                         new Range<>(getTokenAtIndex(5), sstable.getLast().getToken()));
        requestedRanges = Range.normalize(requestedRanges);

        List<SSTableReader.PartitionPositionBounds> sections = sstable.getPositionsForRanges(requestedRanges);
        CassandraOutgoingFile cof = new CassandraOutgoingFile(StreamOperation.BOOTSTRAP, sstable.ref(),
                                                              sections,
                                                              requestedRanges, sstable.estimatedKeys());

        assertTrue(cof.contained(sections, sstable));
    }

    @Test
    public void validateFullyContainedIn_MovedStart_Fails()
    {
        SSTableReader movedStart = sstable.cloneWithNewStart(getKeyAtIndex(3));
        try
        {
            assertEquals(SSTableReader.OpenReason.MOVED_START, movedStart.openReason);

            Token minimumToken = store.getPartitioner().getMinimumToken();
            List<Range<Token>> requestedRanges = Collections.singletonList(new Range<>(minimumToken,
                                                                                       movedStart.getLast().getToken()));
            List<SSTableReader.PartitionPositionBounds> sections = movedStart.getPositionsForRanges(requestedRanges);
            SSTableReader.PartitionPositionBounds fullRange = movedStart.getPositionsForFullRange();
            assertNotNull(fullRange);

            long transferLength = sections.stream().mapToLong(p -> p.upperPosition - p.lowerPosition).sum();
            assertEquals(fullRange.upperPosition - fullRange.lowerPosition, transferLength);
            assertTrue("the moved start must hide a physical prefix", transferLength < movedStart.uncompressedLength());

            CassandraOutgoingFile outgoing = new CassandraOutgoingFile(StreamOperation.BOOTSTRAP,
                                                                        movedStart.ref(),
                                                                        sections,
                                                                        requestedRanges,
                                                                        movedStart.estimatedKeys());
            try
            {
                assertFalse("whole-file streaming would reintroduce the moved-away prefix",
                            outgoing.contained(sections, movedStart));
                assertFalse(outgoing.computeShouldStreamEntireSSTables());
            }
            finally
            {
                outgoing.finish();
            }
        }
        finally
        {
            movedStart.selfRef().release();
        }
    }

    @Test
    public void validateFullyContainedIn_DeadPrefixChild_Succeeds()
    {
        Assume.assumeTrue(BigFormat.isSelected());

        ColumnFamilyStore compressedStore = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        for (int i = 0; i < 1000; i++)
        {
            new RowUpdateBuilder(compressedStore.metadata(), i, String.valueOf(i))
            .clustering("0")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }
        Util.flush(compressedStore);

        SSTableReader parent = compressedStore.getLiveSSTables().iterator().next();
        assertTrue(ZeroCopySSTableSplitter.isSupported(parent));

        long targetSize = Math.max(1, parent.getCompressionMetadata().compressedFileLength / 3);
        try (LifecycleTransaction transaction = LifecycleTransaction.offline(OperationType.UNKNOWN, parent))
        {
            ZeroCopySSTableSplitter.Result split = ZeroCopySSTableSplitter.splitBySize(parent,
                                                                                       targetSize,
                                                                                       transaction);
            try
            {
                ZeroCopySSTableSplitter.Child child = null;
                for (ZeroCopySSTableSplitter.Child candidate : split.children)
                {
                    if (candidate.deadPrefixBytes > 0)
                    {
                        child = candidate;
                        break;
                    }
                }

                assertNotNull("expected a split child with a retained compression-chunk prefix", child);
                SSTableReader childReader = child.reader;
                SSTableReader.PartitionPositionBounds fullRange = childReader.getPositionsForFullRange();
                assertTrue(fullRange.lowerPosition > 0);

                Token minimumToken = compressedStore.getPartitioner().getMinimumToken();
                List<Range<Token>> requestedRanges = Collections.singletonList(new Range<>(minimumToken,
                                                                                           childReader.getLast().getToken()));
                List<SSTableReader.PartitionPositionBounds> sections = childReader.getPositionsForRanges(requestedRanges);
                long transferLength = sections.stream().mapToLong(p -> p.upperPosition - p.lowerPosition).sum();
                assertEquals(fullRange.upperPosition - fullRange.lowerPosition, transferLength);
                assertTrue(transferLength < childReader.uncompressedLength());

                CassandraOutgoingFile oldPeer = new CassandraOutgoingFile(StreamOperation.BOOTSTRAP,
                                                                           childReader.ref(),
                                                                           sections,
                                                                           requestedRanges,
                                                                           childReader.estimatedKeys(),
                                                                           new CassandraVersion("6.0"));
                try
                {
                    assertTrue(oldPeer.contained(sections, childReader));
                    assertFalse("a pa reader does not understand the split-prefix marker",
                                oldPeer.computeShouldStreamEntireSSTables());
                    assertEquals(1, oldPeer.getNumFiles());
                }
                finally
                {
                    oldPeer.finish();
                }

                CassandraOutgoingFile currentPeer = new CassandraOutgoingFile(StreamOperation.BOOTSTRAP,
                                                                               childReader.ref(),
                                                                               sections,
                                                                               requestedRanges,
                                                                               childReader.estimatedKeys(),
                                                                               new CassandraVersion("7.0"));
                try
                {
                    assertTrue(currentPeer.contained(sections, childReader));
                    assertTrue(currentPeer.computeShouldStreamEntireSSTables());
                    assertTrue(currentPeer.getNumFiles() > 1);
                }
                finally
                {
                    currentPeer.finish();
                }
            }
            finally
            {
                for (ZeroCopySSTableSplitter.Child child : split.children)
                    child.reader.selfRef().release();
            }
        }
    }

    private DecoratedKey getKeyAtIndex(int i)
    {
        int count = 0;
        DecoratedKey key;

        try (KeyIterator iter = sstable.keyIterator())
        {
            do
            {
                key = iter.next();
                count++;
            } while (iter.hasNext() && count < i);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return key;
    }

    private Token getTokenAtIndex(int i)
    {
        return getKeyAtIndex(i).getToken();
    }
}
