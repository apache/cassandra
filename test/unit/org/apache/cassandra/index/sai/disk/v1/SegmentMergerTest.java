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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class SegmentMergerTest extends SAITester
{
    protected static final Injections.Counter SEGMENT_BUILD_COUNTER = Injections.newCounter("SegmentBuildCounter")
                                                                                .add(newInvokePoint().onClass(SSTableIndexWriter.class).onMethod("newSegmentBuilder"))
                                                                                .build();

    @Before
    public void setup() throws Throwable
    {
        setSegmentWriteBufferSpace(70000);
        requireNetwork();
        SEGMENT_BUILD_COUNTER.reset();
    }

    @Test
    public void literalIndexTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, value text, PRIMARY KEY(pk))");
        disableCompaction();

        Injections.inject(SEGMENT_BUILD_COUNTER);

        // Insert sufficient rows to make sure more than 1 segments are created before segment compaction
        Map<String, List<Integer>> expected = new HashMap<>();

        for (int rowId = 0; rowId < getRandom().nextIntBetween(50000, 100000); rowId++)
        {
            String value = Integer.toString(getRandom().nextIntBetween(0, 1000));
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", rowId, value);
            List<Integer> postings;
            if (expected.containsKey(value))
                postings = expected.get(value);
            else
            {
                postings = new ArrayList<>();
                expected.put(value, postings);
            }
            postings.add(rowId);

        }
        flush();

        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        // All we are interested in is that before the segment compaction there were more than 1 segment created
        assertTrue(SEGMENT_BUILD_COUNTER.get() > 1);

        List<SegmentMetadata> segments = getSegments(indexName);

        // Post-build the index only has 1 segment
        assertEquals(1, segments.size());

        for (Map.Entry<String, List<Integer>> entry : expected.entrySet())
        {
            String value = entry.getKey();
            List<Integer> expectedPostings = entry.getValue();
            UntypedResultSet results = execute("SELECT * FROM %s WHERE value = ?", value);
            List<Integer> actualPostings = new ArrayList<>();
            results.forEach(row -> actualPostings.add(row.getInt("pk")));
            actualPostings.sort(Integer::compareTo);
            assertEquals("Postings comparison failed for term = " + value, expectedPostings, actualPostings);
        }
    }

    @Test
    public void numericIndexTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, value int, PRIMARY KEY(pk))");
        disableCompaction();

        Injections.inject(SEGMENT_BUILD_COUNTER);

        // Insert sufficient rows to make sure more than 1 segments are created before segment compaction
        Map<Integer, List<Integer>> expected = new HashMap<>();

        for (int rowId = 0; rowId < getRandom().nextIntBetween(10000, 50000); rowId++)
        {
            int value = getRandom().nextIntBetween(0, 1000);
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", rowId, value);
            List<Integer> postings;
            if (expected.containsKey(value))
                postings = expected.get(value);
            else
            {
                postings = new ArrayList<>();
                expected.put(value, postings);
            }
            postings.add(rowId);

        }
        flush();

        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        // All we are interested in is that before the segment compaction there were more than 1 segment created
        assertTrue(SEGMENT_BUILD_COUNTER.get() > 1);

        List<SegmentMetadata> segments = getSegments(indexName);

        // Post-build the index only has 1 segment
        assertEquals(1, segments.size());

        for (Map.Entry<Integer, List<Integer>> entry : expected.entrySet())
        {
            Integer value = entry.getKey();
            List<Integer> expectedPostings = entry.getValue();
            UntypedResultSet results = execute("SELECT * FROM %s WHERE value = ?", value);
            List<Integer> actualPostings = new ArrayList<>();
            results.forEach(row -> actualPostings.add(row.getInt("pk")));
            actualPostings.sort(Integer::compareTo);
            assertEquals("Postings comparison failed for term = " + value, expectedPostings, actualPostings);
        }
    }

    @Test
    public void literalIndexNoCompactionTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, value text, PRIMARY KEY(pk))");
        disableCompaction();

        // Insert sufficient rows to make sure more than 1 segments are created before segment compaction
        Map<String, List<Integer>> expected = new HashMap<>();

        for (int rowId = 0; rowId < getRandom().nextIntBetween(50000, 100000); rowId++)
        {
            String value = Integer.toString(getRandom().nextIntBetween(0, 1000));
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", rowId, value);
            List<Integer> postings;
            if (expected.containsKey(value))
                postings = expected.get(value);
            else
            {
                postings = new ArrayList<>();
                expected.put(value, postings);
            }
            postings.add(rowId);

        }
        flush();

        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex' " +
                                       "WITH OPTIONS = {'enable_segment_compaction': 'false'}");
        waitForIndexQueryable();

        List<SegmentMetadata> segments = getSegments(indexName);
        assertTrue(segments.size() > 1);
        logger.info("Segments count: {}", segments.size());

        for (Map.Entry<String, List<Integer>> entry : expected.entrySet())
        {
            String value = entry.getKey();
            List<Integer> expectedPostings = entry.getValue();
            UntypedResultSet results = execute("SELECT * FROM %s WHERE value = ?", value);
            List<Integer> actualPostings = new ArrayList<>();
            results.forEach(row -> actualPostings.add(row.getInt("pk")));
            actualPostings.sort(Integer::compareTo);
            assertEquals("Postings comparison failed for term = " + value, expectedPostings, actualPostings);
        }
    }

    @Test
    public void numericIndexNoCompactionTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, value int, PRIMARY KEY(pk))");
        disableCompaction();

        // Insert sufficient rows to make sure more than 1 segments are created before segment compaction
        Map<Integer, List<Integer>> expected = new HashMap<>();

        for (int rowId = 0; rowId < getRandom().nextIntBetween(10000, 50000); rowId++)
        {
            int value = getRandom().nextIntBetween(0, 1000);
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", rowId, value);
            List<Integer> postings;
            if (expected.containsKey(value))
                postings = expected.get(value);
            else
            {
                postings = new ArrayList<>();
                expected.put(value, postings);
            }
            postings.add(rowId);

        }
        flush();

        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex' " +
                                       "WITH OPTIONS = {'enable_segment_compaction': 'false'}");
        waitForIndexQueryable();

        List<SegmentMetadata> segments = getSegments(indexName);
        assertTrue(segments.size() > 1);

        logger.info("Segments count: {}", segments.size());

        for (Map.Entry<Integer, List<Integer>> entry : expected.entrySet())
        {
            Integer value = entry.getKey();
            List<Integer> expectedPostings = entry.getValue();
            UntypedResultSet results = execute("SELECT * FROM %s WHERE value = ?", value);
            List<Integer> actualPostings = new ArrayList<>();
            results.forEach(row -> actualPostings.add(row.getInt("pk")));
            actualPostings.sort(Integer::compareTo);
            assertEquals("Postings comparison failed for term = " + value, expectedPostings, actualPostings);
        }
    }


    private List<SegmentMetadata> getSegments(String indexName) throws Throwable
    {
        Descriptor descriptor = Iterables.getOnlyElement(getCurrentColumnFamilyStore().getLiveSSTables()).descriptor;
        TableMetadata table = currentTableMetadata();
        IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor, table.partitioner, table.comparator);
        assertTrue(indexDescriptor.isPerSSTableBuildComplete());
        IndexMetadata index = table.indexes.get(indexName).get();
        IndexContext indexContext = new IndexContext(table, index);
        assertTrue(indexDescriptor.isPerIndexBuildComplete(indexContext));
        final MetadataSource source = MetadataSource.loadColumnMetadata(indexDescriptor, indexContext);
        return SegmentMetadata.load(source, indexDescriptor.primaryKeyFactory);
    }
}
