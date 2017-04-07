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
package org.apache.cassandra.db.partition;

import static org.junit.Assert.*;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.partitions.AbstractBTreePartition;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.rows.Row.Deletion;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.SearchIterator;

public class PartitionImplementationTest
{
    private static final String KEYSPACE = "PartitionImplementationTest";
    private static final String CF = "Standard";

    private static final int ENTRIES = 250;
    private static final int TESTS = 1000;
    private static final int KEY_RANGE = ENTRIES * 5;

    private static final int TIMESTAMP = KEY_RANGE + 1;

    private static CFMetaData cfm;
    private Random rand = new Random(2);

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();

        cfm = CFMetaData.Builder.create(KEYSPACE, CF)
                                        .addPartitionKey("pk", AsciiType.instance)
                                        .addClusteringColumn("ck", AsciiType.instance)
                                        .addRegularColumn("col", AsciiType.instance)
                                        .addStaticColumn("static_col", AsciiType.instance)
                                        .build();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    cfm);
    }

    private List<Row> generateRows()
    {
        List<Row> content = new ArrayList<>();
        Set<Integer> keysUsed = new HashSet<>();
        for (int i = 0; i < ENTRIES; ++i)
        {
            int rk;
            do
            {
                rk = rand.nextInt(KEY_RANGE);
            }
            while (!keysUsed.add(rk));
            content.add(makeRow(clustering(rk), "Col" + rk));
        }
        return content; // not sorted
    }

    Row makeRow(Clustering clustering, String colValue)
    {
        ColumnDefinition defCol = cfm.getColumnDefinition(new ColumnIdentifier("col", true));
        Row.Builder row = BTreeRow.unsortedBuilder(TIMESTAMP);
        row.newRow(clustering);
        row.addCell(BufferCell.live(defCol, TIMESTAMP, ByteBufferUtil.bytes(colValue)));
        return row.build();
    }

    Row makeStaticRow()
    {
        ColumnDefinition defCol = cfm.getColumnDefinition(new ColumnIdentifier("static_col", true));
        Row.Builder row = BTreeRow.unsortedBuilder(TIMESTAMP);
        row.newRow(Clustering.STATIC_CLUSTERING);
        row.addCell(BufferCell.live(defCol, TIMESTAMP, ByteBufferUtil.bytes("static value")));
        return row.build();
    }

    private List<Unfiltered> generateMarkersOnly()
    {
        return addMarkers(new ArrayList<>());
    }

    private List<Unfiltered> generateUnfiltereds()
    {
        List<Unfiltered> content = new ArrayList<>(generateRows());
        return addMarkers(content);
    }

    List<Unfiltered> addMarkers(List<Unfiltered> content)
    {
        List<RangeTombstoneMarker> markers = new ArrayList<>();
        Set<Integer> delTimes = new HashSet<>();
        for (int i = 0; i < ENTRIES / 10; ++i)
        {
            int delTime;
            do
            {
                delTime = rand.nextInt(KEY_RANGE);
            }
            while (!delTimes.add(delTime));

            int start = rand.nextInt(KEY_RANGE);
            DeletionTime dt = new DeletionTime(delTime, delTime);
            RangeTombstoneMarker open = RangeTombstoneBoundMarker.inclusiveOpen(false, clustering(start).getRawValues(), dt);
            int end = start + rand.nextInt((KEY_RANGE - start) / 4 + 1);
            RangeTombstoneMarker close = RangeTombstoneBoundMarker.inclusiveClose(false, clustering(end).getRawValues(), dt);
            markers.add(open);
            markers.add(close);
        }
        markers.sort(cfm.comparator);

        RangeTombstoneMarker toAdd = null;
        Set<DeletionTime> open = new HashSet<>();
        DeletionTime current = DeletionTime.LIVE;
        for (RangeTombstoneMarker marker : markers)
        {
            if (marker.isOpen(false))
            {
                DeletionTime delTime = marker.openDeletionTime(false);
                open.add(delTime);
                if (delTime.supersedes(current))
                {
                    if (toAdd != null)
                    {
                        if (cfm.comparator.compare(toAdd, marker) != 0)
                            content.add(toAdd);
                        else
                        {
                            // gotta join
                            current = toAdd.isClose(false) ? toAdd.closeDeletionTime(false) : DeletionTime.LIVE;
                        }
                    }
                    if (current != DeletionTime.LIVE)
                        marker = RangeTombstoneBoundaryMarker.makeBoundary(false, marker.openBound(false).invert(), marker.openBound(false), current, delTime);
                    toAdd = marker;
                    current = delTime;
                }
            }
            else
            {
                assert marker.isClose(false);
                DeletionTime delTime = marker.closeDeletionTime(false);
                boolean removed = open.remove(delTime);
                assert removed;
                if (current.equals(delTime))
                {
                    if (toAdd != null)
                    {
                        if (cfm.comparator.compare(toAdd, marker) != 0)
                            content.add(toAdd);
                        else
                        {
                            // gotta join
                            current = toAdd.closeDeletionTime(false);
                            marker = new RangeTombstoneBoundMarker(marker.closeBound(false), current);
                        }
                    }
                    DeletionTime best = open.stream().max(DeletionTime::compareTo).orElse(DeletionTime.LIVE);
                    if (best != DeletionTime.LIVE)
                        marker = RangeTombstoneBoundaryMarker.makeBoundary(false, marker.closeBound(false), marker.closeBound(false).invert(), current, best);
                    toAdd = marker;
                    current = best;
                }
            }
        }
        content.add(toAdd);
        assert current == DeletionTime.LIVE;
        assert open.isEmpty();
        return content;
    }

    private Clustering clustering(int i)
    {
        return cfm.comparator.make(String.format("Row%06d", i));
    }

    private void test(Supplier<Collection<? extends Unfiltered>> content, Row staticRow)
    {
        for (int i = 0; i<TESTS; ++i)
        {
            try
            {
                rand = new Random(i);
                testIter(content, staticRow);
            }
            catch (Throwable t)
            {
                throw new AssertionError("Test failed with seed " + i, t);
            }
        }
    }

    private void testIter(Supplier<Collection<? extends Unfiltered>> contentSupplier, Row staticRow)
    {
        NavigableSet<Clusterable> sortedContent = new TreeSet<Clusterable>(cfm.comparator);
        sortedContent.addAll(contentSupplier.get());
        AbstractBTreePartition partition;
        try (UnfilteredRowIterator iter = new Util.UnfilteredSource(cfm, Util.dk("pk"), staticRow, sortedContent.stream().map(x -> (Unfiltered) x).iterator()))
        {
            partition = ImmutableBTreePartition.create(iter);
        }

        ColumnDefinition defCol = cfm.getColumnDefinition(new ColumnIdentifier("col", true));
        ColumnFilter cf = ColumnFilter.selectionBuilder().add(defCol).build();
        Function<? super Clusterable, ? extends Clusterable> colFilter = x -> x instanceof Row ? ((Row) x).filter(cf, cfm) : x;
        Slices slices = Slices.with(cfm.comparator, Slice.make(clustering(KEY_RANGE / 4), clustering(KEY_RANGE * 3 / 4)));
        Slices multiSlices = makeSlices();

        // lastRow
        assertRowsEqual((Row) get(sortedContent.descendingSet(), x -> x instanceof Row),
                        partition.lastRow());
        // get(static)
        assertRowsEqual(staticRow,
                        partition.getRow(Clustering.STATIC_CLUSTERING));

        // get
        for (int i=0; i < KEY_RANGE; ++i)
        {
            Clustering cl = clustering(i);
            assertRowsEqual(getRow(sortedContent, cl),
                            partition.getRow(cl));
        }
        // isEmpty
        assertEquals(sortedContent.isEmpty() && staticRow == null,
                     partition.isEmpty());
        // hasRows
        assertEquals(sortedContent.stream().anyMatch(x -> x instanceof Row),
                     partition.hasRows());

        // iterator
        assertIteratorsEqual(sortedContent.stream().filter(x -> x instanceof Row).iterator(),
                             partition.iterator());

        // unfiltered iterator
        assertIteratorsEqual(sortedContent.iterator(),
                             partition.unfilteredIterator());

        // unfiltered iterator
        assertIteratorsEqual(sortedContent.iterator(),
                             partition.unfilteredIterator(ColumnFilter.all(cfm), Slices.ALL, false));
        // column-filtered
        assertIteratorsEqual(sortedContent.stream().map(colFilter).iterator(),
                             partition.unfilteredIterator(cf, Slices.ALL, false));
        // sliced
        assertIteratorsEqual(slice(sortedContent, slices.get(0)),
                             partition.unfilteredIterator(ColumnFilter.all(cfm), slices, false));
        assertIteratorsEqual(streamOf(slice(sortedContent, slices.get(0))).map(colFilter).iterator(),
                             partition.unfilteredIterator(cf, slices, false));
        // randomly multi-sliced
        assertIteratorsEqual(slice(sortedContent, multiSlices),
                             partition.unfilteredIterator(ColumnFilter.all(cfm), multiSlices, false));
        assertIteratorsEqual(streamOf(slice(sortedContent, multiSlices)).map(colFilter).iterator(),
                             partition.unfilteredIterator(cf, multiSlices, false));
        // reversed
        assertIteratorsEqual(sortedContent.descendingIterator(),
                             partition.unfilteredIterator(ColumnFilter.all(cfm), Slices.ALL, true));
        assertIteratorsEqual(sortedContent.descendingSet().stream().map(colFilter).iterator(),
                             partition.unfilteredIterator(cf, Slices.ALL, true));
        assertIteratorsEqual(invert(slice(sortedContent, slices.get(0))),
                             partition.unfilteredIterator(ColumnFilter.all(cfm), slices, true));
        assertIteratorsEqual(streamOf(invert(slice(sortedContent, slices.get(0)))).map(colFilter).iterator(),
                             partition.unfilteredIterator(cf, slices, true));
        assertIteratorsEqual(invert(slice(sortedContent, multiSlices)),
                             partition.unfilteredIterator(ColumnFilter.all(cfm), multiSlices, true));
        assertIteratorsEqual(streamOf(invert(slice(sortedContent, multiSlices))).map(colFilter).iterator(),
                             partition.unfilteredIterator(cf, multiSlices, true));

        // search iterator
        testSearchIterator(sortedContent, partition, ColumnFilter.all(cfm), false);
        testSearchIterator(sortedContent, partition, cf, false);
        testSearchIterator(sortedContent, partition, ColumnFilter.all(cfm), true);
        testSearchIterator(sortedContent, partition, cf, true);

        // sliceable iter
        testSlicingOfIterators(sortedContent, partition, ColumnFilter.all(cfm), false);
        testSlicingOfIterators(sortedContent, partition, cf, false);
        testSlicingOfIterators(sortedContent, partition, ColumnFilter.all(cfm), true);
        testSlicingOfIterators(sortedContent, partition, cf, true);
    }

    void testSearchIterator(NavigableSet<Clusterable> sortedContent, Partition partition, ColumnFilter cf, boolean reversed)
    {
        SearchIterator<Clustering, Row> searchIter = partition.searchIterator(cf, reversed);
        int pos = reversed ? KEY_RANGE : 0;
        int mul = reversed ? -1 : 1;
        boolean started = false;
        while (pos < KEY_RANGE)
        {
            int skip = rand.nextInt(KEY_RANGE / 10);
            pos += skip * mul;
            Clustering cl = clustering(pos);
            Row row = searchIter.next(cl);  // returns row with deletion, incl. empty row with deletion
            if (row == null && skip == 0 && started)    // allowed to return null if already reported row
                continue;
            started = true;
            Row expected = getRow(sortedContent, cl);
            assertEquals(expected == null, row == null);
            if (row == null)
                continue;
            assertRowsEqual(expected.filter(cf, cfm), row);
        }
    }

    Slices makeSlices()
    {
        int pos = 0;
        Slices.Builder builder = new Slices.Builder(cfm.comparator);
        while (pos <= KEY_RANGE)
        {
            int skip = rand.nextInt(KEY_RANGE / 10) * (rand.nextInt(3) + 2 / 3); // increased chance of getting 0
            pos += skip;
            int sz = rand.nextInt(KEY_RANGE / 10) + (skip == 0 ? 1 : 0);    // if start is exclusive need at least sz 1
            Clustering start = clustering(pos);
            pos += sz;
            Clustering end = clustering(pos);
            Slice slice = Slice.make(skip == 0 ? ClusteringBound.exclusiveStartOf(start) : ClusteringBound.inclusiveStartOf(start), ClusteringBound.inclusiveEndOf(end));
            builder.add(slice);
        }
        return builder.build();
    }

    void testSlicingOfIterators(NavigableSet<Clusterable> sortedContent, AbstractBTreePartition partition, ColumnFilter cf, boolean reversed)
    {
        Function<? super Clusterable, ? extends Clusterable> colFilter = x -> x instanceof Row ? ((Row) x).filter(cf, cfm) : x;
        Slices slices = makeSlices();

        // fetch each slice in turn
        for (Slice slice : (Iterable<Slice>) () -> directed(slices, reversed))
        {
            try (UnfilteredRowIterator slicedIter = partition.unfilteredIterator(cf, Slices.with(cfm.comparator, slice), reversed))
            {
                assertIteratorsEqual(streamOf(directed(slice(sortedContent, slice), reversed)).map(colFilter).iterator(),
                                     slicedIter);
            }
        }

        // Fetch all slices at once
        try (UnfilteredRowIterator slicedIter = partition.unfilteredIterator(cf, slices, reversed))
        {
            List<Iterator<? extends Clusterable>> slicelist = new ArrayList<>();
            slices.forEach(slice -> slicelist.add(directed(slice(sortedContent, slice), reversed)));
            if (reversed)
                Collections.reverse(slicelist);

            assertIteratorsEqual(Iterators.concat(slicelist.toArray(new Iterator[0])), slicedIter);
        }
    }

    private<T> Iterator<T> invert(Iterator<T> slice)
    {
        Deque<T> dest = new LinkedList<>();
        Iterators.addAll(dest, slice);
        return dest.descendingIterator();
    }

    private Iterator<Clusterable> slice(NavigableSet<Clusterable> sortedContent, Slices slices)
    {
        return Iterators.concat(streamOf(slices).map(slice -> slice(sortedContent, slice)).iterator());
    }

    private Iterator<Clusterable> slice(NavigableSet<Clusterable> sortedContent, Slice slice)
    {
        // Slice bounds are inclusive bounds, equal only to markers. Matched markers should be returned as one-sided boundaries.
        RangeTombstoneMarker prev = (RangeTombstoneMarker) sortedContent.headSet(slice.start(), true).descendingSet().stream().filter(x -> x instanceof RangeTombstoneMarker).findFirst().orElse(null);
        RangeTombstoneMarker next = (RangeTombstoneMarker) sortedContent.tailSet(slice.end(), true).stream().filter(x -> x instanceof RangeTombstoneMarker).findFirst().orElse(null);
        Iterator<Clusterable> result = sortedContent.subSet(slice.start(), false, slice.end(), false).iterator();
        if (prev != null && prev.isOpen(false))
            result = Iterators.concat(Iterators.singletonIterator(new RangeTombstoneBoundMarker(slice.start(), prev.openDeletionTime(false))), result);
        if (next != null && next.isClose(false))
            result = Iterators.concat(result, Iterators.singletonIterator(new RangeTombstoneBoundMarker(slice.end(), next.closeDeletionTime(false))));
        return result;
    }

    private Iterator<Slice> directed(Slices slices, boolean reversed)
    {
        return directed(slices.iterator(), reversed);
    }

    private <T> Iterator<T> directed(Iterator<T> iter, boolean reversed)
    {
        if (!reversed)
            return iter;
        return invert(iter);
    }

    private <T> Stream<T> streamOf(Iterator<T> iterator)
    {
        Iterable<T> iterable = () -> iterator;
        return streamOf(iterable);
    }

    <T> Stream<T> streamOf(Iterable<T> iterable)
    {
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    private void assertIteratorsEqual(Iterator<? extends Clusterable> it1, Iterator<? extends Clusterable> it2)
    {
        Clusterable[] a1 = (Clusterable[]) Iterators.toArray(it1, Clusterable.class);
        Clusterable[] a2 = (Clusterable[]) Iterators.toArray(it2, Clusterable.class);
        if (Arrays.equals(a1, a2))
            return;
        String a1s = Stream.of(a1).map(x -> "\n" + (x instanceof Unfiltered ? ((Unfiltered) x).toString(cfm) : x.toString())).collect(Collectors.toList()).toString();
        String a2s = Stream.of(a2).map(x -> "\n" + (x instanceof Unfiltered ? ((Unfiltered) x).toString(cfm) : x.toString())).collect(Collectors.toList()).toString();
        assertArrayEquals("Arrays differ. Expected " + a1s + " was " + a2s, a1, a2);
    }

    private Row getRow(NavigableSet<Clusterable> sortedContent, Clustering cl)
    {
        NavigableSet<Clusterable> nexts = sortedContent.tailSet(cl, true);
        if (nexts.isEmpty())
            return null;
        Row row = nexts.first() instanceof Row && cfm.comparator.compare(cl, nexts.first()) == 0 ? (Row) nexts.first() : null;
        for (Clusterable next : nexts)
            if (next instanceof RangeTombstoneMarker)
            {
                RangeTombstoneMarker rt = (RangeTombstoneMarker) next;
                if (!rt.isClose(false))
                    return row;
                DeletionTime delTime = rt.closeDeletionTime(false);
                return row == null ? BTreeRow.emptyDeletedRow(cl, Deletion.regular(delTime)) : row.filter(ColumnFilter.all(cfm), delTime, true, cfm);
            }
        return row;
    }

    private void assertRowsEqual(Row expected, Row actual)
    {
        try
        {
            assertEquals(expected == null, actual == null);
            if (expected == null)
                return;
            assertEquals(expected.clustering(), actual.clustering());
            assertEquals(expected.deletion(), actual.deletion());
            assertArrayEquals(Iterables.toArray(expected.cells(), Cell.class), Iterables.toArray(expected.cells(), Cell.class));
        } catch (Throwable t)
        {
            throw new AssertionError(String.format("Row comparison failed, expected %s got %s", expected, actual), t);
        }
    }

    private static<T> T get(NavigableSet<T> sortedContent, Predicate<T> test)
    {
        return sortedContent.stream().filter(test).findFirst().orElse(null);
    }

    @Test
    public void testEmpty()
    {
        test(() -> Collections.<Row>emptyList(), null);
    }

    @Test
    public void testStaticOnly()
    {
        test(() -> Collections.<Row>emptyList(), makeStaticRow());
    }

    @Test
    public void testRows()
    {
        test(this::generateRows, null);
    }

    @Test
    public void testRowsWithStatic()
    {
        test(this::generateRows, makeStaticRow());
    }

    @Test
    public void testMarkersOnly()
    {
        test(this::generateMarkersOnly, null);
    }

    @Test
    public void testMarkersWithStatic()
    {
        test(this::generateMarkersOnly, makeStaticRow());
    }

    @Test
    public void testUnfiltereds()
    {
        test(this::generateUnfiltereds, makeStaticRow());
    }

}
