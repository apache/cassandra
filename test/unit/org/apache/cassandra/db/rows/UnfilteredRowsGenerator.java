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
package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Unfiltered.Kind;
import org.apache.cassandra.utils.btree.BTree;

public class UnfilteredRowsGenerator
{
    final boolean reversed;
    final Comparator<Clusterable> comparator;

    public UnfilteredRowsGenerator(Comparator<Clusterable> comparator, boolean reversed)
    {
        this.reversed = reversed;
        this.comparator = comparator;
    }

    String str(Clusterable curr)
    {
        if (curr == null)
            return "null";
        String val = Int32Type.instance.getString(curr.clustering().get(0));
        if (curr instanceof RangeTombstoneMarker)
        {
            RangeTombstoneMarker marker = (RangeTombstoneMarker) curr;
            if (marker.isClose(reversed))
                val = "[" + marker.closeDeletionTime(reversed).markedForDeleteAt() + "]" + (marker.closeIsInclusive(reversed) ? "<=" : "<") + val;
            if (marker.isOpen(reversed))
                val = val + (marker.openIsInclusive(reversed) ? "<=" : "<") + "[" + marker.openDeletionTime(reversed).markedForDeleteAt() + "]";
        }
        else if (curr instanceof Row)
        {
            Row row = (Row) curr;
            String delTime = "";
            if (!row.deletion().time().isLive())
                delTime = "D" + row.deletion().time().markedForDeleteAt();
            val = val + "[" + row.primaryKeyLivenessInfo().timestamp() + delTime + "]";
        }
        return val;
    }

    public void verifyValid(List<Unfiltered> list)
    {
        verifyValid(list, reversed);
    }

    void verifyValid(List<Unfiltered> list, boolean reversed)
    {
        int reversedAsMultiplier = reversed ? -1 : 1;
        try {
            RangeTombstoneMarker prev = null;
            Unfiltered prevUnfiltered = null;
            for (Unfiltered unfiltered : list)
            {
                Assert.assertTrue("Order violation prev " + str(prevUnfiltered) + " curr " + str(unfiltered),
                                  prevUnfiltered == null || comparator.compare(prevUnfiltered, unfiltered) * reversedAsMultiplier < 0);
                prevUnfiltered = unfiltered;

                if (unfiltered.kind() == Kind.RANGE_TOMBSTONE_MARKER)
                {
                    RangeTombstoneMarker curr = (RangeTombstoneMarker) unfiltered;
                    if (prev != null)
                    {
                        if (curr.isClose(reversed))
                        {
                            Assert.assertTrue(str(unfiltered) + " follows another close marker " + str(prev), prev.isOpen(reversed));
                            Assert.assertEquals("Deletion time mismatch for open " + str(prev) + " and close " + str(unfiltered),
                                                prev.openDeletionTime(reversed),
                                                curr.closeDeletionTime(reversed));
                        }
                        else
                            Assert.assertFalse(str(curr) + " follows another open marker " + str(prev), prev.isOpen(reversed));
                    }

                    prev = curr;
                }
            }
            Assert.assertFalse("Cannot end in open marker " + str(prev), prev != null && prev.isOpen(reversed));

        } catch (AssertionError e) {
            System.out.println(e);
            dumpList(list);
            throw e;
        }
    }

    public List<Unfiltered> generateSource(Random r, int items, int range, int del_range, Function<Integer, Integer> timeGenerator)
    {
        int[] positions = new int[items + 1];
        for (int i=0; i<items; ++i)
            positions[i] = r.nextInt(range);
        positions[items] = range;
        Arrays.sort(positions);

        List<Unfiltered> content = new ArrayList<>(items);
        int prev = -1;
        for (int i=0; i<items; ++i)
        {
            int pos = positions[i];
            int sz = positions[i + 1] - pos;
            if (sz == 0 && pos == prev)
                // Filter out more than two of the same position.
                continue;
            if (r.nextBoolean() || pos == prev)
            {
                int span;
                boolean includesStart;
                boolean includesEnd;
                if (pos > prev)
                {
                    span = r.nextInt(sz + 1);
                    includesStart = span > 0 ? r.nextBoolean() : true;
                    includesEnd = span > 0 ? r.nextBoolean() : true;
                }
                else
                {
                    span = 1 + r.nextInt(sz);
                    includesStart = false;
                    includesEnd = r.nextBoolean();
                }
                int deltime = r.nextInt(del_range);
                DeletionTime dt = new DeletionTime(deltime, deltime);
                content.add(new RangeTombstoneBoundMarker(boundFor(pos, true, includesStart), dt));
                content.add(new RangeTombstoneBoundMarker(boundFor(pos + span, false, includesEnd), dt));
                prev = pos + span - (includesEnd ? 0 : 1);
            }
            else
            {
                content.add(emptyRowAt(pos, timeGenerator));
                prev = pos;
            }
        }

        attachBoundaries(content);
        if (reversed)
        {
            Collections.reverse(content);
        }
        verifyValid(content);
        if (items <= 20)
            dumpList(content);
        return content;
    }

    /**
     * Constructs a list of unfiltereds with integer clustering according to the specification string.
     *
     * The string is a space-delimited sorted list that can contain:
     *  * open tombstone markers, e.g. xx<[yy] where xx is the clustering, yy is the deletion time, and "<" stands for
     *    non-inclusive (<= for inclusive).
     *  * close tombstone markers, e.g. [yy]<=xx. Adjacent close and open markers (e.g. [yy]<=xx xx<[zz]) are combined
     *    into boundary markers.
     *  * empty rows, e.g. xx or xx[yy] or xx[yyDzz] where xx is the clustering, yy is the live time and zz is deletion
     *    time.
     *
     * @param input Specification.
     * @param default_liveness Liveness to use for rows if not explicitly specified.
     * @return Parsed list.
     */
    public List<Unfiltered> parse(String input, int default_liveness)
    {
        String[] split = input.split(" ");
        Pattern open = Pattern.compile("(\\d+)<(=)?\\[(\\d+)\\]");
        Pattern close = Pattern.compile("\\[(\\d+)\\]<(=)?(\\d+)");
        Pattern row = Pattern.compile("(\\d+)(\\[(\\d+)(?:D(\\d+))?\\])?");
        List<Unfiltered> out = new ArrayList<>(split.length);
        for (String s : split)
        {
            Matcher m = open.matcher(s);
            if (m.matches())
            {
                out.add(openMarker(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(3)), m.group(2) != null));
                continue;
            }
            m = close.matcher(s);
            if (m.matches())
            {
                out.add(closeMarker(Integer.parseInt(m.group(3)), Integer.parseInt(m.group(1)), m.group(2) != null));
                continue;
            }
            m = row.matcher(s);
            if (m.matches())
            {
                int live = m.group(3) != null ? Integer.parseInt(m.group(3)) : default_liveness;
                int delTime = m.group(4) != null ? Integer.parseInt(m.group(4)) : -1;
                out.add(emptyRowAt(Integer.parseInt(m.group(1)), live, delTime));
                continue;
            }
            Assert.fail("Can't parse " + s);
        }
        attachBoundaries(out);
        return out;
    }

    static Row emptyRowAt(int pos, Function<Integer, Integer> timeGenerator)
    {
        final Clustering clustering = clusteringFor(pos);
        final LivenessInfo live = LivenessInfo.create(timeGenerator.apply(pos), UnfilteredRowIteratorsMergeTest.nowInSec);
        return BTreeRow.noCellLiveRow(clustering, live);
    }

    static Row emptyRowAt(int pos, int time, int deletionTime)
    {
        final Clustering clustering = clusteringFor(pos);
        final LivenessInfo live = LivenessInfo.create(time, UnfilteredRowIteratorsMergeTest.nowInSec);
        final DeletionTime delTime = deletionTime == -1 ? DeletionTime.LIVE : new DeletionTime(deletionTime, deletionTime);
        return BTreeRow.create(clustering, live, Row.Deletion.regular(delTime), BTree.empty());
    }

    static Clustering clusteringFor(int i)
    {
        return Clustering.make(Int32Type.instance.decompose(i));
    }

    static ClusteringBound boundFor(int pos, boolean start, boolean inclusive)
    {
        return ClusteringBound.create(ClusteringBound.boundKind(start, inclusive), new ByteBuffer[] {Int32Type.instance.decompose(pos)});
    }

    static void attachBoundaries(List<Unfiltered> content)
    {
        int di = 0;
        RangeTombstoneMarker prev = null;
        for (int si = 0; si < content.size(); ++si)
        {
            Unfiltered currUnfiltered = content.get(si);
            RangeTombstoneMarker curr = currUnfiltered.kind() == Kind.RANGE_TOMBSTONE_MARKER ?
                                        (RangeTombstoneMarker) currUnfiltered :
                                        null;
            if (prev != null && curr != null && prev.isClose(false) && curr.isOpen(false) && prev.clustering().invert().equals(curr.clustering()))
            {
                // Join. Prefer not to use merger to check its correctness.
                ClusteringBound b = (ClusteringBound) prev.clustering();
                ClusteringBoundary boundary = ClusteringBoundary.create(
                        b.isInclusive() ? ClusteringBound.Kind.INCL_END_EXCL_START_BOUNDARY : ClusteringBound.Kind.EXCL_END_INCL_START_BOUNDARY,
                        b.getRawValues());
                prev = new RangeTombstoneBoundaryMarker(boundary, prev.closeDeletionTime(false), curr.openDeletionTime(false));
                currUnfiltered = prev;
                --di;
            }
            content.set(di++, currUnfiltered);
            prev = curr;
        }
        for (int pos = content.size() - 1; pos >= di; --pos)
            content.remove(pos);
    }

    static RangeTombstoneMarker openMarker(int pos, int delTime, boolean inclusive)
    {
        return marker(pos, delTime, true, inclusive);
    }

    static RangeTombstoneMarker closeMarker(int pos, int delTime, boolean inclusive)
    {
        return marker(pos, delTime, false, inclusive);
    }

    private static RangeTombstoneMarker marker(int pos, int delTime, boolean isStart, boolean inclusive)
    {
        return new RangeTombstoneBoundMarker(ClusteringBound.create(ClusteringBound.boundKind(isStart, inclusive),
                                                                    new ByteBuffer[] {clusteringFor(pos).get(0)}),
                                             new DeletionTime(delTime, delTime));
    }

    public static UnfilteredRowIterator source(Iterable<Unfiltered> content, CFMetaData metadata, DecoratedKey partitionKey)
    {
        return source(content, metadata, partitionKey, DeletionTime.LIVE);
    }

    public static UnfilteredRowIterator source(Iterable<Unfiltered> content, CFMetaData metadata, DecoratedKey partitionKey, DeletionTime delTime)
    {
        return new Source(content.iterator(), metadata, partitionKey, delTime, false);
    }

    static class Source extends AbstractUnfilteredRowIterator implements UnfilteredRowIterator
    {
        Iterator<Unfiltered> content;

        protected Source(Iterator<Unfiltered> content, CFMetaData metadata, DecoratedKey partitionKey, DeletionTime partitionLevelDeletion, boolean reversed)
        {
            super(metadata,
                  partitionKey,
                  partitionLevelDeletion,
                  metadata.partitionColumns(),
                  Rows.EMPTY_STATIC_ROW,
                  reversed,
                  EncodingStats.NO_STATS);
            this.content = content;
        }

        @Override
        protected Unfiltered computeNext()
        {
            return content.hasNext() ? content.next() : endOfData();
        }
    }

    public String str(List<Unfiltered> list)
    {
        StringBuilder builder = new StringBuilder();
        for (Unfiltered u : list)
        {
            builder.append(str(u));
            builder.append(' ');
        }
        return builder.toString();
    }

    public void dumpList(List<Unfiltered> list)
    {
        System.out.println(str(list));
    }
}