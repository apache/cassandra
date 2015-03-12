package org.apache.cassandra.db.lifecycle;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class SSTableIntervalTree extends IntervalTree<RowPosition, SSTableReader, Interval<RowPosition, SSTableReader>>
{
    private static final SSTableIntervalTree EMPTY = new SSTableIntervalTree(null);

    SSTableIntervalTree(Collection<Interval<RowPosition, SSTableReader>> intervals)
    {
        super(intervals);
    }

    public static SSTableIntervalTree empty()
    {
        return EMPTY;
    }

    public static SSTableIntervalTree build(Iterable<SSTableReader> sstables)
    {
        return new SSTableIntervalTree(buildIntervals(sstables));
    }

    public static List<Interval<RowPosition, SSTableReader>> buildIntervals(Iterable<SSTableReader> sstables)
    {
        List<Interval<RowPosition, SSTableReader>> intervals = new ArrayList<>(Iterables.size(sstables));
        for (SSTableReader sstable : sstables)
            intervals.add(Interval.<RowPosition, SSTableReader>create(sstable.first, sstable.last, sstable));
        return intervals;
    }
}