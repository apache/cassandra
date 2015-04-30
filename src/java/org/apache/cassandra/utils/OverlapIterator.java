package org.apache.cassandra.utils;

import java.util.*;

/**
 * A class for iterating sequentially through an ordered collection and efficiently
 * finding the overlapping set of matching intervals.
 *
 * The algorithm is quite simple: the intervals are sorted ascending by both min and max
 * in two separate lists. These lists are walked forwards each time we visit a new point,
 * with the set of intervals in the min-ordered list being added to our set of overlaps,
 * and those in the max-ordered list being removed.
 */
public class OverlapIterator<I extends Comparable<? super I>, V>
{
    // indexing into sortedByMin, tracks the next interval to include
    int nextToInclude;
    final List<Interval<I, V>> sortedByMin;
    // indexing into sortedByMax, tracks the next interval to exclude
    int nextToExclude;
    final List<Interval<I, V>> sortedByMax;
    final Set<V> overlaps = new HashSet<>();
    final Set<V> accessible = Collections.unmodifiableSet(overlaps);

    public OverlapIterator(Collection<Interval<I, V>> intervals)
    {
        sortedByMax = new ArrayList<>(intervals);
        Collections.sort(sortedByMax, Interval.<I, V>maxOrdering());
        // we clone after first sorting by max;  this is quite likely to make sort cheaper, since a.max < b.max
        // generally increases the likelihood that a.min < b.min, so the list may be partially sorted already.
        // this also means if (in future) we sort either collection (or a subset thereof) by the other's comparator
        // all items, including equal, will occur in the same order, including
        sortedByMin = new ArrayList<>(sortedByMax);
        Collections.sort(sortedByMin, Interval.<I, V>minOrdering());
    }

    // move the iterator forwards to the overlaps matching point
    public void update(I point)
    {
        // we don't use binary search here since we expect points to be a superset of the min/max values

        // add those we are now after the start of
        while (nextToInclude < sortedByMin.size() && sortedByMin.get(nextToInclude).min.compareTo(point) <= 0)
            overlaps.add(sortedByMin.get(nextToInclude++).data);
        // remove those we are now after the end of
        while (nextToExclude < sortedByMax.size() && sortedByMax.get(nextToExclude).max.compareTo(point) < 0)
            overlaps.remove(sortedByMax.get(nextToExclude++).data);
    }

    public Set<V> overlaps()
    {
        return accessible;
    }
}