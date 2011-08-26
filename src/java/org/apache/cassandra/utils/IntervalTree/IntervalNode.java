package org.apache.cassandra.utils.IntervalTree;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

public class IntervalNode
{
    Interval interval;
    Comparable v_pt;
    List<Interval> v_left;
    List<Interval> v_right;
    IntervalNode left = null;
    IntervalNode right = null;

    public IntervalNode(List<Interval> toBisect)
    {
        if (toBisect.size() > 0)
        {
            v_pt = findMedianEndpoint(toBisect);
            v_left = interval.minOrdering.sortedCopy(getIntersectingIntervals(toBisect));
            v_right = interval.maxOrdering.reverse().sortedCopy(getIntersectingIntervals(toBisect));
            //if i.min < v_pt then it goes to the left subtree
            List<Interval> leftSegment = getLeftIntervals(toBisect);
            List<Interval> rightSegment = getRightIntervals(toBisect);
            if (leftSegment.size() > 0)
                this.left = new IntervalNode(leftSegment);
            if (rightSegment.size() > 0)
                this.right = new IntervalNode(rightSegment);
        }
    }

    public List<Interval> getLeftIntervals(List<Interval> candidates)
    {
        List<Interval> retval = new ArrayList<Interval>();
        for (Interval candidate : candidates)
        {
            if (candidate.max.compareTo(v_pt) < 0)
                retval.add(candidate);
        }
        return retval;
    }

    public List<Interval> getRightIntervals(List<Interval> candidates)
    {
        List<Interval> retval = new ArrayList<Interval>();
        for (Interval candidate : candidates)
        {
            if (candidate.min.compareTo(v_pt) > 0)
                retval.add(candidate);
        }
        return retval;
    }

    public List<Interval> getIntersectingIntervals(List<Interval> candidates)
    {
        List<Interval> retval = new ArrayList<Interval>();
        for (Interval candidate : candidates)
        {
            if (candidate.min.compareTo(v_pt) <= 0
                && candidate.max.compareTo(v_pt) >= 0)
                retval.add(candidate);
        }
        return retval;
    }

    public Comparable findMedianEndpoint(List<Interval> intervals)
    {

        ConcurrentSkipListSet<Comparable> sortedSet = new ConcurrentSkipListSet<Comparable>();

        for (Interval interval : intervals)
        {
            sortedSet.add(interval.min);
            sortedSet.add(interval.max);
        }
        int medianIndex = sortedSet.size() / 2;
        if (sortedSet.size() > 0)
        {
            return (Comparable) sortedSet.toArray()[medianIndex];
        }
        return null;
    }

}
