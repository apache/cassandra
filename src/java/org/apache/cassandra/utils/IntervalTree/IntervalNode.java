package org.apache.cassandra.utils.IntervalTree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import com.google.common.collect.ImmutableList;

public class IntervalNode
{
    Comparable v_pt;
    Comparable v_min;
    Comparable v_max;
    List<Interval> intersects_left;
    List<Interval> intersects_right;
    IntervalNode left = null;
    IntervalNode right = null;

    public IntervalNode(List<Interval> toBisect)
    {
        if (toBisect.size() > 0)
        {
            findMinMedianMax(toBisect);
            List<Interval> intersects = getIntersectingIntervals(toBisect);
            intersects_left = Interval.minOrdering.sortedCopy(intersects);
            intersects_right = Interval.maxOrdering.reverse().sortedCopy(intersects);
            //if i.max < v_pt then it goes to the left subtree
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

    public void findMinMedianMax(List<Interval> intervals)
    {
        if (intervals.size() > 0)
        {
            List<Comparable> allEndpoints = new ArrayList<Comparable>(intervals.size() * 2);

            for (Interval interval : intervals)
            {
                allEndpoints.add(interval.min);
                allEndpoints.add(interval.max);
            }
            Collections.sort(allEndpoints);
            v_pt = allEndpoints.get(intervals.size());
            v_min = allEndpoints.get(0);
            v_max = allEndpoints.get(allEndpoints.size() - 1);
        }
    }
}
