package org.apache.cassandra.utils.IntervalTree;

import java.util.LinkedList;
import java.util.List;

public class IntervalTree<T>
{
    private final IntervalNode head;

    public IntervalTree()
    {
        head = null;
    }

    public IntervalTree(List<Interval> intervals)
    {
        head = new IntervalNode(intervals);
    }

    public Comparable max()
    {
        return head.v_max;
    }

    public Comparable min()
    {
        return head.v_min;
    }

    public List<T> search(Interval<T> searchInterval)
    {
        List<T> retlist = new LinkedList<T>();
        searchInternal(head, searchInterval, retlist);
        return retlist;
    }

    protected void searchInternal(IntervalNode node, Interval<T> searchInterval, List<T> retList)
    {
        if (null == head)
            return;
        if (null == node || node.v_pt == null)
            return;
        //if searchInterval.contains(node.v_pt)
        //then add every interval contained in this node to the result set then search left and right for further
        //overlapping intervals
        if (searchInterval.contains(node.v_pt))
        {
            for (Interval<T> interval : node.intersects_left)
            {
                retList.add(interval.Data);
            }

            searchInternal(node.left, searchInterval, retList);
            searchInternal(node.right, searchInterval, retList);
            return;
        }

        //if v.pt < searchInterval.left
        //add intervals in v with v[i].right >= searchInterval.left
        //L contains no overlaps
        //R May
        if (node.v_pt.compareTo(searchInterval.min) < 0)
        {
            for (Interval<T> interval : node.intersects_right)
            {
                if (interval.max.compareTo(searchInterval.min) >= 0)
                {
                    retList.add(interval.Data);
                }
                else break;
            }
            searchInternal(node.right, searchInterval, retList);
            return;
        }

        //if v.pt > searchInterval.right
        //add intervals in v with [i].left <= searchInterval.right
        //R contains no overlaps
        //L May
        if (node.v_pt.compareTo(searchInterval.max) > 0)
        {
            for (Interval<T> interval : node.intersects_left)
            {
                if (interval.min.compareTo(searchInterval.max) <= 0)
                {
                    retList.add(interval.Data);
                }
                else break;
            }
            searchInternal(node.left, searchInterval, retList);
            return;
        }
    }
}
