package org.apache.cassandra.utils;

import junit.framework.TestCase;
import org.apache.cassandra.utils.IntervalTree.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class IntervalTreeTest extends TestCase
{
    @Test
    public void testSearch() throws Exception
    {
        List<Interval> intervals = new ArrayList<Interval>();

        intervals.add(new Interval(-300, -200));
        intervals.add(new Interval(-3, -2));
        intervals.add(new Interval(1,2));
        intervals.add(new Interval(3,6));
        intervals.add(new Interval(2,4));
        intervals.add(new Interval(5,7));
        intervals.add(new Interval(1,3));
        intervals.add(new Interval(4,6));
        intervals.add(new Interval(8,9));
        intervals.add(new Interval(15,20));
        intervals.add(new Interval(40,50));
        intervals.add(new Interval(49,60));


        IntervalTree it = new IntervalTree(intervals);

        assertEquals(3,it.search(new Interval(4,4)).size());

        assertEquals(4, it.search(new Interval(4, 5)).size());

        assertEquals(7, it.search(new Interval(-1,10)).size());

        assertEquals(0, it.search(new Interval(-1,-1)).size());

        assertEquals(5, it.search(new Interval(1,4)).size());

        assertEquals(2, it.search(new Interval(0,1)).size());

        assertEquals(0, it.search(new Interval(10,12)).size());

        List<Interval> intervals2 = new ArrayList<Interval>();

        //stravinsky 1880-1971
        intervals2.add(new Interval(1880, 1971));
        //Schoenberg
        intervals2.add(new Interval(1874, 1951));
        //Grieg
        intervals2.add(new Interval(1843, 1907));
        //Schubert
        intervals2.add(new Interval(1779, 1828));
        //Mozart
        intervals2.add(new Interval(1756, 1828));
        //Schuetz
        intervals2.add(new Interval(1585, 1672));

        IntervalTree it2 = new IntervalTree(intervals2);

        assertEquals(0, it2.search(new Interval(1829, 1842)).size());

        List<Interval> intersection1 = it2.search(new Interval(1907, 1907));
        assertEquals(3, intersection1.size());

        intersection1 = it2.search(new Interval(1780, 1790));
        assertEquals(2, intersection1.size());

    }
}
