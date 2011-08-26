package org.apache.cassandra.utils;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.cassandra.utils.IntervalTree.Interval;
import org.junit.Test;

public class IntervalTest extends TestCase
{
    @Test
    public void testEncloses() throws Exception
    {
        Interval interval = new Interval(0,5,null);
        Interval interval1 = new Interval(0, 10, null);
        Interval interval2 = new Interval(5,10,null);
        Interval interval3 = new Interval(0, 11, null);


        assertTrue(interval.encloses(interval));
        assertTrue(interval1.encloses(interval));
        assertFalse(interval.encloses(interval2));
        assertTrue(interval1.encloses(interval2));
        assertFalse(interval1.encloses(interval3));
    }
    @Test
    public void testContains() throws Exception
    {
        Interval interval = new Interval(0, 5, null);
        assertTrue(interval.contains(0));
        assertTrue(interval.contains(5));
        assertFalse(interval.contains(-1));
        assertFalse(interval.contains(6));
    }
    @Test
    public void testIntersects() throws Exception
    {
        Interval interval = new Interval(0,5,null);
        Interval interval1 = new Interval(0, 10, null);
        Interval interval2 = new Interval(5,10,null);
        Interval interval3 = new Interval(0, 11, null);
        Interval interval5 = new Interval(6,12,null);

        assertTrue(interval.intersects(interval1));
        assertTrue(interval.intersects(interval2));
        assertTrue(interval.intersects(interval3));
        assertFalse(interval.intersects(interval5));
    }
}
