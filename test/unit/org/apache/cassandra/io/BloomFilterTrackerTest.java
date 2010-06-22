package org.apache.cassandra.io;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.io.sstable.BloomFilterTracker;

import static org.junit.Assert.assertEquals;

public class BloomFilterTrackerTest extends CleanupHelper
{
    @Test
    public void testAddingFalsePositives()
    {
        BloomFilterTracker bft = new BloomFilterTracker();
        assertEquals(0L, bft.getFalsePositiveCount());
        assertEquals(0L, bft.getRecentFalsePositiveCount());
        bft.addFalsePositive();
        bft.addFalsePositive();
        assertEquals(2L, bft.getFalsePositiveCount());
        assertEquals(2L, bft.getRecentFalsePositiveCount());
        assertEquals(0L, bft.getRecentFalsePositiveCount());
        assertEquals(2L, bft.getFalsePositiveCount()); // sanity check
    }

    @Test
    public void testAddingTruePositives()
    {
        BloomFilterTracker bft = new BloomFilterTracker();
        assertEquals(0L, bft.getTruePositiveCount());
        assertEquals(0L, bft.getRecentTruePositiveCount());
        bft.addTruePositive();
        bft.addTruePositive();
        assertEquals(2L, bft.getTruePositiveCount());
        assertEquals(2L, bft.getRecentTruePositiveCount());
        assertEquals(0L, bft.getRecentTruePositiveCount());
        assertEquals(2L, bft.getTruePositiveCount()); // sanity check
    }

    @Test
    public void testAddingToOneLeavesTheOtherAlone()
    {
        BloomFilterTracker bft = new BloomFilterTracker();
        bft.addFalsePositive();
        assertEquals(0L, bft.getTruePositiveCount());
        assertEquals(0L, bft.getRecentTruePositiveCount());
        bft.addTruePositive();
        assertEquals(1L, bft.getFalsePositiveCount());
        assertEquals(1L, bft.getRecentFalsePositiveCount());
    }
}
