package org.apache.cassandra.db.commitlog;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;

public class CommitLogCQLTest extends CQLTester
{
    @Test
    public void testTruncateSegmentDiscard() throws Throwable
    {
        String otherTable = createTable("CREATE TABLE %s (idx INT, data TEXT, PRIMARY KEY(idx));");

        createTable("CREATE TABLE %s (idx INT, data TEXT, PRIMARY KEY(idx));");
        execute("INSERT INTO %s (idx, data) VALUES (?, ?)", 15, Integer.toString(15));
        flush();

        // We write something in different table to advance the commit log position. Current table remains clean.
        executeFormattedQuery(String.format("INSERT INTO %s.%s (idx, data) VALUES (?, ?)", keyspace(), otherTable), 16, Integer.toString(16));

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assert cfs.getTracker().getView().getCurrentMemtable().isClean();
        // Calling switchMemtable directly applies Flush even though memtable is empty. This can happen with some races
        // (flush with recycling by segment manager). It should still tell commitlog that the memtable's region is clean.
        // CASSANDRA-12436
        cfs.switchMemtable();

        execute("INSERT INTO %s (idx, data) VALUES (?, ?)", 15, Integer.toString(17));

        Collection<CommitLogSegment> active = new ArrayList<>(CommitLog.instance.segmentManager.getActiveSegments());
        CommitLog.instance.forceRecycleAllSegments();

        // If one of the previous segments remains, it wasn't clean.
        active.retainAll(CommitLog.instance.segmentManager.getActiveSegments());
        assert active.isEmpty();
    }
}
