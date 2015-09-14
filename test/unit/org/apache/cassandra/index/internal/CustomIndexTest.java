package org.apache.cassandra.index.internal;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.schema.IndexMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CustomIndexTest extends CQLTester
{
    @Test
    public void testInserts() throws Throwable
    {
        // test to ensure that we don't deadlock when flushing CFS backed custom indexers
        // see CASSANDRA-10181
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b))");
        createIndex("CREATE CUSTOM INDEX myindex ON %s(c) USING 'org.apache.cassandra.index.internal.CustomCassandraIndex'");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 2);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 1);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 2, 0, 0);
    }

    @Test
    public void indexControlsIfIncludedInBuildOnNewSSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, PRIMARY KEY (a))");
        String toInclude = "include";
        String toExclude = "exclude";
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(b) USING '%s'",
                                  toInclude, IndexIncludedInBuild.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX %s ON %%s(b) USING '%s'",
                                  toExclude, IndexExcludedFromBuild.class.getName()));

        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 0, 0);
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1);
        execute("INSERT INTO %s (a, b) VALUES (?, ?)", 2, 2);
        flush();

        SecondaryIndexManager indexManager = getCurrentColumnFamilyStore().indexManager;
        IndexIncludedInBuild included = (IndexIncludedInBuild)indexManager.getIndexByName(toInclude);
        included.reset();
        assertTrue(included.rowsInserted.isEmpty());

        IndexExcludedFromBuild excluded = (IndexExcludedFromBuild)indexManager.getIndexByName(toExclude);
        excluded.reset();
        assertTrue(excluded.rowsInserted.isEmpty());

        indexManager.buildAllIndexesBlocking(getCurrentColumnFamilyStore().getLiveSSTables());

        assertEquals(3, included.rowsInserted.size());
        assertTrue(excluded.rowsInserted.isEmpty());
    }

    public static final class IndexIncludedInBuild extends StubIndex
    {
        public IndexIncludedInBuild(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        public boolean shouldBuildBlocking()
        {
            return true;
        }
    }

    public static final class IndexExcludedFromBuild extends StubIndex
    {
        public IndexExcludedFromBuild(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        public boolean shouldBuildBlocking()
        {
            return false;
        }
    }
}
