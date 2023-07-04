/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.ToIntFunction;

import com.google.common.collect.Iterables;
import org.junit.Test;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.CompactionParams.TombstoneOption;
import org.apache.cassandra.utils.FBUtilities;

public class GcCompactionTest extends CQLTester
{
    static final int KEY_COUNT = 10;
    static final int CLUSTERING_COUNT = 20;

    // Test needs synchronous table drop to avoid flushes causing flaky failures

    @Override
    protected String createTable(String query)
    {
        return super.createTable(KEYSPACE_PER_TEST, query);
    }

    @Override
    protected UntypedResultSet execute(String query, Object... values)
    {
        return executeFormattedQuery(formatQuery(KEYSPACE_PER_TEST, query), values);
    }

    @Override
    public ColumnFamilyStore getCurrentColumnFamilyStore()
    {
        return super.getCurrentColumnFamilyStore(KEYSPACE_PER_TEST);
    }

    public void flush()
    {
        flush(KEYSPACE_PER_TEST);
    }

    @Test
    public void testGcCompactionPartitions() throws Throwable
    {
        testGcCompactionPartitions(false);
    }

    @Test
    public void testGcCompactionPartitionsUnrepaired() throws Throwable
    {
        testGcCompactionPartitions(true);
    }

    public void testGcCompactionPartitions(boolean onlyPurgeRepairedTombstones) throws Throwable
    {
        runCompactionTest("CREATE TABLE %s(" +
                          "  key int," +
                          "  column int," +
                          "  data int," +
                          "  extra text," +
                          "  PRIMARY KEY((key, column), data)" +
                          ") WITH compaction = { 'class' :  'SizeTieredCompactionStrategy', " +
                          "'provide_overlapping_tombstones' : 'row', " +
                          "'only_purge_repaired_tombstones': " + onlyPurgeRepairedTombstones + " };",
                          onlyPurgeRepairedTombstones);
    }

    @Test
    public void testGcCompactionRows() throws Throwable
    {
        testGcCompactionRows(false);
    }

    @Test
    public void testGcCompactionRowsUnrepaired() throws Throwable
    {
        testGcCompactionRows(true);
    }

    public void testGcCompactionRows(boolean onlyPurgeRepairedTombstones) throws Throwable
    {
        runCompactionTest("CREATE TABLE %s(" +
                          "  key int," +
                          "  column int," +
                          "  data int," +
                          "  extra text," +
                          "  PRIMARY KEY(key, column)" +
                          ") WITH compaction = { 'class' :  'SizeTieredCompactionStrategy', " +
                          "'provide_overlapping_tombstones' : 'row', " +
                          "'only_purge_repaired_tombstones': " + onlyPurgeRepairedTombstones + " };",
                          onlyPurgeRepairedTombstones
        );

    }

    @Test
    public void testGcCompactionRanges() throws Throwable
    {
        testGcCompactionRanges(false);
    }

    @Test
    public void testGcCompactionRangesUnrepaired() throws Throwable
    {
        testGcCompactionRanges(true);
    }

    public void testGcCompactionRanges(boolean onlyPurgeRepairedTombstones) throws Throwable
    {

        runCompactionTest("CREATE TABLE %s(" +
                          "  key int," +
                          "  column int," +
                          "  col2 int," +
                          "  data int," +
                          "  extra text," +
                          "  PRIMARY KEY(key, column, data)" +
                          ") WITH compaction = { 'class' :  'SizeTieredCompactionStrategy', " +
                          "'provide_overlapping_tombstones' : 'row', " +
                          "'only_purge_repaired_tombstones': " + onlyPurgeRepairedTombstones + " };",
                          onlyPurgeRepairedTombstones
        );
    }

    private void runCompactionTest(String tableDef, boolean onlyPurgeRepairedTombstones) throws Throwable
    {
        createTable(tableDef);

        for (int i = 0; i < KEY_COUNT; ++i)
            for (int j = 0; j < CLUSTERING_COUNT; ++j)
                execute("INSERT INTO %s (key, column, data, extra) VALUES (?, ?, ?, ?)", i, j, i+j, "" + i + ":" + j);

        Set<SSTableReader> readers = new HashSet<>();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        flush();
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader table0 = getNewTable(readers);
        assertEquals(0, countTombstoneMarkers(table0));
        int rowCount = countRows(table0);

        deleteWithSomeInserts(3, 5, 10);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());
        SSTableReader table1 = getNewTable(readers);
        assertTrue(countRows(table1) > 0);
        assertTrue(countTombstoneMarkers(table1) > 0);

        deleteWithSomeInserts(5, 6, 0);
        flush();
        assertEquals(3, cfs.getLiveSSTables().size());
        SSTableReader table2 = getNewTable(readers);
        assertEquals(0, countRows(table2));
        assertTrue(countTombstoneMarkers(table2) > 0);

        CompactionManager.instance.forceUserDefinedCompaction(table0.getFilename());

        assertEquals(3, cfs.getLiveSSTables().size());
        SSTableReader table3 = getNewTable(readers);
        assertEquals(0, countTombstoneMarkers(table3));
        assertTrue(onlyPurgeRepairedTombstones ? rowCount == countRows(table3) : rowCount > countRows(table3));
    }

    @Test
    public void testGarbageCollectRetainsLCSLevel() throws Throwable
    {

      createTable("CREATE TABLE %s(" +
                  "  key int," +
                  "  column int," +
                  "  data int," +
                  "  PRIMARY KEY ((key), column)" +
                  ") WITH compaction = { 'class' : 'LeveledCompactionStrategy' };");

      assertEquals("LeveledCompactionStrategy", getCurrentColumnFamilyStore().getCompactionStrategyManager().getName());

      for (int i = 0; i < KEY_COUNT; ++i)
          for (int j = 0; j < CLUSTERING_COUNT; ++j)
              execute("INSERT INTO %s (key, column, data) VALUES (?, ?, ?)", i, j, i * j + j);

      ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
      cfs.disableAutoCompaction();
      flush();
      assertEquals(1, cfs.getLiveSSTables().size());
      SSTableReader table = cfs.getLiveSSTables().iterator().next();
      SSTableId gen = table.descriptor.id;
      assertEquals(KEY_COUNT * CLUSTERING_COUNT, countRows(table));

      assertEquals(0, table.getSSTableLevel()); // flush writes to L0

      CompactionManager.AllSSTableOpStatus status;
      status = CompactionManager.instance.performGarbageCollection(cfs, TombstoneOption.ROW, 1);
      assertEquals(CompactionManager.AllSSTableOpStatus.SUCCESSFUL, status);

      assertEquals(1, cfs.getLiveSSTables().size());
      SSTableReader collected = cfs.getLiveSSTables().iterator().next();
      SSTableId collectedGen = collected.descriptor.id;
      assertTrue(SSTableIdFactory.COMPARATOR.compare(collectedGen, gen) > 0);
      assertEquals(KEY_COUNT * CLUSTERING_COUNT, countRows(collected));

      assertEquals(0, collected.getSSTableLevel()); // garbagecollect should leave the LCS level where it was

      CompactionManager.instance.performMaximal(cfs, false);

      assertEquals(1, cfs.getLiveSSTables().size());
      SSTableReader compacted = cfs.getLiveSSTables().iterator().next();
      SSTableId compactedGen = compacted.descriptor.id;
      assertTrue(SSTableIdFactory.COMPARATOR.compare(compactedGen, collectedGen) > 0);
      assertEquals(KEY_COUNT * CLUSTERING_COUNT, countRows(compacted));

      assertEquals(1, compacted.getSSTableLevel()); // full compaction with LCS should move to L1

      status = CompactionManager.instance.performGarbageCollection(cfs, TombstoneOption.ROW, 1);
      assertEquals(CompactionManager.AllSSTableOpStatus.SUCCESSFUL, status);

      assertEquals(1, cfs.getLiveSSTables().size());
      SSTableReader collected2 = cfs.getLiveSSTables().iterator().next();
      assertTrue(SSTableIdFactory.COMPARATOR.compare(collected2.descriptor.id, compactedGen) > 0);
      assertEquals(KEY_COUNT * CLUSTERING_COUNT, countRows(collected2));

      assertEquals(1, collected2.getSSTableLevel()); // garbagecollect should leave the LCS level where it was
    }

    @Test
    public void testGarbageCollectOrder() throws Throwable
    {
        // partition-level deletions, 0 gc_grace
        createTable("CREATE TABLE %s(" +
                    "  key int," +
                    "  column int," +
                    "  col2 int," +
                    "  data int," +
                    "  extra text," +
                    "  PRIMARY KEY((key, column))" +
                    ") WITH gc_grace_seconds = 0;"
        );

        assertEquals(1, getCurrentColumnFamilyStore().gcBefore(1)); // make sure gc_grace is 0

        for (int i = 0; i < KEY_COUNT; ++i)
            for (int j = 0; j < CLUSTERING_COUNT; ++j)
                execute("INSERT INTO %s (key, column, data, extra) VALUES (?, ?, ?, ?)", i, j, i+j, "" + i + ":" + j);


        Set<SSTableReader> readers = new HashSet<>();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        flush();
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader table0 = getNewTable(readers);
        assertEquals(0, countTombstoneMarkers(table0));
        int rowCount0 = countRows(table0);

        deleteWithSomeInserts(3, 5, 10);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());
        SSTableReader table1 = getNewTable(readers);
        final int rowCount1 = countRows(table1);
        assertTrue(rowCount1 > 0);
        assertTrue(countTombstoneMarkers(table1) > 0);

        deleteWithSomeInserts(2, 4, 0);
        flush();
        assertEquals(3, cfs.getLiveSSTables().size());
        SSTableReader table2 = getNewTable(readers);
        assertEquals(0, countRows(table2));
        assertTrue(countTombstoneMarkers(table2) > 0);

        // Wait a little to make sure nowInSeconds is greater than gcBefore
        Thread.sleep(1000);

        CompactionManager.AllSSTableOpStatus status =
                CompactionManager.instance.performGarbageCollection(getCurrentColumnFamilyStore(), CompactionParams.TombstoneOption.ROW, 1);
        assertEquals(CompactionManager.AllSSTableOpStatus.SUCCESSFUL, status);

        SSTableReader[] tables = cfs.getLiveSSTables().toArray(new SSTableReader[0]);
        Arrays.sort(tables, SSTableReader.idComparator);  // by order of compaction

        // Make sure deleted data was removed
        assertTrue(rowCount0 > countRows(tables[0]));
        assertTrue(rowCount1 > countRows(tables[1]));

        // Make sure all tombstones got purged
        for (SSTableReader t : tables)
        {
            assertEquals("Table " + t + " has tombstones", 0, countTombstoneMarkers(t));
        }

        // The last table should have become empty and be removed
        assertEquals(2, tables.length);
    }

    @Test
    public void testGcCompactionCells() throws Throwable
    {
        createTable("CREATE TABLE %s(" +
                          "  key int," +
                          "  column int," +
                          "  data int," +
                          "  extra text," +
                          "  PRIMARY KEY(key)" +
                          ") WITH compaction = { 'class' :  'SizeTieredCompactionStrategy', 'provide_overlapping_tombstones' : 'cell'  };"
                          );

        for (int i = 0; i < KEY_COUNT; ++i)
            for (int j = 0; j < CLUSTERING_COUNT; ++j)
                execute("INSERT INTO %s (key, column, data, extra) VALUES (?, ?, ?, ?)", i, j, i+j, "" + i + ":" + j);

        Set<SSTableReader> readers = new HashSet<>();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        flush();
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader table0 = getNewTable(readers);
        assertEquals(0, countTombstoneMarkers(table0));
        int cellCount = countCells(table0);

        deleteWithSomeInserts(3, 0, 2);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());
        SSTableReader table1 = getNewTable(readers);
        assertTrue(countCells(table1) > 0);
        assertEquals(0, countTombstoneMarkers(table0));

        CompactionManager.instance.forceUserDefinedCompaction(table0.getFilename());

        assertEquals(2, cfs.getLiveSSTables().size());
        SSTableReader table3 = getNewTable(readers);
        assertEquals(0, countTombstoneMarkers(table3));
        assertTrue(cellCount > countCells(table3));
    }

    @Test
    public void testGcCompactionStatic() throws Throwable
    {
        createTable("CREATE TABLE %s(" +
                          "  key int," +
                          "  column int," +
                          "  data int static," +
                          "  extra text," +
                          "  PRIMARY KEY(key, column)" +
                          ") WITH compaction = { 'class' :  'SizeTieredCompactionStrategy', 'provide_overlapping_tombstones' : 'cell'  };"
                          );

        for (int i = 0; i < KEY_COUNT; ++i)
            for (int j = 0; j < CLUSTERING_COUNT; ++j)
                execute("INSERT INTO %s (key, column, data, extra) VALUES (?, ?, ?, ?)", i, j, i+j, "" + i + ":" + j);

        Set<SSTableReader> readers = new HashSet<>();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        flush();
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader table0 = getNewTable(readers);
        assertEquals(0, countTombstoneMarkers(table0));
        int cellCount = countStaticCells(table0);
        assertEquals(KEY_COUNT, cellCount);

        execute("DELETE data FROM %s WHERE key = 0");   // delete static cell
        execute("INSERT INTO %s (key, data) VALUES (1, 0)");  // overwrite static cell
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());
        SSTableReader table1 = getNewTable(readers);
        assertTrue(countStaticCells(table1) > 0);
        assertEquals(0, countTombstoneMarkers(table0));

        CompactionManager.instance.forceUserDefinedCompaction(table0.getFilename());

        assertEquals(2, cfs.getLiveSSTables().size());
        SSTableReader table3 = getNewTable(readers);
        assertEquals(0, countTombstoneMarkers(table3));
        assertEquals(cellCount - 2, countStaticCells(table3));
    }

    @Test
    public void testGcCompactionComplexColumn() throws Throwable
    {
        createTable("CREATE TABLE %s(" +
                          "  key int," +
                          "  data map<int, int>," +
                          "  extra text," +
                          "  PRIMARY KEY(key)" +
                          ") WITH compaction = { 'class' :  'SizeTieredCompactionStrategy', 'provide_overlapping_tombstones' : 'cell'  };"
                          );

        for (int i = 0; i < KEY_COUNT; ++i)
            for (int j = 0; j < CLUSTERING_COUNT; ++j)
                execute("UPDATE %s SET data[?] = ? WHERE key = ?", j, i+j, i);

        Set<SSTableReader> readers = new HashSet<>();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        flush();
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader table0 = getNewTable(readers);
        assertEquals(0, countTombstoneMarkers(table0));
        int cellCount = countComplexCells(table0);

        deleteWithSomeInsertsComplexColumn(3, 5, 8);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());
        SSTableReader table1 = getNewTable(readers);
        assertTrue(countComplexCells(table1) > 0);
        assertEquals(0, countTombstoneMarkers(table0));

        CompactionManager.instance.forceUserDefinedCompaction(table0.getFilename());

        assertEquals(2, cfs.getLiveSSTables().size());
        SSTableReader table3 = getNewTable(readers);
        assertEquals(0, countTombstoneMarkers(table3));
        assertEquals(cellCount - 23, countComplexCells(table3));
    }

    @Test
    public void testLocalDeletionTime() throws Throwable
    {
        createTable("create table %s (k int, c1 int, primary key (k, c1)) with compaction = {'class': 'SizeTieredCompactionStrategy', 'provide_overlapping_tombstones':'row'}");
        execute("delete from %s where k = 1");
        Set<SSTableReader> readers = new HashSet<>(getCurrentColumnFamilyStore().getLiveSSTables());
        flush();
        SSTableReader oldSSTable = getNewTable(readers);
        Thread.sleep(2000);
        execute("delete from %s where k = 1");
        flush();
        SSTableReader newTable = getNewTable(readers);

        CompactionManager.instance.forceUserDefinedCompaction(oldSSTable.getFilename());

        // Old table now doesn't contain any data and should disappear.
        assertEquals(Collections.singleton(newTable), getCurrentColumnFamilyStore().getLiveSSTables());
    }

    private SSTableReader getNewTable(Set<SSTableReader> readers)
    {
        Set<SSTableReader> newOnes = new HashSet<>(getCurrentColumnFamilyStore().getLiveSSTables());
        newOnes.removeAll(readers);
        assertEquals(1, newOnes.size());
        readers.addAll(newOnes);
        return Iterables.get(newOnes, 0);
    }

    void deleteWithSomeInserts(int key_step, int delete_step, int readd_step) throws Throwable
    {
        for (int i = 0; i < KEY_COUNT; i += key_step)
        {
            if (delete_step > 0)
                for (int j = i % delete_step; j < CLUSTERING_COUNT; j += delete_step)
                {
                    execute("DELETE FROM %s WHERE key = ? AND column = ?", i, j);
                }
            if (readd_step > 0)
                for (int j = i % readd_step; j < CLUSTERING_COUNT; j += readd_step)
                {
                    execute("INSERT INTO %s (key, column, data, extra) VALUES (?, ?, ?, ?)", i, j, i-j, "readded " + i + ":" + j);
                }
        }
    }

    void deleteWithSomeInsertsComplexColumn(int key_step, int delete_step, int readd_step) throws Throwable
    {
        for (int i = 0; i < KEY_COUNT; i += key_step)
        {
            if (delete_step > 0)
                for (int j = i % delete_step; j < CLUSTERING_COUNT; j += delete_step)
                {
                    execute("DELETE data[?] FROM %s WHERE key = ?", j, i);
                }
            if (readd_step > 0)
                for (int j = i % readd_step; j < CLUSTERING_COUNT; j += readd_step)
                {
                    execute("UPDATE %s SET data[?] = ? WHERE key = ?", j, -(i+j), i);
                }
        }
    }

    int countTombstoneMarkers(SSTableReader reader)
    {
        long nowInSec = FBUtilities.nowInSeconds();
        return count(reader, x -> x.isRangeTombstoneMarker() || x.isRow() && ((Row) x).hasDeletion(nowInSec) ? 1 : 0, x -> x.partitionLevelDeletion().isLive() ? 0 : 1);
    }

    int countRows(SSTableReader reader)
    {
        boolean enforceStrictLiveness = reader.metadata().enforceStrictLiveness();
        long nowInSec = FBUtilities.nowInSeconds();
        return count(reader, x -> x.isRow() && ((Row) x).hasLiveData(nowInSec, enforceStrictLiveness) ? 1 : 0, x -> 0);
    }

    int countCells(SSTableReader reader)
    {
        return count(reader, x -> x.isRow() ? Iterables.size((Row) x) : 0, x -> 0);
    }

    int countStaticCells(SSTableReader reader)
    {
        return count(reader, x -> 0, x -> Iterables.size(x.staticRow()));
    }

    int countComplexCells(SSTableReader reader)
    {
        return count(reader, x -> x.isRow() ? ((Row) x).columnData().stream().mapToInt(this::countComplex).sum() : 0, x -> 0);
    }

    int countComplex(ColumnData c)
    {
        if (!(c instanceof ComplexColumnData))
            return 0;
        ComplexColumnData ccd = (ComplexColumnData) c;
        return ccd.cellsCount();
    }

    int count(SSTableReader reader, ToIntFunction<Unfiltered> predicate, ToIntFunction<UnfilteredRowIterator> partitionPredicate)
    {
        int instances = 0;
        try (ISSTableScanner partitions = reader.getScanner())
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator iter = partitions.next())
                {
                    instances += partitionPredicate.applyAsInt(iter);
                    while (iter.hasNext())
                    {
                        Unfiltered atom = iter.next();
                        instances += predicate.applyAsInt(atom);
                    }
                }
            }
        }
        return instances;
    }
}
