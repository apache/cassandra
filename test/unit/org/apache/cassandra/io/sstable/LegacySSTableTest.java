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
package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SinglePartitionSliceCommandTest;
import org.apache.cassandra.db.compaction.Verifier;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.CQLTester.assertRows;
import static org.apache.cassandra.cql3.CQLTester.row;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests backwards compatibility for SSTables
 */
public class LegacySSTableTest
{
    private static final Logger logger = LoggerFactory.getLogger(LegacySSTableTest.class);

    public static final String LEGACY_SSTABLE_PROP = "legacy-sstable-root";

    public static File LEGACY_SSTABLE_ROOT;

    /**
     * When adding a new sstable version, add that one here.
     * See {@link #testGenerateSstables()} to generate sstables.
     * Take care on commit as you need to add the sstable files using {@code git add -f}
     */
    public static final String[] legacyVersions = {"mc", "mb", "ma", "la", "ka", "jb"};

    // 1200 chars
    static final String longString = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        String scp = System.getProperty(LEGACY_SSTABLE_PROP);
        Assert.assertNotNull("System property " + LEGACY_SSTABLE_PROP + " not set", scp);
        
        LEGACY_SSTABLE_ROOT = new File(scp).getAbsoluteFile();
        Assert.assertTrue("System property " + LEGACY_SSTABLE_ROOT + " does not specify a directory", LEGACY_SSTABLE_ROOT.isDirectory());

        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
        Keyspace.setInitialized();
        createKeyspace();
        for (String legacyVersion : legacyVersions)
        {
            createTables(legacyVersion);
        }

    }

    @After
    public void tearDown()
    {
        for (String legacyVersion : legacyVersions)
        {
            truncateTables(legacyVersion);
        }
    }

    /**
     * Get a descriptor for the legacy sstable at the given version.
     */
    protected Descriptor getDescriptor(String legacyVersion, String table)
    {
        return new Descriptor(legacyVersion, getTableDir(legacyVersion, table), "legacy_tables", table, 1,
                              BigFormat.instance.getVersion(legacyVersion).hasNewFileName()?
                              SSTableFormat.Type.BIG :SSTableFormat.Type.LEGACY);
    }

    @Test
    public void testLoadLegacyCqlTables() throws Exception
    {
        DatabaseDescriptor.setColumnIndexCacheSize(99999);
        CacheService.instance.invalidateKeyCache();
        doTestLegacyCqlTables();
    }

    @Test
    public void testLoadLegacyCqlTablesShallow() throws Exception
    {
        DatabaseDescriptor.setColumnIndexCacheSize(0);
        CacheService.instance.invalidateKeyCache();
        doTestLegacyCqlTables();
    }

    private void doTestLegacyCqlTables() throws Exception
    {
        for (String legacyVersion : legacyVersions)
        {
            logger.info("Loading legacy version: {}", legacyVersion);
            truncateLegacyTables(legacyVersion);
            loadLegacyTables(legacyVersion);
            CacheService.instance.invalidateKeyCache();
            long startCount = CacheService.instance.keyCache.size();
            verifyReads(legacyVersion);
            verifyCache(legacyVersion, startCount);
            compactLegacyTables(legacyVersion);
        }
    }

    @Test
    public void testStreamLegacyCqlTables() throws Exception
    {
        for (String legacyVersion : legacyVersions)
        {
            streamLegacyTables(legacyVersion);
            verifyReads(legacyVersion);
        }
    }
    @Test
    public void testReverseIterationOfLegacyIndexedSSTable() throws Exception
    {
        // During upgrades from 2.1 to 3.0, reverse queries can drop rows before upgradesstables is completed
        QueryProcessor.executeInternal("CREATE TABLE legacy_tables.legacy_ka_indexed (" +
                                       "  p int," +
                                       "  c int," +
                                       "  v1 int," +
                                       "  v2 int," +
                                       "  PRIMARY KEY(p, c)" +
                                       ")");
        loadLegacyTable("legacy_%s_indexed%s", "ka", "");
        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * " +
                                                             "FROM legacy_tables.legacy_ka_indexed " +
                                                             "WHERE p=1 " +
                                                             "ORDER BY c DESC");
        assertEquals(5000, rs.size());
    }

    @Test
    public void testReadingLegacyIndexedSSTableWithStaticColumns() throws Exception
    {
        // During upgrades from 2.1 to 3.0, reading from tables with static columns errors before upgradesstables
        // is completed
        QueryProcessor.executeInternal("CREATE TABLE legacy_tables.legacy_ka_indexed_static (" +
                                       "  p int," +
                                       "  c int," +
                                       "  v1 int," +
                                       "  v2 int," +
                                       "  s1 int static," +
                                       "  s2 int static," +
                                       "  PRIMARY KEY(p, c)" +
                                       ")");
        loadLegacyTable("legacy_%s_indexed_static%s", "ka", "");
        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * " +
                                                             "FROM legacy_tables.legacy_ka_indexed_static " +
                                                             "WHERE p=1 ");
        assertEquals(5000, rs.size());
    }

    @Test
    public void test14766() throws Exception
    {
        /*
         * During upgrades from 2.1 to 3.0, reading from old sstables in reverse order could omit the very last row if the
         * last indexed block had only two Unfiltered-s. See CASSANDRA-14766 for details.
         *
         * The sstable used here has two indexed blocks, with 2 cells/rows of ~500 bytes each, with column index interval of 1kb.
         * Without the fix SELECT * returns 4 rows in ASC order, but only 3 rows in DESC order, omitting the last one.
         */

        QueryProcessor.executeInternal("CREATE TABLE legacy_tables.legacy_ka_14766 (pk int, ck int, value text, PRIMARY KEY (pk, ck));");
        loadLegacyTable("legacy_%s_14766%s", "ka", "");

        UntypedResultSet rs;

        // read all rows in ASC order, expect all 4 to be returned
        rs = QueryProcessor.executeInternal("SELECT * FROM legacy_tables.legacy_ka_14766 WHERE pk = 0 ORDER BY ck ASC;");
        assertEquals(4, rs.size());

        // read all rows in DESC order, expect all 4 to be returned
        rs = QueryProcessor.executeInternal("SELECT * FROM legacy_tables.legacy_ka_14766 WHERE pk = 0 ORDER BY ck DESC;");
        assertEquals(4, rs.size());
    }

    @Test
    public void test14803() throws Exception
    {
        /*
         * During upgrades from 2.1 to 3.0, reading from old sstables in reverse order could return early if the sstable
         * reverse iterator encounters an indexed block that only covers a single row, and that row starts in the next
         * indexed block.
         */

        QueryProcessor.executeInternal("CREATE TABLE legacy_tables.legacy_ka_14803 (k int, c int, v1 blob, v2 blob, PRIMARY KEY (k, c));");
        loadLegacyTable("legacy_%s_14803%s", "ka", "");

        UntypedResultSet forward = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM legacy_tables.legacy_ka_14803 WHERE k=100"));
        UntypedResultSet reverse = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM legacy_tables.legacy_ka_14803 WHERE k=100 ORDER BY c DESC"));

        logger.info("{} - {}", forward.size(), reverse.size());
        Assert.assertFalse(forward.isEmpty());
        assertEquals(forward.size(), reverse.size());
    }

    @Test
    public void test14873() throws Exception
    {
        /*
         * When reading 2.1 sstables in 3.0 in reverse order it's possible to wrongly return an empty result set if the
         * partition being read has a static row, and the read is performed backwards.
         */

        /*
         * Contents of the SSTable (column_index_size_in_kb: 1) below:
         *
         * insert into legacy_tables.legacy_ka_14873 (pkc, sc)     values (0, 0);
         * insert into legacy_tables.legacy_ka_14873 (pkc, cc, rc) values (0, 5, '5555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555');
         * insert into legacy_tables.legacy_ka_14873 (pkc, cc, rc) values (0, 4, '4444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444');
         * insert into legacy_tables.legacy_ka_14873 (pkc, cc, rc) values (0, 3, '3333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333');
         * insert into legacy_tables.legacy_ka_14873 (pkc, cc, rc) values (0, 2, '2222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222');
         * insert into legacy_tables.legacy_ka_14873 (pkc, cc, rc) values (0, 1, '1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111');
         */

        String ddl =
            "CREATE TABLE legacy_tables.legacy_ka_14873 ("
            + "pkc int, cc int, sc int static, rc text, PRIMARY KEY (pkc, cc)"
            + ") WITH CLUSTERING ORDER BY (cc DESC) AND compaction = {'enabled' : 'false', 'class' : 'LeveledCompactionStrategy'};";
        QueryProcessor.executeInternal(ddl);
        loadLegacyTable("legacy_%s_14873%s", "ka", "");

        UntypedResultSet forward =
            QueryProcessor.executeOnceInternal(
                String.format("SELECT * FROM legacy_tables.legacy_ka_14873 WHERE pkc = 0 AND cc > 0 ORDER BY cc DESC;"));

        UntypedResultSet reverse =
            QueryProcessor.executeOnceInternal(
                String.format("SELECT * FROM legacy_tables.legacy_ka_14873 WHERE pkc = 0 AND cc > 0 ORDER BY cc ASC;"));

        assertEquals(5, forward.size());
        assertEquals(5, reverse.size());
    }

    @Test
    public void testMultiBlockRangeTombstones() throws Exception
    {
        /**
         * During upgrades from 2.1 to 3.0, reading old sstables in reverse order would generate invalid sequences of
         * range tombstone bounds if their range tombstones spanned multiple column index blocks. The read would fail
         * in different ways depending on whether the 2.1 tables were produced by a flush or a compaction.
         */

        String version = "ka";
        for (String tableFmt : new String[]{"legacy_%s_compacted_multi_block_rt%s", "legacy_%s_flushed_multi_block_rt%s"})
        {
            String table = String.format(tableFmt, version, "");
            QueryProcessor.executeOnceInternal(String.format("CREATE TABLE legacy_tables.%s " +
                                                             "(k int, c1 int, c2 int, v1 blob, v2 blob, " +
                                                             "PRIMARY KEY (k, c1, c2))", table));
            loadLegacyTable(tableFmt, version, "");

            UntypedResultSet forward = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM legacy_tables.%s WHERE k=100", table));
            UntypedResultSet reverse = QueryProcessor.executeOnceInternal(String.format("SELECT * FROM legacy_tables.%s WHERE k=100 ORDER BY c1 DESC, c2 DESC", table));

            Assert.assertFalse(forward.isEmpty());
            assertEquals(table, forward.size(), reverse.size());
        }
    }

    @Test
    public void testInaccurateSSTableMinMax() throws Exception
    {
        QueryProcessor.executeInternal("CREATE TABLE legacy_tables.legacy_mc_inaccurate_min_max (k int, c1 int, c2 int, c3 int, v int, primary key (k, c1, c2, c3))");
        loadLegacyTable("legacy_%s_inaccurate_min_max%s", "mc", "");

        /*
         sstable has the following mutations:
            INSERT INTO legacy_tables.legacy_mc_inaccurate_min_max (k, c1, c2, c3, v) VALUES (100, 4, 4, 4, 4)
            DELETE FROM legacy_tables.legacy_mc_inaccurate_min_max WHERE k=100 AND c1<3
         */

        String query = "SELECT * FROM legacy_tables.legacy_mc_inaccurate_min_max WHERE k=100 AND c1=1 AND c2=1";
        List<Unfiltered> unfiltereds = SinglePartitionSliceCommandTest.getUnfilteredsFromSinglePartition(query);
        assertEquals(2, unfiltereds.size());
        Assert.assertTrue(unfiltereds.get(0).isRangeTombstoneMarker());
        Assert.assertTrue(((RangeTombstoneMarker) unfiltereds.get(0)).isOpen(false));
        Assert.assertTrue(unfiltereds.get(1).isRangeTombstoneMarker());
        Assert.assertTrue(((RangeTombstoneMarker) unfiltereds.get(1)).isClose(false));
    }

    @Test
    public void testVerifyOldSSTables() throws Exception
    {
        for (String legacyVersion : legacyVersions)
        {
            loadLegacyTables(legacyVersion);
            ColumnFamilyStore cfs = Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion));
            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                try (Verifier verifier = new Verifier(cfs, sstable, false))
                {
                    verifier.verify(true);
                }
            }
        }
    }

    @Test
    public void test14912() throws Exception
    {
        /*
         * When reading 2.1 sstables in 3.0, collection tombstones need to be checked against
         * the dropped columns stored in table metadata. Failure to do so can result in unreadable
         * rows if a column with the same name but incompatible type has subsequently been added.
         *
         * The original (i.e. pre-any ALTER statements) table definition for this test is:
         * CREATE TABLE legacy_tables.legacy_ka_14912 (k int PRIMARY KEY, v1 set<text>, v2 text);
         *
         * The SSTable loaded emulates data being written before the table is ALTERed and contains:
         *
         * insert into legacy_tables.legacy_ka_14912 (k, v1, v2) values (0, {}, 'abc') USING TIMESTAMP 1543244999672280;
         * insert into legacy_tables.legacy_ka_14912 (k, v1, v2) values (1, {'abc'}, 'abc') USING TIMESTAMP 1543244999672280;
         *
         * The timestamps of the (generated) collection tombstones are 1543244999672279, e.g. the <TIMESTAMP of the mutation> - 1
         */

        QueryProcessor.executeInternal("CREATE TABLE legacy_tables.legacy_ka_14912 (k int PRIMARY KEY, v1 text, v2 text)");
        loadLegacyTable("legacy_%s_14912%s", "ka", "");
        CFMetaData cfm = Keyspace.open("legacy_tables").getColumnFamilyStore("legacy_ka_14912").metadata;
        ColumnDefinition columnToDrop;

        /*
         * This first variant simulates the original v1 set<text> column being dropped
         * then re-added with the text type:
         * CREATE TABLE legacy_tables.legacy_ka_14912 (k int PRIMARY KEY, v1 set<text>, v2 text);
         * INSERT INTO legacy_tables.legacy)ka_14912 (k, v1, v2)...
         * ALTER TABLE legacy_tables.legacy_ka_14912 DROP v1;
         * ALTER TABLE legacy_tables.legacy_ka_14912 ADD v1 text;
         */
        columnToDrop = ColumnDefinition.regularDef(cfm,
                                                   UTF8Type.instance.fromString("v1"),
                                                   SetType.getInstance(UTF8Type.instance, true));
        cfm.recordColumnDrop(columnToDrop, 1543244999700000L);
        assertExpectedRowsWithDroppedCollection(true);
        // repeat the query, but simulate clock drift by shifting the recorded
        // drop time forward so that it occurs before the collection timestamp
        cfm.recordColumnDrop(columnToDrop, 1543244999600000L);
        assertExpectedRowsWithDroppedCollection(false);

        /*
         * This second test simulates the original v1 set<text> column being dropped
         * then re-added with some other, non-collection type (overwriting the dropped
         * columns record), then dropping and re-adding again as text type:
         * CREATE TABLE legacy_tables.legacy_ka_14912 (k int PRIMARY KEY, v1 set<text>, v2 text);
         * INSERT INTO legacy_tables.legacy_ka_14912 (k, v1, v2)...
         * ALTER TABLE legacy_tables.legacy_ka_14912 DROP v1;
         * ALTER TABLE legacy_tables.legacy_ka_14912 ADD v1 blob;
         * ALTER TABLE legacy_tables.legacy_ka_14912 DROP v1;
         * ALTER TABLE legacy_tables.legacy_ka_14912 ADD v1 text;
         */
        columnToDrop = ColumnDefinition.regularDef(cfm,
                                                   UTF8Type.instance.fromString("v1"),
                                                   BytesType.instance);
        cfm.recordColumnDrop(columnToDrop, 1543244999700000L);
        assertExpectedRowsWithDroppedCollection(true);
        // repeat the query, but simulate clock drift by shifting the recorded
        // drop time forward so that it occurs before the collection timestamp
        cfm.recordColumnDrop(columnToDrop, 1543244999600000L);
        assertExpectedRowsWithDroppedCollection(false);
    }

    @Test
    public void test15081() throws Exception
    {
        QueryProcessor.executeInternal("CREATE TABLE legacy_tables.legacy_ka_15081 (id int primary key, payload text)");
        loadLegacyTable("legacy_%s_15081%s", "ka", "");
        UntypedResultSet results =
            QueryProcessor.executeOnceInternal(
                String.format("SELECT * FROM legacy_tables.legacy_ka_15081"));
        assertRows(results, row(1, "hello world"));
    }

    @Test
    public void testReadingLegacyTablesWithIllegalCellNames() throws Exception {
        /**
         * The sstable can be generated externally with SSTableSimpleUnsortedWriter:
         *
         * [
         * {"key": "1",
         *  "cells": [["a:aa:c1","61",1555000750634000],
         *            ["a:aa:c2","6161",1555000750634000],
         *            ["a:aa:pk","00000001",1555000750634000],
         *            ["a:aa:v1","aaa",1555000750634000]]},
         * {"key": "2",
         *  "cells": [["b:bb:c1","62",1555000750634000],
         *            ["b:bb:c2","6262",1555000750634000],
         *            ["b:bb:pk","00000002",1555000750634000],
         *            ["b:bb:v1","bbb",1555000750634000]]}
         * ]
         * and an extra sstable with only the invalid cell name
         * [
         * {"key": "3",
         *  "cells": [["a:aa:pk","68656c6c6f30",1570466358949]]}
         * ]
         *
         */
        String table = "legacy_ka_with_illegal_cell_names";
        QueryProcessor.executeInternal("CREATE TABLE legacy_tables." + table + " (" +
                                       " pk int," +
                                       " c1 text," +
                                       " c2 text," +
                                       " v1 text," +
                                       " PRIMARY KEY(pk, c1, c2))");
        loadLegacyTable("legacy_%s_with_illegal_cell_names%s", "ka", "");
        UntypedResultSet results =
            QueryProcessor.executeOnceInternal("SELECT * FROM legacy_tables."+table);

        assertRows(results, row(1, "a", "aa", "aaa"), row(2, "b", "bb", "bbb"), row (3, "a", "aa", null));
        Keyspace.open("legacy_tables").getColumnFamilyStore(table).forceMajorCompaction();
    }

    @Test
    public void testReadingLegacyTablesWithIllegalCellNamesPKLI() throws Exception {
        /**
         *
         * Makes sure we grab the correct PKLI when we have illegal columns
         *
         * sstable looks like this:
         * [
         * {"key": "3",
         *  "cells": [["a:aa:","",100],
         *            ["a:aa:pk","6d656570",200]]}
         * ]
         */
        /*
        this generates the stable on 2.1:
        CFMetaData metadata = CFMetaData.compile("create table legacy_tables.legacy_ka_with_illegal_cell_names_2 (pk int, c1 text, c2 text, v1 text, primary key (pk, c1, c2))", "legacy_tables");
        try (SSTableSimpleUnsortedWriter writer = new SSTableSimpleUnsortedWriter(new File("/tmp/sstable21"),
                                                                                  metadata,
                                                                                  new ByteOrderedPartitioner(),
                                                                                  10))
        {
            writer.newRow(bytes(3));
            writer.addColumn(new BufferCell(Util.cellname("a", "aa", ""), bytes(""), 100));
            writer.addColumn(new BufferCell(Util.cellname("a", "aa", "pk"), bytes("meep"), 200));
        }
        */
        String table = "legacy_ka_with_illegal_cell_names_2";
        QueryProcessor.executeInternal("CREATE TABLE legacy_tables." + table + " (" +
                                       " pk int," +
                                       " c1 text," +
                                       " c2 text," +
                                       " v1 text," +
                                       " PRIMARY KEY(pk, c1, c2))");
        loadLegacyTable("legacy_%s_with_illegal_cell_names_2%s", "ka", "");
        ColumnFamilyStore cfs = Keyspace.open("legacy_tables").getColumnFamilyStore(table);
        assertEquals(1, Iterables.size(cfs.getSSTables(SSTableSet.CANONICAL)));
        cfs.forceMajorCompaction();
        assertEquals(1, Iterables.size(cfs.getSSTables(SSTableSet.CANONICAL)));
        SSTableReader sstable = Iterables.getFirst(cfs.getSSTables(SSTableSet.CANONICAL), null);
        LivenessInfo livenessInfo = null;
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator iter = scanner.next())
                {
                    while (iter.hasNext())
                    {
                        Unfiltered uf = iter.next();
                        livenessInfo = ((Row)uf).primaryKeyLivenessInfo();
                    }
                }
            }
        }
        assertNotNull(livenessInfo);
        assertEquals(100, livenessInfo.timestamp());
    }

    @Test
    public void testReadingIndexedLegacyTablesWithIllegalCellNames() throws Exception {
        /**
         * The sstable can be generated externally with SSTableSimpleUnsortedWriter:
         * column_index_size_in_kb: 1
         * [
         *   {"key": "key",
         *    "cells": [
         *               ["00000:000000:a","00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",0],
         *               ["00000:000000:b","00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",0]
         *               ["00000:000000:c","00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",0]
         *               ["00000:000000:z","00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",0]
         *               ["00001:000001:a","00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",0],
         *               ["00001:000001:b","00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",0]
         *               ["00001:000001:c","00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",0]
         *               ["00001:000001:z","00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",0]
         *               .
         *               .
         *               .
         *               ["00010:000010:a","00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",0],
         *               ["00010:000010:b","00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",0]
         *               ["00010:000010:c","00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",0]
         *               ["00010:000010:z","00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",0]
         *           ]
         *   }
         * ]
         * Each row in the partition contains only 1 valid cell. The ones with the column name components 'a', 'b' & 'z' are illegal as they refer to PRIMARY KEY
         * columns, but SSTables such as this can be generated with offline tools and loaded via SSTableLoader or nodetool refresh (see CASSANDRA-15086) (see
         * CASSANDRA-15086) Only 'c' is a valid REGULAR column in the table schema.
         * In the initial fix for CASSANDRA-15086, the bytes read by OldFormatDeserializer for these invalid cells are not correctly accounted for, causing
         * ReverseIndexedReader to assert that the end of a block has been reached earlier than it actually has, which in turn causes rows to be incorrectly
         * ommitted from the results.
         *
         * This sstable has been crafted to hit a further potential error condition. Rows 00001:00001 and 00008:00008 interact with the index block boundaries
         * in a very specific way; for both of these rows, the (illegal) cells 'a' & 'b', along with the valid 'c' cell are at the end of an index block, but
         * the 'z' cell is over the boundary, in the following block. We need to ensure that the bytes consumed for the 'z' cell are properly accounted for and
         * not counted toward those for the next row on disk.
         */
        QueryProcessor.executeInternal("CREATE TABLE legacy_tables.legacy_ka_with_illegal_cell_names_indexed (" +
                                       " a text," +
                                       " b text," +
                                       " z text," +
                                       " c text," +
                                       " PRIMARY KEY(a, b, z))");
        loadLegacyTable("legacy_%s_with_illegal_cell_names_indexed%s", "ka", "");
        String queryForward = "SELECT * FROM legacy_tables.legacy_ka_with_illegal_cell_names_indexed WHERE a = 'key'";
        String queryReverse = queryForward + " ORDER BY b DESC, z DESC";

        List<String> forward = new ArrayList<>();
        QueryProcessor.executeOnceInternal(queryForward).forEach(r -> forward.add(r.getString("b") + ":" +  r.getString("z")));

        List<String> reverse = new ArrayList<>();
        QueryProcessor.executeOnceInternal(queryReverse).forEach(r -> reverse.add(r.getString("b") + ":" +  r.getString("z")));

        assertEquals(11, reverse.size());
        assertEquals(11, forward.size());
        for (int i=0; i < 11; i++)
            assertEquals(forward.get(i), reverse.get(10 - i));
    }

    private void assertExpectedRowsWithDroppedCollection(boolean droppedCheckSuccessful)
    {
        for (int i=0; i<=1; i++)
        {
            UntypedResultSet rows =
                QueryProcessor.executeOnceInternal(
                    String.format("SELECT * FROM legacy_tables.legacy_ka_14912 WHERE k = %s;", i));
            assertEquals(1, rows.size());
            UntypedResultSet.Row row = rows.one();

            // If the best-effort attempt to filter dropped columns was successful, then the row
            // should not contain the v1 column at all. Likewise, if no column data was written,
            // only a tombstone, then no v1 column should be present.
            // However, if collection data was written (i.e. where k=1), then if the dropped column
            // check didn't filter the legacy cells, we should expect an empty column value as the
            // legacy collection tombstone won't cover it and the dropped column check doesn't filter
            // it.
            if (droppedCheckSuccessful || i == 0)
                Assert.assertFalse(row.has("v1"));
            else
                assertEquals("", row.getString("v1"));

            assertEquals("abc", row.getString("v2"));
        }
    }

    private void streamLegacyTables(String legacyVersion) throws Exception
    {
        for (int compact = 0; compact <= 1; compact++)
        {
            logger.info("Streaming legacy version {}{}", legacyVersion, getCompactNameSuffix(compact));
            streamLegacyTable("legacy_%s_simple%s", legacyVersion, getCompactNameSuffix(compact));
            streamLegacyTable("legacy_%s_simple_counter%s", legacyVersion, getCompactNameSuffix(compact));
            streamLegacyTable("legacy_%s_clust%s", legacyVersion, getCompactNameSuffix(compact));
            streamLegacyTable("legacy_%s_clust_counter%s", legacyVersion, getCompactNameSuffix(compact));
        }
    }

    private void streamLegacyTable(String tablePattern, String legacyVersion, String compactNameSuffix) throws Exception
    {
        String table = String.format(tablePattern, legacyVersion, compactNameSuffix);
        SSTableReader sstable = SSTableReader.open(getDescriptor(legacyVersion, table));
        IPartitioner p = sstable.getPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes("100"))));
        ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("100")), p.getMinimumToken()));
        ArrayList<StreamSession.SSTableStreamingSections> details = new ArrayList<>();
        details.add(new StreamSession.SSTableStreamingSections(sstable.ref(),
                                                               sstable.getPositionsForRanges(ranges),
                                                               sstable.estimatedKeysForRanges(ranges), sstable.getSSTableMetadata().repairedAt));
        new StreamPlan("LegacyStreamingTest").transferFiles(FBUtilities.getBroadcastAddress(), details)
                                             .execute().get();
    }

    private static void truncateLegacyTables(String legacyVersion) throws Exception
    {
        for (int compact = 0; compact <= 1; compact++)
        {
            logger.info("Truncating legacy version {}{}", legacyVersion, getCompactNameSuffix(compact));
            Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_simple%s", legacyVersion, getCompactNameSuffix(compact))).truncateBlocking();
            Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_simple_counter%s", legacyVersion, getCompactNameSuffix(compact))).truncateBlocking();
            Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_clust%s", legacyVersion, getCompactNameSuffix(compact))).truncateBlocking();
            Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_clust_counter%s", legacyVersion, getCompactNameSuffix(compact))).truncateBlocking();
        }
    }

    private static void compactLegacyTables(String legacyVersion) throws Exception
    {
        for (int compact = 0; compact <= 1; compact++)
        {
            logger.info("Compacting legacy version {}{}", legacyVersion, getCompactNameSuffix(compact));
            Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_simple%s", legacyVersion, getCompactNameSuffix(compact))).forceMajorCompaction();
            Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_simple_counter%s", legacyVersion, getCompactNameSuffix(compact))).forceMajorCompaction();
            Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_clust%s", legacyVersion, getCompactNameSuffix(compact))).forceMajorCompaction();
            Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_clust_counter%s", legacyVersion, getCompactNameSuffix(compact))).forceMajorCompaction();
        }
    }

    private static void loadLegacyTables(String legacyVersion) throws Exception
    {
        for (int compact = 0; compact <= 1; compact++)
        {
            logger.info("Preparing legacy version {}{}", legacyVersion, getCompactNameSuffix(compact));
            loadLegacyTable("legacy_%s_simple%s", legacyVersion, getCompactNameSuffix(compact));
            loadLegacyTable("legacy_%s_simple_counter%s", legacyVersion, getCompactNameSuffix(compact));
            loadLegacyTable("legacy_%s_clust%s", legacyVersion, getCompactNameSuffix(compact));
            loadLegacyTable("legacy_%s_clust_counter%s", legacyVersion, getCompactNameSuffix(compact));
        }
    }

    private static void verifyCache(String legacyVersion, long startCount) throws InterruptedException, java.util.concurrent.ExecutionException
    {
        //For https://issues.apache.org/jira/browse/CASSANDRA-10778
        //Validate whether the key cache successfully saves in the presence of old keys as
        //well as loads the correct number of keys
        long endCount = CacheService.instance.keyCache.size();
        Assert.assertTrue(endCount > startCount);
        CacheService.instance.keyCache.submitWrite(Integer.MAX_VALUE).get();
        CacheService.instance.invalidateKeyCache();
        assertEquals(startCount, CacheService.instance.keyCache.size());
        CacheService.instance.keyCache.loadSaved();
        if (BigFormat.instance.getVersion(legacyVersion).storeRows())
            assertEquals(endCount, CacheService.instance.keyCache.size());
        else
            assertEquals(startCount, CacheService.instance.keyCache.size());
    }

    private static void verifyReads(String legacyVersion)
    {
        for (int compact = 0; compact <= 1; compact++)
        {
            for (int ck = 0; ck < 50; ck++)
            {
                String ckValue = Integer.toString(ck) + longString;
                for (int pk = 0; pk < 5; pk++)
                {
                    logger.debug("for pk={} ck={}", pk, ck);

                    String pkValue = Integer.toString(pk);
                    UntypedResultSet rs;
                    if (ck == 0)
                    {
                        readSimpleTable(legacyVersion, getCompactNameSuffix(compact),  pkValue);
                        readSimpleCounterTable(legacyVersion, getCompactNameSuffix(compact), pkValue);
                    }

                    readClusteringTable(legacyVersion, getCompactNameSuffix(compact), ck, ckValue, pkValue);
                    readClusteringCounterTable(legacyVersion, getCompactNameSuffix(compact), ckValue, pkValue);
                }
            }
        }
    }

    private static void readClusteringCounterTable(String legacyVersion, String compactSuffix, String ckValue, String pkValue)
    {
        logger.debug("Read legacy_{}_clust_counter{}", legacyVersion, compactSuffix);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust_counter%s WHERE pk=? AND ck=?", legacyVersion, compactSuffix), pkValue, ckValue);
        Assert.assertNotNull(rs);
        assertEquals(1, rs.size());
        assertEquals(1L, rs.one().getLong("val"));
    }

    private static void readClusteringTable(String legacyVersion, String compactSuffix, int ck, String ckValue, String pkValue)
    {
        logger.debug("Read legacy_{}_clust{}", legacyVersion, compactSuffix);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust%s WHERE pk=? AND ck=?", legacyVersion, compactSuffix), pkValue, ckValue);
        assertLegacyClustRows(1, rs);

        String ckValue2 = Integer.toString(ck < 10 ? 40 : ck - 1) + longString;
        String ckValue3 = Integer.toString(ck > 39 ? 10 : ck + 1) + longString;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust%s WHERE pk=? AND ck IN (?, ?, ?)", legacyVersion, compactSuffix), pkValue, ckValue, ckValue2, ckValue3);
        assertLegacyClustRows(3, rs);
    }

    private static void readSimpleCounterTable(String legacyVersion, String compactSuffix, String pkValue)
    {
        logger.debug("Read legacy_{}_simple_counter{}", legacyVersion, compactSuffix);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_simple_counter%s WHERE pk=?", legacyVersion, compactSuffix), pkValue);
        Assert.assertNotNull(rs);
        assertEquals(1, rs.size());
        assertEquals(1L, rs.one().getLong("val"));
    }

    private static void readSimpleTable(String legacyVersion, String compactSuffix, String pkValue)
    {
        logger.debug("Read simple: legacy_{}_simple{}", legacyVersion, compactSuffix);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_simple%s WHERE pk=?", legacyVersion, compactSuffix), pkValue);
        Assert.assertNotNull(rs);
        assertEquals(1, rs.size());
        assertEquals("foo bar baz", rs.one().getString("val"));
    }

    private static void createKeyspace()
    {
        QueryProcessor.executeInternal("CREATE KEYSPACE legacy_tables WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
    }

    private static void createTables(String legacyVersion)
    {
        for (int i=0; i<=1; i++)
        {
            String compactSuffix = getCompactNameSuffix(i);
            String tableSuffix = i == 0? "" : " WITH COMPACT STORAGE";
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_simple%s (pk text PRIMARY KEY, val text)%s", legacyVersion, compactSuffix, tableSuffix));
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_simple_counter%s (pk text PRIMARY KEY, val counter)%s", legacyVersion, compactSuffix, tableSuffix));
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_clust%s (pk text, ck text, val text, PRIMARY KEY (pk, ck))%s", legacyVersion, compactSuffix, tableSuffix));
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_clust_counter%s (pk text, ck text, val counter, PRIMARY KEY (pk, ck))%s", legacyVersion, compactSuffix, tableSuffix));
        }
    }

    private static String getCompactNameSuffix(int i)
    {
        return i == 0? "" : "_compact";
    }

    private static void truncateTables(String legacyVersion)
    {
        for (int compact = 0; compact <= 1; compact++)
        {
            QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_simple%s", legacyVersion, getCompactNameSuffix(compact)));
            QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_simple_counter%s", legacyVersion, getCompactNameSuffix(compact)));
            QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_clust%s", legacyVersion, getCompactNameSuffix(compact)));
            QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_clust_counter%s", legacyVersion, getCompactNameSuffix(compact)));
        }
        CacheService.instance.invalidateCounterCache();
        CacheService.instance.invalidateKeyCache();
    }

    private static void assertLegacyClustRows(int count, UntypedResultSet rs)
    {
        Assert.assertNotNull(rs);
        assertEquals(count, rs.size());
        for (int i = 0; i < count; i++)
        {
            for (UntypedResultSet.Row r : rs)
            {
                assertEquals(128, r.getString("val").length());
            }
        }
    }

    private static void loadLegacyTable(String tablePattern, String legacyVersion, String compactSuffix) throws IOException
    {
        String table = String.format(tablePattern, legacyVersion, compactSuffix);

        logger.info("Loading legacy table {}", table);

        ColumnFamilyStore cfs = Keyspace.open("legacy_tables").getColumnFamilyStore(table);

        for (File cfDir : cfs.getDirectories().getCFDirectories())
        {
            copySstablesToTestData(legacyVersion, table, cfDir);
        }

        cfs.loadNewSSTables();
    }

    /**
     * Generates sstables for 8 CQL tables (see {@link #createTables(String)}) in <i>current</i>
     * sstable format (version) into {@code test/data/legacy-sstables/VERSION}, where
     * {@code VERSION} matches {@link Version#getVersion() BigFormat.latestVersion.getVersion()}.
     * <p>
     * Run this test alone (e.g. from your IDE) when a new version is introduced or format changed
     * during development. I.e. remove the {@code @Ignore} annotation temporarily.
     * </p>
     */
    @Ignore
    @Test
    public void testGenerateSstables() throws Throwable
    {
        Random rand = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 128; i++)
        {
            sb.append((char)('a' + rand.nextInt(26)));
        }
        String randomString = sb.toString();

        for (int compact = 0; compact <= 1; compact++)
        {
            for (int pk = 0; pk < 5; pk++)
            {
                String valPk = Integer.toString(pk);
                QueryProcessor.executeInternal(String.format("INSERT INTO legacy_tables.legacy_%s_simple%s (pk, val) VALUES ('%s', '%s')",
                                                             BigFormat.latestVersion, getCompactNameSuffix(compact), valPk, "foo bar baz"));

                QueryProcessor.executeInternal(String.format("UPDATE legacy_tables.legacy_%s_simple_counter%s SET val = val + 1 WHERE pk = '%s'",
                                                             BigFormat.latestVersion, getCompactNameSuffix(compact), valPk));

                for (int ck = 0; ck < 50; ck++)
                {
                    String valCk = Integer.toString(ck);

                    QueryProcessor.executeInternal(String.format("INSERT INTO legacy_tables.legacy_%s_clust%s (pk, ck, val) VALUES ('%s', '%s', '%s')",
                                                                 BigFormat.latestVersion, getCompactNameSuffix(compact), valPk, valCk + longString, randomString));

                    QueryProcessor.executeInternal(String.format("UPDATE legacy_tables.legacy_%s_clust_counter%s SET val = val + 1 WHERE pk = '%s' AND ck='%s'",
                                                                 BigFormat.latestVersion, getCompactNameSuffix(compact), valPk, valCk + longString));

                }
            }
        }

        StorageService.instance.forceKeyspaceFlush("legacy_tables");

        File ksDir = new File(LEGACY_SSTABLE_ROOT, String.format("%s/legacy_tables", BigFormat.latestVersion));
        ksDir.mkdirs();
        for (int compact = 0; compact <= 1; compact++)
        {
            copySstablesFromTestData(String.format("legacy_%s_simple%s", BigFormat.latestVersion, getCompactNameSuffix(compact)), ksDir);
            copySstablesFromTestData(String.format("legacy_%s_simple_counter%s", BigFormat.latestVersion, getCompactNameSuffix(compact)), ksDir);
            copySstablesFromTestData(String.format("legacy_%s_clust%s", BigFormat.latestVersion, getCompactNameSuffix(compact)), ksDir);
            copySstablesFromTestData(String.format("legacy_%s_clust_counter%s", BigFormat.latestVersion, getCompactNameSuffix(compact)), ksDir);
        }
    }

    public static void copySstablesFromTestData(String table, File ksDir) throws IOException
    {
        File cfDir = new File(ksDir, table);
        cfDir.mkdir();

        for (File srcDir : Keyspace.open("legacy_tables").getColumnFamilyStore(table).getDirectories().getCFDirectories())
        {
            for (File file : srcDir.listFiles())
            {
                copyFile(cfDir, file);
            }
        }
    }

    private static void copySstablesToTestData(String legacyVersion, String table, File cfDir) throws IOException
    {
        File tableDir = getTableDir(legacyVersion, table);
        Assert.assertTrue("The table directory " + tableDir + " was not found", tableDir.isDirectory());
        for (File file : tableDir.listFiles())
        {
            copyFile(cfDir, file);
        }
    }

    private static File getTableDir(String legacyVersion, String table)
    {
        return new File(LEGACY_SSTABLE_ROOT, String.format("%s/legacy_tables/%s", legacyVersion, table));
    }

    private static void copyFile(File cfDir, File file) throws IOException
    {
        byte[] buf = new byte[65536];
        if (file.isFile())
        {
            File target = new File(cfDir, file.getName());
            int rd;
            try (FileInputStream is = new FileInputStream(file);
                 FileOutputStream os = new FileOutputStream(target);) {
                while ((rd = is.read(buf)) >= 0)
                    os.write(buf, 0, rd);
                }
        }
    }
}
