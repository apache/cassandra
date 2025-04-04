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

package org.apache.cassandra.metrics;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class RowsScannedShadowedTest extends CQLTester
{
    private static final String KEYSPACE = "rowsscannedshadowedtest";
    private static final String CF1 = "cf1";
    private ColumnFamilyStore cfs;

    @BeforeClass
    public static void init()
    {
        requireNetwork();
    }

    @Before
    public void defineSchema() throws Throwable
    {
        execute(format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
        execute(format("CREATE TABLE IF NOT EXISTS %s.%s (k text, c1 int, c2 int, v text, PRIMARY KEY(k, c1, c2)) WITH CLUSTERING ORDER BY (c1 ASC, c2 ASC)", KEYSPACE, CF1));
        cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1);
        cfs.disableAutoCompaction();
        DatabaseDescriptor.setShadowRowsTrackingEnabled(true);
    }

    @After
    public void teardown() throws Throwable
    {
        DatabaseDescriptor.setShadowRowsTrackingEnabled(false); // expected to be disabled
        execute(format("DROP KEYSPACE IF EXISTS %s", KEYSPACE));
    }

    private void cleanUpMetric()
    {
        ((ClearableHistogram) cfs.metric.rowsScannedShadowedByPartitionTombstoneHistogram.cf).clear();
        ((ClearableHistogram) cfs.metric.rowsScannedShadowedByRowTombstoneHistogram.cf).clear();
        ((ClearableHistogram) cfs.metric.rowsScannedShadowedByRangeTombstoneHistogram.cf).clear();
        ((ClearableHistogram) cfs.metric.rowsScannedShadowedByOtherRowsHistogram.cf).clear();
    }

    @Test
    public void naiveSingleSSTableTest() throws Throwable
    {
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 1, 'v')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 2, 'v')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 3, 'v')", KEYSPACE, CF1));
        Util.flush(cfs);

        cleanUpMetric();
        execute(format("SELECT * FROM %s.%s WHERE k='k';", KEYSPACE, CF1));
        assertEquals(0, cfs.metric.rowsScannedShadowedByPartitionTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedByRowTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedByRangeTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedByOtherRowsHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedHistogram.cf.getSnapshot().getMax());
    }

    @Test
    public void multipleSSTablesMultipleVersionsTest() throws Throwable
    {
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 1, 'v')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 2, 'v')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 3, 'v')", KEYSPACE, CF1));
        Util.flush(cfs);
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 1, 'vv')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 2, 'vv')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 3, 'vv')", KEYSPACE, CF1));
        Util.flush(cfs);
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 1, 'vvv')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 2, 'vvv')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 3, 'vvv')", KEYSPACE, CF1));
        Util.flush(cfs);

        cleanUpMetric();
        execute(format("SELECT * FROM %s.%s WHERE k='k';", KEYSPACE, CF1));
        assertEquals(0, cfs.metric.rowsScannedShadowedByPartitionTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedByRowTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedByRangeTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(6, cfs.metric.rowsScannedShadowedByOtherRowsHistogram.cf.getSnapshot().getMax());
        assertEquals(6, cfs.metric.rowsScannedShadowedHistogram.cf.getSnapshot().getMax());
    }

    @Test
    public void rangeTombstonesTest() throws Throwable
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1);
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 1, '1')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 2, 1, '2')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 3, 1, '3')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 4, 1, '4')", KEYSPACE, CF1));
        Util.flush(cfs);
        // delete 2 and 3 by range TS
        execute(format("DELETE FROM %s.%s WHERE k='k' AND c1 > 1 AND c1 < 4", KEYSPACE, CF1));
        Util.flush(cfs);

        // to get the first 2 live rows, all c1 in [1,4] need to be scanned, and c1=2,3 are shadowed by range ts
        cleanUpMetric();
        execute(format("SELECT * FROM %s.%s WHERE k='k' LIMIT 2;", KEYSPACE, CF1));
        assertEquals(2, cfs.metric.rowsScannedShadowedByRangeTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedByRowTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedByPartitionTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedByOtherRowsHistogram.cf.getSnapshot().getMax());
        assertEquals(2, cfs.metric.rowsScannedShadowedHistogram.cf.getSnapshot().getMax());
    }

    @Test
    public void updatesTest() throws Throwable
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1);
        execute(format("DELETE FROM %s.%s WHERE k='k' AND c1 = 2 AND c2 = 1", KEYSPACE, CF1));
        Util.flush(cfs);
        // update will insert a row without row liveness info
        execute(format("UPDATE %s.%s SET v='1' WHERE k='k' AND c1 = 2 AND c2 = 1", KEYSPACE, CF1));
        Util.flush(cfs);

        cleanUpMetric();
        execute(format("SELECT * FROM %s.%s WHERE k='k';", KEYSPACE, CF1));
        assertEquals(0, cfs.metric.rowsScannedShadowedByRangeTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedByRowTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedByPartitionTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(1, cfs.metric.rowsScannedShadowedByOtherRowsHistogram.cf.getSnapshot().getMax());
        assertEquals(1, cfs.metric.rowsScannedShadowedHistogram.cf.getSnapshot().getMax());
    }

    @Test
    public void rowTombstonesTest() throws Throwable
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1);
        // inserts
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 1, '1')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 2, 1, '2')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 3, 1, '3')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 4, 1, '4')", KEYSPACE, CF1));
        Util.flush(cfs);
        // delete 2
        execute(format("DELETE FROM %s.%s WHERE k='k' AND c1 = 2 AND c2 = 1", KEYSPACE, CF1));
        Util.flush(cfs);

        // this will need to read the shadowed c1=2
        cleanUpMetric();
        execute(format("SELECT * FROM %s.%s WHERE k='k';", KEYSPACE, CF1));
        assertEquals(0, cfs.metric.rowsScannedShadowedByRangeTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(1, cfs.metric.rowsScannedShadowedByRowTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedByPartitionTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedByOtherRowsHistogram.cf.getSnapshot().getMax());
        assertEquals(1, cfs.metric.rowsScannedShadowedHistogram.cf.getSnapshot().getMax());
    }

    @Test
    public void partitionTombstonesTestOnSkippedCase() throws Throwable
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1);
        // inserts
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 1, '1')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 2, 1, '2')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 3, 1, '3')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 4, 1, '4')", KEYSPACE, CF1));
        Util.flush(cfs);
        // delete by partition key
        execute(format("DELETE FROM %s.%s WHERE k='k'", KEYSPACE, CF1));
        Util.flush(cfs);

        // first sstable got skipped by mostRecentPartitionTombstone, latest partition deletion ts > max ts on the first sstable
        cleanUpMetric();
        execute(format("SELECT * FROM %s.%s WHERE k='k';", KEYSPACE, CF1));
        assertEquals(0, cfs.metric.rowsScannedShadowedByRangeTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedByRowTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedByPartitionTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedByOtherRowsHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedHistogram.cf.getSnapshot().getMax());
    }

    @Test
    public void partitionTombstonesTest() throws Throwable
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1);
        // inserts
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 1, '1') USING TIMESTAMP 0", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 2, 1, '2') USING TIMESTAMP 0", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 3, 1, '3') USING TIMESTAMP 0", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 4, 1, '4') USING TIMESTAMP 2", KEYSPACE, CF1));
        Util.flush(cfs);
        // delete by partition key, the first sstable is included
        execute(format("DELETE FROM %s.%s USING TIMESTAMP 1 WHERE k='k'", KEYSPACE, CF1));
        Util.flush(cfs);

        // should scan c1=1,2,3
        cleanUpMetric();
        execute(format("SELECT * FROM %s.%s WHERE k='k';", KEYSPACE, CF1));
        assertEquals(0, cfs.metric.rowsScannedShadowedByRangeTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedByRowTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(3, cfs.metric.rowsScannedShadowedByPartitionTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedByOtherRowsHistogram.cf.getSnapshot().getMax());
        assertEquals(3, cfs.metric.rowsScannedShadowedHistogram.cf.getSnapshot().getMax());
    }

    @Test
    public void queryMemtableAndSSTablesInTimestampOrderRowTSTest() throws Throwable
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF1);
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 1, 'v')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 2, 'v')", KEYSPACE, CF1));
        Util.flush(cfs);
        // row ts
        execute(format("DELETE FROM %s.%s WHERE k='k' AND c1 = 1 AND c2 = 2", KEYSPACE, CF1));
        Util.flush(cfs);
        // new value
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 1, 'vv')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 2, 'vv')", KEYSPACE, CF1));
        Util.flush(cfs);

        // here we only read the new value from the newest sstable, nothing shadowed scanned
        cleanUpMetric();
        execute(format("SELECT * FROM %s.%s WHERE k='k' AND c1 = 1 AND c2 = 2;", KEYSPACE, CF1));
        assertEquals(0, cfs.metric.rowsScannedShadowedByPartitionTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedByRowTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedByRangeTombstoneHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedByOtherRowsHistogram.cf.getSnapshot().getMax());
        assertEquals(0, cfs.metric.rowsScannedShadowedHistogram.cf.getSnapshot().getMax());
    }

    @Test
    public void multipleTypesOfShadowRows() throws Throwable
    {
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 1, 'v')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 2, 'v')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 3, 'v')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 4, 'v')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('t', 1, 1, 'v')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('t', 1, 2, 'v')", KEYSPACE, CF1));
        Util.flush(cfs);
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 1, 'vv')", KEYSPACE, CF1));
        execute(format("DELETE FROM %s.%s WHERE k='k' AND c1 = 1 and c2 > 1 AND c2 < 5", KEYSPACE, CF1));
        execute(format("DELETE FROM %s.%s WHERE k='t'", KEYSPACE, CF1));
        Util.flush(cfs);
        execute(format("DELETE FROM %s.%s WHERE k='k' AND c1 = 1 AND c2 = 1", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 2, 'vvv')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 1, 3, 'vvv')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('k', 2, 2, 'v')", KEYSPACE, CF1));
        execute(format("INSERT INTO %s.%s (k, c1, c2, v) VALUES ('t', 1, 2, 'v')", KEYSPACE, CF1));
        Util.flush(cfs);

        cleanUpMetric();
        execute(format("SELECT * FROM %s.%s WHERE k='k';", KEYSPACE, CF1));
        execute(format("SELECT * FROM %s.%s WHERE k='v';", KEYSPACE, CF1));
        // older sstable skipped
        assertEquals(0, cfs.metric.rowsScannedShadowedByPartitionTombstoneHistogram.cf.getSnapshot().getMax());
        // (k,1,1,v) and (k,1,1,vv) shadowed by del(k,1,(1,5))
        assertEquals(2, cfs.metric.rowsScannedShadowedByRowTombstoneHistogram.cf.getSnapshot().getMax());
        // (k,1,4,v) shadowed by del(k,1,1)
        assertEquals(1, cfs.metric.rowsScannedShadowedByRangeTombstoneHistogram.cf.getSnapshot().getMax());
        // (k,1,2,v) and (k,1,3,v) shadowed by (k,1,2,vvv) and (k,1,3,vvv)
        assertEquals(2, cfs.metric.rowsScannedShadowedByOtherRowsHistogram.cf.getSnapshot().getMax());
        // for pk=k, total 8 rows with 2 live rows (k,1,2,vvv), (k,1,3,vvv), 1 row tombstone (k,1,1) not counted
        assertEquals(5, cfs.metric.rowsScannedShadowedHistogram.cf.getSnapshot().getMax());
    }
}
