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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.SchemaConstants;

import static org.junit.Assert.assertEquals;

/* ViewComplexTest class has been split into multiple ones because of timeout issues (CASSANDRA-16670, CASSANDRA-17167)
 * Any changes here check if they apply to the other classes:
 * - ViewComplexUpdatesTest
 * - ViewComplexDeletionsTest
 * - ViewComplexTTLTest
 * - ViewComplexTest
 * - ViewComplexLivenessTest
 * - ...
 * - ViewComplex*Test
 */
public class ViewComplexDeletionsTest extends ViewAbstractParameterizedTest
{
    @Test
    public void testCommutativeRowDeletionFlush() throws Throwable
    {
        // CASSANDRA-13409
        testCommutativeRowDeletion(true);
    }

    @Test
    public void testCommutativeRowDeletionWithoutFlush() throws Throwable
    {
        // CASSANDRA-13409
        testCommutativeRowDeletion(false);
    }

    private void testCommutativeRowDeletion(boolean flush) throws Throwable
    {
        // CASSANDRA-13409 new update should not resurrect previous deleted data in view
        createTable("create table %s (p int primary key, v1 int, v2 int)");

        Keyspace ks = Keyspace.open(keyspace());

        createView("create materialized view %s as select * from %s " +
                     "where p is not null and v1 is not null primary key (v1, p)");
        ks.getColumnFamilyStore(currentView()).disableAutoCompaction();

        // sstable-1, Set initial values TS=1
        updateView("Insert into %s (p, v1, v2) values (3, 1, 3) using timestamp 1;");

        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(executeView("SELECT v2, WRITETIME(v2) from %s WHERE v1 = ? AND p = ?", 1, 3), row(3, 1L));
        // sstable-2
        updateView("Delete from %s using timestamp 2 where p = 3;");

        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(executeView("SELECT v1, p, v2, WRITETIME(v2) from %s"));
        // sstable-3
        updateView("Insert into %s (p, v1) values (3, 1) using timestamp 3;");

        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(executeView("SELECT v1, p, v2, WRITETIME(v2) from %s"), row(1, 3, null, null));
        // sstable-4
        updateView("UPdate %s using timestamp 4 set v1 = 2 where p = 3;");

        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(executeView("SELECT v1, p, v2, WRITETIME(v2) from %s"), row(2, 3, null, null));
        // sstable-5
        updateView("UPdate %s using timestamp 5 set v1 = 1 where p = 3;");

        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(executeView("SELECT v1, p, v2, WRITETIME(v2) from %s"), row(1, 3, null, null));

        if (flush)
        {
            // compact sstable 2 and 4, 5;
            ColumnFamilyStore cfs = ks.getColumnFamilyStore(currentView());
            List<String> sstables = cfs.getLiveSSTables()
                                       .stream()
                                       .sorted(SSTableReader.idComparator)
                                       .map(SSTableReader::getFilename)
                                       .collect(Collectors.toList());
            String dataFiles = String.join(",", Arrays.asList(sstables.get(1), sstables.get(3), sstables.get(4)));
            CompactionManager.instance.forceUserDefinedCompaction(dataFiles);
            assertEquals(3, cfs.getLiveSSTables().size());
        }
        // regular tombstone should be retained after compaction
        assertRowsIgnoringOrder(executeView("SELECT v1, p, v2, WRITETIME(v2) from %s"), row(1, 3, null, null));
    }

    @Test
    public void testComplexTimestampDeletionTestWithFlush() throws Throwable
    {
        complexTimestampWithbaseNonPKColumnsInViewPKDeletionTest(true);
        complexTimestampWithbasePKColumnsInViewPKDeletionTest(true);
    }

    @Test
    public void testComplexTimestampDeletionTestWithoutFlush() throws Throwable
    {
        complexTimestampWithbaseNonPKColumnsInViewPKDeletionTest(false);
        complexTimestampWithbasePKColumnsInViewPKDeletionTest(false);
    }

    private void complexTimestampWithbasePKColumnsInViewPKDeletionTest(boolean flush) throws Throwable
    {
        createTable("create table %s (p1 int, p2 int, v1 int, v2 int, primary key(p1, p2))");

        Keyspace ks = Keyspace.open(keyspace());

        createView("create materialized view %s as select * from %s " +
                     "where p1 is not null and p2 is not null primary key (p2, p1)");
        ks.getColumnFamilyStore(currentView()).disableAutoCompaction();

        // Set initial values TS=1
        updateView("Insert into %s (p1, p2, v1, v2) values (1, 2, 3, 4) using timestamp 1;");

        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(executeView("SELECT v1, v2, WRITETIME(v2) from %s WHERE p1 = ? AND p2 = ?", 1, 2),
                                row(3, 4, 1L));
        // remove row/mv TS=2
        updateView("Delete from %s using timestamp 2 where p1 = 1 and p2 = 2;");

        if (flush)
            Util.flush(ks);
        // view are empty
        assertRowsIgnoringOrder(executeView("SELECT * FROM %s"));
        // insert PK with TS=3
        updateView("Insert into %s (p1, p2) values (1, 2) using timestamp 3;");

        if (flush)
            Util.flush(ks);
        // deleted column in MV remained dead
        assertRowsIgnoringOrder(executeView("SELECT * FROM %s"), row(2, 1, null, null));

        ks.getColumnFamilyStore(currentView()).forceMajorCompaction();
        assertRowsIgnoringOrder(executeView("SELECT * FROM %s"), row(2, 1, null, null));

        // reset values
        updateView("Insert into %s (p1, p2, v1, v2) values (1, 2, 3, 4) using timestamp 10;");
        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(executeView("SELECT v1, v2, WRITETIME(v2) from %s WHERE p1 = ? AND p2 = ?", 1, 2),
                                row(3, 4, 10L));

        updateView("UPDATE %s using timestamp 20 SET v2 = 5 WHERE p1 = 1 and p2 = 2");
        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(executeView("SELECT v1, v2, WRITETIME(v2) from %s WHERE p1 = ? AND p2 = ?", 1, 2),
                                row(3, 5, 20L));

        updateView("DELETE FROM %s using timestamp 10 WHERE p1 = 1 and p2 = 2");
        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(executeView("SELECT v1, v2, WRITETIME(v2) from %s WHERE p1 = ? AND p2 = ?", 1, 2),
                                row(null, 5, 20L));
    }

    private void complexTimestampWithbaseNonPKColumnsInViewPKDeletionTest(boolean flush) throws Throwable
    {
        createTable("create table %s (p int primary key, v1 int, v2 int)");

        Keyspace ks = Keyspace.open(keyspace());

        createView("create materialized view %s as select * from %s " +
                     "where p is not null and v1 is not null primary key (v1, p)");
        ks.getColumnFamilyStore(currentView()).disableAutoCompaction();

        // Set initial values TS=1
        updateView("Insert into %s (p, v1, v2) values (3, 1, 5) using timestamp 1;");

        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(executeView("SELECT v2, WRITETIME(v2) from %s WHERE v1 = ? AND p = ?", 1, 3), row(5, 1L));
        // remove row/mv TS=2
        updateView("Delete from %s using timestamp 2 where p = 3;");

        if (flush)
            Util.flush(ks);
        // view are empty
        assertRowsIgnoringOrder(executeView("SELECT * FROM %s"));
        // insert PK with TS=3
        updateView("Insert into %s (p, v1) values (3, 1) using timestamp 3;");

        if (flush)
            Util.flush(ks);
        // deleted column in MV remained dead
        assertRowsIgnoringOrder(executeView("SELECT * FROM %s"), row(1, 3, null));

        // insert values TS=2, it should be considered dead due to previous tombstone
        updateView("Insert into %s (p, v1, v2) values (3, 1, 5) using timestamp 2;");

        if (flush)
            Util.flush(ks);
        // deleted column in MV remained dead
        assertRowsIgnoringOrder(executeView("SELECT * FROM %s"), row(1, 3, null));
        assertRowsIgnoringOrder(executeView("SELECT * from %s limit 1"), row(1, 3, null));

        // insert values TS=2, it should be considered dead due to previous tombstone
        executeNet("UPDATE %s USING TIMESTAMP 3 SET v2 = ? WHERE p = ?", 4, 3);

        if (flush)
            Util.flush(ks);

        assertRows(execute("SELECT v1, p, v2, WRITETIME(v2) from %s"), row(1, 3, 4, 3L));

        ks.getColumnFamilyStore(currentView()).forceMajorCompaction();
        assertRows(executeView("SELECT v1, p, v2, WRITETIME(v2) from %s"), row(1, 3, 4, 3L));
        assertRows(executeView("SELECT v1, p, v2, WRITETIME(v2) from %s limit 1"), row(1, 3, 4, 3L));
    }

    @Test
    public void testNoBatchlogCleanupForLocalMutations() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int primary key, v1 int)");
        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                   "WHERE k1 IS NOT NULL AND v1 IS NOT NULL PRIMARY KEY (v1, k1)");

        ColumnFamilyStore batchlog = Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.BATCHES);
        batchlog.disableAutoCompaction();
        Util.flush(batchlog);
        int batchlogSSTables = batchlog.getLiveSSTables().size();

        updateView("INSERT INTO %s(k1, v1) VALUES(1, 1)");
        Util.flush(batchlog);
        assertEquals(batchlogSSTables, batchlog.getLiveSSTables().size());
    }
}
