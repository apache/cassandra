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
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.sstable.format.SSTableReader;

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
public class ViewComplexTombstoneTest extends ViewAbstractParameterizedTest
{
    @Test
    public void testCellTombstoneAndShadowableTombstonesWithFlush() throws Throwable
    {
        testCellTombstoneAndShadowableTombstones(true);
    }

    @Test
    public void testCellTombstoneAndShadowableTombstonesWithoutFlush() throws Throwable
    {
        testCellTombstoneAndShadowableTombstones(false);
    }

    private void testCellTombstoneAndShadowableTombstones(boolean flush) throws Throwable
    {
        createTable("create table %s (p int primary key, v1 int, v2 int)");

        Keyspace ks = Keyspace.open(keyspace());

        createView("create materialized view %s as select * from %s " +
                   "where p is not null and v1 is not null primary key (v1, p)");
        ks.getColumnFamilyStore(currentView()).disableAutoCompaction();

        // sstable 1, Set initial values TS=1
        updateView("Insert into %s (p, v1, v2) values (3, 1, 3) using timestamp 1;");

        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(executeView("SELECT v2, WRITETIME(v2) from %s WHERE v1 = ? AND p = ?", 1, 3), row(3, 1L));
        // sstable 2
        updateView("UPdate %s using timestamp 2 set v2 = null where p = 3");

        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(executeView("SELECT v2, WRITETIME(v2) from %s WHERE v1 = ? AND p = ?", 1, 3),
                                row(null, null));
        // sstable 3
        updateView("UPdate %s using timestamp 3 set v1 = 2 where p = 3");

        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(executeView("SELECT v1, p, v2, WRITETIME(v2) from %s"), row(2, 3, null, null));
        // sstable 4
        updateView("UPdate %s using timestamp 4 set v1 = 1 where p = 3");

        if (flush)
            Util.flush(ks);

        assertRowsIgnoringOrder(executeView("SELECT v1, p, v2, WRITETIME(v2) from %s"), row(1, 3, null, null));

        if (flush)
        {
            // compact sstable 2 and 3;
            ColumnFamilyStore cfs = ks.getColumnFamilyStore(currentView());
            List<String> sstables = cfs.getLiveSSTables()
                                       .stream()
                                       .sorted(Comparator.comparing(s -> s.descriptor.id, SSTableIdFactory.COMPARATOR))
                                       .map(SSTableReader::getFilename)
                                       .collect(Collectors.toList());
            String dataFiles = String.join(",", Arrays.asList(sstables.get(1), sstables.get(2)));
            CompactionManager.instance.forceUserDefinedCompaction(dataFiles);
        }
        // cell-tombstone in sstable 4 is not compacted away, because the shadowable tombstone is shadowed by new row.
        assertRowsIgnoringOrder(executeView("SELECT v1, p, v2, WRITETIME(v2) from %s"), row(1, 3, null, null));
        assertRowsIgnoringOrder(executeView("SELECT v1, p, v2, WRITETIME(v2) from %s limit 1"), row(1, 3, null, null));
    }
}
