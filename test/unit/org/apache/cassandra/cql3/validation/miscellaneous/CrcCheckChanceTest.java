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
package org.apache.cassandra.cql3.validation.miscellaneous;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Test;

import org.junit.Assert;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.FBUtilities;

public class CrcCheckChanceTest extends CQLTester
{
    @Test
    public void testChangingCrcCheckChance()
    {
        createTable("CREATE TABLE %s (p text, c text, v text, s text static, PRIMARY KEY (p, c)) WITH compression = {'class': 'LZ4Compressor'} AND crc_check_chance = 0.99;");

        execute("CREATE INDEX foo ON %s(v)");

        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k1", "v1", "sv1");
        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", "k2", "v2");
        execute("INSERT INTO %s(p, s) values (?, ?)", "p2", "sv2");

        ColumnFamilyStore cfs = Keyspace.open(CQLTester.KEYSPACE).getColumnFamilyStore(currentTable());
        ColumnFamilyStore indexCfs = cfs.indexManager.getAllIndexColumnFamilyStores().iterator().next();
        Util.flush(cfs);

        Assert.assertEquals(0.99, cfs.getCrcCheckChance(), 0.0);
        Assert.assertEquals(0.99, cfs.getLiveSSTables().iterator().next().getCrcCheckChance(), 0.0);

        Assert.assertEquals(0.99, indexCfs.getCrcCheckChance(), 0.0);
        Assert.assertEquals(0.99, indexCfs.getLiveSSTables().iterator().next().getCrcCheckChance(), 0.0);

        //Test for stack overflow
        alterTable("ALTER TABLE %s WITH crc_check_chance = 0.99");

        assertRows(execute("SELECT * FROM %s WHERE p=?", "p1"),
                   row("p1", "k1", "sv1", "v1"),
                   row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE v=?", "v1"),
                   row("p1", "k1", "sv1", "v1")
        );

        //Write a few SSTables then Compact
        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k1", "v1", "sv1");
        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", "k2", "v2");
        execute("INSERT INTO %s(p, s) values (?, ?)", "p2", "sv2");

        Util.flush(cfs);

        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k1", "v1", "sv1");
        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", "k2", "v2");
        execute("INSERT INTO %s(p, s) values (?, ?)", "p2", "sv2");

        Util.flush(cfs);

        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k1", "v1", "sv1");
        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", "k2", "v2");
        execute("INSERT INTO %s(p, s) values (?, ?)", "p2", "sv2");

        Util.flush(cfs);
        cfs.forceMajorCompaction();

        //Now let's change via JMX
        cfs.setCrcCheckChance(0.01);

        Assert.assertEquals(0.01, cfs.getCrcCheckChance(), 0.0);
        Assert.assertEquals(0.01, cfs.getLiveSSTables().iterator().next().getCrcCheckChance(), 0.0);
        Assert.assertEquals(0.01, indexCfs.getCrcCheckChance(), 0.0);
        Assert.assertEquals(0.01, indexCfs.getLiveSSTables().iterator().next().getCrcCheckChance(), 0.0);

        assertRows(execute("SELECT * FROM %s WHERE p=?", "p1"),
                   row("p1", "k1", "sv1", "v1"),
                   row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE v=?", "v1"),
                   row("p1", "k1", "sv1", "v1")
        );

        //Alter again via schema
        alterTable("ALTER TABLE %s WITH crc_check_chance = 0.5");

        //We should be able to get the new value by accessing directly the schema metadata
        Assert.assertEquals(0.5, cfs.metadata().params.crcCheckChance, 0.0);

        //but previous JMX-set value will persist until next restart
        Assert.assertEquals(0.01, cfs.getLiveSSTables().iterator().next().getCrcCheckChance(), 0.0);
        Assert.assertEquals(0.01, indexCfs.getCrcCheckChance(), 0.0);
        Assert.assertEquals(0.01, indexCfs.getLiveSSTables().iterator().next().getCrcCheckChance(), 0.0);

        //Verify the call used by JMX still works
        cfs.setCrcCheckChance(0.03);
        Assert.assertEquals(0.03, cfs.getCrcCheckChance(), 0.0);
        Assert.assertEquals(0.03, cfs.getLiveSSTables().iterator().next().getCrcCheckChance(), 0.0);
        Assert.assertEquals(0.03, indexCfs.getCrcCheckChance(), 0.0);
        Assert.assertEquals(0.03, indexCfs.getLiveSSTables().iterator().next().getCrcCheckChance(), 0.0);

        // Also check that any open readers also use the updated value
        // note: only compressed files currently perform crc checks, so only the dfile reader is relevant here
        SSTableReader baseSSTable = cfs.getLiveSSTables().iterator().next();
        SSTableReader idxSSTable = indexCfs.getLiveSSTables().iterator().next();
        try (RandomAccessReader baseDataReader = baseSSTable.openDataReader();
             RandomAccessReader idxDataReader = idxSSTable.openDataReader())
        {
            Assert.assertEquals(0.03, baseDataReader.getCrcCheckChance(), 0.0);
            Assert.assertEquals(0.03, idxDataReader.getCrcCheckChance(), 0.0);

            cfs.setCrcCheckChance(0.31);
            Assert.assertEquals(0.31, baseDataReader.getCrcCheckChance(), 0.0);
            Assert.assertEquals(0.31, idxDataReader.getCrcCheckChance(), 0.0);
        }
    }

    @Test
    public void testDropDuringCompaction()
    {
        CompactionManager.instance.disableAutoCompaction();

        //Start with crc_check_chance of 99%
        createTable("CREATE TABLE %s (p text, c text, v text, s text static, PRIMARY KEY (p, c)) WITH compression = {'class': 'LZ4Compressor'} AND crc_check_chance = 0.99");

        ColumnFamilyStore cfs = Keyspace.open(CQLTester.KEYSPACE).getColumnFamilyStore(currentTable());

        //Write a few SSTables then Compact, and drop
        for (int i = 0; i < 100; i++)
        {
            execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k1", "v1", "sv1");
            execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", "k2", "v2");
            execute("INSERT INTO %s(p, s) values (?, ?)", "p2", "sv2");

            Util.flush(cfs);
        }

        DatabaseDescriptor.setCompactionThroughputMebibytesPerSec(1);
        List<? extends Future<?>> futures = CompactionManager.instance.submitMaximal(cfs, CompactionManager.getDefaultGcBefore(cfs, FBUtilities.nowInSeconds()), false);
        execute("DROP TABLE %s");

        try
        {
            FBUtilities.waitOnFutures(futures);
        }
        catch (Throwable t)
        {
            if (!(t.getCause() instanceof ExecutionException) || !(t.getCause().getCause() instanceof CompactionInterruptedException))
                throw t;
        }
    }
}
