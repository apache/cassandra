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

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import junit.framework.Assert;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.utils.FBUtilities;

import org.junit.Test;


public class CrcCheckChanceTest extends CQLTester
{
    @Test
    public void testChangingCrcCheckChance() throws Throwable
    {
        //Start with crc_check_chance of 99%
        createTable("CREATE TABLE %s (p text, c text, v text, s text static, PRIMARY KEY (p, c)) WITH compression = {'sstable_compression': 'LZ4Compressor', 'crc_check_chance' : 0.99}");

        execute("CREATE INDEX foo ON %s(v)");

        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k1", "v1", "sv1");
        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", "k2", "v2");
        execute("INSERT INTO %s(p, s) values (?, ?)", "p2", "sv2");


        ColumnFamilyStore cfs = Keyspace.open(CQLTester.KEYSPACE).getColumnFamilyStore(currentTable());
        ColumnFamilyStore indexCfs = cfs.indexManager.getIndexesBackedByCfs().iterator().next();
        cfs.forceBlockingFlush();

        Assert.assertEquals(0.99, cfs.metadata.compressionParameters.getCrcCheckChance());
        Assert.assertEquals(0.99, cfs.getSSTables().iterator().next().getCompressionMetadata().parameters.getCrcCheckChance());
        Assert.assertEquals(0.99, indexCfs.metadata.compressionParameters.getCrcCheckChance());
        Assert.assertEquals(0.99, indexCfs.getSSTables().iterator().next().getCompressionMetadata().parameters.getCrcCheckChance());

        //Test for stack overflow
        cfs.setCrcCheckChance(0.99);

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

        cfs.forceBlockingFlush();


        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k1", "v1", "sv1");
        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", "k2", "v2");
        execute("INSERT INTO %s(p, s) values (?, ?)", "p2", "sv2");

        cfs.forceBlockingFlush();

        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k1", "v1", "sv1");
        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", "k2", "v2");
        execute("INSERT INTO %s(p, s) values (?, ?)", "p2", "sv2");

        cfs.forceBlockingFlush();

        cfs.forceMajorCompaction();

        //Verify when we alter the value the live sstable readers hold the new one
        alterTable("ALTER TABLE %s WITH compression = {'sstable_compression': 'LZ4Compressor', 'crc_check_chance': 0.01}");

        Assert.assertEquals( 0.01, cfs.metadata.compressionParameters.getCrcCheckChance());
        Assert.assertEquals( 0.01, cfs.getSSTables().iterator().next().getCompressionMetadata().parameters.getCrcCheckChance());
        Assert.assertEquals( 0.01, indexCfs.metadata.compressionParameters.getCrcCheckChance());
        Assert.assertEquals( 0.01, indexCfs.getSSTables().iterator().next().getCompressionMetadata().parameters.getCrcCheckChance());

        assertRows(execute("SELECT * FROM %s WHERE p=?", "p1"),
                row("p1", "k1", "sv1", "v1"),
                row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE v=?", "v1"),
                row("p1", "k1", "sv1", "v1")
        );


        //Verify the call used by JMX still works
        cfs.setCrcCheckChance(0.03);
        Assert.assertEquals( 0.03, cfs.metadata.compressionParameters.getCrcCheckChance());
        Assert.assertEquals( 0.03, cfs.getSSTables().iterator().next().getCompressionMetadata().parameters.getCrcCheckChance());
        Assert.assertEquals( 0.03, indexCfs.metadata.compressionParameters.getCrcCheckChance());
        Assert.assertEquals( 0.03, indexCfs.getSSTables().iterator().next().getCompressionMetadata().parameters.getCrcCheckChance());

    }


    @Test
    public void testDropDuringCompaction() throws Throwable
    {
        CompactionManager.instance.disableAutoCompaction();

        //Start with crc_check_chance of 99%
        createTable("CREATE TABLE %s (p text, c text, v text, s text static, PRIMARY KEY (p, c)) WITH compression = {'sstable_compression': 'LZ4Compressor', 'crc_check_chance' : 0.99}");

        ColumnFamilyStore cfs = Keyspace.open(CQLTester.KEYSPACE).getColumnFamilyStore(currentTable());

        //Write a few SSTables then Compact, and drop
        for (int i = 0; i < 100; i++)
        {
            execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k1", "v1", "sv1");
            execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", "k2", "v2");
            execute("INSERT INTO %s(p, s) values (?, ?)", "p2", "sv2");

            cfs.forceBlockingFlush();
        }

        DatabaseDescriptor.setCompactionThroughputMbPerSec(1);
        List<Future<?>> futures = CompactionManager.instance.submitMaximal(cfs, CompactionManager.GC_ALL); 
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

