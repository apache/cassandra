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

import org.junit.*;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.sstable.SSTableMetadata;
import org.apache.cassandra.service.ClientState;
import static org.apache.cassandra.cql3.QueryProcessor.process;
import static org.apache.cassandra.cql3.QueryProcessor.processInternal;
import static org.junit.Assert.assertEquals;

public class SSTableMetadataTrackingTest
{
    private static String keyspace = "sstable_metadata_tracking_test";
    private static ClientState clientState;

    @BeforeClass
    public static void setup() throws Throwable
    {
        SchemaLoader.loadSchema();
        createKeyspace("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        clientState = ClientState.forInternalCalls();
    }

    
    @Test
    public void baseCheck() throws Throwable
    {
        createTable("CREATE TABLE %s.basecheck (a int, b int, c text, PRIMARY KEY (a, b))");
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore("basecheck");
        execute("INSERT INTO %s.basecheck (a,b,c) VALUES (1,1,'1') using timestamp 9999");
        cfs.forceBlockingFlush();
        SSTableMetadata metadata = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(9999, metadata.minTimestamp);
        assertEquals(Integer.MAX_VALUE, metadata.maxLocalDeletionTime);
        cfs.forceMajorCompaction();
        metadata = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(9999, metadata.minTimestamp);
        assertEquals(Integer.MAX_VALUE, metadata.maxLocalDeletionTime);
    }

    @Test
    public void testMinMaxtimestampRange() throws Throwable
    {
        createTable("CREATE TABLE %s.minmaxtsrange (a int, b int, c text, PRIMARY KEY (a, b))");
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore("minmaxtsrange");
        execute("INSERT INTO %s.minmaxtsrange (a,b,c) VALUES (1,1,'1') using timestamp 10000");
        execute("DELETE FROM %s.minmaxtsrange USING TIMESTAMP 9999 WHERE a = 1 and b = 1");
        cfs.forceBlockingFlush();
        SSTableMetadata metadata = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(9999, metadata.minTimestamp);
        assertEquals(10000, metadata.maxTimestamp);
        assertEquals(Integer.MAX_VALUE, metadata.maxLocalDeletionTime, 5);
        cfs.forceMajorCompaction();
        metadata = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(9999, metadata.minTimestamp);
        assertEquals(10000, metadata.maxTimestamp);
        assertEquals(Integer.MAX_VALUE, metadata.maxLocalDeletionTime, 5);
    }

    @Test
    public void testMinMaxtimestampRow() throws Throwable
    {
        createTable("CREATE TABLE %s.minmaxtsrow (a int, b int, c text, PRIMARY KEY (a, b))");
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore("minmaxtsrow");
        execute("INSERT INTO %s.minmaxtsrow (a,b,c) VALUES (1,1,'1') using timestamp 10000");
        execute("DELETE FROM %s.minmaxtsrow USING TIMESTAMP 9999 WHERE a = 1");
        cfs.forceBlockingFlush();
        SSTableMetadata metadata = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(9999, metadata.minTimestamp);
        assertEquals(10000, metadata.maxTimestamp);
        assertEquals(Integer.MAX_VALUE, metadata.maxLocalDeletionTime, 5);
        cfs.forceMajorCompaction();
        metadata = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(9999, metadata.minTimestamp);
        assertEquals(10000, metadata.maxTimestamp);
        assertEquals(Integer.MAX_VALUE, metadata.maxLocalDeletionTime, 5);
    }


    @Test
    public void testTrackMetadata_rangeTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s.rangetombstone (a int, b int, c text, PRIMARY KEY (a, b)) WITH gc_grace_seconds = 10000");
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore("rangetombstone");
        execute("DELETE FROM %s.rangetombstone USING TIMESTAMP 9999 WHERE a = 1 and b = 1");
        cfs.forceBlockingFlush();
        assertEquals(1, cfs.getSSTables().size());
        SSTableMetadata metadata = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(9999, metadata.minTimestamp);
        assertEquals(9999, metadata.maxTimestamp);
        assertEquals(System.currentTimeMillis()/1000, metadata.maxLocalDeletionTime, 5);
        cfs.forceMajorCompaction();
        SSTableMetadata metadata2 = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(metadata.maxLocalDeletionTime, metadata2.maxLocalDeletionTime);
        assertEquals(metadata.minTimestamp, metadata2.minTimestamp);
        assertEquals(metadata.maxTimestamp, metadata2.maxTimestamp);
    }

    @Test
    public void testTrackMetadata_rowTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s.rowtombstone (a int, b int, c text, PRIMARY KEY (a, b))");
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore("rowtombstone");
        execute("DELETE FROM %s.rowtombstone USING TIMESTAMP 9999 WHERE a = 1");

        cfs.forceBlockingFlush();
        assertEquals(1, cfs.getSSTables().size());
        SSTableMetadata metadata = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(9999, metadata.minTimestamp);
        assertEquals(9999, metadata.maxTimestamp);
        assertEquals(System.currentTimeMillis()/1000, metadata.maxLocalDeletionTime, 5);
        cfs.forceMajorCompaction();
        SSTableMetadata metadata2 = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(metadata.maxLocalDeletionTime, metadata2.maxLocalDeletionTime);
        assertEquals(metadata.minTimestamp, metadata2.minTimestamp);
        assertEquals(metadata.maxTimestamp, metadata2.maxTimestamp);
    }

    @Test
    public void testTrackMetadata_rowMarker() throws Throwable
    {
        createTable("CREATE TABLE %s.rowmarker (a int, PRIMARY KEY (a))");
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore("rowmarker");
        execute("INSERT INTO %s.rowmarker (a) VALUES (1) USING TIMESTAMP 9999");

        cfs.forceBlockingFlush();
        assertEquals(1, cfs.getSSTables().size());
        SSTableMetadata metadata = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(9999, metadata.minTimestamp);
        assertEquals(9999, metadata.maxTimestamp);
        assertEquals(Integer.MAX_VALUE, metadata.maxLocalDeletionTime);
        cfs.forceMajorCompaction();
        SSTableMetadata metadata2 = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(metadata.maxLocalDeletionTime, metadata2.maxLocalDeletionTime);
        assertEquals(metadata.minTimestamp, metadata2.minTimestamp);
        assertEquals(metadata.maxTimestamp, metadata2.maxTimestamp);
    }
    @Test
    public void testTrackMetadata_rowMarkerDelete() throws Throwable
    {
        createTable("CREATE TABLE %s.rowmarkerdel (a int, PRIMARY KEY (a))");
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore("rowmarkerdel");
        execute("DELETE FROM %s.rowmarkerdel USING TIMESTAMP 9999 WHERE a=1");
        cfs.forceBlockingFlush();
        assertEquals(1, cfs.getSSTables().size());
        SSTableMetadata metadata = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(9999, metadata.minTimestamp);
        assertEquals(9999, metadata.maxTimestamp);
        assertEquals(System.currentTimeMillis()/1000, metadata.maxLocalDeletionTime, 5);
        cfs.forceMajorCompaction();
        SSTableMetadata metadata2 = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(metadata.maxLocalDeletionTime, metadata2.maxLocalDeletionTime);
        assertEquals(metadata.minTimestamp, metadata2.minTimestamp);
        assertEquals(metadata.maxTimestamp, metadata2.maxTimestamp);
    }

    @Test
    public void testChangingCrcCheckChance() throws Throwable
    {
        String currentTable = "crctest";

        //Start with crc_check_chance of 99%
        createTable("CREATE TABLE %s.crctest (p text, c text, v text, s text static, PRIMARY KEY (p, c)) WITH compression = {'sstable_compression': 'LZ4Compressor', 'crc_check_chance' : 0.99}");

        createTable("CREATE INDEX foo ON %s.crctest(v)");

        execute("INSERT INTO %s.crctest(p, c, v, s) values ('p1', 'k1', 'v1', 'sv1')");
        execute("INSERT INTO %s.crctest(p, c, v) values ('p1', 'k2', 'v2')");
        execute("INSERT INTO %s.crctest(p, s) values ('p2', 'sv2')");


        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(currentTable);
        ColumnFamilyStore indexCfs = cfs.indexManager.getIndexesBackedByCfs().iterator().next();
        cfs.forceBlockingFlush();

        Assert.assertEquals(0.99, cfs.metadata.compressionParameters.getCrcCheckChance(), 0.00);
        Assert.assertEquals(0.99, cfs.getSSTables().iterator().next().getCompressionMetadata().parameters.getCrcCheckChance(), 0.00);
        Assert.assertEquals(0.99, indexCfs.metadata.compressionParameters.getCrcCheckChance(), 0.00);
        Assert.assertEquals(0.99, indexCfs.getSSTables().iterator().next().getCompressionMetadata().parameters.getCrcCheckChance(), 0.00);


        Assert.assertEquals(execute("SELECT * FROM %s.crctest WHERE p='p1'").size(), 2);
        Assert.assertEquals(execute("SELECT * FROM %s.crctest WHERE v='v1'").size(), 1);




        //Write a few SSTables then Compact

        execute("INSERT INTO %s.crctest(p, c, v, s) values ('p1', 'k1', 'v1', 'sv1')");
        execute("INSERT INTO %s.crctest(p, c, v) values ('p1', 'k2', 'v2')");
        execute("INSERT INTO %s.crctest(p, s) values ('p2', 'sv2')");

        cfs.forceBlockingFlush();

        execute("INSERT INTO %s.crctest(p, c, v, s) values ('p1', 'k1', 'v1', 'sv1')");
        execute("INSERT INTO %s.crctest(p, c, v) values ('p1', 'k2', 'v2')");
        execute("INSERT INTO %s.crctest(p, s) values ('p2', 'sv2')");


        cfs.forceBlockingFlush();

        execute("INSERT INTO %s.crctest(p, c, v, s) values ('p1', 'k1', 'v1', 'sv1')");
        execute("INSERT INTO %s.crctest(p, c, v) values ('p1', 'k2', 'v2')");
        execute("INSERT INTO %s.crctest(p, s) values ('p2', 'sv2')");


        cfs.forceBlockingFlush();

        cfs.forceMajorCompaction();

        //Verify when we alter the value the live sstable readers hold the new one
        createTable("ALTER TABLE %s.crctest WITH compression = {'sstable_compression': 'LZ4Compressor', 'crc_check_chance': 0.01}");

        Assert.assertEquals( 0.01, cfs.metadata.compressionParameters.getCrcCheckChance(), 0.00);
        Assert.assertEquals( 0.01, cfs.getSSTables().iterator().next().getCompressionMetadata().parameters.getCrcCheckChance(), 0.00);
        Assert.assertEquals( 0.01, indexCfs.metadata.compressionParameters.getCrcCheckChance(), 0.00);
        Assert.assertEquals( 0.01, indexCfs.getSSTables().iterator().next().getCompressionMetadata().parameters.getCrcCheckChance(), 0.00);


        Assert.assertEquals(execute("SELECT * FROM %s.crctest WHERE p='p1'").size(), 2);
        Assert.assertEquals(execute("SELECT * FROM %s.crctest WHERE v='v1'").size(), 1);


        //Verify the call used by JMX still works
        cfs.setCrcCheckChance(0.03);
        Assert.assertEquals( 0.03, cfs.metadata.compressionParameters.getCrcCheckChance(), 0.00);
        Assert.assertEquals( 0.03, cfs.getSSTables().iterator().next().getCompressionMetadata().parameters.getCrcCheckChance(), 0.00);
        Assert.assertEquals( 0.03, indexCfs.metadata.compressionParameters.getCrcCheckChance(), 0.00);
        Assert.assertEquals( 0.03, indexCfs.getSSTables().iterator().next().getCompressionMetadata().parameters.getCrcCheckChance(), 0.00);

    }

    @AfterClass
    public static void stopGossiper()
    {
        Gossiper.instance.stop();
    }

    private static void createKeyspace(String query) throws Throwable
    {
        try
        {
            process(String.format(query, keyspace), ConsistencyLevel.ONE);
        } catch (RuntimeException exc)
        {
            throw exc.getCause();
        }
    }


    private void createTable(String query) throws Throwable
    {
        try
        {
            process(String.format(query, keyspace), ConsistencyLevel.ONE);
        } catch (RuntimeException exc)
        {
            throw exc.getCause();
        }
    }

    private UntypedResultSet execute(String query) throws Throwable
    {
        try
        {
            return processInternal(String.format(query, keyspace));
        } catch (RuntimeException exc)
        {
            if (exc.getCause() != null)
                throw exc.getCause();
            throw exc;
        }
    }
}
